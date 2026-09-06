// forge-signal is a bidirectional Signal bridge (DESIGN.md §22, the plugin wire
// contract in docs/PLUGINS.md). Outbound: it consumes the daemon's journal and
// sends a Signal message per new question, target failure, and proposal (toggle
// each in signal.toml). Inbound: it polls signal-cli for messages FROM the one
// configured recipient and turns them into work — a bare message files a task,
// "/answer <id> <text>" resolves a waiting question, "/status" replies with the
// queue. signal-cli must be installed and its account registered out-of-band
// (see plugin.toml); this plugin only shells out to it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// pollEvery is the journal fallback cadence when SSE is unavailable.
const pollEvery = 3 * time.Second

func main() {
	log := newLogger(os.Getenv("FORGE_LOG_LEVEL"), os.Getenv("FORGE_LOG_FORMAT"))
	if err := run(log); err != nil {
		log.Error("signal plugin exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	socket := os.Getenv("FORGE_SOCKET")
	if socket == "" {
		return errors.New("FORGE_SOCKET is not set (the daemon sets it for supervised plugins)")
	}
	token := os.Getenv("FORGE_TOKEN")
	if token == "" {
		return errors.New("FORGE_TOKEN is not set")
	}
	cfgPath := ""
	if dir := os.Getenv("FORGE_PLUGIN_DIR"); dir != "" {
		cfgPath = filepath.Join(dir, "signal.toml")
	}
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Unconfigured: idle instead of crash-looping, so the daemon's restart=always
	// does not spin. The operator fills in account + recipient and restarts.
	if cfg.Account == "" || cfg.Recipient == "" {
		log.Warn("signal plugin not configured — set [signal] account and recipient in signal.toml, then restart", "config", cfgPath)
		<-ctx.Done()
		return nil
	}

	b := &bridge{
		api: newClient(socket, token),
		sig: signalCLI{bin: cfg.SignalCLI, account: cfg.Account, dir: stableDir(), mu: &sync.Mutex{}},
		cfg: cfg, log: log, seen: map[string]bool{}, pending: map[int64]pendingQ{}, filed: map[int64]filedT{},
	}
	if dir := os.Getenv("FORGE_PLUGIN_DIR"); dir != "" {
		b.stateFile = filepath.Join(dir, "pending.json")
		b.loadPending()
	}
	log.Info("signal starting", "account", cfg.Account, "recipient", cfg.Recipient, "intake", cfg.Intake)

	errc := make(chan error, 2)
	go func() { errc <- b.runOutbound(ctx) }()
	if cfg.Intake {
		go func() { errc <- b.runInbound(ctx) }()
	}
	err = <-errc
	stop()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// bridge holds the two directions' shared state.
type bridge struct {
	api  *client
	sig  signalCLI
	cfg  config
	log  *slog.Logger
	mu   sync.Mutex
	seen map[string]bool // question ids already sent (dedup across replayed events)
	// pending maps a sent question-notification timestamp to the question it
	// asked, so a Signal *reply* to that message answers it — no /answer <id>.
	// Bounded and persisted (stateFile) so it neither grows without limit nor is
	// lost across a restart.
	pending map[int64]pendingQ
	// filed maps a sender's message timestamp to the work the concierge
	// filed from it, so a ✅ reaction can land on that message when the
	// work settles, and a ❓ reaction can ask after it.
	filed     map[int64]filedT
	stateFile string
}

type pendingQ struct {
	QuestionID string   `json:"q"`
	WorkID     string   `json:"w"`
	Options    []string `json:"o,omitempty"`
}

type filedT struct {
	From   string `json:"f"`
	WorkID string `json:"w"`
}

// pendingCap bounds the question→reply map so it never grows without limit;
// oldest (smallest timestamp) entries are evicted first.
const pendingCap = 200

// send notifies the primary recipient (events, questions); sendTo replies to
// whichever sender asked. A failure is logged, not fatal. send returns the
// sent-message timestamp so a question can be matched to a reply.
func (b *bridge) send(ctx context.Context, msg string) int64 {
	return b.sendTo(ctx, b.cfg.Recipient, msg)
}

func (b *bridge) sendTo(ctx context.Context, to, msg string) int64 {
	ts, err := b.sig.send(ctx, to, msg)
	if err != nil && ctx.Err() == nil {
		b.log.Warn("signal send", "err", err, "to", to)
	}
	return ts
}

// allowedSender reports whether a sender is in the honored set.
func (b *bridge) allowedSender(from string) bool {
	for _, r := range b.cfg.Recipients {
		if sameNumber(from, r) {
			return true
		}
	}
	return false
}

// ---- outbound: journal -> Signal ----

func (b *bridge) runOutbound(ctx context.Context) error {
	cursor := b.startCursor(ctx)
	emit := func(e journalEntry) error {
		if e.ID <= cursor {
			return nil
		}
		cursor = e.ID
		b.handle(ctx, e)
		if err := b.api.Ack(ctx, cursor); err != nil && ctx.Err() == nil {
			b.log.Warn("ack cursor", "cursor", cursor, "err", err)
		}
		return nil
	}
	backoff, maxBackoff := 500*time.Millisecond, 30*time.Second
	for ctx.Err() == nil {
		err := b.api.Follow(ctx, cursor, emit)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, errSSEUnsupported):
			return b.poll(ctx, &cursor, emit)
		case err != nil:
			b.log.Warn("journal stream failed; reconnecting", "err", err)
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return ctx.Err()
}

func (b *bridge) poll(ctx context.Context, cursor *int64, emit func(journalEntry) error) error {
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			entries, err := b.api.Since(ctx, *cursor, 500)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				b.log.Warn("journal poll", "err", err)
				continue
			}
			for _, e := range entries {
				if err := emit(e); err != nil {
					return err
				}
			}
		}
	}
}

// startCursor pages to the journal tail so a start/restart does not replay old
// events as fresh Signal messages.
func (b *bridge) startCursor(ctx context.Context) int64 {
	var cursor int64
	for ctx.Err() == nil {
		entries, err := b.api.Since(ctx, cursor, 1000)
		if err != nil {
			if !sleepCtx(ctx, 2*time.Second) {
				return cursor
			}
			continue
		}
		if len(entries) == 0 {
			break
		}
		last := entries[len(entries)-1].ID
		if last <= cursor {
			break
		}
		cursor = last
	}
	b.log.Info("journal cursor at tail", "cursor", cursor)
	return cursor
}

// handle turns one journal entry into a Signal message per the toggles.
func (b *bridge) handle(ctx context.Context, e journalEntry) {
	switch e.Kind {
	case "question.asked":
		if b.cfg.Questions {
			b.onQuestion(ctx)
		}
	case "question.answered":
		// Answered anywhere (UI, CLI, another channel): the notification
		// message gets its ✅ so the phone shows it's handled.
		if ts := b.takePendingByQuestion(e.EntityID); ts != 0 {
			b.react(ctx, b.cfg.Recipient, b.cfg.Account, ts, "✅")
		}
	case "target.transition":
		b.onTransition(ctx, e) // gates failure sends on cfg.Failures itself
	case "proposal.created":
		if b.cfg.Proposals {
			b.onProposal(ctx, e)
		}
	case "notify.test":
		b.send(ctx, "Forge: test message. Reply with a request to file a task.")
	}
}

// onQuestion sends any open question not sent before (deduped by id), with the
// short work id to answer against.
func (b *bridge) onQuestion(ctx context.Context) {
	at, err := b.api.Attention(ctx)
	if err != nil {
		b.log.Warn("read attention", "err", err)
		return
	}
	for _, q := range at.Questions {
		if b.seen[q.ID] {
			continue
		}
		b.seen[q.ID] = true
		short := shortID(q.WorkID)
		var msg strings.Builder
		fmt.Fprintf(&msg, "Forge needs input on task %s:\n%s\n", short, q.Text)
		for i, opt := range q.Options {
			fmt.Fprintf(&msg, "%d) %s\n", i+1, opt)
		}
		hint := "Reply to this message with your answer"
		if len(q.Options) > 0 {
			hint = "Reply with a number, or your own answer if none fit"
		}
		fmt.Fprintf(&msg, "%s/tasks/%s\n%s (or /answer %s <text>).", b.cfg.UI, q.WorkID, hint, short)
		ts := b.send(ctx, msg.String())
		if ts != 0 {
			b.rememberQuestion(ts, q.ID, q.WorkID, q.Options)
		}
	}
}

// transitionPayload is the slice of a target.transition journal payload used to
// notify a failure.
type transitionPayload struct {
	Repository string `json:"repository"`
	WorkID     string `json:"work_id"`
	To         string `json:"to"`
	State      string `json:"state"`
	Reason     string `json:"reason"`
}

func (b *bridge) onTransition(ctx context.Context, e journalEntry) {
	var p transitionPayload
	if json.Unmarshal(e.Payload, &p) != nil {
		return
	}
	state := p.To
	if state == "" {
		state = p.State
	}
	// A work filed from a chat message that lands well gets a ✅ on the
	// message that asked for it.
	if state == "merged" || state == "succeeded" {
		if f, ts, ok := b.takeFiledByWork(p.WorkID); ok {
			b.react(ctx, f.From, f.From, ts, "✅")
		}
		return
	}
	if state != "failed" && state != "unverified" {
		return // only surface the bad transitions
	}
	if !b.cfg.Failures {
		return
	}
	if strings.HasPrefix(p.Repository, "bench-") {
		return // bench trees fail and self-correct by design; not page-worthy
	}
	repo := p.Repository
	if repo == "" {
		repo = shortID(e.EntityID)
	}
	msg := fmt.Sprintf("Forge: %s %s", repo, state)
	if p.Reason != "" {
		msg += " — " + p.Reason
	}
	if p.WorkID != "" {
		msg += fmt.Sprintf("\n%s/tasks/%s", b.cfg.UI, p.WorkID)
	}
	b.send(ctx, msg)
}

type proposalPayload struct {
	Kind   string `json:"kind"`
	Target string `json:"target"`
}

func (b *bridge) onProposal(ctx context.Context, e journalEntry) {
	var p proposalPayload
	if json.Unmarshal(e.Payload, &p) != nil || p.Kind == "" {
		return
	}
	b.send(ctx, fmt.Sprintf("Forge: new proposal (%s) %s\n%s/proposals/%s", p.Kind, p.Target, b.cfg.UI, e.EntityID))
}

// ---- inbound: Signal -> work ----

func (b *bridge) runInbound(ctx context.Context) error {
	timeout := time.Duration(b.cfg.PollSeconds) * time.Second
	for ctx.Err() == nil {
		msgs, err := b.sig.receive(ctx, timeout)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			b.log.Warn("signal receive", "err", err)
			if !sleepCtx(ctx, timeout) {
				return ctx.Err()
			}
			continue
		}
		for _, m := range msgs {
			if !b.allowedSender(m.From) {
				b.log.Info("ignoring message from unknown sender", "from", m.From)
				continue
			}
			if m.Emoji != "" {
				b.onReaction(ctx, m)
				continue
			}
			b.command(ctx, m)
		}
	}
	return ctx.Err()
}

// onReaction interprets an emoji reaction on one of our messages (or on a
// message that filed a task): 👍/👎 answers a yes/no question, ❓ asks for a
// plain-language summary, anything else is ignored.
func (b *bridge) onReaction(ctx context.Context, m incoming) {
	reply := func(msg string) { b.sendTo(ctx, m.From, msg) }
	operator := sameNumber(m.From, b.cfg.Recipient)
	explain := strings.HasPrefix(m.Emoji, "❓") || strings.HasPrefix(m.Emoji, "❔") || strings.HasPrefix(m.Emoji, "🤔")
	if pq, ok := b.peekPending(m.ReactedTo); ok {
		switch {
		case strings.HasPrefix(m.Emoji, "👍"), strings.HasPrefix(m.Emoji, "👎"):
			if !operator {
				reply("Only the operator can answer Forge's questions.")
				return
			}
			answer := "yes"
			if strings.HasPrefix(m.Emoji, "👎") {
				answer = "no"
			}
			if err := b.api.AnswerQuestion(ctx, pq.QuestionID, answer); err != nil {
				reply("Answer failed: " + errMessage(err))
				return
			}
			// The ✅ lands via the question.answered journal event.
		case explain:
			prompt := "In a few plain sentences, explain what this question from task " + shortID(pq.WorkID) +
				" is asking me to decide, what each option would mean in practice, and what you'd pick:\n" + questionRecap(ctx, b, pq)
			b.withTyping(ctx, m.From, func() {
				out, err := b.api.Assistant(ctx, m.From, prompt)
				if err != nil {
					reply("Couldn't summarize that: " + errMessage(err))
					return
				}
				reply(out.Reply)
			})
		}
		return
	}
	if explain {
		b.mu.Lock()
		f, ok := b.filed[m.ReactedTo]
		b.mu.Unlock()
		if ok {
			b.withTyping(ctx, m.From, func() {
				out, err := b.api.Assistant(ctx, m.From, "Briefly: what is task "+shortID(f.WorkID)+" doing, what state is it in, and does it need anything from me?")
				if err != nil {
					reply("Couldn't check on that: " + errMessage(err))
					return
				}
				reply(out.Reply)
			})
		}
	}
}

// questionRecap renders the open question's text and options for a summary
// prompt; falls back to the task id when the question is no longer open.
func questionRecap(ctx context.Context, b *bridge, pq pendingQ) string {
	at, err := b.api.Attention(ctx)
	if err == nil {
		for _, q := range at.Questions {
			if q.ID == pq.QuestionID {
				s := q.Text
				for i, o := range q.Options {
					s += fmt.Sprintf("\n%d) %s", i+1, o)
				}
				return s
			}
		}
	}
	return "(the question on task " + shortID(pq.WorkID) + " — it may already be closed)"
}

// command interprets one inbound message: a reply to a question (answers it), a
// /-command, or a bare request that becomes a task.
func (b *bridge) command(ctx context.Context, m incoming) {
	text, quoteID, from := m.Text, m.QuoteID, m.From
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	reply := func(msg string) { b.sendTo(ctx, from, msg) }
	operator := sameNumber(from, b.cfg.Recipient)
	// A reply always means "answer" — never a new task. If it matches a known
	// question, answer it; if not, say so rather than filing a stray task.
	if quoteID != 0 {
		if !operator {
			reply("Only the operator can answer Forge's questions — but ask me anything as a normal message.")
			return
		}
		pq, ok := b.takePending(quoteID)
		if !ok {
			reply("That question isn't open anymore. Reply to a current one, or send a new request as a fresh message (not a reply).")
			return
		}
		answer := text
		// A bare number selects that option from the question's list.
		if n, err := strconv.Atoi(text); err == nil && n >= 1 && n <= len(pq.Options) {
			answer = pq.Options[n-1]
		}
		if err := b.api.AnswerQuestion(ctx, pq.QuestionID, answer); err != nil {
			b.rememberQuestion(quoteID, pq.QuestionID, pq.WorkID, pq.Options) // put it back to retry
			reply("Answer failed: " + errMessage(err))
			return
		}
		b.mu.Lock()
		delete(b.seen, pq.QuestionID)
		b.mu.Unlock()
		// takePending consumed the entry, so the question.answered event
		// can't find it — put the ✅ on the question message here.
		b.react(ctx, b.cfg.Recipient, b.cfg.Account, quoteID, "✅")
		reply("Answered task " + shortID(pq.WorkID) + ".")
		return
	}
	// /answer <id> <text> stays a deterministic fast-path.
	if fields := strings.Fields(text); strings.EqualFold(fields[0], "/answer") {
		if !operator {
			reply("Only the operator can answer Forge's questions — but ask me anything as a normal message.")
			return
		}
		if len(fields) < 3 {
			reply("Usage: /answer <task-id> <your answer>")
			return
		}
		b.answer(ctx, from, fields[1], strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(text, fields[0]), " "+fields[1])))
		return
	}
	// Everything else goes to the concierge: natural language in, action out.
	b.withTyping(ctx, from, func() {
		out, err := b.api.Assistant(ctx, from, text)
		if err != nil {
			reply("Sorry, I hit an error reaching Forge: " + errMessage(err))
			return
		}
		if id, ok := strings.CutPrefix(out.Ref, "work:"); ok {
			b.rememberFiled(m.Timestamp, from, id)
		}
		if out.Reply != "" {
			reply(out.Reply)
		}
	})
}

func (b *bridge) answer(ctx context.Context, from, taskRef, answer string) {
	reply := func(msg string) { b.sendTo(ctx, from, msg) }
	if strings.TrimSpace(answer) == "" {
		reply("Give an answer: /answer <task-id> <text>")
		return
	}
	at, err := b.api.Attention(ctx)
	if err != nil {
		reply("Couldn't read the queue: " + errMessage(err))
		return
	}
	for _, q := range at.Questions {
		if strings.HasPrefix(q.WorkID, taskRef) {
			if err := b.api.AnswerQuestion(ctx, q.ID, answer); err != nil {
				reply("Answer failed: " + errMessage(err))
				return
			}
			delete(b.seen, q.ID)
			reply("Answered task " + shortID(q.WorkID) + ".")
			return
		}
	}
	reply("No open question for task " + taskRef + ".")
}

// ---- helpers ----

// stableDir is a working directory for signal-cli that will not vanish under it
// (the plugin's own CWD can be unlinked by a reinstall); the user home, else "/".
func stableDir() string {
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return h
	}
	return "/"
}

// rememberQuestion records a sent question notification's timestamp → question,
// evicts the oldest entries past the cap, and persists the map.
func (b *bridge) rememberQuestion(ts int64, questionID, workID string, options []string) {
	b.mu.Lock()
	b.pending[ts] = pendingQ{QuestionID: questionID, WorkID: workID, Options: options}
	for len(b.pending) > pendingCap {
		oldest, first := int64(0), true
		for k := range b.pending {
			if first || k < oldest {
				oldest, first = k, false
			}
		}
		delete(b.pending, oldest)
	}
	b.savePendingLocked()
	b.mu.Unlock()
}

// takePending removes and returns the question a reply's quote refers to.
func (b *bridge) takePending(quoteID int64) (pendingQ, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pq, ok := b.pending[quoteID]
	if ok {
		delete(b.pending, quoteID)
		b.savePendingLocked()
	}
	return pq, ok
}

// peekPending is takePending without consuming — for reactions that inform
// rather than answer.
func (b *bridge) peekPending(ts int64) (pendingQ, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pq, ok := b.pending[ts]
	return pq, ok
}

// takePendingByQuestion removes a pending entry by question id and returns the
// message timestamp it was announced with (0 if unknown).
func (b *bridge) takePendingByQuestion(questionID string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ts, pq := range b.pending {
		if pq.QuestionID == questionID {
			delete(b.pending, ts)
			b.savePendingLocked()
			return ts
		}
	}
	return 0
}

// rememberFiled records sender-message-ts → filed work, capped like pending.
func (b *bridge) rememberFiled(ts int64, from, workID string) {
	if ts == 0 || workID == "" {
		return
	}
	b.mu.Lock()
	b.filed[ts] = filedT{From: from, WorkID: workID}
	for len(b.filed) > pendingCap {
		oldest, first := int64(0), true
		for k := range b.filed {
			if first || k < oldest {
				oldest, first = k, false
			}
		}
		delete(b.filed, oldest)
	}
	b.savePendingLocked()
	b.mu.Unlock()
}

// takeFiledByWork removes a filed entry by work id, returning the sender and
// message timestamp (ok=false if the work wasn't filed from a message).
func (b *bridge) takeFiledByWork(workID string) (filedT, int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ts, f := range b.filed {
		if f.WorkID == workID {
			delete(b.filed, ts)
			b.savePendingLocked()
			return f, ts, true
		}
	}
	return filedT{}, 0, false
}

// react puts an emoji on a message in the chat with `chat`; best effort.
func (b *bridge) react(ctx context.Context, chat, author string, ts int64, emoji string) {
	if ts == 0 {
		return
	}
	if err := b.sig.react(ctx, chat, author, ts, emoji); err != nil && ctx.Err() == nil {
		b.log.Warn("signal react", "err", err)
	}
}

// withTyping shows the "is typing…" indicator for the duration of fn,
// refreshing it (clients expire it after ~15s) and clearing it after.
func (b *bridge) withTyping(ctx context.Context, to string, fn func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		_ = b.sig.typing(ctx, to, false)
		for {
			select {
			case <-done:
				_ = b.sig.typing(ctx, to, true)
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_ = b.sig.typing(ctx, to, false)
			}
		}
	}()
	fn()
	close(done)
}

// persistedState is the stateFile shape; JSON keys are strings, so the
// millisecond timestamps are stringified.
type persistedState struct {
	Pending map[string]pendingQ `json:"pending"`
	Filed   map[string]filedT   `json:"filed,omitempty"`
}

// savePendingLocked writes both maps to stateFile (caller holds b.mu).
func (b *bridge) savePendingLocked() {
	if b.stateFile == "" {
		return
	}
	st := persistedState{Pending: make(map[string]pendingQ, len(b.pending)), Filed: make(map[string]filedT, len(b.filed))}
	for k, v := range b.pending {
		st.Pending[strconv.FormatInt(k, 10)] = v
	}
	for k, v := range b.filed {
		st.Filed[strconv.FormatInt(k, 10)] = v
	}
	data, err := json.Marshal(st)
	if err != nil {
		return
	}
	if err := os.WriteFile(b.stateFile, data, 0o600); err != nil {
		b.log.Warn("save pending", "err", err)
	}
}

// loadPending restores the maps from stateFile on start (best effort). The
// pre-filed format was the bare pending map; both shapes load.
func (b *bridge) loadPending() {
	data, err := os.ReadFile(b.stateFile)
	if err != nil {
		return
	}
	var st persistedState
	if json.Unmarshal(data, &st) != nil || st.Pending == nil {
		var legacy map[string]pendingQ
		if json.Unmarshal(data, &legacy) != nil {
			return
		}
		st = persistedState{Pending: legacy}
	}
	b.mu.Lock()
	for k, v := range st.Pending {
		if ts, perr := strconv.ParseInt(k, 10, 64); perr == nil {
			b.pending[ts] = v
		}
	}
	for k, v := range st.Filed {
		if ts, perr := strconv.ParseInt(k, 10, 64); perr == nil {
			b.filed[ts] = v
		}
	}
	b.mu.Unlock()
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// sameNumber compares Signal identifiers loosely: exact, or one is a suffix of
// the other (a bare number vs a +E.164 form).
func sameNumber(a, bb string) bool {
	a, bb = strings.TrimSpace(a), strings.TrimSpace(bb)
	return a != "" && (a == bb || strings.HasSuffix(a, bb) || strings.HasSuffix(bb, a))
}

func errMessage(err error) string {
	var ae *apiError
	if errors.As(err, &ae) {
		return strings.TrimSpace(ae.Body)
	}
	return err.Error()
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func newLogger(level, format string) *slog.Logger {
	lv := slog.LevelInfo
	if err := lv.UnmarshalText([]byte(strings.TrimSpace(level))); err != nil {
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if strings.EqualFold(strings.TrimSpace(format), "json") {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h).With("component", "plugin.signal")
}
