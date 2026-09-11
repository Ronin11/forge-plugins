// forge-namecheap exposes Namecheap domain operations as agent tools over the
// plugin MCP bridge (capability "tools"). Research (check, pricing) and DNS
// are open; registration spends real money and is triple-gated: an approved
// Human Queue question naming the domain, the config spend cap, and the
// config TLD allowlist. The API key stays in namecheap.toml — agents call
// tools with structured I/O and never see credentials.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type config struct {
	Namecheap struct {
		APIUser           string   `toml:"api_user"`
		APIKey            string   `toml:"api_key"`
		Username          string   `toml:"username"`
		ClientIP          string   `toml:"client_ip"`
		MaxUSDPerPurchase float64  `toml:"max_usd_per_purchase"`
		AllowedTLDs       []string `toml:"allowed_tlds"`
	} `toml:"namecheap"`
	// Registrant is the contact record Namecheap legally requires on
	// domains.create (sent for all four roles: registrant/tech/admin/aux
	// billing). Config-only, like the API key — never in agent context.
	// Phone must be "+NNN.NNNNNNNNNN" (e.g. "+1.8015551234").
	Registrant struct {
		FirstName     string `toml:"first_name"`
		LastName      string `toml:"last_name"`
		Address1      string `toml:"address1"`
		City          string `toml:"city"`
		StateProvince string `toml:"state_province"`
		PostalCode    string `toml:"postal_code"`
		Country       string `toml:"country"`
		Phone         string `toml:"phone"`
		Email         string `toml:"email"`
	} `toml:"registrant"`
}

// registrantParams builds the four contact-role parameter sets domains.create
// requires, or an error naming the missing config fields.
func (c *config) registrantParams() (map[string]string, error) {
	r := c.Registrant
	fields := map[string]string{
		"FirstName": r.FirstName, "LastName": r.LastName, "Address1": r.Address1,
		"City": r.City, "StateProvince": r.StateProvince, "PostalCode": r.PostalCode,
		"Country": r.Country, "Phone": r.Phone, "EmailAddress": r.Email,
	}
	var missing []string
	for k, v := range fields {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("registration needs the [registrant] section in the plugin's namecheap.toml (missing: %s) — the OPERATOR must fill it in the config file; do not ask for these values in chat", strings.Join(missing, ", "))
	}
	out := map[string]string{}
	for _, role := range []string{"Registrant", "Tech", "Admin", "AuxBilling"} {
		for k, v := range fields {
			out[role+k] = v
		}
	}
	return out, nil
}

type server struct {
	log   *slog.Logger
	cfg   config
	nc    *ncClient
	forge *forgeClient
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("namecheap plugin exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dir := os.Getenv("FORGE_PLUGIN_DIR")
	if dir == "" {
		return errors.New("FORGE_PLUGIN_DIR is not set")
	}
	var cfg config
	if _, err := toml.DecodeFile(filepath.Join(dir, "namecheap.toml"), &cfg); err != nil {
		return fmt.Errorf("namecheap.toml: %w", err)
	}
	n := cfg.Namecheap
	if n.APIUser == "" || n.APIKey == "" || n.ClientIP == "" {
		return errors.New("namecheap.toml needs api_user, api_key, and client_ip")
	}
	if n.Username == "" {
		cfg.Namecheap.Username = n.APIUser
	}
	s := &server{
		log: log, cfg: cfg,
		nc:    &ncClient{apiUser: n.APIUser, apiKey: n.APIKey, username: cfg.Namecheap.Username, clientIP: n.ClientIP, hc: &http.Client{}},
		forge: newForgeClient(os.Getenv("FORGE_SOCKET"), os.Getenv("FORGE_TOKEN")),
	}
	return s.serve(os.Stdin, os.Stdout)
}

// ---- MCP stdio server: initialize, tools/list, tools/call ----

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func (s *server) serve(in *os.File, out *os.File) error {
	enc := json.NewEncoder(out)
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	respond := func(id json.RawMessage, result any, rpcErr string) {
		if id == nil {
			return
		}
		msg := map[string]any{"jsonrpc": "2.0", "id": id}
		if rpcErr != "" {
			msg["error"] = map[string]any{"code": -32000, "message": rpcErr}
		} else {
			msg["result"] = result
		}
		if err := enc.Encode(msg); err != nil {
			s.log.Error("write response", "err", err)
		}
	}
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		switch req.Method {
		case "initialize":
			respond(req.ID, map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "forge-namecheap", "version": "0.1.0"},
			}, "")
		case "notifications/initialized":
			// no response
		case "tools/list":
			respond(req.ID, map[string]any{"tools": toolDefs()}, "")
		case "tools/call":
			var p struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(req.Params, &p); err != nil {
				respond(req.ID, nil, "bad params")
				continue
			}
			text, callErr := s.dispatch(context.Background(), p.Name, p.Arguments)
			if callErr != nil {
				respond(req.ID, map[string]any{"content": []map[string]any{{"type": "text", "text": callErr.Error()}}, "isError": true}, "")
				continue
			}
			respond(req.ID, map[string]any{"content": []map[string]any{{"type": "text", "text": text}}}, "")
		default:
			respond(req.ID, nil, "method not supported: "+req.Method)
		}
	}
	return sc.Err()
}

func toolDefs() []map[string]any {
	obj := func(schema string) json.RawMessage { return json.RawMessage(schema) }
	return []map[string]any{
		{"name": "check", "description": "Check registration availability for up to 20 domain names at once (premium names are flagged with their price).",
			"inputSchema": obj(`{"type":"object","properties":{"domains":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":20}},"required":["domains"],"additionalProperties":false}`)},
		{"name": "pricing", "description": "First-year registration price per TLD (USD).",
			"inputSchema": obj(`{"type":"object","properties":{"tld":{"type":"string","description":"e.g. \"com\", no dot"}},"required":["tld"],"additionalProperties":false}`)},
		{"name": "register", "description": "Register a domain — SPENDS REAL MONEY from the Namecheap account balance. Requires question_id: an answered Human Queue question whose text names this exact domain and whose answer approves. Also gated by the plugin's spend cap and TLD allowlist. Ask the question with forge_ask first, wait for the answer, then call this.",
			"inputSchema": obj(`{"type":"object","properties":{"domain":{"type":"string"},"years":{"type":"integer","minimum":1,"maximum":2,"default":1},"question_id":{"type":"string"}},"required":["domain","question_id"],"additionalProperties":false}`)},
		{"name": "dns_get", "description": "Read a registered domain's current DNS host records and email_type, in the exact shape dns_set accepts. ALWAYS call this before dns_set: dns_set replaces the whole zone, so the safe pattern is get → edit the list → set.",
			"inputSchema": obj(`{"type":"object","properties":{"domain":{"type":"string"}},"required":["domain"],"additionalProperties":false}`)},
		{"name": "dns_set", "description": "Replace a registered domain's DNS host records (replaces ALL records — include every record the domain should have, so start from dns_get). Pass email_type back unchanged (e.g. FWD for Namecheap email forwarding) or the account's mail setting is reset. For GitHub Pages: four A records @ 185.199.108-111.153 and a www CNAME to <user>.github.io.",
			"inputSchema": obj(`{"type":"object","properties":{"domain":{"type":"string"},"email_type":{"type":"string","enum":["MX","MXE","FWD","OX"]},"records":{"type":"array","items":{"type":"object","properties":{"host":{"type":"string","description":"@ or a subdomain"},"type":{"type":"string","enum":["A","AAAA","CNAME","TXT","MX","URL","URL301","FRAME"]},"value":{"type":"string"},"mxpref":{"type":"integer","description":"MX priority; MX records only"},"ttl":{"type":"integer","default":1800}},"required":["host","type","value"],"additionalProperties":false},"minItems":1}},"required":["domain","records"],"additionalProperties":false}`)},
	}
}

func (s *server) dispatch(ctx context.Context, name string, args json.RawMessage) (string, error) {
	switch name {
	case "check":
		return s.check(ctx, args)
	case "pricing":
		return s.pricing(ctx, args)
	case "register":
		return s.register(ctx, args)
	case "dns_get":
		return s.dnsGet(ctx, args)
	case "dns_set":
		return s.dnsSet(ctx, args)
	}
	return "", fmt.Errorf("unknown tool %s", name)
}

func (s *server) check(ctx context.Context, args json.RawMessage) (string, error) {
	var in struct {
		Domains []string `json:"domains"`
	}
	if err := json.Unmarshal(args, &in); err != nil || len(in.Domains) == 0 {
		return "", errors.New("domains[] is required")
	}
	resp, err := s.nc.call(ctx, "namecheap.domains.check", map[string]string{"DomainList": strings.Join(in.Domains, ",")})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, r := range resp.Result.Checks {
		line := r.Domain + ": "
		if strings.EqualFold(r.Available, "true") {
			line += "AVAILABLE"
			if strings.EqualFold(r.Premium, "true") {
				line += " (PREMIUM $" + r.PremiumPrice + ")"
			}
		} else {
			line += "taken"
		}
		b.WriteString(line + "\n")
	}
	return b.String(), nil
}

func (s *server) pricing(ctx context.Context, args json.RawMessage) (string, error) {
	var in struct {
		TLD string `json:"tld"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.TLD == "" {
		return "", errors.New("tld is required")
	}
	resp, err := s.nc.call(ctx, "namecheap.users.getPricing", map[string]string{
		"ProductType": "DOMAIN", "ProductCategory": "DOMAINS", "ActionName": "REGISTER", "ProductName": strings.TrimPrefix(in.TLD, "."),
	})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, cat := range resp.Result.Pricing {
		for _, p := range cat.Products {
			for _, price := range p.Prices {
				if price.Duration == "1" {
					fmt.Fprintf(&b, ".%s register 1yr: %s %s\n", cat.Name, price.Price, price.Currency)
				}
			}
		}
	}
	if b.Len() == 0 {
		return "no pricing rows returned for ." + in.TLD, nil
	}
	return b.String(), nil
}

func (s *server) register(ctx context.Context, args json.RawMessage) (string, error) {
	var in struct {
		Domain     string `json:"domain"`
		Years      int    `json:"years"`
		QuestionID string `json:"question_id"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.Domain == "" || in.QuestionID == "" {
		return "", errors.New("domain and question_id are required")
	}
	if in.Years <= 0 {
		in.Years = 1
	}
	domain := strings.ToLower(strings.TrimSpace(in.Domain))
	dot := strings.LastIndex(domain, ".")
	if dot <= 0 {
		return "", fmt.Errorf("%q is not a registrable domain", domain)
	}
	tld := domain[dot+1:]
	allowed := false
	for _, t := range s.cfg.Namecheap.AllowedTLDs {
		if strings.EqualFold(t, tld) {
			allowed = true
		}
	}
	if !allowed {
		return "", fmt.Errorf("TLD .%s is not in the plugin's allowed_tlds", tld)
	}
	// The human gate: the referenced question must exist, be answered, name
	// this exact domain, and read as approval.
	q, err := s.forge.question(ctx, in.QuestionID)
	if err != nil {
		return "", fmt.Errorf("authorization question: %w", err)
	}
	if q.AnsweredAt.IsZero() {
		return "", errors.New("the authorization question has not been answered — wait for the human")
	}
	if !strings.Contains(strings.ToLower(q.Text), domain) {
		return "", fmt.Errorf("the authorization question does not name %s — ask a question that states the exact domain and price", domain)
	}
	ans := strings.ToLower(q.Answer)
	if !strings.Contains(ans, "approve") && !strings.Contains(ans, "yes") && !strings.Contains(ans, "buy") {
		return "", fmt.Errorf("the answer %q does not read as approval", q.Answer)
	}
	params, err := s.cfg.registrantParams()
	if err != nil {
		return "", err
	}
	params["DomainName"], params["Years"] = domain, strconv.Itoa(in.Years)
	params["AddFreeWhoisguard"], params["WGEnabled"] = "yes", "yes"
	resp, err := s.nc.call(ctx, "namecheap.domains.create", params)
	if err != nil {
		return "", err
	}
	c := resp.Result.Created
	if !strings.EqualFold(c.Registered, "true") {
		return "", fmt.Errorf("registration did not complete for %s", domain)
	}
	if charged, err := strconv.ParseFloat(c.Charged, 64); err == nil && s.cfg.Namecheap.MaxUSDPerPurchase > 0 && charged > s.cfg.Namecheap.MaxUSDPerPurchase {
		s.log.Error("charge exceeded cap AFTER registration", "domain", domain, "charged", charged)
		return fmt.Sprintf("REGISTERED %s but the charge $%s exceeded the configured cap — flag this to the human", domain, c.Charged), nil
	}
	s.log.Info("domain registered", "domain", domain, "charged", c.Charged, "question", in.QuestionID)
	return fmt.Sprintf("REGISTERED %s for %d year(s), charged $%s (authorized by question %s)", domain, in.Years, c.Charged, in.QuestionID), nil
}

// splitDomain gives Namecheap its SLD/TLD pair, or an error for a bare label.
func splitDomain(raw string) (sld, tld string, err error) {
	domain := strings.ToLower(strings.TrimSpace(raw))
	dot := strings.LastIndex(domain, ".")
	if dot <= 0 {
		return "", "", fmt.Errorf("%q is not a domain", domain)
	}
	return domain[:dot], domain[dot+1:], nil
}

func (s *server) dnsGet(ctx context.Context, args json.RawMessage) (string, error) {
	var in struct {
		Domain string `json:"domain"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.Domain == "" {
		return "", errors.New("domain is required")
	}
	sld, tld, err := splitDomain(in.Domain)
	if err != nil {
		return "", err
	}
	resp, err := s.nc.call(ctx, "namecheap.domains.dns.getHosts", map[string]string{"SLD": sld, "TLD": tld})
	if err != nil {
		return "", err
	}
	// Emitted in dns_set's input shape so an agent can round-trip it verbatim.
	type rec struct {
		Host   string `json:"host"`
		Type   string `json:"type"`
		Value  string `json:"value"`
		MXPref int    `json:"mxpref,omitempty"`
		TTL    int    `json:"ttl"`
	}
	out := struct {
		Domain    string `json:"domain"`
		EmailType string `json:"email_type"`
		Records   []rec  `json:"records"`
	}{Domain: sld + "." + tld, EmailType: resp.Result.DNSGet.EmailType}
	for _, h := range resp.Result.DNSGet.Hosts {
		pref, _ := strconv.Atoi(h.MXPref)
		ttl, _ := strconv.Atoi(h.TTL)
		out.Records = append(out.Records, rec{Host: h.Name, Type: h.Type, Value: h.Address, MXPref: pref, TTL: ttl})
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *server) dnsSet(ctx context.Context, args json.RawMessage) (string, error) {
	var in struct {
		Domain    string `json:"domain"`
		EmailType string `json:"email_type"`
		Records   []struct {
			Host, Type, Value string
			MXPref            int
			TTL               int
		} `json:"records"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.Domain == "" || len(in.Records) == 0 {
		return "", errors.New("domain and records[] are required")
	}
	sld, tld, err := splitDomain(in.Domain)
	if err != nil {
		return "", err
	}
	domain := sld + "." + tld
	params := map[string]string{"SLD": sld, "TLD": tld}
	if in.EmailType != "" {
		params["EmailType"] = in.EmailType
	}
	for i, r := range in.Records {
		n := strconv.Itoa(i + 1)
		ttl := r.TTL
		if ttl <= 0 {
			ttl = 1800
		}
		params["HostName"+n] = r.Host
		params["RecordType"+n] = r.Type
		params["Address"+n] = r.Value
		params["TTL"+n] = strconv.Itoa(ttl)
		if strings.EqualFold(r.Type, "MX") {
			params["MXPref"+n] = strconv.Itoa(r.MXPref)
		}
	}
	resp, err := s.nc.call(ctx, "namecheap.domains.dns.setHosts", params)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(resp.Result.DNSSet.Success, "true") {
		return "", fmt.Errorf("setHosts did not report success for %s", domain)
	}
	return fmt.Sprintf("DNS updated for %s: %d records set", domain, len(in.Records)), nil
}

// ---- minimal forge client for the authorization lookup ----

type forgeClient struct {
	hc    *http.Client
	token string
}

type question struct {
	ID         string    `json:"id"`
	Text       string    `json:"text"`
	Answer     string    `json:"answer"`
	AnsweredAt time.Time `json:"answered_at"`
}

func newForgeClient(socket, token string) *forgeClient {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}
	return &forgeClient{hc: &http.Client{Transport: tr, Timeout: 10 * time.Second}, token: token}
}

func (c *forgeClient) question(ctx context.Context, id string) (*question, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://forge/api/v1/questions/"+id, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("daemon answered %d", resp.StatusCode)
	}
	var q question
	if err := json.NewDecoder(resp.Body).Decode(&q); err != nil {
		return nil, err
	}
	return &q, nil
}
