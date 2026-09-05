# forge-plugins

First-party Forge plugins — each directory is one standalone binary speaking
the daemon's wire contract (HTTP over the Unix socket + the journal stream).
Plugins deliberately import no Forge packages: the wire contract IS the
boundary, so this repo builds and tests on its own.

Mounted as a submodule at `plugins/` in the forge repo; `forge plugin
install NAME` finds `plugins/NAME/plugin.toml` by walking up from the forge
binary and builds in place, so the mount point must be initialized
(`git submodule update --init`).

Check: `go build ./... && go test ./...`
