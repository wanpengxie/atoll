# Coagent

## Quickstart

```bash
cp ops/env.example .env
chmod 600 .env
# edit .env: ANTHROPIC_API_KEY, SERVER_URL, ADMIN_TOKEN, DB_*, COAGENT_PROJECT_KEY
make deploy
make logrotate-config
make register
make doctor
pm2 start ecosystem.config.cjs
pm2 save
make smoke -- --real
```

The lightcone server creates and updates its MySQL schema through `initDb()` on startup. There is no separate db-init or db-migrate command.

## Commands

```bash
make install
make build
make deploy
make logrotate-config
make register
make doctor
make doctor-offline
make doctor -- --offline
make smoke
make smoke -- --real

coagent channel ls
coagent channel show <channel-id>
coagent channel start <channel-id>
coagent channel restart <channel-id>
coagent channel stop <channel-id>
coagent channel archive <channel-id>

coagent message send --channel <channel-id> --text "hello"
coagent message history --channel <channel-id>
coagent message search --channel <channel-id> --query hello

coagent admin status
coagent admin machines
coagent xhs publish --title "Title" --content note.md --images /tmp/a.png
```

## Paths

Runtime daemon state lives under `~/.coagent/{COAGENT_PROJECT_KEY}/`:

- `machine.key`: machine API key written by `make register`, chmod `600`
- `daemon.sock`: local daemon socket unless `COAGENT_DAEMON_SOCKET` overrides it
- `channels/`: active channel workdirs
- `archived/`: archived channel workdirs

Project-local scratch state should use `.coagent-local/`; it is ignored by git.

Planning documents under `.dalek/pm/` are kept in git; `.dalek/runtime/` and worker-local dalek state are managed by dalek and ignored.

## Operations

Use pm2 directly for process lifecycle:

```bash
pm2 start ecosystem.config.cjs
pm2 list
pm2 jlist
pm2 reload coagent-daemon
pm2 conf pm2-logrotate
pm2 logs coagent-daemon --raw --lines 200
pm2 save
pm2 startup
```

`make logrotate-config` installs the pm2-logrotate module settings used by deployment: `max_size=100M`, `retain=10`, and `compress=true`. `pm2 startup` prints the platform-specific command for boot persistence and may require sudo. The app itself does not require sudo.

## Troubleshooting

`make doctor` checks PATH tools, env, MySQL connectivity, schema presence, pm2 state, daemon health, file permissions, and project runtime files. `make doctor-offline` and `make doctor -- --offline` skip daemon and database reads; offline mode tails `~/.pm2/logs/*.log` files directly and uses `ps` output so it still works when pm2 or the daemon is down.

Key server, daemon, and agent events are JSON Lines on stdout using the `event` field and can be inspected with:

```bash
pm2 logs coagent-daemon --raw --lines 500 | jq 'select(.event)'
```
