# Changelog

## v1.0.1 — 2026-04-08

Initial fork of a2a-gateway as an OpenClaw A2A v0.3.0 plugin.

- Full A2A v0.3.0 protocol surface (JSON-RPC + gRPC)
- 244 passing tests covering tasks, streaming, push notifications, and error paths
- Metrics endpoint with optional auth
- Audit logging to `~/.openclaw/a2a-audit.jsonl`
- CI on Node 22 and 25
