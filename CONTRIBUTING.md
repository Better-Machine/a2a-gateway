# Contributing to a2a-gateway

## Branch naming

Use one of the following prefixes:

- `liz/*` — Liz's work
- `ray/*` — Ray's work
- `woodhouse/*` — Woodhouse's work
- `feat/*` — feature branches
- `fix/*` — bug-fix branches

## PR process

1. Always work on a branch — **never push directly to `main`**.
2. Open a pull request against `main` for review.
3. Ensure CI passes (tests + type check) before requesting review.
4. Squash-merge when approved.

## In-progress: agent state changes

The agent state model (`dormant → ready → busy → unreachable`) is still in design.
Any changes to agent state transitions **must go through an RFC before merging**.
Do not merge agent-state PRs without team sign-off on the RFC.
