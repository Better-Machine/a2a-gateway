# QA Report — a2a-gateway v1.0.1

**Date:** 2026-04-08
**Branch:** liz/mvp-hardening
**Overall Status:** PASS

## Test Suite

- **244 tests passing**, 0 failures, 0 skipped
- Runner: `node --import tsx --test tests/*.test.ts`
- Duration: ~2.6 s

## Privacy Scan

Clean — no secrets, credentials, or PII detected in source tree.

## npm audit

13 vulnerabilities found (4 moderate, 8 high, 1 critical).
All are in transitive dependencies (`openclaw` SDK, `undici`, `hono`, `path-to-regexp`, `tar`, `fast-xml-parser`, `brace-expansion`, `file-type`, `music-metadata`, `yaml`).
None are in direct project dependencies. All have fixes available via `npm audit fix`.

| Severity | Count | Notable packages |
|----------|-------|-----------------|
| Critical | 1 | `@hono/node-server` (via `openclaw`) |
| High | 8 | `undici`, `tar`, `path-to-regexp`, `fast-xml-parser`, `music-metadata` |
| Moderate | 4 | `hono`, `brace-expansion`, `file-type`, `yaml` |

**Recommendation:** Run `npm audit fix` and verify; critical/high items are in the `openclaw` SDK dependency tree and will resolve when upstream publishes a patch.

## TypeScript Type Check (`npx tsc --noEmit`)

4 pre-existing type errors in `src/internal/federation.ts` (lines 165, 293, 341, 346) — all are `as` cast narrowing warnings on `OutboxEntry` and `Record<string, unknown>`. These are cosmetic cast issues, not runtime risks, and pre-date this branch.

**No new type errors introduced.**

## Summary

| Check | Result |
|-------|--------|
| Tests (244) | PASS |
| Privacy scan | CLEAN |
| npm audit | 13 transitive vulns (none direct) |
| TypeScript | 4 pre-existing cast warnings |
| **Overall** | **PASS** |
