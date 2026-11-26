## Summary

<!-- What changed, and why? -->

## Verification

- [ ] `pnpm check`
- [ ] `pnpm test`
- [ ] `pnpm test:integration` when connection, TLS, LISTEN/NOTIFY, locks, readiness or migrations changed
- [ ] `pnpm pack --out /tmp/pg.tgz && pnpm test:package /tmp/pg.tgz` when package contents or metadata changed

## Documentation

<!-- Note README and docs/ changes (new exports, options, defaults, log events), or explain why none are needed. -->
