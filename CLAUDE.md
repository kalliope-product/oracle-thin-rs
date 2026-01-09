# Oracle Thin Client for Rust

## Directive Layer (Intent & Constraints)

### Project Goal
Pure Rust Oracle Database thin client - no Oracle Instant Client dependency.

### Non-Negotiables
- Async-first using tokio
- **Must work with both Oracle 19c and 23ai** - 19c is production reality
- Protocol behavior must match python-oracledb thin client exactly
- All public APIs must have doc comments
- No unsafe code without explicit justification

### Version Compatibility
| Version | Environment | Priority |
|---------|-------------|----------|
| 19c | AWS RDS | Production target - most deployments |
| 23ai (Free) | Docker local | Dev/test convenience |

### Quality Gates
- `cargo clippy` clean before any PR
- Integration tests pass against **both** Oracle versions
- New protocol code requires corresponding test case

### Source of Truth
When in doubt about protocol behavior, the Python reference implementation is authoritative:
`python-ref/python-oracledb/src/oracledb/impl/thin/`

### Learned Directives
**Read `/directives/` for accumulated operational knowledge.**

When you discover a better way to do something (or a mistake you keep repeating), create or update a directive file. Categories:
- `directives/environment.md` - Local setup, tools, paths
- `directives/workflow.md` - Patterns that work, anti-patterns to avoid
- `directives/protocol-learnings.md` - Oracle TNS quirks discovered during implementation
- `directives/rust-patterns.md` - Important Rust information related to this project, take note when implementing, debugging or fixing issues.
- `directives/scripts/*.py` - Helper scripts for testing/debugging or protocol exploration
- `directives/current-plan.md` - Current implementation plan and next steps. Update these when we finalizing the plan and start execution.

**Update directives proactively** - if you catch yourself about to repeat a mistake or reinvent a solution, write it down first.

---

## Orchestration Layer (How to Work)

### Task Decomposition Pattern
1. **Check directives** - Read `/directives/` before starting
2. **Understand** - Read relevant Python reference file first
3. **Translate** - Map Python idioms to Rust equivalents
4. **Implement** - Write Rust code with tests
5. **Verify** - Run against both Oracle versions
6. **Capture** - Update directives if you learned something new

### Version-Specific Debugging
| Symptom | 19c likely cause | 23ai likely cause |
|---------|------------------|-------------------|
| Auth failure | Check 12c verifier path | Check encryption requirements |
| Capability mismatch | Older flag set expected | New flags not handled |
| Connection refused | Security group / network | Docker networking |

### When Stuck on Protocol Issues
1. Check Python reference file with `--nocapture` debug output
2. Use Wireshark to compare packet bytes if needed
3. Test against 23ai first (easier to iterate), then validate on 19c
4. The constants in `constants.pxi` are the rosetta stone

### File Navigation Heuristics
| Task | Start Here |
|------|------------|
| Packet parsing bugs | `src/protocol/buffer.rs` |
| Auth failures | `src/protocol/auth.rs` + `crypto.rs` |
| Connection refused | `src/protocol/connect.rs` |
| New SQL feature | `python-ref/.../execute.pyx` |

### Subagent Delegation
- Use subagents for: researching Python reference, running cargo commands
- Keep in main context: protocol state machine reasoning, crypto debugging

### Directive Maintenance
When creating/updating directives:
1. Be specific - include exact commands, paths, error messages
2. Explain *why* not just *what*
3. Date the entry if it might become stale
4. Keep each file focused - split if it grows beyond one screen

---

## Execution Layer (Concrete Actions)

### Test Environments

**Local (Oracle 23ai Free - Docker)**
```bash
cd tests && docker compose up -d
```
```
Host: localhost:1521
Service: FREEPDB1
User: read_user
Password: ThisIsASecret123
```

**RDS (Oracle 19c)**
```
Host: <rds-endpoint>
Port: 1521
Service: <service-name>
User: <rds-user>
Password: <from-secrets-manager>
```

### Commands
```bash
# Run all tests (defaults to local 23ai)
cargo test

# Debug specific test
cargo test --test integration_test -- --nocapture

# Test against 19c RDS (set env vars first)
ORACLE_HOST=<rds-endpoint> ORACLE_USER=<user> ORACLE_PASS=<pass> cargo test
```