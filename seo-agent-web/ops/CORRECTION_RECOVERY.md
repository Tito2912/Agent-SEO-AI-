# Pending Correction Recovery

Run from `seo-agent-web` on the service, using its existing database and stored
GitHub connection. Do not paste a token into a command. This tool does not start
workers, call Claude, write to GitHub, merge PRs, or change billing events.

```sh
python ops/correction_doctor.py list
python ops/correction_doctor.py list --payer ACCOUNT_ID
python ops/correction_doctor.py inspect OPERATION_ID
```

`list` is database-only and redacts saved previews. `inspect` acquires the same
payer lock as the corrector. It reads the frozen repository, all PR states for
the exact head (without a base filter), the starting commit, and the correction branch. It never
unlocks work. Missing credentials, permission errors, rate limits, redirects,
malformed responses and lost leases prevent a decision.

## Explicit Abandonment

An operator can release an unpaid operation with no saved preview or PR when:

- There was no external write intent (for example, a provider crash).
- GitHub confirms the repository and starting commit are readable and its correction branch is absent.
- GitHub confirms the branch still points at the frozen starting commit.

```sh
python ops/correction_doctor.py abandon OPERATION_ID \
  --operator OPERATOR_NAME --reason "Reviewed interrupted correction"
```

If unpublished commits exist, the ordinary command refuses. After reviewing
them and deciding to restart from the current project base, explicitly retain
that branch:

```sh
python ops/correction_doctor.py abandon OPERATION_ID --retain-branch \
  --operator OPERATOR_NAME --reason "Reviewed partial branch; restart approved"
```

The branch and its commits stay on GitHub for review; the tool never deletes
them. It records the operator, timestamp, reason and observed SHA, marks only
that operation abandoned and releases its request identity. A new correction
uses a new branch. A previously paid preview remains available without another
AI debit. Abandoning one operation does not release other pending operations.

## Refusals

Any PR, including a closed or merged PR, prevents abandonment. Recover an
accepted PR or saved result through the original correction request instead;
do not erase its receipt. Charged work cannot be abandoned by this tool.
Legacy operations without a frozen repository snapshot require manual review.

Do not create, merge or change that branch manually during reconciliation.
The payer lock excludes application workers, not external GitHub actors.
An operator must also review project configuration before a fresh request:
abandonment is not a promise that a partially written correction was completed.

Exit codes: `0` success, `2` refusal/invalid arguments, `3` active payer worker,
`4` unavailable verification. Inspection and abandonment re-read the operation
under the lock; abandonment always performs fresh remote checks.
