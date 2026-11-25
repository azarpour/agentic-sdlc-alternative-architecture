# Reference engine (alternative implementation)
This is a new orchestration engine layered on the existing Java service and provider adapters. Historical evidence in this repository belongs to the older LangGraph engine, not this engine. The new engine uses SQLite task checkpoints, parallel code/test tasks, independent completion commits, dependency-based invalidation, bounded automatic task retries, explicit revision and stop, hash-bound human gates, and a locally invoked Maven runner. Audit events are hash-linked, not signed or tamper-proof. Metrics are derived from durable events per run.

## Run on a disposable VM
Use Python 3.12, Java 21, Maven, Docker for Testcontainers. Do not execute AI-generated Maven projects on a workstation with secrets. The runner removes provider keys from the child environment, but is not a sandbox; generated code can access the VM filesystem/network and Docker. Use a disposable isolated VM with no personal credentials, restrict network and review generated files before enabling execution. The runner rejects skipped tests and verifies source hash before and after Maven. It does not prove the tests are meaningful or prevent malicious build plugins from forging output. No cloud credentials are included.

PowerShell (from extracted root):
```powershell
python -m pip install -r requirements.txt
python -m pytest -q
$env:LLM_MODE='live'
$env:ANTHROPIC_MODEL='claude-sonnet-5-5'
# Set ANTHROPIC_API_KEY securely in your session, never commit it.
$env:ALLOW_LOCAL_MAVEN='1' # disposable VM only
python -m orchestrator.reference_cli start scenarios/greenfield.json
```
Copy run_id and pending.hash from output. Approve design only after reviewing the tasks and exact file paths. `approve` executes generation and Maven; it can incur provider charges. Inspect candidate, verification log and review before release approval.
```powershell
python -m orchestrator.reference_cli approve RUN_ID DESIGN_HASH
python -m orchestrator.reference_cli status RUN_ID
python -m orchestrator.reference_cli approve RUN_ID RELEASE_HASH
python -m orchestrator.reference_cli audit RUN_ID
python -m orchestrator.reference_cli metrics RUN_ID
```
For Brownfield use `scenarios/brownfield.json`; for ambiguous use `scenarios/ambiguous.json`, then `clarify RUN_ID "Default 30 days; maximum 90 days inclusive; existing links unchanged"`. At a waiting or retryable checkpoint use `revise RUN_ID requirements --requirement "..."` or `revise RUN_ID tests` to regenerate tests and downstream tasks while retaining code. `advance RUN_ID` resumes interrupted execution or retries if budget permits; `stop RUN_ID` archives candidate and stops. Do not run simultaneous CLI processes against one database; concurrency is within a single process. Task retry budget defaults to two, revision budget three. A failed verification can require explicit revision of code or tests. No arbitrary mid-task cancellation or automatic semantic repair is claimed. Provider batch cache reuses completed files on retry. New engine CLI is separate from the original FastAPI/LangGraph endpoints, which remain available for comparison.

## Validation
New tests use injected deterministic agents and runner; no live provider or Java execution is claimed. Existing Java and legacy engine evidence is retained separately. Mock provider is a fixture, not an AI generation demonstration. The live Greenfield requirement is deliberately comprehensive; generation success is not guaranteed. The task graph is stage-granular, not a dynamic per-file dependency graph. The provider adapter retains its existing bounded retry/fallback and path policy. No deployment is performed.
