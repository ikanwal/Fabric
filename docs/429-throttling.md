# 429 throttling and upstream maintenance

## What was found

The supplied customer log contained 152 HTTP 429 retry notices and 207 HTTP 500 retry notices. It did not contain retry exhaustion, a terminal 429, a traceback, or a failed-task message, and it ended while the run was active. The log therefore demonstrates sustained throttling and retry amplification, but not a terminal 429 failure.

The previous implementation retried individual calls but did not coordinate requests across collectors:

- `Context.invokeAPI` used an async retry path.
- FabricItems, ItemJobs, and ControlPlane used independent threaded request paths.
- Those collectors could produce approximately 11 simultaneous Fabric calls before other modules were counted.
- Generic HTTP 500 responses from the beta relations endpoints were retried repeatedly.
- ControlPlane could start before Catalog and ItemJobs had finished writing its inputs.

## Implemented workaround

`env/utility/http.py` now provides one request coordinator shared by the async and threaded HTTP paths. It:

- limits all in-flight calls in a process with `HTTP_MAX_CONCURRENCY`;
- publishes a 429 `Retry-After` value as a shared cooldown;
- retries only network failures and HTTP 429, 502, 503, and 504 by default;
- defaults to three retries instead of six to reduce retry amplification; and
- keeps query strings and continuation tokens out of retry logs.

ControlPlane now runs in a second phase after the other selected collectors complete. This ensures Catalog and ItemJobs output is available before ControlPlane reads it.

Recommended normal settings:

```text
HTTP_MAX_CONCURRENCY=3
HTTP_MAX_RETRIES=3
ITEMJOBS_CONCURRENCY=3
```

For a tenant currently experiencing sustained throttling, start with:

```text
HTTP_MAX_CONCURRENCY=1
HTTP_MAX_RETRIES=3
ITEMJOBS_CONCURRENCY=1
```

Also use `INCLUDE_WORKSPACE_IDS` to reduce a backfill's scope. Increase concurrency only after several successful runs without repeated 429 cooldowns.

## Reading the logs

`HTTP retry: status=429 attempt=1/4 wait=30.0s path=/v1/...` means the request was throttled, the service delay is being honored, and three retry attempts remain. A run is not failed unless the final response is returned and the owning module reports an error.

HTTP 500 is no longer retried automatically. Persistent 500s from beta endpoints should be investigated as endpoint or request failures rather than replayed enough times to create additional throttling.

## Maintaining this fork

Treat `Coding-Forge/Fabric` as an upstream remote and integrate it through review branches. Do not copy individual files over the fork: that can silently discard the custom ItemJobs, ControlPlane, workspace-filter, and throttling integration.

One-time remote setup:

```powershell
git remote add upstream https://github.com/Coding-Forge/Fabric.git
git fetch upstream
```

For each upstream update:

```powershell
git checkout main
git pull --ff-only origin main
git checkout -b sync/upstream-YYYY-MM-DD
git merge --no-ff upstream/main
```

Resolve conflicts on the sync branch, preserving local behavior deliberately rather than choosing an entire side. Pay particular attention to:

- `env/context.py` and `env/utility/http.py` for shared request coordination;
- `env/audit.py` for ControlPlane ordering;
- `env/config.py` for configuration wiring; and
- `env/modules/fabricitems.py`, `itemjobs.py`, and `controlplane.py` for custom collectors.

Run the relevant tests, push the branch, and merge it through a pull request:

```powershell
python -m pytest -q
git push -u origin sync/upstream-YYYY-MM-DD
```

Keep `main` protected, require passing tests, and sync upstream regularly in small batches. Feature work should also use a branch and pull request. This keeps future conflicts reviewable and prevents an incomplete manual port from appearing successful on `main`.
