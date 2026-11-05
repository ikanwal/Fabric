# Configuration Reference

All settings are environment variables. Locally they are set in `local.settings.json` under `Values`. In Azure they are set as **Function App → Settings → Environment variables**.

For standalone Python and the Windows UI, the same values can also be supplied in a JSON profile and passed with:

```powershell
python -m app.monitor --profile .\profiles\local-monitor.json
```

---

## Required Settings

| Variable | Description |
|---|---|
| `TENANT_ID` | Azure AD tenant ID. Used by MSAL for Power BI / Fabric API token acquisition. |
| `CLIENT_ID` | App registration (service principal) client ID. |
| `CLIENT_SECRET` | App registration client secret. **Use a Key Vault reference in production.** |
| `CLOUD_ENVIRONMENT` | Cloud profile for endpoints and token scopes. Supported values: `Commercial`, `Gcc`, `GccHigh`, `Dod`. Defaults to `Commercial`. |
| `APPLICATION_MODULES` | Comma-separated list of modules to run. See [Modules](#modules) below. |

---

## Storage Settings

Configure one storage mode: **local files**, **Blob Storage URL** (recommended), **Blob Storage connection string**, or **Fabric Lakehouse**. If no storage settings are provided, standalone Python defaults to `OUTPUT_PATH=Data`.

### Option A — Local files

| Variable | Required | Description |
|---|---|---|
| `OUTPUT_PATH` | No | Local folder for output files. Defaults to `Data` for standalone Python when no other storage mode is configured. |

### Option B — Blob Storage with URL auth (recommended)

Uses `DefaultAzureCredential` (Managed Identity in Azure, service principal locally). Works when shared key access is disabled on your storage account.

| Variable | Required | Description |
|---|---|---|
| `STORAGE_ACCOUNT_URL` | Yes | Commercial/GCC: `https://<account>.blob.core.windows.net`; GCC High/DoD Azure Government: `https://<account>.blob.core.usgovcloudapi.net` |
| `STORAGE_ACCOUNT_CONTAINER_NAME` | Yes | Container name, e.g. `monitor` |
| `STORAGE_ACCOUNT_CONTAINER_ROOT_PATH` | No | Root path prefix inside the container, e.g. `stage` |
| `AZURE_TENANT_ID` | Yes (local) | Same as `TENANT_ID`. Read by `DefaultAzureCredential`. |
| `AZURE_CLIENT_ID` | Yes (local) | Same as `CLIENT_ID`. Read by `DefaultAzureCredential`. |
| `AZURE_CLIENT_SECRET` | Yes (local) | Same as `CLIENT_SECRET`. Read by `DefaultAzureCredential`. |

> In Azure with Managed Identity, `AZURE_*` variables are **not needed** — the identity is resolved automatically.

### Option C — Blob Storage with connection string

| Variable | Required | Description |
|---|---|---|
| `STORAGE_ACCOUNT_CONN_STR` or `STORAGE_ACCOUNT_CONNECTION_STRING` | Yes | Full connection string from the storage account Access keys blade. |
| `STORAGE_ACCOUNT_CONTAINER_NAME` | Yes | Container name. |
| `STORAGE_ACCOUNT_CONTAINER_ROOT_PATH` | No | Root path prefix. |

> Not recommended if "Allow storage account key access" is disabled on your storage account.

### Option D — Fabric Lakehouse

Used when running inside a Fabric environment.

> Fabric Lakehouse / OneLake output is currently enabled only for `CLOUD_ENVIRONMENT=Commercial`. Use local files or Blob Storage for `Gcc`, `GccHigh`, and `Dod` until Fabric Gov support becomes public preview.

| Variable | Required | Description |
|---|---|---|
| `LAKEHOUSE_NAME` | Yes | Name of the Lakehouse, e.g. `LochMonitor` |
| `WORKSPACE_NAME` | Yes | Name of the Fabric workspace |
| `PATH_IN_LAKEHOUSE` | No | Subfolder path inside the Lakehouse Files, e.g. `stage` |
| `ON_FABRIC` | No | `true` if running inside Fabric runtime (uses `/lakehouse/default/Files/...`). Default: `true` |

---

## Optional Settings

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `false` | When `true`, prints what would be written to blob instead of writing it. Useful for local testing. |
| `ALL_WORKSPACES` | `false` | When `true`, retrieves all workspaces regardless of modification date. |
| `EXCLUDE_PERSONAL_WORKSPACES` | `true` | Excludes personal workspaces from catalog modified-workspace scans. |
| `EXCLUDE_INACTIVE_WORKSPACES` | `true` | Excludes inactive workspaces from catalog modified-workspace scans. |
| `INCLUDE_WORKSPACE_IDS` | _(none)_ | Comma-separated workspace GUID allowlist. When set, Catalog scans these workspaces regardless of modification date; Fabric Items and Item Jobs also process only items in these workspaces.|
| `EXCLUDE_WORKSPACE_IDS` | _(none)_ | Comma-separated workspace GUID denylist. Exclusions take precedence over `INCLUDE_WORKSPACE_IDS`.|
| `HTTP_MAX_CONCURRENCY` | `3` | Maximum in-flight REST requests across async and threaded collectors in one monitor process. Use `1` as a temporary mitigation for a heavily throttled tenant. `API_MAX_CONCURRENCY` is accepted as a compatibility alias. |
| `HTTP_MAX_RETRIES` | `3` | Max retries for network errors and HTTP 429/502/503/504. The shared cooldown honors `Retry-After` for up to 1 hour; otherwise exponential backoff is used. Generic HTTP 500 and other 4xx responses are not retried. |
| `SCAN_STATUS_POLL_SECONDS` | `20` | Seconds between Scanner `scanStatus` polls. |
| `ITEMJOBS_CONCURRENCY` | `3` | Concurrent ItemJobs work. Actual REST calls remain bounded by `HTTP_MAX_CONCURRENCY`. |
| `IMPERSONATED_USER_NAME` | _(none)_ | UPN of a user to impersonate for API calls. |
| `CAPACITY_METRICS_DATASET_ID` | _(none)_ | Dataset ID for the Capacity Metrics module. |

---

## Per-Module Cron Overrides

Each module runs on the global timer schedule (`0 0 */4 * * *` — every 4 hours). You can override the effective schedule per module using cron syntax. If not set, the module runs every time the function fires.

| Variable | Module |
|---|---|
| `ACTIVITY_CRON` | Activity Events |
| `APPS_CRON` | Apps |
| `CAPACITY_CRON` | Capacity |
| `CAPACITY_METRICS_CRON` | Capacity Metrics |
| `CATALOG_CRON` | Catalog Scans |
| `DOMAINS_CRON` | Domains |
| `FABRICITEMS_CRON` | Fabric Items |
| `GATEWAY_CRON` | Gateways |
| `GRAPH_CRON` | Graph (AAD Groups) |
| `REFRESHABLES_CRON` | Refreshables |
| `REFRESHHISTORY_CRON` | Refresh History |
| `ROLES_CRON` | Workspace Roles |
| `TENANT_CRON` | Tenant Settings |
| `WORKSPACES_CRON` | Workspaces |

**Example:** run Catalog only once a day at midnight UTC:
```
CATALOG_CRON=0 0 * * *
```

---

## Modules

All available module names for `APPLICATION_MODULES`:

| Name | Description |
|---|---|
| `Activity` | Power BI activity log events |
| `Apps` | Published Power BI apps |
| `Capacity` | Capacity details |
| `Catalog` | Full workspace/item catalog scan |
| `Domains` | Fabric domains |
| `FabricItems` | All Fabric items across the tenant |
| `Gateway` | On-premise and VNet gateways |
| `Graph` | Azure AD group membership |
| `Refreshables` | Refreshable datasets |
| `RefreshHistory` | Dataset refresh history |
| `Roles` | Workspace role assignments |
| `Tenant` | Tenant settings |
| `Workspaces` | Workspace metadata |

---

## Cloud Environment Behavior

`CLOUD_ENVIRONMENT` controls the identity authority, API roots, and token scopes used by the monitor.

| Value | Power BI API root | Graph root | Fabric REST / OneLake |
|---|---|---|---|
| `Commercial` | `https://api.powerbi.com/v1.0/myorg/` | `https://graph.microsoft.com/beta` | Enabled |
| `Gcc` | `https://api.powerbigov.us/v1.0/myorg/` | `https://graph.microsoft.com/beta` | Disabled |
| `GccHigh` | `https://api.high.powerbigov.us/v1.0/myorg/` | `https://graph.microsoft.us/beta` | Disabled |
| `Dod` | `https://api.mil.powerbigov.us/v1.0/myorg/` | `https://graph.microsoft.us/beta` | Disabled |

The monitor automatically skips Fabric REST API modules outside Commercial until Fabric Gov becomes public preview. Skipped modules: `Domains`, `FabricItems`, `Roles`, `Tenant`, and `Workspaces`.

Classic Power BI REST modules such as `Activity`, `Apps`, `Capacity`, `CapacityMetrics`, `Catalog`, `Gateway`, `Refreshables`, and `RefreshHistory` continue to use the configured Power BI government endpoint.

---

## Timer Schedule

The function is triggered on a CRON schedule defined in `function_app.py`:

```python
@app.schedule(schedule="0 0 */4 * * *", ...)
```

This fires every 4 hours at the top of the hour (UTC). Azure Functions uses a 6-field NCRONTAB format:

```
{second} {minute} {hour} {day} {month} {day-of-week}
```

To change the schedule, edit the `schedule` value in `function_app.py` before deploying.
