import asyncio
import base64
import json
import re
import time

from datetime import datetime, timedelta

from env.utility.http import (
    DEFAULT_MAX_RETRIES,
    parse_retry_after,
    request_with_retry,
)
from env.utility.workspace_filter import filter_workspace_records, log_workspace_filter_counts

JOB_ITEM_TYPES = {"CopyJob", "DataPipeline", "Notebook", "SynapseNotebook", "Dataflow", "SparkJobDefinition"}
DEFINITION_ITEM_TYPES = {"CopyJob", "DataPipeline", "Notebook", "Report", "SemanticModel", "Monitoring", "KQLDatabase"}
TOPOLOGY_ITEM_TYPES = {"Eventstream"}
SHORTCUT_ITEM_TYPES = {"Lakehouse"}
JSON_DEFINITION_PARTS = {
    "pipeline-content.json", "copyjob-content.json", "definition.pbir", "MonitoringProperties.json",
    "DatabaseProperties.json",
}
# Power Query functions that read from an external source; only the first literal argument is kept.
M_SOURCE_FUNCTIONS = {
    "Sql.Database", "Sql.Databases", "Folder.Files", "Folder.Contents", "File.Contents", "Web.Contents",
    "Web.Page", "AzureStorage.Blobs", "AzureStorage.DataLake", "AzureStorage.DataLakeContents",
    "AzureStorage.Tables", "Snowflake.Databases", "Odbc.DataSource", "Odbc.Query", "OleDb.DataSource",
    "Oracle.Database", "PostgreSQL.Database", "MySQL.Database", "SharePoint.Files", "SharePoint.Contents",
    "SharePoint.Tables", "AzureDataExplorer.Contents", "GoogleBigQuery.Database", "Databricks.Catalogs",
    "Cosmos.Contents", "AmazonS3.Contents", "Fabric.Warehouse", "Lakehouse.Contents",
}
_M_CALL = re.compile(r'\b([A-Z][A-Za-z0-9]*\.[A-Z][A-Za-z0-9]*)\(\s*"((?:[^"]|"")*)"')
SCAN_LOOKBACK_DAYS = 3
SCAN_MAX_MISSES = 3
SCAN_MAX_FILES = 500
_max_retries = DEFAULT_MAX_RETRIES
_logger = None
_coordinator = None


def _call(method, url, headers, **kwargs):
    return request_with_retry(
        method,
        url,
        max_retries=_max_retries,
        logger=_logger,
        coordinator=_coordinator,
        headers=headers,
        timeout=120,
        **kwargs,
    )


def _find(node, key):
    """First value for `key` anywhere in a nested dict/list."""
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for v in node.values():
            found = _find(v, key)
            if found is not None:
                return found
    elif isinstance(node, list):
        for v in node:
            found = _find(v, key)
            if found is not None:
                return found
    return None


def _list_jobs(item, headers, base):
    url = f"{base}/v1/workspaces/{item['workspaceId']}/items/{item['id']}/jobs/instances"
    runs = []
    while url:
        r = _call("GET", url, headers)
        if r.status_code != 200:
            return None, r
        body = r.json()
        runs.extend(body.get("value", []))
        url = body.get("continuationUri")
    return runs, None


def _get_definition(item, headers, base):
    url = f"{base}/v1/workspaces/{item['workspaceId']}/items/{item['id']}/getDefinition"
    r = _call("POST", url, headers)
    if r.status_code == 202:
        location = r.headers.get("Location")
        for _ in range(30):
            delay = parse_retry_after(r.headers.get("Retry-After"))
            time.sleep(
                min(delay, 3600.0) if delay is not None else 2.0
            )
            s = _call("GET", location, headers)
            if s.status_code == 200 and s.json().get("status") == "Succeeded":
                r = _call("GET", location.rstrip("/") + "/result", headers)
                break
            if s.status_code == 200 and s.json().get("status") == "Failed":
                return None, s
        else:
            return None, r
    if r.status_code != 200:
        return None, r
    parts = {}
    for p in r.json().get("definition", {}).get("parts", []):
        path = p["path"]
        is_model = path == "definition/expressions.tmdl" or (path.startswith("definition/tables/") and path.endswith(".tmdl"))
        if not (path.startswith("notebook-content.") or path in JSON_DEFINITION_PARTS or is_model):
            continue
        try:
            text = base64.b64decode(p["payload"]).decode("utf-8-sig")
            if path.startswith("notebook-content."):
                # Only the dependency metadata is kept; notebook code is never stored.
                parts["notebook-dependencies"] = _notebook_dependencies(path, text)
            elif is_model:
                # Only external source locations are kept; model code and queries are never stored.
                parts.setdefault("model-sources", []).extend(_model_sources(text))
            else:
                parts[path] = json.loads(text)
        except Exception:
            continue
    return parts, None


def _strip_query(value):
    """Drop URL query strings (they can carry SAS tokens or keys) before a location is stored."""
    return value.split("?", 1)[0] if value.lower().startswith(("http://", "https://")) else value


def _model_sources(text):
    return [(fn, _strip_query(arg.replace('""', '"'))) for fn, arg in _M_CALL.findall(text) if fn in M_SOURCE_FUNCTIONS]


def _get_eventstream_topology(item, headers, base):
    url = f"{base}/v1/workspaces/{item['workspaceId']}/eventstreams/{item['id']}/topology"
    r = _call("GET", url, headers)
    return (r.json(), None) if r.status_code == 200 else (None, r)


def _notebook_dependencies(path, text):
    """Notebook-level `dependencies` metadata from a .ipynb or .py (Fabric git format) definition."""
    if path.endswith(".ipynb"):
        return (json.loads(text).get("metadata") or {}).get("dependencies") or {}
    # .py: the first "# META" block after "# METADATA" holds the notebook-level metadata
    block, started = [], False
    for line in text.splitlines():
        if not started:
            started = line.startswith("# METADATA")
            continue
        if line.startswith("# META"):
            block.append(line[len("# META"):].lstrip(" ") if line != "# META" else "")
        elif block:
            break
    return (json.loads("\n".join(block)) if block else {}).get("dependencies") or {}


def _flatten_notebook(parts, names, item_index):
    lakehouse = (parts.get("notebook-dependencies") or {}).get("lakehouse") or {}
    default_id = lakehouse.get("default_lakehouse")
    rows = []

    def row(role, lh_id, workspace_id, name):
        known = item_index.get(str(lh_id).lower()) or {}
        return {
            "Role": role,
            "ActivityName": None,
            "EndpointType": "Lakehouse",
            "ConnectionType": None,
            "TargetWorkspaceId": workspace_id or known.get("workspaceId"),
            "TargetItemId": lh_id,
            "TargetItemName": name or names.get(lh_id) or known.get("name"),
        }

    if default_id:
        rows.append(row("AttachedLakehouse", default_id, lakehouse.get("default_lakehouse_workspace_id"),
                        lakehouse.get("default_lakehouse_name")))
    for known in lakehouse.get("known_lakehouses") or []:
        known_id = known.get("id")
        if known_id and known_id != default_id:
            rows.append(row("KnownLakehouse", known_id, known.get("workspaceId"), known.get("name")))
    return rows


def _list_shortcuts(item, headers, base):
    url = f"{base}/v1/workspaces/{item['workspaceId']}/items/{item['id']}/shortcuts"
    shortcuts = []
    while url:
        r = _call("GET", url, headers)
        if r.status_code != 200:
            return None, r
        body = r.json()
        shortcuts.extend(body.get("value", []))
        url = body.get("continuationUri")
    return shortcuts, None


def _flatten_shortcuts(shortcuts, names):
    rows = []
    for sc in shortcuts:
        target = sc.get("target") or {}
        target_type = target.get("type")
        # The target details sit under a key named after the type (oneLake, adlsGen2, amazonS3, ...)
        detail = next((v for k, v in target.items() if k != "type" and isinstance(v, dict)), {})
        location = detail.get("location")
        sub_path = detail.get("subpath") or detail.get("path")
        target_id = detail.get("itemId")
        rows.append({
            "Role": "ShortcutSource",
            "ActivityName": None,
            "EndpointType": target_type,
            "ConnectionType": target_type,
            "TargetType": target_type,
            "TargetLocation": location,
            "TargetPath": sub_path,
            "TargetWorkspaceId": detail.get("workspaceId"),
            "TargetItemId": target_id,
            "TargetItemName": names.get(target_id),
            "ConnectionId": detail.get("connectionId"),
            "ShortcutName": sc.get("name"),
            "ShortcutPath": sc.get("path"),
        })
    return rows


def _datasource_location(instance):
    details = instance.get("connectionDetails") or {}
    parts = [details.get(k) for k in ("server", "database", "url", "path", "extensionDataSourcePath") if details.get(k)]
    return " / ".join(parts) or None


def _flatten_scanner(scans, names, include_ids, exclude_ids):
    """One row per (item, data source) from scanner datasourceUsages, scoped by the workspace filter."""
    rows, seen = [], set()
    for scan in scans:
        instances = {str(i.get("datasourceId")).lower(): i for i in scan.get("datasourceInstances") or []}
        workspaces, _ = filter_workspace_records(
            scan.get("workspaces") or [], include_ids, exclude_ids, get_workspace_id=lambda w: w.get("id")
        )
        for ws in workspaces:
            for key, value in ws.items():
                if not isinstance(value, list):
                    continue
                for it in value:
                    if not isinstance(it, dict) or not it.get("id"):
                        continue
                    for use in it.get("datasourceUsages") or []:
                        ds_id = str(use.get("datasourceInstanceId")).lower()
                        inst = instances.get(ds_id)
                        if not inst or (it["id"], ds_id) in seen:
                            continue
                        seen.add((it["id"], ds_id))
                        ds_type = inst.get("datasourceType")
                        rows.append({
                            "ItemId": it["id"],
                            "ItemName": it.get("name") or it.get("displayName") or names.get(it["id"]),
                            "ItemType": it.get("type") or SCANNER_ITEM_TYPES.get(key, key),
                            "WorkspaceId": ws.get("id"),
                            "Collector": "Scanner",
                            "Role": "DataSource",
                            "ActivityName": None,
                            "EndpointType": ds_type,
                            "ConnectionType": ds_type,
                            "TargetWorkspaceId": None,
                            "TargetItemId": None,
                            "TargetItemName": None,
                            "TargetLocation": _datasource_location(inst),
                            "DataSourceId": inst.get("datasourceId"),
                            "GatewayId": inst.get("gatewayId"),
                        })
    return rows


SCANNER_ITEM_TYPES = {
    "reports": "Report", "dashboards": "Dashboard", "datasets": "SemanticModel", "dataflows": "Dataflow",
    "datamarts": "Datamart", "warehouses": "Warehouse", "lakehouses": "Lakehouse",
}


async def _load_scans(context, today):
    """Scan results saved by the Catalog module (newest day within the lookback that has files)."""
    for back in range(SCAN_LOOKBACK_DAYS):
        day = today - timedelta(days=back)
        path = f"catalog/scans/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/"
        scans, misses = [], 0
        for index in range(1, SCAN_MAX_FILES + 1):
            content = None
            for suffix in ("scanResults.json", "scanResults.fullscan.json"):
                content = await context.fm.read(path=path, file_name=f"{day.strftime('%Y%m%d')}_{str(index).zfill(5)}.{suffix}")
                if content:
                    break
            if not content:
                misses += 1
                if misses >= SCAN_MAX_MISSES:
                    break
                continue
            misses = 0
            try:
                scans.append(json.loads(content))
            except ValueError:
                context.logger.warning(f"ItemJobs: unreadable scan file {path}{index}")
        if scans:
            return scans
    return []


def _endpoint(role, kind, node, names, activity_name=None, **extra):
    settings = (node or {}).get("connectionSettings") or {}
    target_id = _find(settings, "artifactId")
    return {
        "Role": role,
        "ActivityName": activity_name,
        "EndpointType": (node or {}).get("type") or kind,
        "ConnectionType": settings.get("type"),
        "TargetWorkspaceId": _find(settings, "workspaceId"),
        "TargetItemId": target_id,
        "TargetItemName": names.get(target_id),
        **extra,
    }


def _query_activity_runs(job, headers, base, note, run_id=None, parent_run_id=None, depth=0,
                         window=None, names=None):
    """Per-activity volume (rows/bytes) for a pipeline run, following ExecutePipeline into child runs."""
    run_id = run_id or job["JobInstanceId"]
    if window is None:
        start = datetime.fromisoformat(job["StartTimeUtc"][:26])
        end = datetime.fromisoformat((job["EndTimeUtc"] or job["StartTimeUtc"])[:26])
        window = (start, end)
    body = {
        "filters": [],
        "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}],
        "lastUpdatedAfter": (window[0] - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lastUpdatedBefore": (window[1] + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    url = f"{base}/v1/workspaces/{job['WorkspaceId']}/datapipelines/pipelineruns/{run_id}/queryactivityruns"
    r = _call("POST", url, headers, json=body)
    if r.status_code != 200:
        note({"type": job["ItemType"]}, "activityruns", r)
        return []
    rows = []
    for a in r.json().get("value", []):
        out = a.get("output") or {}
        rows.append({
            "ItemId": job["ItemId"],
            "ItemName": job["ItemName"],
            "ItemType": job["ItemType"],
            "WorkspaceId": job["WorkspaceId"],
            "JobInstanceId": job["JobInstanceId"],
            "PipelineRunId": run_id,
            "ParentPipelineRunId": parent_run_id,
            "NestingLevel": depth,
            "ChildPipelineName": (names or {}).get(out.get("pipelineName"), out.get("pipelineName"))
            if a.get("activityType") == "ExecutePipeline" else None,
            "ActivityName": a.get("activityName"),
            "ActivityType": a.get("activityType"),
            "ActivityStatus": a.get("status"),
            "ActivityStart": a.get("activityRunStart"),
            "ActivityEnd": a.get("activityRunEnd"),
            "DurationMs": a.get("durationInMs"),
            "RowsRead": out.get("rowsRead"),
            "RowsCopied": out.get("rowsCopied"),
            "DataReadBytes": out.get("dataRead"),
            "DataWrittenBytes": out.get("dataWritten"),
            "ThroughputKBps": out.get("throughput"),
            "Error": json.dumps(a.get("error")) if a.get("error") and a["error"].get("message") else None,
        })
        child_run = out.get("pipelineRunId")
        if a.get("activityType") == "ExecutePipeline" and child_run and depth < 4:
            try:
                a_start = datetime.fromisoformat(a["activityRunStart"][:26])
                a_end = datetime.fromisoformat((a.get("activityRunEnd") or a["activityRunStart"])[:26])
            except (KeyError, TypeError, ValueError):
                a_start, a_end = window
            rows.extend(_query_activity_runs(job, headers, base, note, run_id=child_run, parent_run_id=run_id,
                                             depth=depth + 1, window=(a_start, a_end), names=names))
    return rows


def _flatten_copyjob(content, names):
    props = content.get("properties", {})
    rows = []
    activities = content.get("activities") or [{}]
    for act in activities:
        a = act.get("properties", {})
        src_ds = (a.get("source") or {}).get("datasetSettings") or {}
        dst = a.get("destination") or {}
        dst_ds = dst.get("datasetSettings") or {}
        rows.append(_endpoint("Source", "Source", props.get("source"), names,
                              Schema=src_ds.get("schema"), Table=src_ds.get("table"), JobMode=props.get("jobMode")))
        rows.append(_endpoint("Destination", "Destination", props.get("destination"), names,
                              Schema=dst_ds.get("schema"), Table=dst_ds.get("table"),
                              WriteBehavior=dst.get("writeBehavior"), JobMode=props.get("jobMode")))
    return rows


def _lookup(item_index, item_id):
    return item_index.get(str(item_id).lower()) or {} if item_id else {}


def _flatten_report(parts, names, item_index):
    ref = (parts.get("definition.pbir") or {}).get("datasetReference") or {}
    conn = (ref.get("byConnection") or {}).get("connectionString") or ""
    match = re.search(r"semanticmodelid=([0-9a-fA-F-]{36})", conn)
    if match:
        model_id = match.group(1)
        known = _lookup(item_index, model_id)
        workspace = re.search(r"/myorg/([^;]+)", conn)
        return [{
            "Role": "Source", "ActivityName": None, "EndpointType": "SemanticModel", "ConnectionType": "byConnection",
            "TargetWorkspaceId": known.get("workspaceId"), "TargetItemId": model_id,
            "TargetItemName": known.get("name") or names.get(model_id),
            "TargetLocation": workspace.group(1) if workspace else None,
        }]
    path = (ref.get("byPath") or {}).get("path")
    if path:
        return [{"Role": "Source", "ActivityName": None, "EndpointType": "SemanticModel", "ConnectionType": "byPath",
                 "TargetLocation": path}]
    return []


def _flatten_semantic_model(parts, names, item_index):
    rows, seen = [], set()
    for function, location in parts.get("model-sources") or []:
        if (function, location) in seen:
            continue
        seen.add((function, location))
        rows.append({"Role": "DataSource", "ActivityName": None, "EndpointType": function, "ConnectionType": function,
                     "TargetLocation": location})
    return rows


def _flatten_monitoring(parts, names, item_index):
    rows = []
    for setting in (parts.get("MonitoringProperties.json") or {}).get("dataCollectionSettings") or []:
        for role, key in (("Source", "sources"), ("Destination", "destinations")):
            for kind, detail in (setting.get(key) or {}).items():
                rows.append({"Role": role, "ActivityName": None, "EndpointType": kind, "ConnectionType": None,
                             "TargetLocation": (detail or {}).get("mode")})
    return rows


def _flatten_kqldatabase(parts, names, item_index):
    parent = (parts.get("DatabaseProperties.json") or {}).get("parentEventhouseItemId")
    if not parent:
        return []
    known = _lookup(item_index, parent)
    return [{"Role": "ParentEventhouse", "ActivityName": None, "EndpointType": "Eventhouse", "ConnectionType": None,
             "TargetWorkspaceId": known.get("workspaceId"), "TargetItemId": parent,
             "TargetItemName": known.get("name") or names.get(parent)}]


def _flatten_eventstream(topology, names, item_index):
    rows = []
    for role, key in (("Source", "sources"), ("Destination", "destinations")):
        for node in (topology or {}).get(key) or []:
            props = node.get("properties") or {}
            target_id = props.get("itemId")
            known = _lookup(item_index, target_id)
            rows.append({
                "Role": role,
                "ActivityName": node.get("name"),
                "EndpointType": node.get("type"),
                "ConnectionType": node.get("type"),
                "TargetWorkspaceId": props.get("workspaceId") or known.get("workspaceId"),
                "TargetItemId": target_id,
                "TargetItemName": known.get("name") or names.get(target_id),
                "Table": props.get("tableName"),
                "ConnectionId": props.get("dataConnectionId"),
            })
    return rows


def _flatten_pipeline(content, names):
    rows = []

    def walk(activities):
        for act in activities or []:
            tp = act.get("typeProperties") or {}
            if act.get("type") == "Copy":
                for role, key in (("Source", "source"), ("Destination", "sink")):
                    node = tp.get(key) or {}
                    ds = node.get("datasetSettings") or {}
                    ds_props = ds.get("typeProperties") or {}
                    target_id = _find(ds, "artifactId")
                    rows.append({
                        "Role": role,
                        "ActivityName": act.get("name"),
                        "EndpointType": node.get("type") or ds.get("type"),
                        "ConnectionType": ds.get("type"),
                        "TargetWorkspaceId": _find(ds, "workspaceId"),
                        "TargetItemId": target_id,
                        "TargetItemName": names.get(target_id),
                        "Schema": ds_props.get("schema"),
                        "Table": ds_props.get("table"),
                        "WriteBehavior": node.get("writeBehavior"),
                    })
            else:
                row = {"Role": "Activity", "ActivityName": act.get("name"), "EndpointType": act.get("type")}
                if act.get("type") == "TridentNotebook" and tp.get("notebookId"):
                    row.update({"TargetWorkspaceId": tp.get("workspaceId"), "TargetItemId": tp["notebookId"],
                                "TargetItemName": names.get(tp["notebookId"])})
                rows.append(row)
            for child in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
                walk(tp.get(child))
            for case in tp.get("cases") or []:
                walk(case.get("activities"))

    walk(content.get("properties", {}).get("activities"))
    return rows


async def main(context=None):
    """
    Item Jobs
    """
    if context is None:
        raise RuntimeError("Context is None")

    global _max_retries, _logger, _coordinator
    _max_retries = context.http_max_retries
    _logger = context.logger
    _coordinator = getattr(context, "http_coordinator", None)
    headers = context.clients['tenant'].get_headers()
    base = context.get_fabric_url("").rstrip("/")
    today = datetime.now()

    items = []
    url = context.get_fabric_url("v1/admin/items")
    while url:
        response = await context.invokeAPI(url, headers=headers)
        if "ERROR" in response:
            context.logger.error(
                f"ItemJobs: admin/items listing stopped: HTTP {response.get('status_code')} {response.get('message')}"
            )
            break
        items.extend(response.get("itemEntities") or [])
        url = response.get("continuationUri")

    all_items = items
    items, workspace_counts = filter_workspace_records(
        items,
        context.include_workspace_ids,
        context.exclude_workspace_ids,
        get_workspace_id=lambda item: item.get("workspaceId"),
    )
    names = {i["id"]: i.get("name") or i.get("displayName") for i in items if i.get("id")}
    # Unfiltered lookup so attached lakehouses in other workspaces can still be resolved
    item_index = {
        str(i["id"]).lower(): {"workspaceId": i.get("workspaceId"), "name": i.get("name") or i.get("displayName")}
        for i in all_items if i.get("id")
    }
    targets = [i for i in items if i.get("id") and i.get("workspaceId") and i.get("type") in JOB_ITEM_TYPES]
    lakehouses = [i for i in items if i.get("id") and i.get("workspaceId") and i.get("type") in SHORTCUT_ITEM_TYPES]
    definition_targets = [i for i in items if i.get("id") and i.get("workspaceId") and i.get("type") in DEFINITION_ITEM_TYPES]
    topology_targets = [i for i in items if i.get("id") and i.get("workspaceId") and i.get("type") in TOPOLOGY_ITEM_TYPES]
    log_workspace_filter_counts(context.logger, "ItemJobs", workspace_counts, len(targets))
    sem = asyncio.Semaphore(context.itemjobs_concurrency)
    errors = {}

    def note(item, kind, r):
        key = f"{kind}:{item.get('type')}:{r.status_code}"
        errors.setdefault(key, [0, r.text[:200]])[0] += 1

    async def job_rows(item):
        async with sem:
            runs, err = await asyncio.to_thread(_list_jobs, item, headers, base)
        if err is not None:
            note(item, "jobs", err)
            return []
        rows = []
        for run in runs:
            start, end = run.get("startTimeUtc"), run.get("endTimeUtc")
            duration = None
            if start and end:
                try:
                    duration = (datetime.fromisoformat(end[:26]) - datetime.fromisoformat(start[:26])).total_seconds()
                except ValueError:
                    pass
            rows.append({
                "ItemId": item["id"],
                "ItemName": names.get(item["id"]),
                "ItemType": item.get("type"),
                "WorkspaceId": item["workspaceId"],
                "JobInstanceId": run.get("id"),
                "JobType": run.get("jobType"),
                "InvokeType": run.get("invokeType"),
                "Status": run.get("status"),
                "FailureReason": json.dumps(run.get("failureReason")) if run.get("failureReason") else None,
                "RootActivityId": run.get("rootActivityId"),
                "StartTimeUtc": start,
                "EndTimeUtc": end,
                "DurationSeconds": duration,
            })
        return rows

    async def definition_rows(item):
        async with sem:
            parts, err = await asyncio.to_thread(_get_definition, item, headers, base)
        if err is not None:
            note(item, "definition", err)
            return []
        if item["type"] == "CopyJob":
            flat = _flatten_copyjob(parts.get("copyjob-content.json", {}), names)
        elif item["type"] == "Notebook":
            flat = _flatten_notebook(parts, names, item_index)
        elif item["type"] == "Report":
            flat = _flatten_report(parts, names, item_index)
        elif item["type"] == "SemanticModel":
            flat = _flatten_semantic_model(parts, names, item_index)
        elif item["type"] == "Monitoring":
            flat = _flatten_monitoring(parts, names, item_index)
        elif item["type"] == "KQLDatabase":
            flat = _flatten_kqldatabase(parts, names, item_index)
        else:
            flat = _flatten_pipeline(parts.get("pipeline-content.json", {}), names)
        return [{
            "ItemId": item["id"],
            "ItemName": names.get(item["id"]),
            "ItemType": item.get("type"),
            "WorkspaceId": item["workspaceId"],
            "Collector": "Definition",
            **row,
        } for row in flat]

    async def topology_rows(item):
        async with sem:
            topology, err = await asyncio.to_thread(_get_eventstream_topology, item, headers, base)
        if err is not None:
            note(item, "topology", err)
            return []
        return [{
            "ItemId": item["id"],
            "ItemName": names.get(item["id"]),
            "ItemType": item.get("type"),
            "WorkspaceId": item["workspaceId"],
            "Collector": "Definition",
            **row,
        } for row in _flatten_eventstream(topology, names, item_index)]

    async def shortcut_rows(item):
        async with sem:
            shortcuts, err = await asyncio.to_thread(_list_shortcuts, item, headers, base)
        if err is not None:
            note(item, "shortcuts", err)
            return []
        return [{
            "ItemId": item["id"],
            "ItemName": names.get(item["id"]),
            "ItemType": item.get("type"),
            "WorkspaceId": item["workspaceId"],
            "Collector": "Shortcuts",
            **row,
        } for row in _flatten_shortcuts(shortcuts, names)]

    job_results = await asyncio.gather(*[job_rows(i) for i in targets])
    def_results = await asyncio.gather(*[definition_rows(i) for i in definition_targets])
    topology_results = await asyncio.gather(*[topology_rows(i) for i in topology_targets])
    shortcut_results = await asyncio.gather(*[shortcut_rows(i) for i in lakehouses])
    jobs = [r for rows in job_results for r in rows]
    definitions = [r for rows in def_results + topology_results for r in rows]
    shortcut_defs = [r for rows in shortcut_results for r in rows]

    scanner_defs = []
    try:
        scans = await _load_scans(context, today)
        scanner_defs = _flatten_scanner(scans, names, context.include_workspace_ids, context.exclude_workspace_ids)
        if not scans:
            context.logger.info(
                "ItemJobs: no catalog scan results found for the last %d days; run the Catalog module "
                "first to include scanner data sources", SCAN_LOOKBACK_DAYS
            )
    except Exception as e:
        context.logger.warning(f"ItemJobs: could not read scanner data sources: {e}")
    context.logger.info(
        "ItemJobs itemdefinitions rows: Definition=%d, Shortcuts=%d, Scanner=%d",
        len(definitions), len(shortcut_defs), len(scanner_defs),
    )
    definitions = definitions + shortcut_defs + scanner_defs

    async def activity_rows(job):
        async with sem:
            return await asyncio.to_thread(_query_activity_runs, job, headers, base, note, names=names)

    volume_jobs = [j for j in jobs if j["ItemType"] == "DataPipeline" and j["StartTimeUtc"]]
    vol_results = await asyncio.gather(*[activity_rows(j) for j in volume_jobs])
    volumes = [r for rows in vol_results for r in rows]

    path = f"jobs/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    await context.fm.save(path=path, file_name="jobs_00001.json", content=jobs)
    path = f"itemdefinitions/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    await context.fm.save(path=path, file_name="itemdefinitions_00001.json", content=definitions)
    path = f"jobactivityruns/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    await context.fm.save(path=path, file_name="jobactivityruns_00001.json", content=volumes)

    print(f"ItemJobs: {len(jobs)} job runs, {len(definitions)} source/destination rows, "
          f"{len(volumes)} activity runs from {len(targets)} items")
    for key, (count, message) in errors.items():
        print(f"  {key} x{count}: {message}")


if __name__ == "__main__":
    asyncio.run(main())
