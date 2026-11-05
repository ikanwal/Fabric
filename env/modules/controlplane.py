import asyncio
import json

from datetime import datetime, timedelta, timezone

from env.modules.itemjobs import SCAN_LOOKBACK_DAYS, SCAN_MAX_FILES, SCANNER_ITEM_TYPES, _load_scans
from env.utility.http import request_with_retry
from env.utility.workspace_filter import filter_workspace_records, log_workspace_filter_counts

MAX_FAILURE_REASON_CHARS = 500
DELTA_MAX_COMMITS = 50
DELTA_MATCH_SLACK_SECONDS = 60
ACTIVITY_LOOKBACK_DAYS = 7
ACTIVITY_MAX_USERS = 10
LINEAGE_MAX_MISSES = 2
ACTIVITY_COLUMNS = ("LastActivity", "LastActivityTime", "LastActivityUser", "LastActivityClientIP",
                    "LastActivityStatus", "ActivityCount", "ActivityUsers",
                    "LastActivityUpn", "ActivityUserUpns", "LastOperation", "LastOperationTime", "LastOperationUpn",
                    "LastOperationUserId", "LastOperationClientIP", "LastOperationStatus", "OperationCount",
                    "LastUserActivity", "LastUserActivityTime", "LastUserActivityUpn", "LastUserActivityClientIP")
READ_PREFIXES = ("Read", "Get", "List", "View", "Preview", "Query", "Search")
ZERO_GUID = "00000000-0000-0000-0000-000000000000"
SERVICE_PRINCIPAL_USER_TYPE = 6
GRAPH_LOOKBACK_DAYS = 30
VOLUME_COLUMNS = ("RowsRead", "RowsCopied", "DataReadBytes", "DataWrittenBytes", "ThroughputKBps")


async def _read_json(context, path, file_name):
    content = await context.fm.read(path=path, file_name=file_name)
    if not content:
        return []
    try:
        data = json.loads(content)
    except ValueError:
        context.logger.warning(f"ControlPlane: unreadable file {path}{file_name}")
        return []
    return data if isinstance(data, list) else []


def _scan_inventory(scans, include_ids, exclude_ids):
    """Items and workspace names from the saved catalog scans, scoped by the workspace filter."""
    items, workspaces = {}, {}
    for scan in scans:
        scoped, _ = filter_workspace_records(
            scan.get("workspaces") or [], include_ids, exclude_ids, get_workspace_id=lambda w: w.get("id")
        )
        for ws in scoped:
            workspaces[ws["id"]] = ws.get("name")
            for key, value in ws.items():
                if not isinstance(value, list):
                    continue
                for it in value:
                    if not isinstance(it, dict) or not it.get("id"):
                        continue
                    items[it["id"]] = {
                        "ItemId": it["id"],
                        "ItemName": it.get("name") or it.get("displayName"),
                        "ItemType": it.get("type") or SCANNER_ITEM_TYPES.get(key, key),
                        "WorkspaceId": ws["id"],
                        "OwnerPrincipal": it.get("createdBy") or it.get("configuredBy"),
                        "ModifiedByPrincipal": it.get("modifiedBy") or it.get("lastUpdatedBy"),
                        "CreatedDateTime": it.get("createdDate") or it.get("createdDateTime"),
                    }
    return items, workspaces


def _latest_runs(jobs):
    """Latest run per item plus run/failure counts, from the ItemJobs `jobs` rows."""
    latest = {}
    for job in jobs:
        item_id = job.get("ItemId")
        if not item_id:
            continue
        entry = latest.setdefault(item_id, {"run": None, "total": 0, "failed": 0})
        entry["total"] += 1
        entry["failed"] += 1 if job.get("Status") == "Failed" else 0
        if entry["run"] is None or (job.get("StartTimeUtc") or "") > (entry["run"].get("StartTimeUtc") or ""):
            entry["run"] = job
    return latest


def _run_columns(entry):
    if not entry:
        return {"LastRunStatus": None, "LastRunStartUtc": None, "LastRunEndUtc": None, "LastRunInvokeType": None,
                "LastJobInstanceId": None, "LastRunFailureReason": None, "RunsCollected": 0, "FailedRunsCollected": 0}
    run = entry["run"]
    reason = run.get("FailureReason")
    return {
        "LastRunStatus": run.get("Status"),
        "LastRunStartUtc": run.get("StartTimeUtc"),
        "LastRunEndUtc": run.get("EndTimeUtc"),
        "LastRunInvokeType": run.get("InvokeType"),
        "LastJobInstanceId": run.get("JobInstanceId"),
        "LastRunFailureReason": reason[:MAX_FAILURE_REASON_CHARS] if reason else None,
        "RunsCollected": entry["total"],
        "FailedRunsCollected": entry["failed"],
    }


def _latest_activity_volume(activity_runs):
    """Latest Copy-style activity run per (pipeline, activity name) that reported any volume."""
    latest = {}
    for run in activity_runs:
        if not any(run.get(c) is not None for c in VOLUME_COLUMNS):
            continue
        key = (run.get("ItemId"), run.get("ActivityName"))
        if key not in latest or (run.get("ActivityStart") or "") > (latest[key].get("ActivityStart") or ""):
            latest[key] = run
    return latest


def _volume_columns(row, volumes):
    run = volumes.get((row.get("ItemId"), row.get("ActivityName"))) if row.get("ActivityName") else None
    if run:
        return {**{c: run.get(c) for c in VOLUME_COLUMNS}, "VolumeSource": "PipelineActivityRun"}
    return {**{c: None for c in VOLUME_COLUMNS}, "VolumeSource": "NotAvailableFromApi"}


def _open_onelake(context, workspace_id):
    from azure.identity import ClientSecretCredential
    from azure.storage.filedatalake import FileSystemClient

    sp = context.ServicePrincipal
    credential = ClientSecretCredential(
        tenant_id=sp.get("TenantId"), client_id=sp.get("AppId"), client_secret=sp.get("AppSecret"),
        authority=context.cloud.azure_authority_host,
    )
    return FileSystemClient(account_url=context.cloud.onelake_dfs_root, file_system_name=workspace_id,
                            credential=credential)


def _read_delta_commits(fsc, item_id, schema, table, max_commits=DELTA_MAX_COMMITS):
    """Rows and bytes added per commit, from the table's Delta log (written by the Copy job)."""
    base = f"{item_id}/Tables/{schema}/{table}" if schema else f"{item_id}/Tables/{table}"
    files = sorted((p for p in fsc.get_paths(path=f"{base}/_delta_log", recursive=False) if p.name.endswith(".json")),
                   key=lambda p: p.name)[-max_commits:]
    commits = []
    for f in files:
        rows, size, operation = 0, 0, None
        for line in fsc.get_file_client(f.name).download_file().readall().decode("utf-8").splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if "commitInfo" in entry:
                operation = entry["commitInfo"].get("operation")
            add = entry.get("add")
            if add:
                size += add.get("size") or 0
                try:
                    rows += json.loads(add.get("stats") or "{}").get("numRecords") or 0
                except ValueError:
                    pass
        stamp = f.last_modified
        if stamp is not None:
            stamp = stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
        commits.append({"Timestamp": stamp, "Operation": operation, "Rows": rows, "Bytes": size})
    return commits


def _parse_utc(value):
    try:
        return datetime.fromisoformat(str(value)[:26]).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _delta_volume(row, run_entry, delta_commits):
    """Rows/bytes written by the latest Copy job run, from Delta commits that landed inside the run window."""
    empty = {"RowsWritten": None, "BytesWritten": None}
    if not (delta_commits and run_entry and row.get("ItemType") == "CopyJob" and row.get("Role") == "Destination"):
        return empty
    commits = delta_commits.get((row.get("TargetItemId"), row.get("Schema"), row.get("Table")))
    run = run_entry["run"]
    start, end = _parse_utc(run.get("StartTimeUtc")), _parse_utc(run.get("EndTimeUtc"))
    if not commits or not start or not end or run.get("Status") != "Completed":
        return empty
    window = [c for c in commits if c["Timestamp"] and
              start - timedelta(seconds=DELTA_MATCH_SLACK_SECONDS) <= c["Timestamp"] <=
              end + timedelta(seconds=DELTA_MATCH_SLACK_SECONDS)]
    if not window:
        return empty
    return {"RowsWritten": sum(c["Rows"] for c in window), "BytesWritten": sum(c["Bytes"] for c in window)}


def _delta_columns(row, run_entry, delta_commits):
    volume = _delta_volume(row, run_entry, delta_commits)
    return {**volume, **({"VolumeSource": "DeltaLog"} if volume["RowsWritten"] is not None else {})}


def _flatten_access(details):
    """Principal name, type, id and role/permissions from a Fabric admin access-details response."""
    rows = []
    for d in details or []:
        principal = d.get("principal") or {}
        name = (principal.get("userDetails") or {}).get("userPrincipalName") or principal.get("displayName")
        row = {"Principal": name, "PrincipalType": principal.get("type"), "PrincipalId": principal.get("id")}
        if "workspaceAccessDetails" in d:
            row["Role"] = (d["workspaceAccessDetails"] or {}).get("workspaceRole")
        else:
            item_access = d.get("itemAccessDetails") or {}
            row["Permissions"] = item_access.get("permissions") or []
            row["AdditionalPermissions"] = item_access.get("additionalPermissions") or []
        rows.append(row)
    return rows


def _get_access(url, headers, call):
    details = []
    while url:
        r = call("GET", url, headers)
        if r.status_code != 200:
            return None, r
        try:
            body = r.json() or {}
        except ValueError:
            body = {}
        details.extend(body.get("accessDetails") or [])
        url = body.get("continuationUri")
    return details, None


TYPE_API_PATHS = {
    "Lakehouse": "lakehouses", "Warehouse": "warehouses", "Eventhouse": "eventhouses",
    "KQLDatabase": "kqlDatabases", "Eventstream": "eventstreams", "Notebook": "notebooks",
    "DataPipeline": "dataPipelines", "CopyJob": "copyJobs", "Reflex": "reflexes",
    "SemanticModel": "semanticModels", "Report": "reports", "SQLEndpoint": "sqlEndpoints",
    "SQLAnalyticsEndpoint": "sqlEndpoints",
}


def _principal_name(principal):
    principal = principal or {}
    return (principal.get("userDetails") or {}).get("userPrincipalName") or principal.get("displayName")


def _item_detail_row(entity, detail, workspace_names):
    """Everything known about one item: admin inventory fields plus the type-specific Fabric REST properties."""
    detail = detail or {}
    creator = entity.get("creatorPrincipal") or {}
    return {
        "ItemId": entity.get("id"),
        "ItemName": entity.get("name") or detail.get("displayName"),
        "ItemType": entity.get("type"),
        "WorkspaceId": entity.get("workspaceId"),
        "WorkspaceName": workspace_names.get(entity.get("workspaceId")),
        "State": entity.get("state"),
        "Description": detail.get("description"),
        "CapacityId": entity.get("capacityId"),
        "LastUpdatedDate": entity.get("lastUpdatedDate"),
        "CreatorPrincipal": _principal_name(creator),
        "CreatorPrincipalType": creator.get("type"),
        "CreatorPrincipalId": creator.get("id"),
        "ItemProperties": detail.get("properties"),
        "DetailAvailable": bool(detail),
    }


def _get_item_detail(item, headers, base, call):
    path = TYPE_API_PATHS.get(item.get("type"))
    url = (f"{base}/v1/workspaces/{item['workspaceId']}/{path}/{item['id']}" if path
           else f"{base}/v1/workspaces/{item['workspaceId']}/items/{item['id']}")
    r = call("GET", url, headers)
    return (r.json(), None) if r.status_code == 200 else (None, r)


def _flat_records(data):
    out = []
    for entry in data or []:
        if isinstance(entry, list):
            out.extend(e for e in entry if isinstance(e, dict))
        elif isinstance(entry, dict):
            out.append(entry)
    return out


async def _read_indexed(context, path, name_for, first_index, single=False):
    records, misses, index = [], 0, first_index
    while misses < LINEAGE_MAX_MISSES and index < first_index + (1 if single else SCAN_MAX_FILES):
        content = await context.fm.read(path=path, file_name=name_for(index))
        index += 1
        if not content:
            misses += 1
            continue
        misses = 0
        try:
            records.extend(_flat_records(json.loads(content)))
        except ValueError:
            context.logger.warning(f"ControlPlane: unreadable file {path}{name_for(index - 1)}")
    return records


def _day_path(prefix, day):
    return f"{prefix}/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/"


async def _load_lineage(context, today):
    """Lineage edges from the FabricItems (lineage/) and Catalog (lineage_edges/) outputs, newest day that has data."""
    edges = []
    for prefix in ("lineage", "lineage_edges"):
        for back in range(SCAN_LOOKBACK_DAYS):
            day = today - timedelta(days=back)
            stamp = day.strftime("%Y%m%d")
            if prefix == "lineage":
                name_for = lambda i: "lineage_00001.json"
            else:
                name_for = lambda i, stamp=stamp: f"{stamp}_{str(i).zfill(5)}.lineage_edges.json"
            found = await _read_indexed(context, _day_path(prefix, day), name_for, 1 if prefix == "lineage" else 0,
                                        single=prefix == "lineage")
            if found:
                edges.extend({**e, "Source": e.get("Source") or "FabricRelations"} for e in found)
                break
    return edges


def _in_scope(workspace_id, include_ids, exclude_ids):
    ws = (workspace_id or "").lower()
    return (not include_ids or ws in include_ids) and ws not in exclude_ids


def _directed_edges(raw_edges, item_info, include_ids=frozenset(), exclude_ids=frozenset()):
    """Merge Scanner and Fabric relations edges into unique upstream -> downstream edges."""
    merged = {}
    for e in raw_edges:
        query_id, related_id = e.get("QueryItemId"), e.get("RelatedItemId")
        if not query_id or not related_id or query_id == related_id:
            continue
        query = {"ItemId": query_id, "ItemName": e.get("QueryItemName"), "ItemType": e.get("QueryItemType"),
                 "WorkspaceId": e.get("QueryWorkspaceId")}
        related = {"ItemId": related_id, "ItemName": e.get("RelatedItemName"), "ItemType": e.get("RelatedItemType"),
                   "WorkspaceId": e.get("RelatedWorkspaceId")}
        up, down = (related, query) if e.get("Direction") == "upstream" else (query, related)
        key = (up["ItemId"].lower(), down["ItemId"].lower(), e.get("RelationType"))
        entry = merged.setdefault(key, {"From": up, "To": down, "RelationType": e.get("RelationType"), "Sources": set()})
        for side in ("From", "To"):
            for field, value in entry[side].items():
                if not value:
                    entry[side][field] = (up if side == "From" else down).get(field)
        entry["Sources"].add(e.get("Source") or "Unknown")

    out = []
    for entry in merged.values():
        for side in ("From", "To"):
            info = item_info.get(entry[side]["ItemId"]) or {}
            for field in ("ItemName", "ItemType", "WorkspaceId"):
                entry[side][field] = entry[side].get(field) or info.get(field)
        if not (_in_scope(entry["From"]["WorkspaceId"], include_ids, exclude_ids)
                or _in_scope(entry["To"]["WorkspaceId"], include_ids, exclude_ids)):
            continue
        out.append({**entry, "Sources": sorted(entry["Sources"])})
    return out


async def _load_activity(context, today):
    events = []
    for back in range(ACTIVITY_LOOKBACK_DAYS):
        day = today - timedelta(days=back)
        events.extend(await _read_indexed(
            context, _day_path("activity", day), lambda i, d=day: f"{d.strftime('%Y%m%d')}_{str(i).zfill(5)}.json", 1))
    return events


def _event_name(ev):
    return ev.get("Activity") or ev.get("Operation") or ""


def is_change_operation(ev):
    """Control plane operation = anything that is not a read/list/get style activity."""
    name = _event_name(ev)
    return bool(name) and not name.startswith(READ_PREFIXES)


def _is_guid(value):
    text = str(value or "")
    return len(text) == 36 and text.count("-") == 4


def collector_ids(events, app_id):
    """Ids (app id plus object id seen as UserKey) of the service principal running this collector."""
    app = (app_id or "").lower()
    ids = {app} if app else set()
    for ev in events:
        if app and (ev.get("UserId") or "").lower() == app and ev.get("UserKey"):
            ids.add(str(ev["UserKey"]).lower())
    return ids


def principal_directory(events, *access_maps):
    """Lower-cased object id -> UPN/display name, from activity (UserKey + UPN UserId) and the access lists."""
    directory = {}
    for ev in events:
        user, key = ev.get("UserId") or "", ev.get("UserKey")
        if "@" in user and key:
            directory[str(key).lower()] = user
    for access in access_maps:
        for entries in (access or {}).values():
            for entry in entries or []:
                if entry.get("PrincipalId") and entry.get("Principal"):
                    directory.setdefault(str(entry["PrincipalId"]).lower(), entry["Principal"])
    return directory


def resolve_actor(ev, directory, collectors):
    """(UPN or best name, raw user id, actor type) for an activity event."""
    raw = ev.get("UserId") or ev.get("UserKey")
    key = str(raw or "").lower()
    upn = raw if "@" in str(raw or "") else directory.get(key) or directory.get(str(ev.get("UserKey") or "").lower()) or raw
    if key in collectors or str(ev.get("UserKey") or "").lower() in collectors:
        kind = "Collector"
    elif ev.get("UserType") == SERVICE_PRINCIPAL_USER_TYPE or (_is_guid(raw) and "@" not in str(upn or "")):
        kind = "ServicePrincipal"
    else:
        kind = "User"
    return upn, raw, kind


def _name_index(item_info):
    names, counts = {}, {}
    for item_id, i in item_info.items():
        name = (i.get("ItemName") or "").lower()
        if name:
            names[((i.get("WorkspaceId") or "").lower(), name)] = item_id.lower()
            counts.setdefault(name, []).append(item_id.lower())
    for name, found in counts.items():
        if len(found) == 1:
            names[("", name)] = found[0]
    return names


def _event_item_id(ev, ids, names):
    """Item id from ObjectId, else from ObjectDisplayName/ObjectId used as a name (in the event's workspace, or unique)."""
    object_id = (ev.get("ObjectId") or "").lower()
    if object_id in ids:
        return object_id
    artifact_id = (ev.get("ArtifactId") or "").lower()
    if artifact_id in ids:
        return artifact_id
    workspace = (ev.get("WorkspaceId") or "").lower()
    for candidate in (ev.get("ObjectDisplayName"), ev.get("ObjectId")):
        name = (candidate or "").lower()
        found = names.get((workspace, name)) or names.get(("", name))
        if found:
            return found
    return object_id or None


def _activity_by_item(events, directory=None, collectors=frozenset(), item_info=None):
    """Latest event, counts and distinct users per item, plus the latest control plane operation and real-user activity."""
    directory = directory or {}
    item_info = item_info or {}
    ids = {k.lower() for k in item_info}
    names = _name_index(item_info)
    grouped = {}
    for ev in events:
        item_id = _event_item_id(ev, ids, names)
        if item_id:
            grouped.setdefault(item_id, []).append(ev)
    out = {}
    for item_id, evs in grouped.items():
        last = max(evs, key=lambda e: e.get("CreationTime") or "")
        upn = lambda e: resolve_actor(e, directory, collectors)[0]
        entry = {
            "LastActivity": _event_name(last),
            "LastActivityTime": last.get("CreationTime"),
            "LastActivityUser": last.get("UserId"),
            "LastActivityClientIP": last.get("ClientIP"),
            "LastActivityStatus": last.get("ResultStatus"),
            "ActivityCount": len(evs),
            "ActivityUsers": sorted({e.get("UserId") for e in evs if e.get("UserId")})[:ACTIVITY_MAX_USERS],
            "LastActivityUpn": upn(last),
            "ActivityUserUpns": sorted({str(upn(e)) for e in evs if upn(e)})[:ACTIVITY_MAX_USERS],
        }
        changes = [e for e in evs if is_change_operation(e)]
        if changes:
            op = max(changes, key=lambda e: e.get("CreationTime") or "")
            entry.update({
                "LastOperation": _event_name(op), "LastOperationTime": op.get("CreationTime"),
                "LastOperationUpn": upn(op), "LastOperationUserId": op.get("UserId") or op.get("UserKey"),
                "LastOperationClientIP": op.get("ClientIP"), "LastOperationStatus": op.get("ResultStatus"),
            })
        entry["OperationCount"] = len(changes)
        users = [e for e in evs if resolve_actor(e, directory, collectors)[2] == "User"]
        if users:
            u = max(users, key=lambda e: e.get("CreationTime") or "")
            entry.update({
                "LastUserActivity": _event_name(u), "LastUserActivityTime": u.get("CreationTime"),
                "LastUserActivityUpn": upn(u), "LastUserActivityClientIP": u.get("ClientIP"),
            })
        out[item_id] = entry
    return out


def _join_names(items):
    seen = []
    for item in items or []:
        name = item.get("ItemName") or item.get("ItemId")
        label = f"{name} ({item['ItemType']})" if item.get("ItemType") else str(name)
        if label not in seen:
            seen.append(label)
    return "; ".join(seen) or None


def _endpoint_text(rows, roles):
    seen = []
    for r in rows:
        if r.get("Role") in roles:
            name = r.get("TargetItemName") or r.get("Table") or r.get("EndpointType")
            label = f"{name} ({r['EndpointType']})" if r.get("EndpointType") and name != r.get("EndpointType") else str(name)
            if label not in seen:
                seen.append(label)
    return "; ".join(seen) or None


def build_item_context(rows):
    """Per-item owner, state, de-duplicated lineage, run status, endpoints and volume from the controlplane rows."""
    grouped = {}
    for r in rows:
        grouped.setdefault((r.get("ItemId") or "").lower(), []).append(r)
    out = {}
    for item_id, group in grouped.items():
        first = group[0]
        volume = {c: sum(r.get(c) or 0 for r in group) if any(r.get(c) is not None for r in group) else None
                  for c in ("RowsRead", "RowsCopied", "DataReadBytes", "DataWrittenBytes")}
        out[item_id] = {
            "ItemOwner": first.get("OwnerPrincipal"), "ItemState": first.get("State"),
            "UpstreamItemNames": _join_names(first.get("UpstreamItems")),
            "DownstreamItemNames": _join_names(first.get("DownstreamItems")),
            "UpstreamCount": first.get("UpstreamCount"), "DownstreamCount": first.get("DownstreamCount"),
            "RunsCollected": first.get("RunsCollected"), "FailedRunsCollected": first.get("FailedRunsCollected"),
            "LastRunStatus": first.get("LastRunStatus"), "LastRunStartUtc": first.get("LastRunStartUtc"),
            "LastRunEndUtc": first.get("LastRunEndUtc"),
            "Sources": _endpoint_text(group, ("Source", "DataSource", "ShortcutSource")),
            "Destinations": _endpoint_text(group, ("Destination",)),
            **volume,
        }
    return out


async def _load_graph_names(context, today):
    """Graph users (newest day that has data): lower-cased UPN or object id -> display name."""
    for back in range(GRAPH_LOOKBACK_DAYS):
        users = await _read_json(context, _day_path("graph", today - timedelta(days=back)), "users.json")
        if users:
            names = {}
            for u in users:
                if u.get("displayName"):
                    for key in (u.get("userPrincipalName"), u.get("mail"), u.get("id")):
                        if key:
                            names[str(key).lower()] = u["displayName"]
            return names
    return {}


def build_consolidated(events, item_info, item_context, directory, collectors, graph_names,
                       include_ids, exclude_ids, collected_at):
    """One row per Activity event (reads and changes): who, what, when, from where, on which item and how it ties together."""
    ids = {k.lower() for k in item_info}
    names = _name_index(item_info)
    by_lower = {k.lower(): v for k, v in item_info.items()}
    rows, seen_ids = [], set()
    for ev in events:
        if ev.get("Id") and ev["Id"] in seen_ids:
            continue
        seen_ids.add(ev.get("Id"))
        item_id = _event_item_id(ev, ids, names)
        info = by_lower.get(item_id) if item_id else None
        workspace = ev.get("WorkspaceId")
        if info and (not workspace or workspace == ZERO_GUID):
            workspace = info.get("WorkspaceId")
        if workspace and workspace != ZERO_GUID:
            if not _in_scope(workspace, include_ids, exclude_ids):
                continue
        elif include_ids and not info:
            continue
        upn, raw, kind = resolve_actor(ev, directory, collectors)
        ctx = (item_context.get(item_id) or {}) if info else {}
        display = graph_names.get(str(upn or "").lower()) or graph_names.get(str(raw or "").lower()) or upn
        time = ev.get("CreationTime")
        rows.append({
            "EventId": ev.get("Id"), "EventTime": time, "EventDate": (time or "")[:10] or None, "Source": "Activity",
            "Operation": ev.get("Operation"), "Activity": ev.get("Activity"),
            "OperationType": "Change" if is_change_operation(ev) else "Read",
            "ResultStatus": ev.get("ResultStatus"), "IsSuccess": ev.get("IsSuccess"),
            "UserId": raw, "UserUpn": upn, "UserDisplayName": display, "UserType": ev.get("UserType"), "ActorType": kind,
            "ClientIP": ev.get("ClientIP"), "UserAgent": ev.get("UserAgent"),
            "WorkspaceId": workspace if workspace != ZERO_GUID else None, "WorkspaceName": ev.get("WorkSpaceName"),
            "CapacityId": ev.get("CapacityId"), "CapacityName": ev.get("CapacityName"),
            "ArtifactId": ev.get("ArtifactId"), "ItemId": item_id if info else None,
            "ItemName": (info or {}).get("ItemName") or ev.get("ItemName") or ev.get("ObjectDisplayName"),
            "ItemType": (info or {}).get("ItemType") or ev.get("ObjectType"),
            "RequestId": ev.get("RequestId"), **ctx, "CollectedAtUtc": collected_at,
        })
    rows.sort(key=lambda r: r["EventTime"] or "", reverse=True)
    return rows


def build_operations(events, item_info, edges, directory, collectors, include_ids, exclude_ids, collected_at):
    """One row per control plane operation (non-read activity), tied to the item, its owner and its lineage."""
    ids = {k.lower() for k in item_info}
    names = _name_index(item_info)
    by_lower = {k.lower(): v for k, v in item_info.items()}
    rows = []
    for ev in events:
        if not is_change_operation(ev):
            continue
        item_id = _event_item_id(ev, ids, names)
        info = by_lower.get(item_id) if item_id else None
        workspace = ev.get("WorkspaceId")
        if info and (not workspace or workspace == ZERO_GUID):
            workspace = info.get("WorkspaceId")
        if workspace and workspace != ZERO_GUID:
            if not _in_scope(workspace, include_ids, exclude_ids):
                continue
        elif include_ids and not info:
            continue
        upn, raw, kind = resolve_actor(ev, directory, collectors)
        lineage = _lineage_columns(item_id, edges) if info else {}
        rows.append({
            "OperationTime": ev.get("CreationTime"), "Operation": _event_name(ev), "ResultStatus": ev.get("ResultStatus"),
            "ActorUpn": upn, "ActorId": raw, "ActorType": kind, "ClientIP": ev.get("ClientIP"),
            "ItemId": item_id if info else None,
            "ItemName": (info or {}).get("ItemName") or ev.get("ObjectDisplayName"),
            "ItemType": (info or {}).get("ItemType") or ev.get("ObjectType"),
            "WorkspaceId": workspace, "WorkspaceName": ev.get("WorkSpaceName"),
            "ItemState": (info or {}).get("State"), "ItemOwner": (info or {}).get("OwnerPrincipal"),
            "UpstreamItemNames": "; ".join(i["ItemName"] or i["ItemId"] for i in lineage.get("UpstreamItems", [])) or None,
            "DownstreamItemNames": "; ".join(i["ItemName"] or i["ItemId"] for i in lineage.get("DownstreamItems", [])) or None,
            "UpstreamCount": lineage.get("UpstreamCount"), "DownstreamCount": lineage.get("DownstreamCount"),
            "RequestId": ev.get("RequestId"), "UserAgent": ev.get("UserAgent"), "CollectedAtUtc": collected_at,
        })
    rows.sort(key=lambda r: r["OperationTime"] or "", reverse=True)
    return rows


def _lineage_columns(item_id, edges):
    key = (item_id or "").lower()
    up = [{"ItemId": e["From"]["ItemId"], "ItemName": e["From"]["ItemName"], "ItemType": e["From"]["ItemType"],
           "WorkspaceId": e["From"]["WorkspaceId"], "RelationType": e["RelationType"], "Sources": e["Sources"]}
          for e in edges if e["To"]["ItemId"].lower() == key]
    down = [{"ItemId": e["To"]["ItemId"], "ItemName": e["To"]["ItemName"], "ItemType": e["To"]["ItemType"],
             "WorkspaceId": e["To"]["WorkspaceId"], "RelationType": e["RelationType"], "Sources": e["Sources"]}
            for e in edges if e["From"]["ItemId"].lower() == key]
    return {"UpstreamItems": up, "DownstreamItems": down, "UpstreamCount": len(up), "DownstreamCount": len(down)}


def _activity_columns(item_id, activity):
    found = activity.get((item_id or "").lower()) or {}
    return {k: found.get(k) for k in ACTIVITY_COLUMNS}


def build_lineage_graph(edges, item_info, activity, collected_at):
    """One row per upstream -> downstream edge with each end's owner, state and latest activity."""
    rows = []
    for e in edges:
        row = {"RelationType": e["RelationType"], "Sources": e["Sources"]}
        for side, label in (("From", "Upstream"), ("To", "Downstream")):
            node = e[side]
            info = item_info.get(node["ItemId"]) or {}
            seen = activity.get(node["ItemId"].lower()) or {}
            row.update({
                f"{label}ItemId": node["ItemId"], f"{label}ItemName": node["ItemName"],
                f"{label}ItemType": node["ItemType"], f"{label}WorkspaceId": node["WorkspaceId"],
                f"{label}State": info.get("State"), f"{label}OwnerPrincipal": info.get("OwnerPrincipal"),
                f"{label}LastActivity": seen.get("LastActivity"),
                f"{label}LastActivityTime": seen.get("LastActivityTime"),
                f"{label}LastActivityUser": seen.get("LastActivityUser"),
                f"{label}LastActivityUpn": seen.get("LastActivityUpn"),
                f"{label}LastOperation": seen.get("LastOperation"),
                f"{label}LastOperationUpn": seen.get("LastOperationUpn"),
            })
        row["CollectedAtUtc"] = collected_at
        rows.append(row)
    return rows


def build_rows(definitions, inventory, runs, volumes, item_access, workspace_access, workspace_names, collected_at,
               details=None, delta_commits=None, edges=None, activity=None):
    """One row per endpoint (itemdefinitions row); items without endpoints get a single Role=Item row."""
    details = details or {}
    edges = edges or []
    activity = activity or {}
    by_item = {}
    for row in definitions:
        by_item.setdefault(row.get("ItemId"), []).append(row)
    for item_id, info in inventory.items():
        if item_id not in by_item:
            by_item[item_id] = [{**info, "Collector": "Scanner", "Role": "Item"}]
    for item_id, d in details.items():
        if item_id not in by_item:
            by_item[item_id] = [{"ItemId": item_id, "ItemName": d["ItemName"], "ItemType": d["ItemType"],
                                 "WorkspaceId": d["WorkspaceId"], "Collector": "Inventory", "Role": "Item"}]

    out = []
    for item_id, rows in by_item.items():
        info = inventory.get(item_id, {})
        detail = details.get(item_id, {})
        for row in rows:
            workspace_id = row.get("WorkspaceId") or info.get("WorkspaceId")
            out.append({
                **row,
                "WorkspaceId": workspace_id,
                "WorkspaceName": workspace_names.get(workspace_id),
                "ItemName": row.get("ItemName") or info.get("ItemName"),
                "ItemType": row.get("ItemType") or info.get("ItemType"),
                "OwnerPrincipal": info.get("OwnerPrincipal") or detail.get("CreatorPrincipal"),
                "ModifiedByPrincipal": info.get("ModifiedByPrincipal"),
                "CreatedDateTime": info.get("CreatedDateTime"),
                "State": detail.get("State"),
                "Description": detail.get("Description"),
                "CapacityId": detail.get("CapacityId"),
                "LastUpdatedDate": detail.get("LastUpdatedDate"),
                "CreatorPrincipal": detail.get("CreatorPrincipal"),
                "ItemProperties": detail.get("ItemProperties"),
                **_run_columns(runs.get(item_id)),
                **_volume_columns(row, volumes),
                **_delta_columns(row, runs.get(item_id), delta_commits),
                **_lineage_columns(item_id, edges),
                **_activity_columns(item_id, activity),
                "ItemAccess": item_access.get(item_id),
                "WorkspaceAccess": workspace_access.get(workspace_id),
                "CollectedAtUtc": collected_at,
            })
    return out


async def main(context=None):
    """
    Control Plane
    """
    if context is None:
        raise RuntimeError("Context is None")

    today = datetime.now()
    day_path = f"{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    definitions = await _read_json(context, f"itemdefinitions/{day_path}", "itemdefinitions_00001.json")
    jobs = await _read_json(context, f"jobs/{day_path}", "jobs_00001.json")
    activity_runs = await _read_json(context, f"jobactivityruns/{day_path}", "jobactivityruns_00001.json")
    scans = await _load_scans(context, today)
    if not definitions:
        context.logger.warning("ControlPlane: no itemdefinitions for today; run Catalog and ItemJobs first")

    inventory, workspace_names = _scan_inventory(scans, context.include_workspace_ids, context.exclude_workspace_ids)
    if context.include_workspace_ids or context.exclude_workspace_ids:
        definitions, _ = filter_workspace_records(
            definitions, context.include_workspace_ids, context.exclude_workspace_ids,
            get_workspace_id=lambda r: r.get("WorkspaceId"),
        )
    for row in definitions:
        if row.get("WorkspaceId"):
            workspace_names.setdefault(row["WorkspaceId"], None)

    headers = context.clients['tenant'].get_headers()
    base = context.get_fabric_url("").rstrip("/")
    max_retries = context.http_max_retries
    sem = asyncio.Semaphore(context.itemjobs_concurrency)
    errors = {}

    entities = []
    list_urls = ([context.get_fabric_url(f"v1/admin/items?workspaceId={w}") for w in sorted(context.include_workspace_ids)]
                 or [context.get_fabric_url("v1/admin/items")])
    for url in list_urls:
        while url:
            response = await context.invokeAPI(url, headers=headers)
            if "ERROR" in response:
                context.logger.error(
                    f"ControlPlane: admin/items listing stopped: HTTP {response.get('status_code')} {response.get('message')}"
                )
                break
            entities.extend(response.get("itemEntities") or [])
            url = response.get("continuationUri")
    entities, item_counts = filter_workspace_records(
        entities, context.include_workspace_ids, context.exclude_workspace_ids,
        get_workspace_id=lambda e: e.get("workspaceId"),
    )
    entities = [e for e in entities if e.get("id") and e.get("workspaceId")]
    for e in entities:
        workspace_names.setdefault(e["workspaceId"], None)
    log_workspace_filter_counts(context.logger, "ControlPlane", item_counts, len(entities))

    def call(method, url, hdrs):
        return request_with_retry(
            method,
            url,
            max_retries=max_retries,
            logger=context.logger,
            coordinator=getattr(context, "http_coordinator", None),
            headers=hdrs,
            timeout=120,
        )

    async def fetch_detail(entity):
        async with sem:
            detail, err = await asyncio.to_thread(_get_item_detail, entity, headers, base, call)
        if err is not None:
            label = f"detail:{entity.get('type')}:{err.status_code}"
            errors.setdefault(label, [0, err.text[:200]])[0] += 1
        return _item_detail_row(entity, detail, workspace_names)

    detail_rows = await asyncio.gather(*[fetch_detail(e) for e in entities])
    details = {r["ItemId"]: r for r in detail_rows}
    await context.fm.save(path=f"itemdetails/{day_path}", file_name="itemdetails_00001.json", content=detail_rows)

    async def fetch(url, key):
        async with sem:
            details, err = await asyncio.to_thread(_get_access, url, headers, call)
        if err is not None:
            label = f"access:{key}:{err.status_code}"
            errors.setdefault(label, [0, err.text[:200]])[0] += 1
            return None
        return _flatten_access(details)

    workspace_ids = sorted(workspace_names)
    item_keys = sorted({(r.get("WorkspaceId"), r.get("ItemId")) for r in definitions if r.get("ItemId")} |
                       {(i["WorkspaceId"], i["ItemId"]) for i in inventory.values()} |
                       {(d["WorkspaceId"], d["ItemId"]) for d in details.values()})
    ws_results = await asyncio.gather(*[fetch(f"{base}/v1/admin/workspaces/{w}/users", "workspace")
                                        for w in workspace_ids])
    item_results = await asyncio.gather(*[fetch(f"{base}/v1/admin/workspaces/{w}/items/{i}/users", "item")
                                          for w, i in item_keys])
    workspace_access = dict(zip(workspace_ids, ws_results))
    item_access = {item_id: result for (_, item_id), result in zip(item_keys, item_results)}

    runs = _latest_runs(jobs)
    delta_targets = {
        (r["TargetItemId"], r.get("Schema"), r.get("Table")): r["TargetWorkspaceId"]
        for r in definitions
        if r.get("ItemType") == "CopyJob" and r.get("Role") == "Destination"
        and r.get("EndpointType") == "LakehouseTable" and r.get("Table")
        and r.get("TargetItemId") and r.get("TargetWorkspaceId")
        and (runs.get(r.get("ItemId")) or {}).get("run", {}).get("Status") == "Completed"
    }

    def read_delta(key, workspace_id):
        item_id, schema, table = key
        return _read_delta_commits(_open_onelake(context, workspace_id), item_id, schema, table)

    async def fetch_delta(key, workspace_id):
        async with sem:
            try:
                return key, await asyncio.to_thread(read_delta, key, workspace_id)
            except Exception as exc:
                errors.setdefault(f"deltalog:{type(exc).__name__}", [0, str(exc)[:200]])[0] += 1
                return key, None

    delta_commits = dict(await asyncio.gather(*[fetch_delta(k, w) for k, w in delta_targets.items()]))

    item_info = {}
    for item_id, info in inventory.items():
        item_info[item_id] = {"ItemName": info.get("ItemName"), "ItemType": info.get("ItemType"),
                              "WorkspaceId": info.get("WorkspaceId"), "OwnerPrincipal": info.get("OwnerPrincipal")}
    for item_id, d in details.items():
        item_info[item_id] = {
            "ItemName": d.get("ItemName"), "ItemType": d.get("ItemType"), "WorkspaceId": d.get("WorkspaceId"),
            "State": d.get("State"), "OwnerPrincipal": (item_info.get(item_id) or {}).get("OwnerPrincipal") or d.get("CreatorPrincipal"),
        }
    for r in definitions:
        if r.get("ItemId") and r["ItemId"] not in item_info:
            item_info[r["ItemId"]] = {"ItemName": r.get("ItemName"), "ItemType": r.get("ItemType"),
                                      "WorkspaceId": r.get("WorkspaceId")}
    edges = _directed_edges(await _load_lineage(context, today), item_info,
                            context.include_workspace_ids, context.exclude_workspace_ids)
    events = await _load_activity(context, today)
    app_id = (getattr(context, "ServicePrincipal", None) or {}).get("AppId")
    collectors = collector_ids(events, app_id)
    directory = principal_directory(events, item_access, workspace_access)
    activity = _activity_by_item(events, directory, collectors, item_info)
    collected_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    operations = build_operations(events, item_info, edges, directory, collectors,
                                  context.include_workspace_ids, context.exclude_workspace_ids, collected_at)
    if not edges:
        context.logger.warning("ControlPlane: no lineage edges found; run Catalog and FabricItems first")
    if not activity:
        context.logger.warning("ControlPlane: no activity events found in the lookback window; run Activity first")

    rows = build_rows(
        definitions, inventory, runs, _latest_activity_volume(activity_runs),
        item_access, workspace_access, workspace_names, datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        details, delta_commits, edges, activity,
    )
    graph_rows = build_lineage_graph(edges, item_info, activity, datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    context.logger.info(
        "ControlPlane rows: %d from %d items in %d workspaces (definitions=%d, jobs=%d, activity runs=%d, "
        "lineage edges=%d, items with activity=%d, control plane operations=%d)",
        len(rows), len({r["ItemId"] for r in rows}), len(workspace_ids), len(definitions), len(jobs),
        len(activity_runs), len(edges), len(activity), len(operations),
    )

    await context.fm.save(path=f"lineagegraph/{day_path}", file_name="lineagegraph_00001.json", content=graph_rows)
    await context.fm.save(path=f"controlplane_operations/{day_path}", file_name="controlplane_operations_00001.json",
                          content=operations)
    graph_names = await _load_graph_names(context, today)
    consolidated = build_consolidated(events, item_info, build_item_context(rows), directory, collectors,
                                      graph_names, context.include_workspace_ids, context.exclude_workspace_ids,
                                      collected_at)
    context.logger.info("ControlPlane fabric_audit events: %d (graph display names: %d)",
                        len(consolidated), len(graph_names))
    await context.fm.save(path=f"fabric_audit/{day_path}", file_name="fabric_audit_00001.json", content=consolidated)
    await context.fm.save(path=f"controlplane/{day_path}", file_name="controlplane_00001.json", content=rows)
    print(f"ControlPlane: {len(rows)} rows from {len({r['ItemId'] for r in rows})} items")
    for key, (count, message) in errors.items():
        print(f"  {key} x{count}: {message}")


if __name__ == "__main__":
    asyncio.run(main())
