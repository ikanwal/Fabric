import asyncio
import json

import random
import time

from datetime import datetime, timedelta

from env.utility.workspace_filter import (
    WorkspaceFilterCounts,
    filter_workspace_records,
    log_workspace_filter_counts,
)


async def main(context=None):
    """
    Fabric Items
    """


    headers = context.clients['tenant'].get_headers()

    today = datetime.now()
    item_counts = {"found": 0, "included": 0, "excluded": 0}

    async def get_data(url, pageIndex=1):
        file_index = str(pageIndex).zfill(5)
        response = await context.invokeAPI(url, headers=headers)
        if "ERROR" in response:
            context.logger.error(
                f"FabricItems: admin/items page failed: HTTP {response.get('status_code')} {response.get('message')}"
            )
            return
        item_entities, counts = filter_workspace_records(
            response.get("itemEntities") or [],
            context.include_workspace_ids,
            context.exclude_workspace_ids,
            get_workspace_id=lambda item: item.get("workspaceId"),
        )
        for key in item_counts:
            item_counts[key] += getattr(counts, key)
        Path = f"items/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
        await context.fm.save(path=Path, file_name=f"fabricitems_{file_index}.json", content=item_entities)

        try:
            continuationUri = response.get("continuationUri")
            if continuationUri:
                await get_data(continuationUri, pageIndex=pageIndex + 1)
        except Exception as e:
            context.logger.error(f"Error paginating fabricitems: {e}")
            return

    url = context.get_fabric_url("v1/admin/items")
    await get_data(url)
    log_workspace_filter_counts(
        context.logger,
        "FabricItems",
        WorkspaceFilterCounts(**item_counts),
        item_counts["included"],
    )

    try:
        await get_lineage(context, headers, today)
    except Exception as e:
        context.logger.error(f"Error collecting item lineage: {e}")
        print(f"Lineage collection failed: {e}")


async def get_lineage(context, headers, today):
    """
    Upstream/downstream relations per item (Fabric Core beta API), saved as flat edge rows.
    """
    from env.utility.http import request_with_retry

    items = []
    url = context.get_fabric_url("v1/admin/items")
    while url:
        response = await context.invokeAPI(url, headers=headers)
        if "ERROR" in response:
            context.logger.error(
                f"FabricItems lineage: admin/items listing stopped: HTTP {response.get('status_code')} {response.get('message')}"
            )
            break
        items.extend(response.get("itemEntities") or [])
        url = response.get("continuationUri")

    items, workspace_counts = filter_workspace_records(
        items,
        context.include_workspace_ids,
        context.exclude_workspace_ids,
        get_workspace_id=lambda item: item.get("workspaceId"),
    )
    items = [item for item in items if item.get("id") and item.get("workspaceId")]
    log_workspace_filter_counts(context.logger, "FabricItems lineage", workspace_counts, len(items))

    sem = asyncio.Semaphore(getattr(context, "http_max_concurrency", 3))
    stats = {"ok": 0, "skipped": 0}
    stats_msg = {}

    def call(item_url):
        return request_with_retry(
            "GET",
            item_url,
            max_retries=context.http_max_retries,
            logger=context.logger,
            coordinator=getattr(context, "http_coordinator", None),
            headers=headers,
            timeout=60,
        )

    async def fetch(item, direction):
        item_id, ws_id = item.get("id"), item.get("workspaceId")
        if not item_id or not ws_id:
            return []
        endpoint = f"v1/workspaces/{ws_id}/items/{item_id}/relations/{direction}?beta=true"
        async with sem:
            r = await asyncio.to_thread(call, context.get_fabric_url(endpoint))
        if r.status_code != 200:
            stats["skipped"] += 1
            key = f"{item.get('type')}:{r.status_code}"
            stats[key] = stats.get(key, 0) + 1
            if key not in stats_msg:
                stats_msg[key] = r.text[:200]
            return []
        stats["ok"] += 1
        body = r.json()
        related = {i["id"]: i for i in body.get("items", [])}
        workspaces = {w["id"]: w.get("displayName") for w in body.get("workspaces", [])}
        rows = []
        for rel in body.get("relations", []):
            other = related.get(rel.get("dependentOnItemId")) if direction == "upstream" else related.get(rel.get("itemId"))
            other = other or {}
            rows.append({
                "QueryItemId": item_id,
                "QueryItemType": item.get("type"),
                "QueryWorkspaceId": ws_id,
                "Direction": direction,
                "ItemId": rel.get("itemId"),
                "DependentOnItemId": rel.get("dependentOnItemId"),
                "RelationType": rel.get("relationType"),
                "RelatedItemId": other.get("id"),
                "RelatedItemName": other.get("displayName"),
                "RelatedItemType": other.get("type"),
                "RelatedWorkspaceId": other.get("workspaceId"),
                "RelatedWorkspaceName": workspaces.get(other.get("workspaceId")),
            })
        return rows

    results = await asyncio.gather(*[fetch(i, d) for i in items for d in ("upstream", "downstream")])
    edges = [row for rows in results for row in rows]

    Path = f"lineage/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    await context.fm.save(path=Path, file_name="lineage_00001.json", content=edges)
    print(f"Lineage: {len(edges)} edges from {stats['ok']} calls ({stats['skipped']} unsupported/failed)")
    print({k: v for k, v in stats.items() if ":" in k})
    for k, v in stats_msg.items():
        context.logger.info(f"Lineage {k}: {v}")
        print(f"  {k}: {v}")

if __name__ == "__main__":
    asyncio.run(main())
