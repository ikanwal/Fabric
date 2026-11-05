import asyncio
from datetime import datetime

from env.utility.http import async_request_with_retry


def _split_ids(value) -> set[str]:
    if not value:
        return set()
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    return {item.strip().lower() for item in str(value).split(",") if item.strip()}


async def _get_all_pages(url: str, headers: dict, context) -> list[dict]:
    results = []
    next_url = url
    while next_url:
        response = await async_request_with_retry(
            "GET",
            next_url,
            headers=headers,
            timeout=120,
            max_retries=context.http_max_retries,
            logger=context.logger,
            coordinator=getattr(context, "http_coordinator", None),
        )
        if response.status_code != 200:
            if response.status_code == 403:
                raise RuntimeError(
                    "Graph request failed with 403 Authorization_RequestDenied. "
                    "Grant admin consent for Microsoft Graph application permissions "
                    "User.Read.All, Group.Read.All, and Directory.Read.All, then rerun Graph. "
                    f"Response: {response.text}"
                )
            raise RuntimeError(f"Graph request failed: {response.status_code} {response.text}")

        payload = response.json()
        results.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")
    return results


async def main(context=None):
    """
    Graph
    """
    if context is None:
        raise RuntimeError("Context is None")

    headers = context.clients["graph"].get_headers()
    headers["Content-Type"] = "application/json"

    today = datetime.now()
    lakehouse_catalog = f"graph/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    graph_url = context.clients["graph"].get_api_root()
    audience_group_ids = _split_ids(context.PowerBIAudienceGroupIds)

    users_url = (
        f"{graph_url}/users?"
        "$select=id,displayName,userPrincipalName,mail,accountEnabled,userType,assignedLicenses"
        "&$top=999"
    )
    users = await _get_all_pages(users_url, headers, context)
    user_licenses = []
    for user in users:
        user_id = user.get("id")
        upn = user.get("userPrincipalName")
        for license_item in user.get("assignedLicenses") or []:
            user_licenses.append(
                {
                    "userId": user_id,
                    "userPrincipalName": upn,
                    "skuId": license_item.get("skuId"),
                    "disabledPlans": license_item.get("disabledPlans") or [],
                }
            )

    subscribed_skus_url = (
        f"{graph_url}/subscribedSkus?"
        "$select=id,capabilityStatus,consumedUnits,prepaidUnits,skuId,skuPartNumber"
    )
    subscribed_skus = await _get_all_pages(subscribed_skus_url, headers, context)

    await context.fm.save(path=lakehouse_catalog, file_name="users.json", content=users)
    await context.fm.save(path=lakehouse_catalog, file_name="userlicenses.json", content=user_licenses)
    await context.fm.save(path=lakehouse_catalog, file_name="subscribedskus.json", content=subscribed_skus)

    extract_groups = True if context.GraphExtractGroups is None else bool(context.GraphExtractGroups)
    if not extract_groups:
        return

    groups_url = (
        f"{graph_url}/groups?"
        "$filter=securityEnabled eq true"
        "&$select=id,displayName,mail,mailEnabled,securityEnabled,groupTypes"
        "&$top=999"
    )
    groups = await _get_all_pages(groups_url, headers, context)
    for group in groups:
        group["isPowerBIAudienceGroup"] = (
            not audience_group_ids or str(group.get("id", "")).lower() in audience_group_ids
        )

    group_members = []
    for group in groups:
        group_id = group.get("id")
        if not group_id:
            continue

        members_url = (
            f"{graph_url}/groups/{group_id}/transitiveMembers/microsoft.graph.user?"
            "$select=id,displayName,userPrincipalName,mail,accountEnabled,userType"
            "&$top=999"
        )
        members = await _get_all_pages(members_url, headers, context)
        for member in members:
            group_members.append(
                {
                    "groupId": group_id,
                    "groupDisplayName": group.get("displayName"),
                    "isPowerBIAudienceGroup": group.get("isPowerBIAudienceGroup"),
                    "memberId": member.get("id"),
                    "displayName": member.get("displayName"),
                    "userPrincipalName": member.get("userPrincipalName"),
                    "mail": member.get("mail"),
                    "accountEnabled": member.get("accountEnabled"),
                    "userType": member.get("userType"),
                }
            )

    await context.fm.save(path=lakehouse_catalog, file_name="groups.json", content=groups)
    await context.fm.save(path=lakehouse_catalog, file_name="groupmembers.json", content=group_members)


if __name__ == "__main__":
    asyncio.run(main())
