import asyncio

from datetime import datetime, timedelta

from env.utility.http import async_request_with_retry


async def main(context=None):
    """
    Roles
    """
    if context is None:
        raise RuntimeError("Context is None")


    headers =  context.clients['tenant'].get_headers()


    items = set()
    continuationToken = None
    while True:

        if continuationToken == None:
            response = await async_request_with_retry(
                "GET",
                context.get_fabric_url("v1/admin/workspaces"),
                headers=headers,
                timeout=120,
                max_retries=context.http_max_retries,
                logger=context.logger,
                coordinator=getattr(context, "http_coordinator", None),
            )
        else:
            response = await async_request_with_retry(
                "GET",
                context.get_fabric_url(
                    f"v1/admin/workspaces?continuationToken='{continuationToken}'"
                ),
                headers=headers,
                timeout=120,
                max_retries=context.http_max_retries,
                logger=context.logger,
                coordinator=getattr(context, "http_coordinator", None),
            )

        if response.ok:
            results = response.json()

            workspace = results.get("workspaces")

            for item in workspace:
                items.add(item["id"])

            continuationToken =  results.get("continuationToken",[])

            if continuationToken == None:
                break
        else:
            raise RuntimeError(
                f"Roles workspace request failed: HTTP {response.status_code} "
                f"{response.text[:500]}"
            )

    ceiling = len(items)
    cnt = 0

    pbi_headers = context.clients['pbi'].get_headers()
    base_url = context.get_powerbi_url('admin/groups?$expand=users&$top=5000')

    all_groups = []
    for i in range(0, ceiling, 5000):
        url = f'{base_url}&$skip={i}'
        response = await async_request_with_retry(
            "GET",
            url,
            headers=pbi_headers,
            timeout=120,
            max_retries=context.http_max_retries,
            logger=context.logger,
            coordinator=getattr(context, "http_coordinator", None),
        )
        if response.status_code == 200:
            groups = response.json().get('value', [])
            all_groups.extend(groups)
        else:
            raise RuntimeError(
                f"Roles groups request failed: HTTP {response.status_code} "
                f"{response.text[:500]}"
            )

    print(f'Total groups retrieved: {len(all_groups)}')
    
    
    pivotDate = datetime.now()
    content = all_groups
    index = str(cnt).zfill(5)

    Path = f"roles/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/{pivotDate.strftime('%d')}/"
    await context.fm.save(path=Path, file_name=f"{datetime.now().strftime('%Y%m%d')}_{index}.roles.json",content=content)


if __name__ == "__main__":
    asyncio.run(main())
