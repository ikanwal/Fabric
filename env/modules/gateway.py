import asyncio
import json
from datetime import datetime

from env.utility.http import async_request_with_retry


async def _get(context, url, headers):
    return await async_request_with_retry(
        "GET",
        url,
        headers=headers,
        timeout=120,
        max_retries=context.http_max_retries,
        logger=context.logger,
        coordinator=getattr(context, "http_coordinator", None),
    )


async def main(context=None):
    """
    Gateways
    """
    
    if not context:
        raise RuntimeError("Context is None")
    
    headers = context.clients['pbi'].get_headers()
    headers['content-type'] = 'application/json'
    headers['pragma'] = 'no-cache'

    
# GET https://api.powerbi.com/v1.0/myorg/gateways
    
    api_url = context.get_powerbi_url('gateways')
    response = await _get(context, api_url, headers)
    if not response.ok:
        context.logger.error("Gateways request failed: HTTP %s", response.status_code)
        return False
    results = response.json()

    gateways = list()
    for gateway in results['value']:
        gateways.append(gateway.get("id"))

    today = datetime.today()
    path = f'gateways/{today.strftime("%Y")}/{today.strftime("%m")}/{today.strftime("%d")}/'

    await context.fm.save(path=path, file_name='gateways.json', content=json.dumps(results['value']))

    status = list()
    users = list()
    datasources = list()
    datasource_details = list()
    failures = 0
    status_base = {   
        "datasourceId":"",
        "gatewayId":"",
        "error":
        {
            "code":"success",
            "pbi.error":{ 
                "code":"0",
                "parameters":{},
                "details":[],
                "exceptionCulprit":0
            }
        }
    }

    r = list()
    for id in gateways:
        response = await _get(
            context, context.get_powerbi_url(f'gateways/{id}/datasources'), headers
        )
        if not response.ok:
            failures += 1
            context.logger.error(
                "Gateway datasources request failed: HTTP %s", response.status_code
            )
            continue
        doc_results = response.json()
        r.append(doc_results['value'])   

    gateways = list()

    for i in range(0, len(r)):
        if isinstance(r[i], list):
            for j in range(0, len(r[i])):
                gateways.append(r[i][j])
        else:
            gateways.append(r[i])


    for i in range(0,len(gateways)):
        api_users = context.get_powerbi_url(f'gateways/{gateways[i]["gatewayId"]}/datasources/{gateways[i]["id"]}/users')
        response = await _get(context, api_users, headers)
        if not response.ok:
            failures += 1
            context.logger.error(
                "Gateway datasource users request failed: HTTP %s", response.status_code
            )
            continue
        results = response.json()

        if "value" in results:
            for l in range(0,len(results['value'])):
                results['value'][l]['datasourceId'] = gateways[i]["id"]
                results['value'][l]['gatewayId'] = gateways[i]["gatewayId"]
                users.append(results['value'][l])

    for i in range(0,len(gateways)):
        api_status = context.get_powerbi_url(f'gateways/{gateways[i]["gatewayId"]}/datasources/{gateways[i]["id"]}/status')
        
        response = await _get(context, api_status, headers)
        if response.ok:
            # response.json() is actually empty when it is a success
            
            status_base['datasourceId'] = gateways[i]["id"]
            status_base['gatewayId'] = gateways[i]["gatewayId"]
            status_base.get("error").get("pbi.error").get("details").append({"message":"success","detail":response.status_code})    
            status.append(status_base)
        else:
            failures += 1
            status.append(
                {
                    "datasourceId": gateways[i]["id"],
                    "gatewayId": gateways[i]["gatewayId"],
                    "error": {
                        "code": "requestFailed",
                        "pbi.error": {
                            "details": [
                                {
                                    "message": "cannot connect",
                                    "detail": response.status_code,
                                }
                            ]
                        },
                    },
                }
            )

    for i in range(0,len(gateways)):
        api_datasource = context.get_powerbi_url(f'gateways/{gateways[i]["gatewayId"]}/datasources/{gateways[i]["id"]}')
        response = await _get(context, api_datasource, headers)
        if not response.ok:
            failures += 1
            context.logger.error(
                "Gateway datasource details request failed: HTTP %s",
                response.status_code,
            )
            continue
        results = response.json()
        results.pop('@odata.context')
        datasource_details.append(results)

    await context.fm.save(path=path, file_name='status.json', content=json.dumps(status))
    await context.fm.save(path=path, file_name='users.json', content=json.dumps(users))
    await context.fm.save(path=path, file_name='datasources.json', content=json.dumps(gateways))
    await context.fm.save(path=path, file_name='datasource_details.json', content=json.dumps(datasource_details))
    return failures == 0


if __name__ == "__main__":
    asyncio.run(main())
