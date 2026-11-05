import sys
import socket
import platform
import aiohttp
import logging
from typing import Dict, Any, Coroutine
from datetime import datetime, timedelta
from urllib.parse import urljoin

from env.cloud import get_cloud_profile
from env.utility.file_management import File_Management
from env.utility.workspace_filter import parse_workspace_ids
from env.utility.http import (
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MAX_RETRIES,
    RequestCoordinator,
    aio_request,
    url_path,
)

# On Linux/WSL, aiohttp tries IPv6 first which causes SSL handshake timeouts.
# Force IPv4 on Linux. On Windows, aiohttp resolves correctly without this constraint.
_CONNECTOR_KWARGS = dict(family=socket.AF_INET) if platform.system() != "Windows" else {}
_TIMEOUT = aiohttp.ClientTimeout(total=300, connect=60)

class Context:
    def __init__(self):
        self.CatalogGetInfoParameters = "lineage=true&datasourceDetails=true&getArtifactUsers=true&datasetSchema=true&datasetExpressions=true"
        self.CatalogGetModifiedParameters = "excludePersonalWorkspaces=false&excludeInActiveWorkspaces=true"
        self.ServicePrincipal = {}
        self.LakehouseName = None
        self.PathInLakehouse = None
        self.Activity_cron = None
        self.Apps_cron = None
        self.Capacity_cron = None
        self.CapacityMetrics_cron = None
        self.Catalog_cron = None
        self.Domains_cron = None
        self.FabricItems_cron = None
        self.ItemJobs_cron = None
        self.Gateway_cron = None
        self.Graph_cron = None
        self.Refreshables_cron = None
        self.RefreshHistory_cron = None
        self.Roles_cron = None
        self.Tenant_cron = None
        self.Workspaces_cron = None
        self.ApplicationModules = None
        self.storage_url = None
        self.StorageAccountConnStr = None
        self.StorageAccountContainerName = None
        self.StorageAccountContainerRootPath = None
        self.OutputPath = None
        self.GraphExtractGroups = None
        self.PowerBIAudienceGroupIds = None
        self.WorkspaceName = None
        self.clients = {}
        self.fm = File_Management()
        self.current_state = None
        self.logging_level = logging.ERROR
        self.logger = logging.getLogger(__name__)
        self.all_workspaces = False
        self.include_workspace_ids = frozenset()
        self.exclude_workspace_ids = frozenset()
        self.http_max_retries = DEFAULT_MAX_RETRIES
        self.http_max_concurrency = DEFAULT_MAX_CONCURRENCY
        self.http_coordinator = RequestCoordinator(self.http_max_concurrency)
        self.scan_status_poll_seconds = 20
        self.itemjobs_concurrency = 3
        self.exclude_personal_workspaces = True
        self.exclude_inactive_workspaces = True
        self.on_fabric = True
        self.capacity_metrics_dataset_id = None
        self.impersonatedUserName = None
        self.cloud = get_cloud_profile(None)

    def __set_log_level(self, level):
        """
        Set the logging level
        """
        # Set the logging level for the Azure SDK
        azure_logger = "azure.core.pipeline.policies.http_logging_policy"
        logging.getLogger(azure_logger).setLevel(logging.WARNING)

        # Basic logging levels
        levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        return levels[level]

    def set_log_config(self, log_level, log_file):
        """
        Set the logging configuration
        """
        stdoutHandler = logging.StreamHandler(sys.stdout)
        errHandler = logging.FileHandler(log_file)
        
        stdoutHandler.setLevel(logging.DEBUG)
        errHandler.setLevel(self.__set_log_level(log_level))

        fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stdoutHandler.setFormatter(fmt)
        errHandler.setFormatter(fmt)

        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', force=True, handlers=[stdoutHandler, errHandler]
        )

    def set_on_fabric(self, on_fabric):
        self.on_fabric = on_fabric
        
    def set_all_workspaces(self, all_workspaces):
        self.all_workspaces = all_workspaces

    def set_include_workspace_ids(self, workspace_ids):
        self.include_workspace_ids = parse_workspace_ids(workspace_ids)

    def set_exclude_workspace_ids(self, workspace_ids):
        self.exclude_workspace_ids = parse_workspace_ids(workspace_ids)

    def set_http_max_retries(self, value):
        value = int(value)
        if value < 0:
            raise ValueError("HTTP_MAX_RETRIES must be >= 0")
        self.http_max_retries = value

    def set_http_max_concurrency(self, value):
        value = int(value)
        if value < 1:
            raise ValueError("HTTP_MAX_CONCURRENCY must be >= 1")
        self.http_max_concurrency = value
        self.http_coordinator = RequestCoordinator(value)

    def set_scan_status_poll_seconds(self, value):
        value = int(value)
        if value < 1:
            raise ValueError("SCAN_STATUS_POLL_SECONDS must be >= 1")
        self.scan_status_poll_seconds = value

    def set_itemjobs_concurrency(self, value):
        value = int(value)
        if value < 1:
            raise ValueError("ITEMJOBS_CONCURRENCY must be >= 1")
        self.itemjobs_concurrency = value

    def set_current_state(self, current_state):
        self.current_state = current_state        

    def set_FileManagement(self, fm):
        self.fm = fm

    def set_WorkspaceName(self, WorkspaceName):
        self.WorkspaceName = WorkspaceName

    def set_StorageAccountConnStr(self, StorageAccountConnStr):
        self.StorageAccountConnStr = StorageAccountConnStr
    
    def set_storage_url(self, storage_url):
        self.storage_url = storage_url

    def set_StorageAccountContainerName(self, StorageAccountContainerName):
        self.StorageAccountContainerName = StorageAccountContainerName

    def set_StorageAccountContainerRootPath(self, StorageAccountContainerRootPath):
        self.StorageAccountContainerRootPath = StorageAccountContainerRootPath

    def get_StorageAccountContainerName(self):
        return self.StorageAccountContainerName

    def set_OutputPath(self, OutputPath):
        self.OutputPath = OutputPath

    def set_CatalogGetInfoParameters(self, CatalogGetInfoParameters):
        self.CatalogGetInfoParameters = CatalogGetInfoParameters

    def set_CatalogGetModifiedParameters(self, CatalogGetModifiedParameters):
        self.CatalogGetModifiedParameters = CatalogGetModifiedParameters

    def set_ServicePrincipal(self, AppId, AppSecret, TenantId, Environment):
        self.cloud = get_cloud_profile(Environment)
        self.ServicePrincipal = {
            "AppId": AppId,
            "AppSecret": AppSecret,
            "TenantId": TenantId,
            "Environment": self.cloud.name
        }

    def set_ImpersonatedUserName(self, impersonatedUserName):
        self.impersonatedUserName = impersonatedUserName

    def set_capacity_metrics_dataset_id(self, capacity_metrics_dataset_id):
        self.capacity_metrics_dataset_id = capacity_metrics_dataset_id

    def set_Domains_cron(self, Domains_cron):
        self.Domains_cron = Domains_cron

    def set_LakehouseName(self, LakehouseName):
        self.LakehouseName = LakehouseName
    
    def set_PathInLakehouse(self, PathInLakehouse):
        self.PathInLakehouse = PathInLakehouse

    def set_Activity_cron(self, Activity_cron):
        self.Activity_cron = Activity_cron
    
    def set_Apps_cron(self, Apps_cron):
        self.Apps_cron = Apps_cron

    def set_Catalog_cron(self, Catalog_cron):
        self.Catalog_cron = Catalog_cron

    def set_Graph_cron(self, Graph_cron):
        self.Graph_cron = Graph_cron

    def set_Tenant_cron(self, Tenant_cron):
        self.Tenant_cron = Tenant_cron

    def set_RefreshHistory_cron(self, RefreshHistory_cron):
        self.RefreshHistory_cron = RefreshHistory_cron
    
    def set_Refreshables_cron(self, Refreshables_cron):
        self.Refreshables_cron = Refreshables_cron

    def set_Gateway_cron(self, Gateway_cron):
        self.Gateway_cron = Gateway_cron

    def set_Capacity_cron(self, Capacity_cron):
        self.Capacity_cron = Capacity_cron

    def set_CapacityMetrics_cron(self, CapacityMetrics_cron):
        self.CapacityMetrics_cron = CapacityMetrics_cron

    def set_Roles_cron(self, Roles_cron):
        self.Roles_cron = Roles_cron

    def set_FabricItems_cron(self, FabricItems_cron):
        self.FabricItems_cron = FabricItems_cron

    def set_ItemJobs_cron(self, ItemJobs_cron):
        self.ItemJobs_cron = ItemJobs_cron

    def set_Workspaces_cron(self, Workspaces_cron):
        self.Workspaces_cron = Workspaces_cron

    def set_ApplicationModules(self, ApplicationModules):
        self.ApplicationModules = ApplicationModules

    def set_GraphExtractGroups(self, GraphExtractGroups):
        self.GraphExtractGroups = GraphExtractGroups

    def set_PowerBIAudienceGroupIds(self, PowerBIAudienceGroupIds):
        self.PowerBIAudienceGroupIds = PowerBIAudienceGroupIds

    def get_ServicePrincipal(self):
        return self.ServicePrincipal
    
    def get_LakehouseName(self):
        return self.LakehouseName
    
    def get_PathInLakehouse(self):
        return self.PathInLakehouse
    
    def set_exclude_personal_workspaces(self, exclude_personal_workspaces):
        self.exclude_personal_workspaces = exclude_personal_workspaces

    def set_exclude_inactive_workspaces(self, exclude_inactive_workspaces):
        self.exclude_inactive_workspaces = exclude_inactive_workspaces
    
    def get_cron(self, cron_name):
        if cron_name == "Activity":
            return self.Activity_cron
        elif cron_name == "Apps":
            return self.Apps_cron
        elif cron_name == "Catalog":
            return self.Catalog_cron
        elif cron_name == "Graph":
            return self.Graph_cron
        elif cron_name == "Tenant":
            return self.Tenant_cron
        elif cron_name == "RefreshHistory":
            return self.RefreshHistory_cron
        elif cron_name == "Refreshables":
            return self.Refreshables_cron
        elif cron_name == "Gateway":
            return self.Gateway_cron
        elif cron_name == "Capacity":
            return self.Capacity_cron
        elif cron_name == "CapacityMetrics":
            return self.CapacityMetrics_cron
        elif cron_name == "Roles":
            return self.Roles_cron
        elif cron_name == "FabricItems":
            return self.FabricItems_cron
        elif cron_name in ("ItemJobs", "ControlPlane"):
            return self.ItemJobs_cron
        elif cron_name == "Workspaces":
            return self.Workspaces_cron
        else:
            return None
    
    def get_ApplicationModules(self):
        return self.ApplicationModules

    def get_powerbi_url(self, rest_api: str) -> str:
        if rest_api.lower().startswith(("http://", "https://")):
            return rest_api
        return urljoin(self.cloud.powerbi_api_root, rest_api)

    def get_fabric_url(self, rest_api: str) -> str:
        if not self.cloud.supports_fabric_api:
            raise RuntimeError(
                f"Fabric REST APIs are not enabled for CLOUD_ENVIRONMENT={self.cloud.name}. "
                "Use Commercial until Fabric Gov is available."
            )
        if rest_api.lower().startswith(("http://", "https://")):
            return rest_api
        return urljoin(self.cloud.fabric_api_root, rest_api)

    def convert_dt_str(self, date_time):
        """
        Convert a datetime object to a string
        date_time: datetime object
        """
        format = "%Y-%m-%dT%H:%M:%S.%fZ"

        if isinstance(date_time, datetime):
            date_time = date_time.strftime(format)

        try:
            datetime_str = datetime.strptime(date_time, format)
            return datetime_str
        except ValueError as ve:
            self.logger.error(f"An exception occurred while converting the datetime object to a string: {ve}")
            print(f"An exception occurred while reading the file: {ve}")
            exit()
    
    
    async def invokeAPI(self, rest_api, headers=None, json=None)-> Coroutine[Dict[str,Any], None, None]:
        """
        Invoke a REST API
        url: str
        headers: dict
        body: dict
        """
        url = self.get_powerbi_url(rest_api)

        def _log(method, url, status, result=None):
            if result is None:
                count = "n/a"
            elif isinstance(result, dict):
                count = len(result.get("value", result))
            elif isinstance(result, list):
                count = len(result)
            else:
                count = "n/a"
            short_url = url if len(url) <= 120 else url[:117] + "..."
            print(f"[API] {method} {short_url} -> HTTP {status} | records: {count}")

        ## The conintuation Token redirects to your organization for Power BI instead of accessing
        ## the REST API the originated the call. Therefore, we need to intercept and call the
        ## using the continuation URI
        if "continuationToken" in rest_api:
            method, url, body, request_headers = "GET", rest_api, None, headers
        elif json:
            method, url, body, request_headers = "POST", url, json, headers
        elif not headers:
            method, url, body, request_headers = "GET", rest_api, None, None
        else:
            if rest_api.lower().startswith(("http://", "https://")):
                url = rest_api
            method, body, request_headers = "GET", None, headers

        response = await aio_request(
            method,
            url,
            headers=request_headers,
            json_body=body,
            max_retries=self.http_max_retries,
            logger=self.logger,
            timeout=_TIMEOUT,
            connector_kwargs=_CONNECTOR_KWARGS,
            coordinator=self.http_coordinator,
        )

        if not response.ok:
            short_url = url if len(url) <= 120 else url[:117] + "..."
            print(f"[API] {method} {short_url} -> HTTP {response.status} | ERROR")
            self.logger.error(
                "API call failed: %s %s -> HTTP %s %s", method, url_path(url), response.status, response.text[:300]
            )
            return response.error()

        try:
            results = response.json()
        except ValueError as e:
            print(f"[API] {method} {url} -> HTTP {response.status} | Error decoding JSON: {e}")
            return {"ERROR": "Invalid JSON in response", "status_code": response.status, "message": response.text[:500]}

        _log(method, url, response.status, results)

        if method == "GET":
            return results

        if isinstance(results, dict) and "error" in results:
            print("Error in results:", results["error"])
            return results
        elif isinstance(results, dict) and len(results) == 0:
            print("No results found")
            return {"message": "No results found"}
        elif isinstance(results, dict):
            return results
        print("Unexpected results format:", results)
        return {"message": "Unexpected results format", "results": results}


        
