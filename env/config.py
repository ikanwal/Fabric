import os
import json
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from env.audit import Audits


DEFAULT_APPLICATION_MODULES = "Activity,Apps,Catalog,Graph,Tenant,RefreshHistory,Gateway"


def _normalize_key(key: str) -> str:
    return key.replace("_", "").lower()


def _settings_lookup(settings: Mapping[str, Any] | None) -> dict[str, Any]:
    if not settings:
        return {}
    return {_normalize_key(key): value for key, value in settings.items()}


def _get_value(
    settings: Mapping[str, Any] | None,
    *names: str,
    default: Any = None,
    required: bool = False,
) -> Any:
    lookup = _settings_lookup(settings)
    for name in names:
        normalized = _normalize_key(name)
        if normalized in lookup and lookup[normalized] not in (None, ""):
            return lookup[normalized]
        for env_name, env_value in os.environ.items():
            if _normalize_key(env_name) == normalized and env_value != "":
                return env_value
    if required:
        raise RuntimeError(f"Missing required configuration value: {names[0]}")
    return default


def _get_bool(settings: Mapping[str, Any] | None, *names: str, default: bool = False) -> bool:
    value = _get_value(settings, *names, default=default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _set_if_present(settings: Mapping[str, Any] | None, names: tuple[str, ...], setter) -> None:
    value = _get_value(settings, *names)
    if value not in (None, ""):
        setter(value)


def load_profile(profile_path: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(profile_path)
    with path.open("r", encoding="utf-8") as profile_file:
        profile = json.load(profile_file)

    if not isinstance(profile, dict):
        raise RuntimeError(f"Profile must contain a JSON object: {path}")
    return profile


def build_audit(
    settings: Mapping[str, Any] | None = None,
    *,
    load_env_file: bool = False,
    env_file: str | os.PathLike[str] | None = None,
) -> Audits:
    if load_env_file:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_file)

    audit = Audits()

    audit.set_ServicePrincipal(
        tenant_id=_get_value(settings, "TENANT_ID", "TenantId", required=True),
        client_id=_get_value(settings, "CLIENT_ID", "ClientId", required=True),
        client_secret=_get_value(settings, "CLIENT_SECRET", "ClientSecret", required=True),
        environment=_get_value(settings, "CLOUD_ENVIRONMENT", "CloudEnvironment", "ENVIRONMENT", "Environment", default="Commercial"),
    )
    audit.set_ApplicationModules(
        _get_value(settings, "APPLICATION_MODULES", "ApplicationModules", default=DEFAULT_APPLICATION_MODULES)
    )

    audit.set_all_workspaces(_get_bool(settings, "ALL_WORKSPACES", "Base", default=False))
    _set_if_present(settings, ("INCLUDE_WORKSPACE_IDS", "IncludeWorkspaceIds"), audit.set_include_workspace_ids)
    _set_if_present(settings, ("EXCLUDE_WORKSPACE_IDS", "ExcludeWorkspaceIds"), audit.set_exclude_workspace_ids)
    _set_if_present(settings, ("HTTP_MAX_RETRIES", "HttpMaxRetries"), audit.set_http_max_retries)
    _set_if_present(
        settings,
        ("HTTP_MAX_CONCURRENCY", "API_MAX_CONCURRENCY", "HttpMaxConcurrency"),
        audit.set_http_max_concurrency,
    )
    _set_if_present(settings, ("SCAN_STATUS_POLL_SECONDS", "ScanStatusPollSeconds"), audit.set_scan_status_poll_seconds)
    _set_if_present(settings, ("ITEMJOBS_CONCURRENCY", "ItemJobsConcurrency"), audit.set_itemjobs_concurrency)
    audit.set_on_fabric(_get_bool(settings, "ON_FABRIC", "OnFabric", default=True))
    if _get_value(settings, "EXCLUDE_PERSONAL_WORKSPACES", "ExcludePersonalWorkspaces") is not None:
        audit.exclude_personal_workspaces(
            _get_bool(settings, "EXCLUDE_PERSONAL_WORKSPACES", "ExcludePersonalWorkspaces")
        )
    if _get_value(settings, "EXCLUDE_INACTIVE_WORKSPACES", "ExcludeInactiveWorkspaces") is not None:
        audit.exclude_inactive_workspaces(
            _get_bool(settings, "EXCLUDE_INACTIVE_WORKSPACES", "ExcludeInactiveWorkspaces")
        )
    if _get_value(settings, "GRAPH_EXTRACT_GROUPS", "GraphExtractGroups") is not None:
        audit.set_GraphExtractGroups(_get_bool(settings, "GRAPH_EXTRACT_GROUPS", "GraphExtractGroups"))

    _set_if_present(settings, ("IMPERSONATED_USER_NAME", "ImpersonatedUserName"), audit.set_ImpersonatedUserName)
    _set_if_present(settings, ("POWER_BI_AUDIENCE_GROUP_IDS", "PowerBIAudienceGroupIds"), audit.set_PowerBIAudienceGroupIds)
    _set_if_present(settings, ("CAPACITY_METRICS_DATASET_ID", "CapacityMetricsDatasetId"), audit.set_capacity_metrics_dataset_id)
    _set_if_present(settings, ("OUTPUT_PATH", "OutputPath"), audit.set_OutputPath)
    _set_if_present(settings, ("LAKEHOUSE_NAME", "LakehouseName"), audit.set_LakehouseName)
    _set_if_present(settings, ("WORKSPACE_NAME", "WorkspaceName"), audit.set_WorkspaceName)
    _set_if_present(settings, ("PATH_IN_LAKEHOUSE", "PathInLakehouse"), audit.set_PathInLakehouse)
    _set_if_present(
        settings,
        ("CATALOG_GET_INFO_PARAMETERS", "CatalogGetInfoParameters"),
        audit.set_CatalogGetInfoParameters,
    )
    _set_if_present(
        settings,
        ("CATALOG_GET_MODIFIED_PARAMETERS", "CatalogGetModifiedParameters"),
        audit.set_CatalogGetModifiedParameters,
    )
    _set_if_present(
        settings,
        ("STORAGE_ACCOUNT_CONN_STR", "STORAGE_ACCOUNT_CONNECTION_STRING", "StorageAccountConnStr"),
        audit.set_StorageAccountConnStr,
    )
    _set_if_present(settings, ("STORAGE_ACCOUNT_URL", "storage_url"), audit.set_storage_url)
    _set_if_present(
        settings,
        ("STORAGE_ACCOUNT_CONTAINER_NAME", "StorageAccountContainerName", "container_name"),
        audit.set_StorageAccountContainerName,
    )
    _set_if_present(
        settings,
        ("STORAGE_ACCOUNT_CONTAINER_ROOT_PATH", "StorageAccountContainerRootPath"),
        audit.set_StorageAccountContainerRootPath,
    )

    if audit.context.StorageAccountContainerName or audit.context.OutputPath:
        audit.set_on_fabric(False)
    elif audit.context.LakehouseName and not audit.context.cloud.supports_onelake:
        raise RuntimeError(
            f"Fabric Lakehouse / OneLake output is not enabled for CLOUD_ENVIRONMENT={audit.context.cloud.name}. "
            "Use OUTPUT_PATH or Blob Storage until Fabric Gov is available."
        )
    elif not audit.context.LakehouseName:
        audit.set_OutputPath("Data")
        audit.set_on_fabric(False)

    cron_map = {
        ("ACTIVITY_CRON", "Activity_cron"): audit.set_Activity_cron,
        ("APPS_CRON", "Apps_cron"): audit.set_Apps_cron,
        ("CAPACITY_CRON", "Capacity_cron"): audit.set_Capacity_cron,
        ("CAPACITY_METRICS_CRON", "CapacityMetrics_cron"): audit.set_CapacityMetrics_cron,
        ("CATALOG_CRON", "Catalog_cron"): audit.set_Catalog_cron,
        ("DOMAINS_CRON", "Domains_cron"): audit.set_Domains_cron,
        ("FABRICITEMS_CRON", "FabricItems_cron"): audit.set_FabricItems_cron,
        ("ITEMJOBS_CRON", "ItemJobs_cron"): audit.set_ItemJobs_cron,
        ("GATEWAY_CRON", "Gateway_cron"): audit.set_Gateway_cron,
        ("GRAPH_CRON", "Graph_cron"): audit.set_Graph_cron,
        ("REFRESHABLES_CRON", "Refreshables_cron"): audit.set_Refreshables_cron,
        ("REFRESHHISTORY_CRON", "RefreshHistory_cron"): audit.set_RefreshHistory_cron,
        ("ROLES_CRON", "Roles_cron"): audit.set_Roles_cron,
        ("TENANT_CRON", "Tenant_cron"): audit.set_Tenant_cron,
        ("WORKSPACES_CRON", "Workspaces_cron"): audit.set_Workspaces_cron,
    }
    for names, setter in cron_map.items():
        _set_if_present(settings, names, setter)

    return audit


async def run_monitor(
    settings: Mapping[str, Any] | None = None,
    *,
    load_env_file: bool = False,
    env_file: str | os.PathLike[str] | None = None,
) -> None:
    audit = build_audit(settings=settings, load_env_file=load_env_file, env_file=env_file)
    await audit.run()
