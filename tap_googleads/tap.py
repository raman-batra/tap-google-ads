"""GoogleAds tap class."""

from datetime import datetime, timedelta, timezone
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.exceptions import ConfigValidationError

from tap_googleads.streams import (
    AccessibleCustomers,
    AdGroupsPerformance,
    AdGroupsStream,
    CampaignConversionActionPerformance,
    CampaignPerformance,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    CampaignStoreVisitPerformance,
    CampaignsStream,
    ClickViewReportStream,
    ConversionActionsStream,
    CustomerHierarchyStream,
    GeoPerformance,
    GeotargetsStream,
)

STREAM_TYPES = [
    CampaignsStream,
    AdGroupsStream,
    AdGroupsPerformance,
    AccessibleCustomers,
    CustomerHierarchyStream,
    CampaignPerformance,
    CampaignStoreVisitPerformance,
    CampaignConversionActionPerformance,
    ConversionActionsStream,
    CampaignPerformanceByAgeRangeAndDevice,
    CampaignPerformanceByGenderAndDevice,
    CampaignPerformanceByLocation,
    GeotargetsStream,
    GeoPerformance,
]

CUSTOMER_ID_TYPE = th.StringType()


class TapGoogleAds(Tap):
    """GoogleAds tap class."""

    name = "tap-googleads"

    _refresh_token = th.Property(
        "refresh_token",
        th.StringType,
        required=True,
        secret=True,
    )
    _end_date = datetime.now(timezone.utc).date()
    _start_date = _end_date - timedelta(days=90)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            secret=True,
        ),
        th.Property(
            "refresh_proxy_url",
            th.StringType,
        ),
        th.Property(
            "refresh_proxy_url_auth",
            th.StringType,
            secret=True,
        ),
        _refresh_token,
        th.Property(
            "developer_token",
            th.StringType,
            required=True,
            secret=True,
        ),
        th.Property(
            "login_customer_id",
            CUSTOMER_ID_TYPE,
            description="Manager account ID (MCC). If provided alone, tap will sync all accessible accounts (federated mode).",
        ),
        th.Property(
            "locations",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType, required=True, description="Google Ads customer ID"),
                    th.Property("name", th.StringType, required=False, description="[Optional] Name for reference only; not used by the tap."),
                )
            ),
            description="Array of locations with 'id' (Google Ads customer ID, required) and optional 'name' for reference. When present, the tap extracts customer IDs from each element's 'id'.",
        ),
        th.Property(
            "start_date",
            th.DateType,
            description="ISO start date for all of the streams that use date-based filtering. Defaults to 90 days before the current day.",
            default=_start_date.isoformat(),
        ),
        th.Property(
            "end_date",
            th.DateType,
            description="ISO end date for all of the streams that use date-based filtering. Defaults to the current day.",
            default=_end_date.isoformat(),
        ),
        th.Property(
            "enable_click_view_report_stream",
            th.BooleanType,
            description="Enables the tap's ClickViewReportStream. This requires setting up / permission on your google ads account(s)",
            default=False,
        ),
    ).to_dict()

    def setup_mapper(self):
        self._config.setdefault("flattening_enabled", True)
        self._config.setdefault("flattening_max_depth", 4)

        return super().setup_mapper()

        
    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        if self.config["enable_click_view_report_stream"]:
            STREAM_TYPES.append(ClickViewReportStream)
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    def _validate_config(self, *, raise_errors: bool = True) -> None:
        """Validate configuration.
        
        Raises:
            ConfigValidationError: If the configuration is invalid.
        """
        super()._validate_config(raise_errors=raise_errors)

        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")
        refresh_proxy_url = self.config.get("refresh_proxy_url")
        refresh_proxy_url_auth = self.config.get("refresh_proxy_url_auth")

        # Validate that either standard OAuth or proxy OAuth credentials are provided
        has_standard_oauth = bool(client_id) and bool(client_secret)
        has_proxy_oauth = bool(refresh_proxy_url) and bool(refresh_proxy_url_auth)

        if not (has_standard_oauth or has_proxy_oauth):
            raise ConfigValidationError(
                "Authentication configuration is invalid. Must provide either:\n"
                "1. Both 'client_id' and 'client_secret' for standard OAuth, or\n" 
                "2. Both 'refresh_proxy_url' and 'refresh_proxy_url_auth' for proxy OAuth"
            )

        if has_standard_oauth and has_proxy_oauth:
            self.logger.warning(
                "Both standard OAuth and proxy OAuth credentials provided. "
                "Standard OAuth credentials will take precedence."
            )

if __name__ == "__main__":
    TapGoogleAds.cli()
