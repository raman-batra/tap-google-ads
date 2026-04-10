"""Stream type classes for tap-googleads."""

from __future__ import annotations

import copy
import datetime
from http import HTTPStatus
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_googleads.client import GoogleAdsStream, ResumableAPIError, _sanitise_customer_id
from pendulum import parse

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers."""

    rest_method = "GET"
    path = "/customers:listAccessibleCustomers"
    gaql = None
    name = "stream_accessible_customers"
    primary_keys = ["resourceNames"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("resourceNames", th.ArrayType(th.StringType)),
    ).to_dict()

    def get_child_context(
        self,
        record: Record,
        context,
    ):
        """Generate child contexts.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        Yields:
            A child context for each child stream.

        """
        accessible_ids = []
        for customer in record.get("resourceNames", []):
            customer_id = customer.split("/")[1]
            accessible_ids.append(customer_id.replace("-", ""))

        # Always try to spawn child streams for customer ids in config
        # If those configured ids are invalid, the child streams will fail
        customer_ids = list(accessible_ids)
        if self.customer_ids:
            customer_ids = list(set(accessible_ids).union(self.customer_ids))

        self.logger.info(
            "Customer IDs — accessible_from_api=%s, configured_customer_ids=%s, configured_campaign_ids=%s, final_hierarchy_set=%s",
            accessible_ids, self.customer_ids or [], self.campaign_ids or [], customer_ids,
        )

        return {"customer_ids": customer_ids}


class CustomerHierarchyStream(GoogleAdsStream):
    """Customer Hierarchy.

    Inspiration from Google here
    https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy.

    This query retrieves all 1-degree subaccounts given a manager account's subaccounts. Subaccounts can be either managers or clients.
    
    This stream spawns child streams only for customers that are active clients (not managers).
    If a locations[] config is provided, only customers in that list (or their children) will be synced.
    """


    def gaql(self, context=None):
        return """
	SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.status,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
	"""

    records_jsonpath = "$.results[*]"
    name = "stream_customer_hierarchy"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = AccessibleCustomers
    state_partitioning_keys = ["customer_id"]
    schema = th.PropertiesList(
        th.Property("customer_id", th.StringType),
        th.Property("resourceName", th.StringType),
        th.Property("clientCustomer", th.StringType),
        th.Property("level", th.StringType),
        th.Property("status", th.StringType),
        th.Property("timeZone", th.StringType),
        th.Property("manager", th.BooleanType),
        th.Property("descriptiveName", th.StringType),
        th.Property("currencyCode", th.StringType),
        th.Property("id", th.StringType),
    ).to_dict()

    seen_customer_ids = set()



    def get_records(self, context):
        configured_customer_ids = set(self.customer_ids or [])

        for customer_id in context.get("customer_ids", []):
            try:
                child_context = dict(context)
                child_context["customer_id"] = customer_id
                yield from super().get_records(child_context)
            except Exception as e:
                if customer_id in configured_customer_ids:
                    raise
                self.logger.error(
                    f"Error processing resource name {customer_id}: {str(e)}"
                )
                continue

    def post_process(self, row, context):
        row = row["customerClient"]
        row["customer_id"] = _sanitise_customer_id(row["id"])
        return row
    
    def _sync_children(self, child_context: dict | None) -> None:
        if child_context:
            self.seen_customer_ids.add(child_context.get("customer_id"))
            super()._sync_children({"customer_id": child_context.get("customer_id")})

    def get_customer_family_line(self, resource_name) -> list:
        # resource name looks like 'customers/8435753557/customerClients/8105937676'
        family_line = [x for x in resource_name.split('/') if not 'customer' in x]
        return family_line

    def get_child_context(self, record: Record, context):
        customer_id = record.get("customer_id")
        is_active_client = record.get("manager") == False and record.get("status") == "ENABLED"
        already_synced = customer_id in self.seen_customer_ids

        family_line = self.get_customer_family_line(record.get("resourceName"))

        if not is_active_client:
            self.logger.debug(
                "Skipping customer %s: not an active client (manager=%s, status=%s)",
                customer_id, record.get("manager"), record.get("status"),
            )
            return None

        if already_synced:
            self.logger.debug("Skipping customer %s: already synced in this run", customer_id)
            return None

        if self.customer_ids and len(set(self.customer_ids).intersection(set(family_line))) == 0:
            self.logger.debug(
                "Skipping customer %s: family_line=%s has no overlap with configured customer_ids=%s",
                customer_id, family_line, self.customer_ids,
            )
            return None

        self.logger.info(
            "Spawning child streams for customer_id=%s (family_line=%s)",
            customer_id, family_line,
        )
        return {"customer_id": record.get("id"), "is_active_client": is_active_client}

class ReportsStream(GoogleAdsStream):
    parent_stream_type = CustomerHierarchyStream
    replication_key = "segments__date"
    supports_campaign_filter = True

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if "query" in params and self.supports_campaign_filter and self.campaign_ids:
            campaign_ids_str = ", ".join(self.campaign_ids)
            query = params["query"]
            if re.search(r'\bWHERE\b', query, re.IGNORECASE):
                params["query"] = query.rstrip() + f" AND campaign.id IN ({campaign_ids_str})"
            elif re.search(r'\bORDER\s+BY\b', query, re.IGNORECASE):
                # WHERE must precede ORDER BY in GAQL — insert before it
                params["query"] = re.sub(
                    r'(\bORDER\s+BY\b)',
                    f'WHERE campaign.id IN ({campaign_ids_str}) \\1',
                    query,
                    count=1,
                    flags=re.IGNORECASE,
                )
            else:
                params["query"] = query.rstrip() + f" WHERE campaign.id IN ({campaign_ids_str})"
            self.logger.debug(
                "Campaign filter applied for stream '%s' (customer_id=%s): campaign.id IN (%s)",
                self.name, (context or {}).get("customer_id"), campaign_ids_str,
            )
        return params

    def get_records(self, context):
        records =  super().get_records(context)
        customer_id = context.get("customer_id")
        if customer_id:
            for record in records:
                record["customer_id"] = customer_id
                yield record

    def post_process(self, row, context):
        row = super().post_process(row, context)
        if self.replication_key == "segments__date":
            row["segments__date"] = row["segments"].pop("date")
        return row

class GeotargetsStream(ReportsStream):
    """Geotargets, worldwide, constant across all customers"""

    supports_campaign_filter = False

    def gaql(self, context=None):
        return """
        SELECT 
            geo_target_constant.canonical_name,
            geo_target_constant.country_code,
            geo_target_constant.id,
            geo_target_constant.name,
            geo_target_constant.status,
        geo_target_constant.target_type
    FROM geo_target_constant
    """
    records_jsonpath = "$.results[*]"
    name = "stream_geo_target_constant"
    primary_keys = ["geoTargetConstant__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "geo_target_constant.json"

    def get_records(self, context: Context) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.

        """
        yield from super().get_records(context)
        self.selected = False  # sync once only


class ClickViewReportStream(ReportsStream):
    date: datetime.date
    supports_campaign_filter = False  # campaign filter applied directly in gaql()

    def gaql(self, context=None):
        campaign_filter = (
            f" AND campaign.id IN ({', '.join(self.campaign_ids)})"
            if self.campaign_ids else ""
        )
        return f"""
        SELECT
            click_view.gclid
            , customer.id
            , click_view.ad_group_ad
            , ad_group.id
            , ad_group.name
            , campaign.id
            , campaign.name
            , segments.ad_network_type
            , segments.device
            , segments.date
            , segments.slot
            , metrics.clicks
            , segments.click_type
            , click_view.keyword
            , click_view.keyword_info.match_type
        FROM click_view
        WHERE segments.date = '{self.date.isoformat()}'{campaign_filter}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_click_view_report"
    primary_keys = [
        "clickView__gclid",
        "clickView__keyword",
        "clickView__keywordInfo__matchType",
        "customer__id",
        "adGroup__id",
        "campaign__id",
        "segments__device",
        "segments__adNetworkType",
        "segments__slot",
        "date",
    ]
    replication_key = "date"
    schema_filepath = SCHEMAS_DIR / "click_view_report.json"

    def post_process(self, row, context):
        row["date"] = row["segments"].pop("date")

        if row.get("clickView", {}).get("keyword") is None:
            row["clickView"]["keyword"] = "null"
            row["clickView"]["keywordInfo"] = {"matchType": "null"}

        return row

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.

        """
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        return params

    def request_records(self, context):
        start_date =  self.start_date
        end_date = parse(self.config["end_date"]).date()

        delta = end_date - start_date
        dates = (start_date + datetime.timedelta(days=i) for i in range(delta.days))

        for self.date in dates:
            records = list(super().request_records(context))

            if not records:
                self._increment_stream_state({"date": self.date.isoformat()}, context=self.context)

            yield from records

    def validate_response(self, response):
        if response.status_code == HTTPStatus.FORBIDDEN:
            try:
                error = response.json()["error"]["details"][0]["errors"][0]
                msg = (
                    "Click view report not accessible to customer "
                    f"'{self.context['customer_id']}': {error['message']}"
                )
            except (KeyError, IndexError, ValueError):
                msg = self.response_error_message(response)
            raise ResumableAPIError(msg, response)
        super().validate_response(response)


class CampaignsStream(ReportsStream):
    """Campaign metadata."""

    def gaql(self, context=None):
        return """
        SELECT
            campaign.id,
            campaign.name,
            campaign.resource_name,
            campaign.status
        FROM campaign
        ORDER BY campaign.id
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign"
    primary_keys = ["customer_id", "campaign__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"


class AdGroupsStream(ReportsStream):
    """Define custom stream."""


    def gaql(self, context=None):
        return """
       SELECT ad_group.url_custom_parameters, 
       ad_group.type, 
       ad_group.tracking_url_template, 
       ad_group.targeting_setting.target_restrictions,
       ad_group.target_roas,
       ad_group.target_cpm_micros,
       ad_group.status,
       ad_group.target_cpa_micros,
       ad_group.resource_name,
       ad_group.percent_cpc_bid_micros,
       ad_group.name,
       ad_group.labels,
       ad_group.id,
       ad_group.final_url_suffix,
       ad_group.excluded_parent_asset_field_types,
       ad_group.effective_target_roas_source,
       ad_group.effective_target_roas,
       ad_group.effective_target_cpa_source,
       ad_group.effective_target_cpa_micros,
       ad_group.display_custom_bid_dimension,
       ad_group.cpv_bid_micros,
       ad_group.cpm_bid_micros,
       ad_group.cpc_bid_micros,
       ad_group.campaign,
       ad_group.base_ad_group,
       ad_group.ad_rotation_mode
       FROM ad_group 
       """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroups"
    primary_keys = ["adGroup__id", "adGroup__campaign", "adGroup__status"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"


class AdGroupsPerformance(ReportsStream):
    """AdGroups Performance"""


    def gaql(self, context=None):
        return f"""
        SELECT campaign.id, ad_group.id, metrics.impressions, metrics.clicks,
               metrics.cost_micros,
               segments.date
               FROM ad_group
               WHERE segments.date >= {self.start_date(context)} and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroupsperformance"
    primary_keys = ["campaign__id", "adGroup__id", "segments__date"]
    
    schema_filepath = SCHEMAS_DIR / "adgroups_performance.json"


class CampaignPerformance(ReportsStream):
    """Campaign Performance"""

    def gaql(self, context=None):
        return f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.resource_name,
            campaign.status,
            segments.device,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpc,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date >= {self.start_date(context)}
          and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance"
    primary_keys = [
        "customer_id",
        "campaign__id",
        "segments__date",
        "segments__device",
    ]

    schema_filepath = SCHEMAS_DIR / "campaign_performance.json"

class CampaignStoreVisitPerformance(ReportsStream):
    """Campaign daily store visit performance.

    Store visit metrics are not compatible with segments.device, so this
    lives in a dedicated stream segmented only by date.
    """

    def gaql(self, context=None):
        return f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.resource_name,
            campaign.status,
            segments.date,
            metrics.all_conversions_from_store_visit
        FROM campaign
        WHERE segments.date >= {self.start_date(context)}
          and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_store_visit_performance"
    primary_keys = [
        "customer_id",
        "campaign__id",
        "segments__date",
    ]

    schema_filepath = SCHEMAS_DIR / "campaign_store_visit_performance.json"

class CampaignConversionActionPerformance(ReportsStream):
    """Campaign daily performance segmented by conversion action."""

    def gaql(self, context=None):
        return f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.resource_name,
            campaign.status,
            segments.date,
            segments.conversion_action,
            metrics.all_conversions
        FROM campaign
        WHERE segments.date >= {self.start_date(context)}
          and segments.date <= {self.end_date}
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_conversion_action_performance"
    primary_keys = [
        "customer_id",
        "campaign__id",
        "segments__date",
        "segments__conversionAction",
    ]
    schema_filepath = SCHEMAS_DIR / "campaign_conversion_action_performance.json"

class ConversionActionsStream(ReportsStream):
    """Conversion action metadata."""

    supports_campaign_filter = False

    def gaql(self, context=None):
        return """
        SELECT
            conversion_action.id,
            conversion_action.resource_name,
            conversion_action.name,
            conversion_action.status,
            conversion_action.category,
            conversion_action.type,
            conversion_action.primary_for_goal
        FROM conversion_action
        """

    records_jsonpath = "$.results[*]"
    name = "stream_conversion_action"
    primary_keys = ["customer_id", "conversionAction__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "conversion_action.json"

class CampaignPerformanceByAgeRangeAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""


    def gaql(self, context=None):
        return f"""
    SELECT ad_group_criterion.age_range.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM age_range_view WHERE segments.date >= {self.start_date(context)} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_age_range_and_device"
    primary_keys = [
        "adGroup__name",
        "adGroupCriterion__ageRange__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_age_range_and_device.json"


class CampaignPerformanceByGenderAndDevice(ReportsStream):
    """Campaign Performance By Age Range and Device"""


    def gaql(self, context=None):
        return f"""
    SELECT ad_group_criterion.gender.type, campaign.name, campaign.status, ad_group.name, segments.date, segments.device, ad_group_criterion.system_serving_status, ad_group_criterion.bid_modifier, metrics.clicks, metrics.impressions, metrics.ctr, metrics.average_cpc, metrics.cost_micros, campaign.advertising_channel_type FROM gender_view WHERE segments.date >= {self.start_date(context)} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_gender_and_device"
    primary_keys = [
        "adGroup__name",
        "adGroupCriterion__gender__type",
        "campaign__name",
        "segments__date",
        "campaign__status",
        "segments__device",
    ]
    
    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_gender_and_device.json"


class CampaignPerformanceByLocation(ReportsStream):
    """Campaign Performance By Location"""

    def gaql(self, context=None):
        return f"""
    SELECT
        campaign.id,
        campaign.name,
        campaign_criterion.criterion_id,
        campaign_criterion.location.geo_target_constant,
        campaign_criterion.bid_modifier,
        segments.date,
        metrics.clicks,
        metrics.impressions,
        metrics.ctr,
        metrics.average_cpc,
        metrics.cost_micros
    FROM location_view
    WHERE segments.date >= {self.start_date(context)}
      and segments.date <= {self.end_date}
      AND campaign_criterion.status != 'REMOVED'
    """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign_performance_by_location"
    primary_keys = [
        "campaign__id",
        "campaignCriterion__criterionId",
        "segments__date",
    ]

    schema_filepath = SCHEMAS_DIR / "campaign_performance_by_location.json"


class GeoPerformance(ReportsStream):
    """Geo performance"""

    supports_campaign_filter = False  # geographic_view requires campaign.id in SELECT to filter by it

    def gaql(self, context=None):
        return f"""
    SELECT 
        campaign.name, 
        campaign.status, 
        segments.date, 
        metrics.clicks, 
        metrics.cost_micros,
        metrics.impressions, 
        metrics.conversions,
        geographic_view.location_type,
        geographic_view.country_criterion_id
    FROM geographic_view 
    WHERE segments.date >= {self.start_date(context)} and segments.date <= {self.end_date} 
    """

    records_jsonpath = "$.results[*]"
    name = "stream_geo_performance"
    primary_keys = [
        "geographicView__countryCriterionId",
        "geographicView__locationType",
        "customer_id",
        "campaign__name",
        "campaign__status",
        "segments__date"
    ]
    
    schema_filepath = SCHEMAS_DIR / "geo_performance.json"
