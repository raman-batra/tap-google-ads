"""REST client handling, including GoogleAdsStream base class."""

from datetime import datetime
from backports.cached_property import cached_property
from typing import Any, Dict, Optional

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream

from tap_googleads.auth import GoogleAdsAuthenticator, ProxyGoogleAdsAuthenticator

from pendulum import parse
import typing as t
import singer_sdk._singerlib as singer
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._typing import (
    conform_record_data_types,
)
from singer_sdk.helpers._util import utc_now


class ResumableAPIError(Exception):
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class GoogleAdsStream(RESTStream):
    """GoogleAds stream class."""

    url_base = "https://googleads.googleapis.com/v23"
    rest_method = "POST"
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.nextPageToken"  # Or override `get_next_page_token`.
    _LOG_REQUEST_METRIC_URLS: bool = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.config.get("locations"):
            self._config["customer_ids"] = [
                _sanitise_customer_id(loc.get("id"))
                for loc in self.config.get("locations", [])
                if "id" in loc and loc.get("id")
            ]
        # No more support for customer_id / customer_ids legacy options.

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A :class:`requests.Response` object.

        Returns:
            str: The error message
        """
        base_msg = super().response_error_message(response)
        try:
            error = response.json()["error"]
            main_message = (
                f"Error {error['code']}: {error['message']} ({error['status']})"
            )

            if "details" in error and error["details"]:
                detail = error["details"][0]
                if "errors" in detail and detail["errors"]:
                    error_detail = detail["errors"][0]
                    detailed_message = error_detail.get("message", "")
                    request_id = detail.get("requestId", "")

                    return f"{base_msg}. {main_message}\nDetails: {detailed_message}\nRequest ID: {request_id}"

            return base_msg + main_message
        except Exception:
            return base_msg

    @cached_property
    def authenticator(self) -> OAuthAuthenticator:
        """Return a new authenticator object."""
        base_auth_url = "https://www.googleapis.com/oauth2/v4/token"
        # Silly way to do parameters but it works

        client_id = self.config.get("client_id", None)
        client_secret = self.config.get(
            "client_secret", None
        )
        refresh_token = self.config.get(
            "refresh_token", None
        )

        auth_url = base_auth_url + f"?refresh_token={refresh_token}"
        auth_url = auth_url + f"&client_id={client_id}"
        auth_url = auth_url + f"&client_secret={client_secret}"
        auth_url = auth_url + "&grant_type=refresh_token"

        if client_id and client_secret and refresh_token:
            return GoogleAdsAuthenticator(stream=self, auth_endpoint=auth_url)

        auth_body = {}
        auth_headers = {}

        auth_body["refresh_token"] = self.config.get("refresh_token")
        auth_body["grant_type"] = "refresh_token"

        auth_headers["authorization"] = self.config.get("refresh_proxy_url_auth")
        auth_headers["Content-Type"] = "application/json"
        auth_headers["Accept"] = "application/json"

        return ProxyGoogleAdsAuthenticator(
            stream=self,
            auth_endpoint=self.config.get("refresh_proxy_url"),
            auth_body=auth_body,
            auth_headers=auth_headers,
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["developer-token"] = self.config["developer_token"]
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id

        self.logger.info(
            "Google Ads request headers prepared: has_developer_token=%s, has_login_customer_id=%s, login_customer_id=%s, has_user_agent=%s",
            bool(self.config.get("developer_token")),
            bool(self.login_customer_id),
            f"{self.login_customer_id[:3]}***{self.login_customer_id[-4:]}" if self.login_customer_id else None,
            "user_agent" in self.config,
        )

        return headers

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["pageToken"] = next_page_token
        if self.gaql:
            params["query"] = self.gaql(context)
        return params

    def get_records(self, context):
        try:
            yield from super().get_records(context)
        except ResumableAPIError as e:
            self.logger.warning(e)

    @property
    def gaql(self):
        raise NotImplementedError

    @property
    def path(self) -> str:
        path = "/customers/{customer_id}/googleAds:search"
        return path

    def start_date(self, context=None):
        start_value = self.get_starting_replication_key_value(context)
        if not start_value:
            start_value = self.config.get("start_date") 
        start_date =  f"'{parse(start_value).date()}'"
        return start_date

    @cached_property
    def end_date(self):
        return parse(self.config["end_date"]).strftime(r"'%Y-%m-%d'") if self.config.get("end_date") else datetime.now().strftime(r"'%Y-%m-%d'")

    @cached_property
    def customer_ids(self):
        # Only support locations[].id. If not present, return None (federated mode or all accessible accounts).
        if self.config.get("locations"):
            ids = [
                _sanitise_customer_id(loc.get("id"))
                for loc in self.config.get("locations", [])
                if "id" in loc and loc.get("id")
            ]
            return ids if ids else None
        return None

    @cached_property
    def login_customer_id(self):
        login_customer_id = self.config.get("login_customer_id")

        if login_customer_id is None:
            return

        return _sanitise_customer_id(login_customer_id)


    def _generate_record_messages(
        self,
        record: dict,
    ) -> t.Generator[singer.RecordMessage, None, None]:
        """Write out a RECORD message.

        Args:
            record: A single stream record.

        Yields:
            Record message objects.
        """
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            pop_deselected_record_properties(mapped_record, self.schema, self.mask, self.logger)
            mapped_record = conform_record_data_types(
                stream_name=self.name,
                record=mapped_record,
                schema=self.schema,
                level=self.TYPE_CONFORMANCE_LEVEL,
                logger=self.logger,
            )
            # Emit record if not filtered
            if mapped_record is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )

def _sanitise_customer_id(customer_id: str):
    return customer_id.replace("-", "").strip()
