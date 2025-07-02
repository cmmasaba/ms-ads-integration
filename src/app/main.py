"""Microsoft Ads Automation to download campaign reports"""

import os
import json
import time
from typing import Any

from bingads import AuthorizationData, OAuthWebAuthCodeGrant, ServiceClient

from utils.logging_util import GclClient

class MsAds:
    """Class definition"""
    def __init__(self) -> None:
        self.__client_id = os.getenv("CLIENT_ID")
        self.__client_secret = os.getenv("CLIENT_SECRET")
        self.__tenant_id = os.getenv("TENANT_ID")
        self.__developer_token = os.getenv("DEVELOPER_TOKEN")
        self.__customer_id  = os.getenv("CUSTOMER_ID")
        self.__customer_account_id = os.getenv("CUSTOMER_ACCOUNT_ID")
        self.__redirection_uri = os.getenv("REDIRECT_URI")
        self.__environment = os.getenv("ENVIRONMENT", "")
        self.__token_cache_file = os.getenv("TOKENS_FILE", "")
        self.__refresh_token = ""
        self.__expires_at = 0
        self.__access_token = ""
        self.__logger = GclClient().get_logger()
        self.__customer_service = None
        self.__reporting_service = None
        self.__campaign_service = None

        self.__authorization_data = AuthorizationData(
            account_id=self.__customer_account_id,
            customer_id=self.__customer_id,
            developer_token=self.__developer_token,
            authentication=None
        )
    def __load_tokens(self) -> bool:
        with open(self.__token_cache_file, "r", encoding="utf-8") as fp:
            try:
                token_data = json.load(fp)
                self.__refresh_token = token_data.get("refresh_token")
                self.__access_token = token_data.get("access_token")
                self.__expires_at = token_data.get("exprires_at", 0)
                return True
            except (FileNotFoundError, json.JSONDecodeError) as e:
                self.__logger.error("[__load_tokens] Error: %s", e)
                return False

    def __save_tokens(self):
        token_data = {
            "refresh_token": self.__refresh_token,
            "access_token": self.__access_token,
            "expires_at": self.__expires_at
        }
        with open(self.__token_cache_file, "w", encoding="utf-8") as fp:
            try:
                json.dump(token_data, fp, indent=4)
            except (FileNotFoundError, IOError) as e:
                self.__logger.error("[__save_tokens] Error: %s", e)

    def __authenticate(self):
        authentication = OAuthWebAuthCodeGrant(
            client_id=self.__client_id,
            client_secret=self.__client_secret,
            redirection_uri=self.__redirection_uri,
            env=self.__environment
        )

        uri = authentication.get_authorization_endpoint()
        self.__logger.info("[__authenticate] Authorization endpoint: %s", uri)

    def __refresh_access_token(self) -> str:
        authentication = OAuthWebAuthCodeGrant(
            client_id=self.__client_id,
            client_secret=self.__client_secret,
            redirection_uri=self.__redirection_uri,
            env=self.__environment
        )

        authentication.request_oauth_tokens_by_refresh_token(self.__refresh_token)
        self.__refresh_token = authentication.oauth_tokens.refresh_token # pyright: ignore
        self.__access_token = authentication.oauth_tokens.access_token # pyright: ignore
        self.__expires_at = authentication.oauth_tokens.access_token_expires_in_seconds # pyright: ignore

        self.__save_tokens()
        return self.__access_token

    def __get_access_token(self) -> str:
        if self.__load_tokens():
            if self.__access_token and self.__expires_at > time.time() + 60:
                return self.__access_token

        return self.__refresh_access_token()
    
    def __set_session_data(self, authentication) -> None:
        self.__authorization_data.authentication = authentication
        self.__customer_service = ServiceClient(
            service="CustomerManagementService",
            authorization_data=self.__authorization_data,
            environment=self.__environment,
            version=os.getenv("MS_API_VERSION")
        )
        self.__reporting_service = ServiceClient(
            service="ReportingManagementService",
            authorization_data=self.__authorization_data,
            environment=self.__environment,
            version=os.getenv("MS_API_VERSION")
        )
        self.__campaign_service = ServiceClient(
            service="CampaignManagementService",
            authorization_data=self.__authorization_data,
            environment=self.__environment,
            version=os.getenv("MS_API_VERSION")
        )

    def __search_accounts_by_id(self, user_id):
        predicates={
            'Predicate': [
                {
                    'Field': 'UserId',
                    'Operator': 'Equals',
                    'Value': user_id,
                },
            ]
        }

        found_last_page = False
        PAGE_SIZE = 100
        page_index = 0
        accounts = []

        while (not found_last_page):
            paging = self.__set_elements_to_none(self.__customer_service.factory.create("ns5:paging")) # pyright: ignore
            paging.Index = page_index
            paging.Size = PAGE_SIZE
            search_accounts_response = self.__customer_service.SearchAccounts( # pyright: ignore
                PageInfo=paging,
                Predicates=predicates
            )

    def __set_elements_to_none(self, suds_object) -> Any:
        for (element) in suds_object:
            suds_object.__setitem__(element[0], None)
        return suds_object
