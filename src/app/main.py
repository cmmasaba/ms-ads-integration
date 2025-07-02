import json
import time
import os
from time import sleep
from typing import Any
import zipfile
import shutil
from datetime import datetime, timedelta, timezone, date

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

from utils.logging_util import GclClient

load_dotenv()

class BingAds:
    """Automation for extracting and loading Ads data to BQ"""
    def __init__(self, token_cache_file="client_bing_ads_token.json") -> None:
        """
        Initializes the BingAdsAuthenticator.
        """
        self.logging_client = GclClient()
        self.logger = self.logging_client.get_logger()
        self.refresh_token: str = ""
        self.token_cache_file: str = token_cache_file
        self.access_token: str = ""
        self.expires_at = 0
        self.bq_client = bigquery.Client(project=os.getenv("PROJECT_NAME"))

    def _load_tokens(self) -> None:
        """Loads tokens from the cache file."""
        try:
            with open(self.token_cache_file, "r", encoding="utf-8") as f:
                token_data = json.load(f)
                self.access_token = token_data.get("access_token")
                self.refresh_token = token_data.get("refresh_token")
                self.expires_at = token_data.get("expires_at", 0)
        except FileNotFoundError as e:
            self.logger.error("[_load_tokens] Cache file not found: %s", e)
        except json.JSONDecodeError as e:
            self.logger.error("[_load_tokens] Error decoding cache file: %s", e)
            os.remove(self.token_cache_file)

    def _save_tokens(self) -> None:
        """Saves tokens to the cache file."""
        token_data = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
        }
        with open(self.token_cache_file, "w", encoding="utf-8") as f:
            json.dump(token_data, f, indent=4, sort_keys=True)

    def _refresh_access_token(self) -> str | None:
        """Refreshes the access token using the refresh token."""
        if not self.refresh_token:
            raise ValueError("Refresh token is missing. Obtain it first.")

        token_url = f"https://login.microsoftonline.com/{os.getenv("TENANT_ID")}/oauth2/v2.0/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": os.getenv("CLIENT_ID"),
            "client_secret": os.getenv("CLIENT_SECRET"),
        }

        response = None
        try:
            response = requests.post(token_url, data=data, timeout=15)
            response.raise_for_status()
            token_data = response.json()

            self.access_token = token_data["access_token"]
            self.refresh_token = token_data["refresh_token"]
            self.expires_at = time.time() + token_data["expires_in"]

            self._save_tokens()
            return self.access_token
        except requests.exceptions.RequestException as e:
            if response is not None:
                try:
                    error_message = json.loads(response.text)
                    if "OperationErrors" in error_message:
                        self.logger.info("[_refresh_access_token] Error refreshing tokens: %s", error_message["OperationErrors"])
                    else:
                        self.logger.info("[_refresh_access_token] API request failed with unexpected response format: %s, %s", e, response.text)
                except json.JSONDecodeError:
                    self.logger.info("[_refresh_access_token] API request failed: %s. Could not decode JSON from response %s", e, response.text)
            else:
                self.logger.info("[_refresh_access_token] API request failed: %s. No response was received", e)
            return None
        except KeyError as e:
            self.logger.info("[_refresh_access_token] Missing key in token response: %s", e)
            return None

    def get_access_token(self) -> str | None:
        """Retrieves a valid access token, refreshing if necessary."""
        self._load_tokens()
        if self.access_token and self.expires_at > time.time() + 60:
            return self.access_token

        return self._refresh_access_token()

    def get_headers(self):
        """Returns headers including the Authorization header."""
        access_token = self.get_access_token()
        if not access_token:
            return None

        headers: dict[str, Any] = {
            'Authorization': f'Bearer {access_token}',
            'CustomerId': os.getenv("CUSTOMER_ID"),
            'CustomerAccountId': os.getenv("CUSTOMER_ACCOUNT_ID"),
            'Content-Type': 'application/json',
            'DeveloperToken': os.getenv("DEVELOPER_TOKEN"),
        }
        return headers

    def submit_download_report(self, headers, body: dict[str, dict[str, Any]]) -> str | None:
        """
        Submit request to download report
        """
        submit_download_api_url = 'https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Submit'

        response = None
        try:
            response = requests.post(submit_download_api_url, headers=headers, timeout=45, data=json.dumps(body))
            response.raise_for_status()
            result: str = response.json()['ReportRequestId']
            return result
        except requests.exceptions.RequestException as e:
            if response is not None:
                try:
                    error_message = json.loads(response.text)
                    if "OperationErrors" in error_message:
                        self.logger.error("[submit_download_report] Error making request: %s", error_message["OperationErrors"])
                    else:
                        self.logger.error("[submit_download_report] API request failed with unexpected response format: %s. Response text: %s", e, response.text)
                except json.JSONDecodeError as err:
                    self.logger.error("[submit_download_report] API request failed: %s. Response text: %s", err, response.text)
            else:
                self.logger.error("[submit_download_report] API request failed: %s", e)
            return None

    def get_execution_tm(self):
        yesterday = datetime.today() - timedelta(days=1)
        return yesterday.strftime('%Y%m%d')

    def campaign_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return campaign performance request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        yesterday = datetime.today() - timedelta(days=1)
        today = datetime.today()
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Campaign Performance Request",
                    "Type": "CampaignPerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountNumber",
                        "AccountId",
                        "TimePeriod",
                        "CampaignStatus",
                        "CampaignId",
                        "CurrencyCode",
                        "CampaignName",
                        "AdDistribution",
                        "Ctr",
                        "AverageCpc",
                        "Impressions",
                        "Clicks",
                        "Conversions",
                        "Spend",
                        "AveragePosition",
                        "ConversionRate",
                        "CostPerConversion",
                        "LowQualityClicks",
                        "LowQualityClicksPercent",
                        "LowQualityImpressions",
                        "LowQualityImpressionsPercent",
                        "LowQualityConversions",
                        "LowQualityConversionRate",
                        "DeviceType",
                        "DeviceOS",
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": today.day,
                            "Month": today.month,
                            "Year": today.year
                        },
                        "CustomDateRangeStart": {
                            "Day": yesterday.day,
                            "Month": yesterday.month,
                            "Year": yesterday.year
                        }
                    }
                }
            }
        return json_body

    def account_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return account performance report request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Account Performance Request",
                    "Type": "AccountPerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountNumber",
                        "AccountId",
                        "TimePeriod",
                        "CurrencyCode",
                        "AdDistribution",
                        "Ctr",
                        "AverageCpc",
                        "Impressions",
                        "Clicks",
                        "Spend",
                        "Conversions",
                        "AveragePosition",
                        "ConversionRate",
                        "CostPerConversion",
                        "LowQualityClicks",
                        "LowQualityClicksPercent",
                        "LowQualityImpressions",
                        "LowQualityImpressionsPercent",
                        "LowQualityConversions",
                        "LowQualityConversionRate",
                        "DeviceType",
                        "DeviceOS"
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": 2,
                            "Month": 6,
                            "Year": 2025
                        },
                        "CustomDateRangeStart": {
                            "Day": 1,
                            "Month": 1,
                            "Year": 2024
                        }
                    }
                }
            }
        return json_body

    def adgroup_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return adgroup performance report request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Ad Group Performance Request",
                    "Type": "AdGroupPerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountNumber",
                        "AccountId",
                        "TimePeriod",
                        "Status",
                        "CampaignName",
                        "CampaignId",
                        "AdGroupName",
                        "AdGroupId",
                        "AdGroupType",
                        "CurrencyCode",
                        "AdDistribution",
                        "Ctr",
                        "AverageCpc",
                        "Impressions",
                        "Clicks",
                        "Spend",
                        "Conversions",
                        "AveragePosition",
                        "ConversionRate",
                        "CostPerConversion",
                        "DeviceType",
                        "DeviceOS"
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": 2,
                            "Month": 6,
                            "Year": 2025
                        },
                        "CustomDateRangeStart": {
                            "Day": 1,
                            "Month": 1,
                            "Year": 2024
                        }
                    }
                }
            }
        return json_body

    def ad_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return ad performance report request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Ad Performance Request",
                    "Type": "AdPerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountNumber",
                        "AccountId",
                        "TimePeriod",
                        "CampaignName",
                        "CampaignId",
                        "AdGroupName",
                        "AdId",
                        "AdTitle",
                        "AdDescription",
                        "AdType",
                        "AdStatus",
                        "AdGroupId",
                        "CurrencyCode",
                        "AdDistribution",
                        "Ctr",
                        "AverageCpc",
                        "Impressions",
                        "Clicks",
                        "Spend",
                        "Conversions",
                        "AveragePosition",
                        "ConversionRate",
                        "CostPerConversion",
                        "DeviceType",
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": 2,
                            "Month": 6,
                            "Year": 2025
                        },
                        "CustomDateRangeStart": {
                            "Day": 1,
                            "Month": 1,
                            "Year": 2024
                        }
                    }
                }
            }
        return json_body

    def asset_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return asset performance report request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Asset Performance Request",
                    "Type": "AssetPerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountId",
                        "TimePeriod",
                        "CampaignName",
                        "CampaignId",
                        "AdGroupId",
                        "AdGroupName",
                        "AssetId",
                        "AssetContent",
                        "AssetType",
                        "AssetSource",
                        "Ctr",
                        "Impressions",
                        "Clicks",
                        "Spend",
                        "Conversions",
                        "Revenue",
                        "VideoViewsAt25Percent",
                        "VideoViewsAt50Percent",
                        "VideoViewsAt75Percent",
                        "CompletedVideoViews",
                        "VideoCompletionRate"
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": 2,
                            "Month": 6,
                            "Year": 2025
                        },
                        "CustomDateRangeStart": {
                            "Day": 1,
                            "Month": 1,
                            "Year": 2024
                        }
                    }
                }
            }
        return json_body

    def audience_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return audience performance report request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Audience Performance Request",
                    "Type": "AudiencePerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountNumber",
                        "AccountId",
                        "TimePeriod",
                        "AccountStatus",
                        "CampaignStatus",
                        "CampaignName",
                        "CampaignId",
                        "AudienceType",
                        "AdGroupName",
                        "AdGroupId",
                        "AudienceId",
                        "AudienceName",
                        "AssociationStatus",
                        "BidAdjustment",
                        "TargetingSetting",
                        "Impressions",
                        "Clicks",
                        "Ctr",
                        "AverageCpc",
                        "Spend",
                        "Conversions",
                        # "ConversionsRate",
                        "ReturnOnAdSpend",
                        "Revenue",
                        "RevenuePerConversion"
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": 2,
                            "Month": 6,
                            "Year": 2025
                        },
                        "CustomDateRangeStart": {
                            "Day": 1,
                            "Month": 1,
                            "Year": 2024
                        }
                    }
                }
            }
        return json_body

    def conversion_performance_request_body(self) -> dict[str, dict[str, Any]]:
        """
        Return conversion performance report request body in json format
        """
        customer_account_id =f"{os.getenv('CUSTOMER_ACCOUNT_ID')}"
        json_body ={
            "ReportRequest":
                {
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ReportName": "Conversion Performance Request",
                    "Type": "ConversionPerformanceReportRequest",
                    "ReturnOnlyCompleteData": False,
                    "ExcludeReportFooter": True,
                    "ExcludeReportHeader": True,
                    "Aggregation": "Daily",
                    "Columns": [
                        "AccountName",
                        "AccountNumber",
                        "AccountId",
                        "TimePeriod",
                        "CampaignName",
                        "CampaignStatus",
                        "CampaignId",
                        "AdGroupName",
                        "AdGroupId",
                        "AdGroupStatus",
                        "Keyword",
                        "KeywordId",
                        "Impressions",
                        "Clicks",
                        "Ctr",
                        "Assists",
                        "Conversions",
                        "ConversionRate",
                        "Spend",
                        "Revenue"
                    ],
                    "Scope": {
                        "AccountIds": [
                            customer_account_id
                        ]
                    },
                    "Time": {
                        "CustomDateRangeEnd": {
                            "Day": 2,
                            "Month": 6,
                            "Year": 2025
                        },
                        "CustomDateRangeStart": {
                            "Day": 1,
                            "Month": 1,
                            "Year": 2024
                        }
                    }
                }
            }
        return json_body

    def poll_generate_report(self, report_id: str, headers: dict[str, Any]) -> str | None:
        """
        Poll submitted report request url to see if it is ready for download
        """
        poll_generate_api_url = 'https://reporting.api.bingads.microsoft.com/Reporting/v13/GenerateReport/Poll'

        download_url = ""
        try:
            while True:
                response = requests.post(poll_generate_api_url, headers=headers, json={"ReportRequestId": report_id}, timeout=30)
                response.raise_for_status()
                response = response.json()
                if response['ReportRequestStatus']['Status'] == 'Success':
                    download_url = response['ReportRequestStatus']['ReportDownloadUrl']
                    return download_url
                sleep(30)
        except requests.exceptions.RequestException as e:
            self.logger.error("[poll_generate_report] API request failed: %s", e)
        return download_url

    def download_and_load_report(self, report_type, body: dict[str, dict[str, Any]], table_id: str) -> bool:
        """
        Download report and load it to BQ
        """
        if not body or not table_id:
            self.logger.error("[download_and_load_report] Missing body or table_id for report %s", report_type)
            return False

        self.logger.info("[download_and_load_report] Fetching performance report for %s", report_type)
        headers = self.get_headers()
        if not headers:
            self.logger.info("[download_and_load_report] Missing headers %s", report_type)
            return False

        report_id = self.submit_download_report(headers, body)
        if not report_id:
            self.logger.error("[download_and_load_report] Error submitting report for report %s", report_type)
            return False

        url = self.poll_generate_report(report_id, headers)

        def download_report(report_type, report_url: str) -> str | None:
            self.logger.info("[download_and_load_report] Downloading performance report for %s", report_type)
            response = None
            try:
                with requests.Session() as s:
                    response = s.get(report_url, stream=True, timeout=60)
                    response.raise_for_status()

                    with open(f"{report_type}.zip", mode="wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    return f"{report_type}.zip"
            except (requests.exceptions.RequestException, Exception) as e:
                if response is not None:
                    self.logger.error("[download_report] Error: %s", e)
                return None

        def unzip_file(file_path) -> str | None:
            self.logger.info("Unzipping zip file %s", file_path)
            try:
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    for member in zip_ref.infolist():
                        self.logger.info(member.filename)
                        _, file_ext = os.path.splitext(member.filename)
                        if file_ext == ".csv":
                            zip_ref.extract(member.filename)
                            return member.filename
            except (zipfile.BadZipFile, FileNotFoundError, Exception) as e:
                self.logger.error("[unzip_file] Error: %s", e)
                return None

        if url:
            downloaded_file = download_report(report_type, url)
            if not downloaded_file:
                self.logger.error("[download_and_load_report] Error downloading report")
                return False

            self.logger.info("[download_and_load_report] Report %s downloaded successfully", downloaded_file)
            saved_file_path = unzip_file(downloaded_file)
            if not saved_file_path:
                self.logger.error("[download_and_load_report] Error saving downloaded file")
                return False

            if not self.write_to_bq(saved_file_path, table_id):
                self.logger.error("[download_and_load_report] Error loading report to BQ")
                return False
            self.logger.info("[download_and_load_report] %s report loaded successfully to BQ", report_type)
        else:
            self.logger.info("[download_and_load_report] No URL found for report type %s", report_type)

        return True

    def write_to_bq(self, file, table_id) -> bool:
        """
        Writes the data into a new table. If the table exists it is appended to.
        Args:
            table_id: the id of the table in Bigquery
            schema: a list of the table schema in Bigquery
            file: path to the jsonl file containing the data to insert
        Returns:
            True if data is successfully inserted, otherwise False
        """
        if os.stat(file).st_size == 0:
            self.logger.info("[write_table_to_bq] File %s is empty, no data to write", file)
            return True

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            column_name_character_map='V2'
        )

        with open(file, "rb") as fp:
            try:
                load_job = self.bq_client.load_table_from_file(
                    file_obj=fp,
                    destination=table_id,
                    job_config=job_config
                )
            except (ValueError, TypeError, Exception) as e:
                self.logger.error("[write_table_to_bq] Error while loading to BigQuery: %s", e)
                return False

            try:
                load_job.result()
            except Exception as e:
                self.logger.error("[write_table_to_bq] Load job failed/did not complete: %s", e)
                return False

        self.logger.info("[write_table_to_bq] Successfully written table: %s", table_id)
        return True

    def start(self) -> None:
        """Start the ELT automation"""
        try:
            report_types = {
                "campaign": {
                    "body": self.campaign_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("CAMPAIGN_PERFORMANCE_TABLE")}"
                },
                "adgroup": {
                    "body": self.adgroup_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("ADGROUP_PERFORMANCE_TABLE")}"
                },
                "account": {
                    "body": self.account_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("ACCOUNT_PERFORMANCE_TABLE")}"
                },
                "ad": {
                    "body": self.ad_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("AD_PERFORMANCE_TABLE")}"
                },
                "asset": {
                    "body": self.asset_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("ASSET_PERFORMANCE_TABLE")}"
                },
                "audience": {
                    "body": self.audience_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("AUDIENCE_PERFORMANCE_TABLE")}"
                },
                "conversion": {
                    "body": self.conversion_performance_request_body(),
                    "table": f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.{os.getenv("CONVERSION_PERFORMANCE_TABLE")}"
                }
            }

            for key, value in report_types.items():
                self.download_and_load_report(key, value["body"], value["table"])

        except Exception as e:
            self.logger.error("[start] Error: %s", e)
        finally:
            self.stop()

    def stop(self) -> None:
        """
        Stop the bot and perform clean up operations.
        """
        try:
            # Delete the downloads folder after the bot is done
            # and is saving content to the cloud. Otherwise don't delete.
            self.logging_client.close_logger()
            shutil.rmtree("downloads")
        except Exception as e:
            self.logger.error("[stop] Error: %s", e)
