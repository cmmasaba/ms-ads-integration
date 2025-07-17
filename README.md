# MICROSOFT ADS INTEGRATION
Download data on ad performance from Microsoft Ads and store it in BigQuery for further processing.<br>
[Official Docs](https://learn.microsoft.com/en-us/advertising/guides/get-started?view=bingads-13)

## Set Up Steps
These steps outline how to get necessary tokens and access to the MS Ads platform:
1. Register your application on Azure to get `CLIENT_ID`, `CLIENT_SECRET` and `TENANT_ID`([docs](https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-register?view=bingads-13)).
2. Get the `DEVELOPER_TOKEN` for MS Ads Developer Portal ([docs](https://learn.microsoft.com/en-us/advertising/guides/get-started?view=bingads-13#get-developer-token)).
3. Get the `CUSTOMER_ID (also called Manager Account ID)` and `CUSTOMER_ACCOUNT_ID (also called Account ID)` from MS Ads portal ([docs](https://avanser.zohodesk.com/portal/en/kb/articles/microsoft-ads-how-to-find-your-customer-id-and-account-id)).
2. Request user consent ([docs](https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-consent?view=bingads-13)).
3. Get access and refresh tokens ([docs](https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-get-tokens?view=bingads-13)).
4. Fill in the necessary environment variables in the .env file
5. Ready to make API calls.

