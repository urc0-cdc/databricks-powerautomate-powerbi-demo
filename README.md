# How to use

## Power BI

### Configure authentication for Databricks data connector in Power BI Desktop

Opening the Power BI report the first time will prompt you to authenticate to the Databricks data source. It is recommended that you use a [personal access token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Click%20Settings%20in%20the%20lower%20left%20corner%20of,a%20secure%20location.%20Revoke%20a%20personal%20access%20token).

![image](https://user-images.githubusercontent.com/129994665/231882221-8a789da1-003b-427b-ab81-13db5a377331.png)

### Configure Power Automate visual connector for Databricks Jobs API

Follow this screenshot guide to edit the visual to add a new flow with an HTTP action configured to send an HTTP request to the Databricks Jobs API

![image](https://user-images.githubusercontent.com/129994665/232079367-ac2158ad-17a3-45da-90cf-dc88acb10287.png)

![image](https://user-images.githubusercontent.com/129994665/232079439-78a87385-ae3f-45e1-b77c-c5e0431af99e.png)

![image](https://user-images.githubusercontent.com/129994665/232079511-d5937253-03c2-422c-934e-b31906673d40.png)

![image](https://user-images.githubusercontent.com/129994665/232079559-9021f463-7ad1-44d5-8791-c36ee8e85f2e.png)

At this stage you'll need to know your databricks url, your personal access token, and the id of the job you'd like to execute.
![image](https://user-images.githubusercontent.com/129994665/232080285-60416419-da09-48c1-9782-f2a691e08118.png)

Finally, make sure you apply the newly created flow to this button.
![image](https://user-images.githubusercontent.com/129994665/232079697-330368e9-7e3c-40df-b6c4-68800b74894c.png)

### Configure Databricks data source connector in app.powerbi.com workspace data source

Note: You will need a premium license to use the HTTP action in the Power Automate visual in a published report through app.powerbi.com.

If you publish the report to an app.powerbi.com, you will need to configure the data source. Find the newly created data source in your workspace.

![image](https://user-images.githubusercontent.com/129994665/232080822-678b3311-51bc-45b2-ac53-82caeedc28aa.png)

Next, use a Databricks personal access token to authenticate to the data source, just as you did in the report.

![image](https://user-images.githubusercontent.com/129994665/232080835-4c4af46a-b0f9-4a7d-a366-c12cd5babd8e.png)

## Databricks

To in each notebook ensure you update the configuration for connecting to your a blob storage account and to use a secret scope connected to a key vault with a valid key to that storage account. For more information on how to configure these items see the appendix.

![image](https://user-images.githubusercontent.com/129994665/231883880-29107a00-79a9-461b-810c-bf3fb47d6335.png)

# Appendix

## Set up a blob storage account, a key vault, a secret scope, and authenticate in a notebook
1. Create blob store and find the access key
2. Grant the 'Storage Blob Data Contributor' access role to the user you will create the Databricks secret scope under
3. Create key vault and create a secret (e.g. called blobStoreKey) where the value is access key of blob store
4. Grant the 'Key Vault Reader' or higher level access role to the user you will create the secret scope under
5. Create a secret scope (e.g. with scope name keyVaultScope), principal can be all users when you want everyone using the managed identity of this user to access the vault, with the dns name and resource id of the key vault
6. Authenticate in a notebook once by setting the spark configuration context like this
spark.conf.set("fs.azure.account.key.<storage account name>.dfs.core.windows.net", dbutils.secrets.get(scope="keyVaultScope", key="blobStoreKey"))
