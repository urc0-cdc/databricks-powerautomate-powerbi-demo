# How to use

## Power BI

Opening the Power BI report the first time will prompt you to authenticate to the Databricks data source. It is recommended that you use a [personal access token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Click%20Settings%20in%20the%20lower%20left%20corner%20of,a%20secure%20location.%20Revoke%20a%20personal%20access%20token).

![image](https://user-images.githubusercontent.com/129994665/231882221-8a789da1-003b-427b-ab81-13db5a377331.png)

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
