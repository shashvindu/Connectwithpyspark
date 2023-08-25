# Connectwithpyspark

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataConnections") \
    .getOrCreate()

# Load credentials from JSON file
with open('credentials.json') as json_file:
    credentials = json.load(json_file)

# Connecting to Teradata
teradata_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:teradata://{credentials['teradata']['host']}/DATABASE={credentials['teradata']['database']}") \
    .option("dbtable", "YourTableName") \
    .option("user", credentials['teradata']['user']) \
    .option("password", credentials['teradata']['password']) \
    .load()

# Connecting to Azure SQL
azure_sql_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{credentials['azure_sql']['server']};databaseName={credentials['azure_sql']['database']}") \
    .option("dbtable", "YourTableName") \
    .option("user", credentials['azure_sql']['user']) \
    .option("password", credentials['azure_sql']['password']) \
    .load()

# Connecting to DB2
db2_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:db2://{credentials['db2']['host']}:{credentials['db2']['port']}/{credentials['db2']['database']}") \
    .option("dbtable", "YourTableName") \
    .option("user", credentials['db2']['user']) \
    .option("password", credentials['db2']['password']) \
    .load()

# Connecting to Oracle
# You would use jdbc with a similar approach as above

# Connecting to MySQL
mysql_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://{credentials['mysql']['host']}/{credentials['mysql']['database']}") \
    .option("dbtable", "YourTableName") \
    .option("user", credentials['mysql']['user']) \
    .option("password", credentials['mysql']['password']) \
    .load()

# Connecting to Azure Blob Storage
azure_blob_df = spark.read \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .load(f"wasbs://{credentials['azure_blob']['container_name']}@{credentials['azure_blob']['storage_account']}.blob.core.windows.net/path/to/data.csv")

# Connecting to Azure ADLS using Service Principal
spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<YOUR_ADLS_ACCOUNT_NAME>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<YOUR_ADLS_ACCOUNT_NAME>.dfs.core.windows.net", credentials['azure_adls']['client_id'])
spark.conf.set("fs.azure.account.oauth2.client.secret.<YOUR_ADLS_ACCOUNT_NAME>.dfs.core.windows.net", credentials['azure_adls']['client_secret'])
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<YOUR_ADLS_ACCOUNT_NAME>.dfs.core.windows.net", f"https://login.microsoftonline.com/{credentials['azure_adls']['tenant_id']}/oauth2/token")

adls_df = spark.read \
    .format("csv") \
    .load("abfss://path/to/data.csv")

# Perform operations with the loaded dataframes
# ...

# Stop the Spark session when done
spark.stop()
