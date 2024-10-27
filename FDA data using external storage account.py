# Databricks notebook source
# MAGIC %md
# MAGIC # Compressing 5 TB of jsons into 5 GB through data normalization
# MAGIC
# MAGIC It's simple: JSONs take up a lot of space. The nested format is very inefficient with redundant value entries. Any data engineer who has written custom code to clean up even the easiest files using progressive explode's and distinct's knows this is a cumbersome and ugly process. Shared here is a module based in [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) that iteratively explodes and normalizes a df of structured data across multiple tables in Hive metastore (call [setCurrentCatalog](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.setCurrentCatalog.html) or [setCurrentDatabase](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.setCurrentDatabase.html) to use Unity Catalog instead) no matter the underlying schema.
# MAGIC
# MAGIC Please understand the functionality extends beyond Pandas' [json_normalize](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html). That method returns one dataframe, whereas by breaking the data into multiple tables based on the objects within a json, proper_json_normalize allows for only distinct values to be stored and drastically cuts down costs.
# MAGIC
# MAGIC I demo this method on the FDA Drug Adverse Event dataset available [here](https://open.fda.gov/data/downloads/). The first four cells demo downloading the data, unzipping, and chunking for ease of ingestion. The later cells demo using the shared module. The script will take care of deleting the original jsons and zipped folders, but will leave any generated tables and csvs (assuming those csvs are not placed in the folder defined as 'folder_for_data')

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grab all files to download
# MAGIC Please view the data disclaimer provided [here](https://open.fda.gov/apis/drug/event/). In addition to the warnings contained there, I have noticed discrepencies between the report counts published via JSON and the counts available on the [FAERS dashboard](https://www.fda.gov/drugs/questions-and-answers-fdas-adverse-event-reporting-system-faers/fda-adverse-event-reporting-system-faers-public-dashboard) that it is supposed to be based upon. I was not able to reconcile these conflicts, so please only view this notebook as a demo of the proper_json_normalize library, not as any sort of report or study of the FDA data.

# COMMAND ----------

import json
import urllib

folder_for_data = "/mnt/jsons/all_fda_drug_adverse_event_files"
dbutils.fs.mkdirs(folder_for_data)
json_file = urllib.request.urlopen('https://api.fda.gov/download.json')
json_data = json.load(json_file)
files_to_retrieve = [j["file"] for j in json_data["results"]["drug"]["event"]["partitions"]]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download and unzip all files
# MAGIC You will see all downloaded files overwrite temp.zip until they're unzipped.

# COMMAND ----------

import urllib
import zipfile

total_size = 0
for url in files_to_retrieve:
    urllib.request.urlretrieve(url, f"/dbfs{folder_for_data}/temp.zip")
    ffirsthalf = url[:url.rfind("/")]
    with zipfile.ZipFile(f"/dbfs{folder_for_data}/temp.zip", 'r') as zip_ref:
        zip_ref.extractall("/dbfs"+folder_for_data+ffirsthalf[ffirsthalf.rfind("/"):])
        total_size += sum([f.size for f in dbutils.fs.ls(folder_for_data+ffirsthalf[ffirsthalf.rfind("/"):])])

total_size


# COMMAND ----------

# MAGIC %md
# MAGIC ### Json chunking with ijson
# MAGIC I've had negative experiences in Pyspark with json objects over 1 GB in size, and since each json file is one object in the FDA dataset, they can get quite big. I take the precaution of using ijson to chunk each file into 100 mb files, each composed only of "results" objects, dropping the extraneous metadata object in each file.

# COMMAND ----------

import ijson
import json
def chunk_file(fname):
    dbutils.fs.mkdirs(fname[len("/dbfs"):])
    num_files_chunked_into = 0
    with open(fname+".json", 'r') as file:
        array_items = ijson.items(file, 'results.item')
        string_to_write = ""

        for item in array_items:
            string_to_write += json.dumps(item) +","
            if len(string_to_write)>100*1024*1024:
                with open(fname+"/chunk"+str(num_files_chunked_into)+".json", "w") as f:
                    f.write("["+string_to_write[:-1]+"]")
                string_to_write = ""
                num_files_chunked_into += 1
        with  open(fname+"/chunk"+str(num_files_chunked_into)+".json", "w") as f:
            f.write("["+string_to_write[:-1]+"]")
        string_to_write = ""
        num_files_chunked_into+=1
    return num_files_chunked_into


for folder in [f[0].replace("dbfs:/", "/dbfs/") for f in dbutils.fs.ls(folder_for_data) if not f[0].endswith(".json")]:
    fnames = [f[0].replace("dbfs:/", "/dbfs/")[:-5] for f in dbutils.fs.ls(folder[5:]) if f[0].endswith(".json")]
    for fname in fnames:
        chunk_file(fname)
        dbutils.fs.rm(fname[len("/dbfs"):]+".json", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculations on JSONs are slow
# MAGIC We read in the json dataset and perform a simple calculation: how many reported events were there for each drug brand name, sorted in descending order by count (NOTE: to keep the query simple, we are ignoring the "duplicate" flag in the original dataset, so again, please don't read into these statistics for a medical decision or a publication).

# COMMAND ----------

df = spark.read.json(f"{folder_for_data}/*/*/chunk*.json", multiLine=True)

# COMMAND ----------

from pyspark.sql import functions as F
df.withColumn(
    "unique_pt", F.monotonically_increasing_id()
).withColumn(
    "patient_drug", F.explode_outer("patient.drug")
).withColumn(
    "brandname", F.explode_outer("patient_drug.openfda.brand_name")
).groupBy(
    "brandname"
).agg(
    F.countDistinct("unique_pt").alias("count")
).orderBy(
    F.desc("count")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert to delta tables
# MAGIC Processing takes two steps: proper_json_normalize first converts data to tables, granting each struct in the original dataset it's own id and table. Afterwards, it creates a second set of tables with only the distinct elements from each. For example, the brand name "HUMIRA" may appear over a half million times, but it only needs one entry in the final brand name table. However, the brand name's distinct id needs to be propagated up to any tables that reference it. Therefore, starting with the most nested columns of the schema, proper_json_normalize will run a distinct on each struct and list (the generated tables will have suffixes "_distinct_objects" and "_distinct_elements", respectively) and will roll up the new ids. This allows all duplication of objects to be captured-no matter the complexity of the original object.
# MAGIC
# MAGIC ###(Optional) Generate csv copies of the tables
# MAGIC The generated tables are an extremely storage-efficient format. However, CSVs can be more popular for dissemination, so the "csv_path" option is offered. Provide a path if you wish for csvs to be generated: you will notice the text format takes significantly more space than the Hive metastore tables, but still much less than the JSONs published by the FDA.

# COMMAND ----------

import time
from proper_json_normalize import proper_json_normalize

data_prefix="fda_compression"
t = time.perf_counter()
final_tables = proper_json_normalize(
  df=df, 
  path_to_tables=data_prefix, 
  csv_path="dbfs:/FileStore/csvs/"
)
print(time.perf_counter()-t)
print(final_tables)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete those JSONs
# MAGIC Next, all the downloaded and chunked jsons are deleted, leaving the final tables in Hive metastore. For my Azure account, leaving five terrabytes of jsons up for a month would cost roughly $750!

# COMMAND ----------

dbutils.fs.rm(folder_for_data, True)

# COMMAND ----------

sql(f"""SHOW TABLES LIke '{data_prefix}*'""").select("tableName").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Query the tables
# MAGIC Normalizing the JSONs creates dozens of tables, and crafting sql against them is cumbersome. The last utility shared returns a query that can be run via spark.sql to reconstruct any desired columns from the original dataset. Use a prefix and underscore to indicate the path to a given struct, and select "_distinct_id" if you wish to identify distinct objects in the dataset.
# MAGIC
# MAGIC For example, to recreate the above query to calculate report counts by brand name, we run the following.

# COMMAND ----------

from proper_json_normalize import generate_query
from pyspark.sql import functions as F

data_prefix="fda_compression"
query = generate_query(
        data_prefix=data_prefix, 
        cols=[f"{data_prefix}_distinct_id", f"{data_prefix}_patient_drug_openfda_brand_name"]
        )
print(query)
spark.sql(
    query
).groupBy(
    f"{data_prefix}_patient_drug_openfda_brand_name"
).agg(
    F.countDistinct(f"{data_prefix}_distinct_id").alias("count")
).orderBy(
    F.desc("count")
).display()
