from pyspark.sql.functions import explode, monotonically_increasing_id, explode_outer, col, struct, collect_set, posexplode_outer, row_number
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import broadcast, spark_partition_id
from databricks.sdk.runtime import *
import re


def clean_up_alligators(text):
    newtext = re.sub(r'<[^<>]*>', '', text)
    if text==newtext:
        return text
    else:
        return clean_up_alligators(newtext)


def pass_down_prefix(lst, sep, dropLevelNames):
    if len(lst)>1 and not dropLevelNames:
        return [lst[0]+sep+lst[1]]+lst[2:]
    elif len(lst)>1:
        return lst[1:]
    else:
        return lst


def flatten_structs(data, sep, dropLevelNames):
    struct_columns = [(column_name, column_dtype) for column_name, column_dtype in data.dtypes if column_dtype.startswith("struct")]
    for column_flatten, column_dtype in struct_columns:
        for column_to_shorten in [column[:column.find(":")] for column in clean_up_alligators(column_dtype[len("struct<"):-1]).split(",")]:
            data = data.withColumn((column_flatten+sep if not dropLevelNames else "") + column_to_shorten, col(column_flatten+'.'+column_to_shorten))
        data = data.drop(column_flatten)
    return data, [s for s,c in struct_columns]


def add_index(data, row_number_name):
    return data.withColumn(row_number_name, monotonically_increasing_id())


def json_normalize_recursive(data, name, prefix, sep="_"):
    struct_columns=[]
    while any([column_name for column_name, column_dtype in data.dtypes if column_dtype.startswith("struct")]):
        data, more_struct_columns = flatten_structs(data, sep, False)
        struct_columns = struct_columns + more_struct_columns
    
    columns_to_explode = [column_name for column_name, column_dtype in data.dtypes if column_dtype.startswith("array")]
    prefix_name = prefix + ("_"+name if name else "")
    index_name = prefix_name + "_row_number"
    data_with_id = add_index(data, index_name).persist()
    data_with_id.select(
        list(set(data_with_id.columns) - set(columns_to_explode))
        ).write.option("overwriteSchema", "true").saveAsTable(prefix_name, mode="overwrite")
    spark.sql("OPTIMIZE "+prefix_name)
    sub_columns_to_explode = []
    for column_to_explode in columns_to_explode:
        sub_columns_to_explode += json_normalize_recursive(
            data_with_id.select(index_name, explode_outer(column_to_explode).alias(column_to_explode)), 
            column_to_explode, 
            prefix, 
            sep
            )
    data_with_id.unpersist()
    return [prefix_name] + sub_columns_to_explode


def save_and_read_intermediate_result(df, table_name):
    df.write.option("overwriteSchema", "true").saveAsTable(table_name, mode="overwrite")
    spark.sql("OPTIMIZE "+table_name)
    return spark.read.table(table_name)


def compress_tables(order_of_columns):
    dict_of_mapping = {}

    for column in reversed(order_of_columns):
        data = spark.read.table(column)
        if column + "_row_number" in dict_of_mapping:
            for c in dict_of_mapping[column + "_row_number"]:
                data = data.join(spark.read.table(c), column + "_row_number")
            data = save_and_read_intermediate_result(
                data,
                column+"_joined_objects"
                )
        distinct_objects = save_and_read_intermediate_result(
            data.groupBy([c for c in data.columns if not c.endswith("_row_number")]).agg(F.max(column + "_row_number").alias(column + "_distinct_id")),
            column+"_distinct_objects"
            )
        
        if any([c for c in data.columns if c.endswith("_row_number") and c != column + "_row_number"]):
            higher_id_name = [c for c in data.columns if c.endswith("_row_number") and c != column + "_row_number"][0]
            data_grouped = data.join(
                distinct_objects, 
                [data[c].eqNullSafe(distinct_objects[c]) for c in data.columns if not c.endswith("_row_number")]
                ).groupBy(
                    higher_id_name
                    ).agg(collect_set(column + "_distinct_id").alias(column + "_distinct_ids"))
            new_data = data_grouped.groupBy(column + "_distinct_ids").agg(F.max(higher_id_name).alias(column + "_list_id"))
            new_data.select(
                explode(column+"_distinct_ids").alias(column+"_distinct_id"), 
                column + "_list_id"
                ).write.option("overwriteSchema", "true").saveAsTable(column+"_distinct_elements", mode="overwrite")
            spark.sql("OPTIMIZE "+column+"_distinct_elements")
            new_data.join(
                data_grouped, 
                column + "_distinct_ids"
                ).drop(
                    column + "_distinct_ids"
                    ).write.option("overwriteSchema", "true").saveAsTable(column+"_mapping", mode="overwrite")
            spark.sql("OPTIMIZE "+column+"_mapping")
            if higher_id_name in dict_of_mapping:
                dict_of_mapping[higher_id_name] = dict_of_mapping[higher_id_name]+[column+"_mapping"]
            else:
                dict_of_mapping[higher_id_name] = [column+"_mapping"]


def delete_duped_tables(data_prefix):
    allt = spark.sql(f"""SHOW TABLES LIKE '{data_prefix}*'""").select("tableName").collect()
    for table_to_be_deleted in [t[0] for t in allt if not '_distinct_elements' in t[0] and not '_distinct_objects' in t[0]]:
        spark.sql(f"drop table {table_to_be_deleted}")
    return [t[0] for t in allt if '_distinct_elements' in t[0] or '_distinct_objects' in t[0]]


def convert_to_csv(tables_to_convert, csv_path):
    tsize = 0
    for table in tables_to_convert:
        spark.table(table).repartition(1).write.csv(csv_path + table[(table.rfind(".")+1):], mode="overwrite")
        isize = max([f[2] for f in dbutils.fs.ls(csv_path + table[(table.rfind(".")+1):])])
        for csv in dbutils.fs.ls(csv_path+table[(table.rfind(".")+1):]):
            if csv[0].endswith(".csv"):
                dbutils.fs.mv(csv[0], csv_path+table[(table.rfind(".")+1):]+".csv")
        dbutils.fs.rm(csv_path+table[(table.rfind(".")+1):], True)
        tsize += isize

    return tsize


def get_size_of_tables(data_prefix):
    t = 0
    for table_name in spark.sql(f"""SHOW TABLES LIKE '{data_prefix}*'""").select("tableName").collect():
        t += spark.sql("describe detail "+table_name[0]).collect()[0].sizeInBytes
    return t


def proper_json_normalize(df, path_to_tables, csv_path=None):
    """Normalizes a dataframe of structured data across multiple tables.


    Parameters:
    df (pyspark.sql.DataFrame): The data as a pyspark dataframe.
    path_to_table (string): The path to tables: will serve as prefix to all data written.
    csv_path (string): Specify if you wish the data to be written to CSVs in addition to tables.

    Returns:
    list[str]: The names of the generated table
    """
    order_of_columns = json_normalize_recursive(df, None, path_to_tables)
    print(f"size of tables (in bytes) just before compressing: {get_size_of_tables(path_to_tables)}")
    compress_tables(order_of_columns)
    final_tables = delete_duped_tables(path_to_tables)
    print(f"size of tables (in bytes) right after compressing: {get_size_of_tables(path_to_tables)}")
    
    if csv_path:
        print(f"size of csvs (in bytes) generated from the tables: {convert_to_csv(final_tables, csv_path)}")
    return final_tables


def generate_query(data_prefix, cols):
    """Generate a query that can be executed in spark sql.
    The proper tables will be joined based on what columns are requested.


    Parameters:
    data_prefix (string): The prefix on all spark tables generated by proper_json_normalize.
    cols (list[str]): Full path in the original json object to retrieve as columns. Underscores or periods may be used.

    Returns:
    string: A query that can be executed in spark sql to retrieve the desired columns.
    """
    all_potential_tables = list(set([i[0][:i[0].find("_distinct")] for i in spark.sql(f"""SHOW TABLES LIke '{data_prefix}*'""").select("tableName").collect() if "_distinct" in i[0]]))
    prefixed_cols = [(f"{data_prefix}_" if not col.startswith(data_prefix) else "") + col.replace(".", "_") for col in cols]
    if len(cols)==0:
        return
    all_potential_tables.sort()
    statement = None
    for table in all_potential_tables:
        if all([col.startswith(table) for col in prefixed_cols]):
            statement = f" from {table}_distinct_objects"
        elif any([col.startswith(table) for col in prefixed_cols]):
            last_table = next((t for t in reversed(all_potential_tables) if table.startswith(t) and t!=table), None)
            statement += f""" join {table}_distinct_elements on {last_table}_distinct_objects.{table}_list_id = {table}_distinct_elements.{table}_list_id join {table}_distinct_objects on {table}_distinct_objects.{table}_distinct_id = {table}_distinct_elements.{table}_distinct_id"""
        
    return "select " + ", ".join([next((t for t in reversed(all_potential_tables) if c.startswith(t)), "")+"_distinct_objects."+(c if c.endswith("_distinct_id") or c.endswith("_list_id") else c[len(f"{data_prefix}_"):])+" as " + c_original for c, c_original in zip(prefixed_cols, cols)]) + statement