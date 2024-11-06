# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def compare_schemas(schema1, schema2):
    fields1 = {item.name: item.dataType for item in schema1}
    fields2 = {item.name: item.dataType for item in schema2}
    keys = set(fields1.keys()).union(set(fields2.keys()))
    discrepancies = []
    for key in keys:
        if key not in fields1:
            discrepancies.append(f"Column {key} is missing in first schema")
        if key not in fields2:
            discrepancies.append(f"Column {key} is missing in second schema")
        if key in fields1 and key in fields2 and fields1[key] != fields2[key]:
            discrepancies.append(f"Column {key} has different types: {fields1[key]} vs {fields2[key]}")

    return discrepancies


def harmonize_schemas(df1, df2):
    # Gather all columns from both DataFrames
    df1_columns = set(df1.columns)
    # Add missing columns as null
    for col in df1_columns:
        if col not in df2.columns:
            df2 = df2.withColumn(col, lit(None).cast(df1.schema[col].dataType))
    return df1, df2


def merge(df_main, df_upd, list_keys): 
  from pyspark.sql.functions import col
  
  df_main_cols = df_main.columns
  df_upd_cols = df_upd.columns
  
  #Adds the suffix "_tmp" to df_upd columns
  add_suffix = [df_upd[f'{col_name}'].alias(col_name + '_tmp') for col_name in df_upd.columns]
  df_upd = df_upd.select(*add_suffix)

  join_condition = [col(f"{k}") == col(f"{k+ '_tmp'}") for k in list_keys]
  #Doing a full outer join
  df_join = df_main.join(df_upd, join_condition, "fullouter")
  #Using the for loop to scroll through the columns
  for col in df_main_cols:
      #Implementing the logic to update the rows
      df_join = df_join.withColumn(col, when((df_main[col] != df_upd[col + '_tmp']) | (df_main[col].isNull()), df_upd[col + '_tmp']).otherwise(df_main[col]))
      
  #Selecting only the columns of df_main (or all columns that do not have the suffix "_tmp")
  df_final = df_join.select(*df_main_cols)
  #Returns the dataframe updated with the merge
  return df_final

if __name__ == "__main__":
    # Define the schemas explicitly
    schema1 = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    schema2 = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    
    data1 = [(1, "John Doe",44), (2, "Jane Doe",88),(3, "Dalton",55)]
    data2 = [(1, "John Doe"), (2, "Jane Doe"),(4, "test"),(5, "test1")]
    
    df1 = spark.createDataFrame(data=data1, schema=schema1)
    df2 = spark.createDataFrame(data=data2, schema=schema2)
    df1.write.format("delta").mode("overwrite").saveAsTable("df1_apo")
    df2.write.format("delta").mode("overwrite").saveAsTable("df2_apo")
    discrepancies = compare_schemas(df1.schema, df2.schema)
    print('discrepancies',discrepancies)
    primary_keys = ['id','name']
    df1, df2 = harmonize_schemas(df1, df2)
    df = merge(df_main=df1, df_upd=df2, list_keys=primary_keys)
    df.orderBy("id").write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("df2_apo")
