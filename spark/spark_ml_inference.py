import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import joblib
from sklearn.preprocessing import StandardScaler
from pyspark.sql.functions import *

import pandas as pd
import numpy as np

# Build SparkSession
spark = SparkSession.builder.appName("spark-cleansing").getOrCreate()

df = spark.read.parquet("/opt/airflow/data_raw/application_record.parquet", header=True, inferSchema=True)
df.printSchema()

rows = df.count()
cols = len(df.columns)

print(f'Dimensions of Data: {(rows,cols)}')
print(f'Rows of Data: {rows}')
print(f'Columns of Data: {cols}')

def convert_to_year(days):
    if days > 0:
        year = 0
        return year
    year = (round(days / -365))
    return year

# Transform DAYS_BIRTH and DAYS_EMPLOYED
df_transform1 = df.withColumn("YEARS_AGE", F.round(F.abs(df["DAYS_BIRTH"] / 365.25))) \
                    .withColumn("YEARS_EMPLOYED", F.round(F.abs(df["DAYS_EMPLOYED"] / 365.25))) \
                    .drop("DAYS_BIRTH") \
                    .drop("DAYS_EMPLOYED")

# df_transform1.show(5)

def occupation_transform(OCCUPATION_TYPE, YEARS_EMPLOYED):
    if OCCUPATION_TYPE is not None:
        return OCCUPATION_TYPE
    elif YEARS_EMPLOYED is not None:
        return 'unknown' if YEARS_EMPLOYED > 0 else 'unemployed'
    else:
        return None

occupation_transform_udf = udf(occupation_transform, StringType())

new_value = when(col("OCCUPATION_TYPE").isNotNull(), col("OCCUPATION_TYPE")) \
    .otherwise(occupation_transform_udf(col("OCCUPATION_TYPE"), col("YEARS_EMPLOYED")))

df_transform2 = df_transform1.withColumn("OCCUPATION_TYPE", new_value)

# Drop nan 
def drop_nan(df):
    return df.dropna()

df_transform3 = drop_nan(df_transform2)

df_bersih = df_transform3.toPandas().to_csv("/opt/airflow/data_clean/application_record_clean.csv", index=False)

df_transform3 = df_transform3.drop("FLAG_MOBIL", "FLAG_EMAIL")

# Encode Categorical Field
encoder = joblib.load('/opt/airflow/spark/encoder.pkl')

categorical_columns = ['CODE_GENDER',
                       'FLAG_OWN_CAR',
                       'FLAG_OWN_REALTY',
                       'NAME_INCOME_TYPE',
                       'NAME_EDUCATION_TYPE',
                       'NAME_FAMILY_STATUS',
                       'NAME_HOUSING_TYPE',
                       'OCCUPATION_TYPE',
                       'FLAG_WORK_PHONE',
                       'FLAG_PHONE'
                       ]

#Mengonversi DataFrame PySpark menjadi DataFrame pandas
df_transform3_pandas = df_transform3.toPandas()
df_transform3_pandas.head()

df_transform3_pandas[categorical_columns] = encoder.fit_transform(df_transform3_pandas[categorical_columns])
df_encoded = df_transform3_pandas.copy()

df_encoded_cleaned = drop_nan(df_encoded)

def scaling(df):
    scaler = StandardScaler()
    return scaler.fit_transform(df)

df_id = df_encoded_cleaned[['ID']]
df_encoded_cleaned = df_encoded_cleaned.drop(["ID"],axis=1)

df_scaled = scaling(df_encoded_cleaned)

ml_model = joblib.load("/opt/airflow/spark/ml_model.pkl")
df_id['CLASS'] = pd.Series(ml_model.predict(df_scaled))
df_id = df_id.dropna()
df_id = df_id[['ID', 'CLASS']]

df_fixed = df_id.to_csv("/opt/airflow/data_clean/ml_result.csv", index=False)
