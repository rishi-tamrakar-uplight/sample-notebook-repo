# Databricks notebook source
print("hello")



df = spark.read.format("csv").load("s3://uplight-oe-sample-stg-us/sample_data.csv")
df.show(10, False)
print(df.count())
df.show(1, False)

df.write.format('csv').save("s3://uplight-oe-sample-stg-us/output_data_run_2.csv")

