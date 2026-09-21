# Databricks notebook source
print("AML Transaction Monitoring Project")


# COMMAND ----------

#Create SparkSession
from pyspark.sql import SparkSession

# COMMAND ----------

# 1. Check Spark
print("Spark Session:", spark)
print("Spark is ready!")

# COMMAND ----------

#2. File path
# IMPORTANT: Replace this with your actual Databricks file path


file_path = "/Volumes/workspace/default/aml/Big_Black_Money_Dataset.csv"


# COMMAND ----------

# 3. Read CSV using PySpark
# ------------------------------------------------------------

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

print("CSV loaded successfully!")

# COMMAND ----------

# 4. Display first 5 rows
# ------------------------------------------------------------

print("\n========== FIRST 5 ROWS ==========")
df.show(5, truncate=False)


# COMMAND ----------

# 5. Display schema
# ------------------------------------------------------------

print("\n========== SCHEMA ==========")
df.printSchema()


# COMMAND ----------

# 6. Display column names
# ------------------------------------------------------------

print("\n========== COLUMNS ==========")
print(df.columns)

# COMMAND ----------

# 7. Count total transactions

total_rows = df.count()

print("\n========== TOTAL TRANSACTIONS ==========")
print("Total rows:", total_rows)

# COMMAND ----------

# 8. Basic statistical summary
# ------------------------------------------------------------

print("\n========== STATISTICAL SUMMARY ==========")
df.describe().show()

# COMMAND ----------

# 9. Check missing values in every column
# ------------------------------------------------------------

from pyspark.sql.functions import col, sum

print("\n========== MISSING VALUES ==========")

missing_values = df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

missing_values.show()

# COMMAND ----------

# 10. Check duplicate transactions
# ------------------------------------------------------------

print("\n========== DUPLICATE ROWS ==========")

duplicate_count = df.count() - df.dropDuplicates().count()

print("Duplicate rows:", duplicate_count)

# COMMAND ----------

# 11. Check transaction types
# ------------------------------------------------------------

print("\n========== TRANSACTION TYPES ==========")

df.groupBy("Transaction Type") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(truncate=False)


# COMMAND ----------

# ------------------------------------------------------------
# 12. Check source of money
# ------------------------------------------------------------

print("\n========== SOURCE OF MONEY ==========")

df.groupBy("Source of Money") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(truncate=False)


# COMMAND ----------

# 13. Check reported-by-authority values
# ------------------------------------------------------------

print("\n========== REPORTED BY AUTHORITY ==========")

df.groupBy("Reported by Authority") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()


# COMMAND ----------

# 14. Check countries
# ------------------------------------------------------------

print("\n========== TOP COUNTRIES ==========")

df.groupBy("Country") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10, truncate=False)

# COMMAND ----------

# 15. Check industries
# ------------------------------------------------------------

print("\n========== INDUSTRIES ==========")

df.groupBy("Industry") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10, truncate=False)



# COMMAND ----------

# 16. Check money laundering risk score
# ------------------------------------------------------------

print("\n========== RISK SCORE SUMMARY ==========")

df.select("Money Laundering Risk Score").describe().show()



# COMMAND ----------

# 17. Check shell company involvement
# ------------------------------------------------------------

print("\n========== SHELL COMPANIES ==========")

df.groupBy("Shell Companies Involved") \
  .count() \
  .orderBy("Shell Companies Involved") \
  .show()


# COMMAND ----------

df_clean = df.dropDuplicates()

print("Original rows:", df.count())
print("Rows after removing duplicates:", df_clean.count())

# COMMAND ----------

#Check missing values again
from pyspark.sql.functions import col, sum

missing = df_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_clean.columns
])

missing.show()

# COMMAND ----------

#Check the important numeric columns
df_clean.select(
    "Amount (USD)",
    "Money Laundering Risk Score",
    "Shell Companies Involved"
).printSchema()

# COMMAND ----------



#Clean the Amount column
df_clean.filter(col("Amount (USD)") < 0).show()
print(
    "Negative amount records:",
    df_clean.filter(col("Amount (USD)") < 0).count()
)

# COMMAND ----------

#Check risk-score range
df_clean.select(
    "Money Laundering Risk Score"
).describe().show()

# COMMAND ----------

df_clean.filter(
    (col("Money Laundering Risk Score") < 0) |
    (col("Money Laundering Risk Score") > 10)
).show()

# COMMAND ----------


#Create a cleaned DataFrame

#If the checks show that negative amounts and invalid risk scores exist, we can remove those records:

df_clean = df_clean.filter(
    col("Amount (USD)") >= 0
).filter(
    (col("Money Laundering Risk Score") >= 0) &
    (col("Money Laundering Risk Score") <= 10)
).show()

# COMMAND ----------

df_clean = df.dropDuplicates()

df_clean.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Data Transformation: Risk Category

df_clean = df_clean.withColumn(
    "Risk_Category",
    when(col("Money Laundering Risk Score") <= 3, "Low")
    .when(col("Money Laundering Risk Score") <= 6, "Medium")
    .otherwise("High")
)

# COMMAND ----------

# Check Risk Categories

df_clean.select(
    "Money Laundering Risk Score",
    "Risk_Category"
).show(20)

# COMMAND ----------

# Count Risk Categories

df_clean.groupBy("Risk_Category") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()


# COMMAND ----------

# Data Transformation: High Risk Flag

df_clean = df_clean.withColumn(
    "High_Risk_Flag",
    when(col("Money Laundering Risk Score") >= 7, 1)
    .otherwise(0)
)
# Check High Risk Flag

df_clean.groupBy("High_Risk_Flag") \
    .count() \
    .show()

# COMMAND ----------

# Data Transformation: Large Transaction Threshold

percentile_95 = df_clean.approxQuantile(
    "Amount (USD)",
    [0.95],
    0.01
)[0]

print("95th percentile transaction amount:", percentile_95)

# Data Transformation: Large Transaction Flag

df_clean = df_clean.withColumn(
    "Large_Transaction_Flag",
    when(col("Amount (USD)") >= percentile_95, 1)
    .otherwise(0)
)

# COMMAND ----------

# Data Transformation: AML Monitoring Flag

df_clean = df_clean.withColumn(
    "AML_Monitoring_Flag",
    when(
        (col("High_Risk_Flag") == 1) |
        (col("Large_Transaction_Flag") == 1),
        1
    ).otherwise(0)
)

# View Transformed Data

df_clean.select(
    "Transaction ID",
    "Amount (USD)",
    "Money Laundering Risk Score",
    "Risk_Category",
    "High_Risk_Flag",
    "Large_Transaction_Flag",
    "AML_Monitoring_Flag"
).show(20, truncate=False)

# AML Monitoring Summary

df_clean.groupBy("AML_Monitoring_Flag") \
    .count() \
    .orderBy("AML_Monitoring_Flag") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Cross-Border Flag
df_clean = df_clean.withColumn(
    "Cross_Border_Flag",
    when(
        (col("Country").isNotNull()) &
        (col("Destination Country").isNotNull()) &
        (col("Country") != col("Destination Country")),
        1
    ).otherwise(0)
)

# Shell Company Flag
df_clean = df_clean.withColumn(
    "Shell_Company_Flag",
    when(col("Shell Companies Involved") > 0, 1)
    .otherwise(0)
)

# Suspicious Score
df_clean = df_clean.withColumn(
    "Suspicious_Score",
    col("High_Risk_Flag") +
    col("Large_Transaction_Flag") +
    col("Cross_Border_Flag") +
    col("Shell_Company_Flag")
)

# Alert Level
df_clean = df_clean.withColumn(
    "Alert_Level",
    when(col("Suspicious_Score") >= 3, "High")
    .when(col("Suspicious_Score") == 2, "Medium")
    .otherwise("Low")
)

# View Monitoring Results
df_clean.select(
    "Transaction ID",
    "Country",
    "Destination Country",
    "Amount (USD)",
    "Money Laundering Risk Score",
    "High_Risk_Flag",
    "Large_Transaction_Flag",
    "Cross_Border_Flag",
    "Shell_Company_Flag",
    "Suspicious_Score",
    "Alert_Level"
).show(20, truncate=False)

# Alert Summary
df_clean.groupBy("Alert_Level") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg

# Alert Level Analysis
df_clean.groupBy("Alert_Level") \
    .agg(
        count("Transaction ID").alias("Transaction_Count"),
        sum("Amount (USD)").alias("Total_Amount"),
        avg("Amount (USD)").alias("Average_Amount")
    ) \
    .orderBy("Transaction_Count", ascending=False) \
    .show()

# Country Risk Analysis
df_clean.groupBy("Country") \
    .agg(
        count("Transaction ID").alias("Transaction_Count"),
        sum("Amount (USD)").alias("Total_Amount"),
        avg("Money Laundering Risk Score").alias("Average_Risk_Score")
    ) \
    .orderBy("Total_Amount", ascending=False) \
    .show(10, truncate=False)

# Transaction Type Analysis
df_clean.groupBy("Transaction Type") \
    .agg(
        count("Transaction ID").alias("Transaction_Count"),
        sum("Amount (USD)").alias("Total_Amount"),
        avg("Money Laundering Risk Score").alias("Average_Risk_Score")
    ) \
    .orderBy("Total_Amount", ascending=False) \
    .show(truncate=False)

# High Alert Transactions
df_clean.filter(
    col("Alert_Level") == "High"
).select(
    "Transaction ID",
    "Country",
    "Destination Country",
    "Amount (USD)",
    "Transaction Type",
    "Suspicious_Score",
    "Alert_Level"
).orderBy(
    col("Amount (USD)").desc()
).show(20, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Anomaly Detection: High Amount Flag
df_clean = df_clean.withColumn(
    "Amount_Anomaly_Flag",
    when(
        col("Amount (USD)") > df_clean.approxQuantile(
            "Amount (USD)", [0.95], 0.01
        )[0],
        1
    ).otherwise(0)
)

# Anomaly Detection: Combined Alert
df_clean = df_clean.withColumn(
    "Final_Alert",
    when(
        (col("Alert_Level") == "High") |
        (col("Amount_Anomaly_Flag") == 1),
        "Review"
    ).otherwise("Normal")
)

# View Anomalies
df_clean.select(
    "Transaction ID",
    "Amount (USD)",
    "Money Laundering Risk Score",
    "Suspicious_Score",
    "Amount_Anomaly_Flag",
    "Alert_Level",
    "Final_Alert"
).orderBy(
    col("Amount (USD)").desc()
).show(20, truncate=False)

# Anomaly Summary
df_clean.groupBy("Final_Alert") \
    .count() \
    .show()

# COMMAND ----------

# Final AML Alert Dataset
df_alerts = df_clean.select(
    "Transaction ID",
    "Date of Transaction",
    "Country",
    "Destination Country",
    "Amount (USD)",
    "Transaction Type",
    "Industry",
    "Financial Institution",
    "Money Laundering Risk Score",
    "Risk_Category",
    "Shell Companies Involved",
    "High_Risk_Flag",
    "Large_Transaction_Flag",
    "Cross_Border_Flag",
    "Shell_Company_Flag",
    "Amount_Anomaly_Flag",
    "Suspicious_Score",
    "Alert_Level",
    "Final_Alert"
)

# View Final Dataset
df_alerts.show(10, truncate=False)

# Check Final Dataset Structure
df_alerts.printSchema()

# Check Number of Alert Records
print("Total alert records:", df_alerts.count())

# COMMAND ----------

df_alerts.show(10, truncate=False)

# COMMAND ----------

# Rename columns for Delta table
df_alerts_clean = df_alerts \
    .withColumnRenamed("Transaction ID", "transaction_id") \
    .withColumnRenamed("Date of Transaction", "transaction_date") \
    .withColumnRenamed("Country", "country") \
    .withColumnRenamed("Destination Country", "destination_country") \
    .withColumnRenamed("Amount (USD)", "amount_usd") \
    .withColumnRenamed("Transaction Type", "transaction_type") \
    .withColumnRenamed("Industry", "industry") \
    .withColumnRenamed("Financial Institution", "financial_institution") \
    .withColumnRenamed("Money Laundering Risk Score", "money_laundering_risk_score") \
    .withColumnRenamed("Risk_Category", "risk_category") \
    .withColumnRenamed("Shell Companies Involved", "shell_companies_involved") \
    .withColumnRenamed("High_Risk_Flag", "high_risk_flag") \
    .withColumnRenamed("Large_Transaction_Flag", "large_transaction_flag") \
    .withColumnRenamed("Cross_Border_Flag", "cross_border_flag") \
    .withColumnRenamed("Shell_Company_Flag", "shell_company_flag") \
    .withColumnRenamed("Amount_Anomaly_Flag", "amount_anomaly_flag") \
    .withColumnRenamed("Suspicious_Score", "suspicious_score") \
    .withColumnRenamed("Alert_Level", "alert_level") \
    .withColumnRenamed("Final_Alert", "final_alert")

# Check the new column names
print(df_alerts_clean.columns)

# COMMAND ----------

# Save AML alerts as Delta table
df_alerts_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("aml_alerts")

# COMMAND ----------

# Check AML table
spark.sql("SELECT * FROM aml_alerts LIMIT 10").show(truncate=False)

# COMMAND ----------

# Check current catalog and schema
spark.sql("SELECT current_catalog(), current_schema()").show()

# Show tables in the current schema
spark.sql("SHOW TABLES").show(truncate=False)