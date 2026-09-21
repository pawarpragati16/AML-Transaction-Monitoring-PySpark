# AML-Transaction-Monitoring-PySpark
AML transaction monitoring project using PySpark, Databricks, Delta Lake and Power BI.
# AML Transaction Monitoring using PySpark

## Project Overview

This project develops an AML (Anti-Money Laundering) Transaction Monitoring system using PySpark and Databricks.

The project processes financial transaction data, performs data cleaning and transformation, creates risk-related features, identifies suspicious transactions, detects high-value anomalies, generates alerts, and presents the results through a Power BI dashboard.

## Problem Statement

Financial institutions process a large number of transactions, making it difficult to manually identify transactions that may require further investigation.

This project uses PySpark to process transaction data, identify risk indicators, detect unusual transactions, generate alerts for review, and visualize the results using Power BI.

## Objectives

- Process financial transaction data using PySpark
- Clean and validate transaction data
- Create risk-related features
- Identify high-risk transactions
- Detect large and anomalous transactions
- Generate suspicious scores and alert levels
- Create final AML review alerts
- Store processed data in Delta Lake
- Build an interactive Power BI dashboard

## Technology Stack

- Python
- PySpark
- Databricks
- Delta Lake
- SQL
- Power BI

## Project Workflow

Dataset
↓
Data Cleaning
↓
Data Transformation
↓
Feature Engineering
↓
AML Monitoring
↓
Anomaly Detection
↓
Risk & Alert Analysis
↓
Final AML Dataset
↓
Delta Lake
↓
Power BI Dashboard

## Data Processing

The dataset contains 10,000 financial transaction records.

The following data-quality checks were performed:

- Missing value checking
- Duplicate checking
- Negative transaction amount checking
- Risk score validation

Risk-related features were then created, including:

- Risk Category
- High Risk Flag
- Large Transaction Flag
- Cross-Border Flag
- Shell Company Flag
- Amount Anomaly Flag
- Suspicious Score
- Alert Level
- Final Alert

## AML Monitoring

A project-defined suspicious score was created using multiple risk indicators:

- High-risk transaction
- Large transaction
- Cross-border transaction
- Shell company involvement

Transactions were then classified into Low, Medium, and High alert levels based on the suspicious score.

## Anomaly Detection

Transaction amounts were analyzed using the 95th percentile as a threshold for identifying unusually high-value transactions.

A transaction was marked for review when:

- Alert Level was High, OR
- Amount Anomaly Flag was 1

## Key Results

- 10,000 transactions processed
- 4,054 high-risk transactions
- 4,392 transactions flagged by the AML monitoring rule
- 3,637 transactions classified as Review
- Final dataset stored as a Delta table
- Power BI dashboard created for interactive analysis

## Delta Lake

The final processed dataset was saved as a Delta table:

`workspace.default.aml_alerts`

The final table contains the processed transaction data together with risk indicators and alert information.

## Power BI Dashboard

The Power BI dashboard provides interactive analysis using:

- Year
- Country
- Transaction Type
- Alert Level

The dashboard includes:

- Total Transactions
- Total Transaction Amount
- High Risk Transactions
- Average Risk Score
- Transaction & Alert Trend
- Transaction Type Analysis
- Transactions by Country
- Risk Category Distribution

## Project Files

- `AML_Transaction_Monitoring.py` – PySpark and Databricks project code
- `README.md` – Project documentation

## Disclaimer

The risk categories, suspicious score, alert levels, and anomaly detection rules used in this project are project-defined rules for analytical and educational purposes. They do not represent official AML regulatory rules.
