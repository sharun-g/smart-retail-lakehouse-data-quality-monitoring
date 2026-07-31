# Databricks notebook source
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo

execution_timestamp = datetime.now(
    ZoneInfo("Asia/Kolkata")
).strftime("%d-%b-%Y %I:%M:%S %p")


TOPIC_ARN = "YOUR_SNS_TOPIC_ARN"

sns = boto3.client(
    "sns",
    region_name="us-east-1",
    aws_access_key_id="YOUR_AWS_ACCESS_KEY",
    aws_secret_access_key="YOUR_AWS_SECRET_KEY"
)

try:

    print("Running Bronze...")
    dbutils.notebook.run("01_bronze", 0)

    print("Running Silver...")
    dbutils.notebook.run("02_silver", 0)

    print("Running Gold...")
    dbutils.notebook.run("03_gold", 0)

    print("Running SNS...")
    dbutils.notebook.run("04_sns_notification", 0)

    print("Pipeline Completed Successfully")

except Exception as e:

    sns.publish(
        TopicArn="YOUR_SNS_TOPIC_ARN",
        Subject="Smart Retail Lakehouse | Pipeline FAILED",
        Message=f"""
Pipeline Status : FAILED

Execution Time : {execution_timestamp}

Error:

{str(e)}
"""
    )
    raise