from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, size, explode
from pyspark.sql.window import Window
from logging_config import logger
import glob
import os

def create_spark_session():
    """
    To create and return spark session
    """
    spark = SparkSession.builder \
        .appName("CRM Campaign Engagement") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    return spark


def find_latest_file(directory, pattern):
    """
    To get the latest file from given directory

    """
    list_of_files = glob.glob(os.path.join(directory, pattern))
    if not list_of_files:
        raise FileNotFoundError(f"No files found in {directory} matching pattern {pattern}")
    latest_file = max(list_of_files, key=os.path.getmtime)
    return latest_file


def process_campaign_data(spark, input_path, output_path):
    """
    To Process campagin data
    This function will data from s3 buckets,
    process them and generate mainly two reports,
    Current_campaign_engagement and campaign_overview.
    
    Args:
        spark : spark session
        input_path : input path where data gets uploaded daily
        output_path : outputh path where reports needs to be generated
    """
    logger.info(f'Define the file patterns')
    campaigns_pattern = "supplier_file_campaigns_*.json"
    engagement_pattern = "supplier_file_engagement_*.json"
    
    logger.info('Read campaigns and engagements file from S3')
    campaigns_file = find_latest_file(os.path.join(input_path, "daily_files"), campaigns_pattern)
    engagement_file = find_latest_file(os.path.join(input_path, "daily_files"), engagement_pattern)

    logger.info(f'load data into campaigns_file: {campaigns_file}')
    campaigns_df = spark.read.option("multiline", "true").json(campaigns_file)
    engagement_df = spark.read.option("multiline", "true").json(engagement_file)

    logger.info(f'campaigns_df: {campaigns_df.show()}')
    logger.info(f'engagement_df: {engagement_df.show()}')

    logger.info(f'Ensure DataFrame schema is as expected')
    if '_corrupt_record' in campaigns_df.columns or '_corrupt_record' in engagement_df.columns:
        raise ValueError("Input JSON files contain corrupt records.")

    logger.info(f'extract campaign information')
    campaigns_info = campaigns_df.select(
        col("id").alias("campaign_id"),
        col("details.name").alias("campaign_name"),
        explode(col("steps")).alias("step"),
        col("details.schedule")[0].alias("start_date"),
        col("details.schedule")[1].alias("end_date")
    ).select(
        col("campaign_id"),
        col("campaign_name"),
        col("step.templateId").alias("template_id"),
        col("start_date"),
        col("end_date")
    )

    logger.info(f'Ensure all steps are delivered at least once')
    engagement_df = engagement_df.withColumn("action", col("action").alias("event"))

    logger.info(f'Handle duplicates (at-least-once delivery)')
    engagement_df = engagement_df.dropDuplicates(["userId", "eventTimestamp", "action", "campaign"])

    logger.info(f'Calculate number of steps for each campaign')
    steps_count = campaigns_info.groupBy("campaign_id", "campaign_name").agg(count("template_id").alias("number_of_steps"))

    logger.info(f'Calculate completion percentage')
    delivered_events = engagement_df.filter(col("action") == "MESSAGE_DELIVERED")
    engagement_count = delivered_events.groupBy("campaign").agg(count("*").alias("count"))

    completion_df = steps_count.join(engagement_count, steps_count.campaign_id == engagement_count.campaign, "left") \
        .withColumn("average_percent_completion", (col("count") / col("number_of_steps")).cast("double")).fillna(0)

    logger.info(f'Generating the Current Campaign Engagement Report')
    window_spec = Window.orderBy(col("average_percent_completion").desc())
    engagement_report = completion_df.select(
        col("campaign_name"),
        col("average_percent_completion"),
        expr("rank() over (order by average_percent_completion desc)").alias("rank")
    )
    logger.info(f'engagement_report: {engagement_report}')

    logger.info(f'Generating the Campaign Overview Report')
    campaign_overview = campaigns_info.select(
        col("campaign_id"),
        col("campaign_name"),
        col("template_id"),
        col("start_date"),
        col("end_date")
    )
    logger.info(f'campaign_overview_report: {campaign_overview}')

    # Save the reports to S3
    engagement_report.write.mode("overwrite").json(f"{output_path}/reports/current_campaign_engagement")
    campaign_overview.write.mode("overwrite").json(f"{output_path}/reports/campaign_overview")
