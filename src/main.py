from data_processing import create_spark_session, process_campaign_data

if __name__ == "__main__":
    spark = create_spark_session()
    input_path = "s3://path_to_bucket/input"
    output_path = "s3://path to bucket/output"
    process_campaign_data(spark, input_path, output_path)
