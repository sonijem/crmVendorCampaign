from pytest_bdd import scenarios, given, when, then
import os
import shutil
from logging_config import logger

scenarios('../features/process_data.feature')

@given("the campaign and engagement data is available")
def campaign_engagement_data():
    logger.info(f'Create test_data directory structure')
    if not os.path.exists("tests/test_data/daily_files"):
        os.makedirs("tests/test_data/daily_files")

    # Copy test data to the directory
    shutil.copy("tests/test_data/supplier_file_campaigns_20230101001500.json", "tests/test_data/daily_files/supplier_file_campaigns_20230101001500.json")
    shutil.copy("tests/test_data/supplier_file_engagement_20230101001500.json", "tests/test_data/daily_files/supplier_file_engagement_20230101001500.json")


@when("the data is processed")
def process_data(spark):
    from src.data_processing import process_campaign_data
    input_path = "tests/test_data"
    output_path = "tests/test_output"
    process_campaign_data(spark, input_path, output_path)


@then("the current campaign engagement report is generated")
def check_engagement_report():
    output_path = "tests/test_output/reports/current_campaign_engagement"
    assert os.path.exists(output_path)
    logger.info(f'current campaign engagement report exist in directory')


@then("the campaign overview report is generated")
def check_overview_report():
    output_path = "tests/test_output/reports/campaign_overview"
    assert os.path.exists(output_path)
    logger.info(f'campaign overview report exist in directory')