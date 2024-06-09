from pytest_bdd import scenarios, given, when, then
import os
import shutil
from logging_config import logger

scenarios('../features/process_data.feature')

@given("the campaign and engagement data is available")
def campaign_engagement_data():
    logger.info(f'Create test_data directory structure')
    test_data_dir = os.path.join("tests", "test_data", "daily_files")
    if not os.path.exists(test_data_dir):
        os.makedirs(test_data_dir)

    # Copy test data to the directory
    shutil.copy(os.path.join("tests", "test_data", "supplier_file_campaigns_20230101001500.json"),
                os.path.join(test_data_dir, "supplier_file_campaigns_20230101001500.json"))
    shutil.copy(os.path.join("tests", "test_data", "supplier_file_engagement_20230101001500.json"),
                os.path.join(test_data_dir, "supplier_file_engagement_20230101001500.json"))


@when("the data is processed")
def process_data(spark):
    logger.info(f'inside process data func')
    from src.data_processing import process_campaign_data
    input_path = os.path.join("tests", "test_data")
    output_path = os.path.join("tests", "test_output")
    process_campaign_data(spark, input_path, output_path)


@then("the current campaign engagement report is generated")
def check_engagement_report():
    output_path = os.path.join("tests", "test_output", "reports", "current_campaign_engagement")
    assert os.path.exists(output_path)
    logger.info(f'current campaign engagement report exist in directory')


@then("the campaign overview report is generated")
def check_overview_report():
    output_path = os.path.join("tests", "test_output", "reports", "campaign_overview")
    assert os.path.exists(output_path)