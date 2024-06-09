# crmVendorCampaign
PySpark app to process api and create campaigns reports

### SLA with Vendor

- All events are in order
- At-least-once delivery guarantees for all messages
- There is a maximum limit of 256 campaigns
- Campaigns can have steps added but not removed as long as it is live

### ITV Architecture Standards
- Each data product has its own S3 bucket
- API responses are to be sent to an S3 bucket that contains the following folder structure:
- /input/daily_files - Where the supplier’s files will land in the bucket. The filename
will look like supplier_file_YYYYMMDDHHmmSS.extension (e.g.
crm_campaign_20230101001500.json)
- /internal - Can be used to store any internal state that is needed and is not exposed to
the end user
- Reports are to be stored in: /output/reports

### Setup
- This project requires following softwares/libraries that also include local testing
1. Hadoop, winutils
2. JAVA - 19
3. Python- 3.10.11
4. Spark

### Implementation
- Project follows standard pyspark project structure where src/main.py calls an application
- Implemented BDD (Behavioural Driven Development) testing.
- To test locally create virtual env using requirements file and execute

```
pytest --html==report  