# Pokemon_DataLake_and_Analytics
Building a cloud-native end-to-end data analytics pipeline project. It ingests data into AWS S3 bucket, in which I utilize Glue to trigger jobs that perform ETL transformations and query tables, then use Tableau to visualize the insights.

Glue controller (triggers ETL on raw data) -> loads processed data to S3 processed zone

Glue Crawler job triggered -> scans Processed and Curated Zone and update Glue Catalog

Run Athena Query -> attain Curated data then store in Curated Zone

Connect Tableau to Athena for visualization/ Download data and load files to Tableau for visualization


