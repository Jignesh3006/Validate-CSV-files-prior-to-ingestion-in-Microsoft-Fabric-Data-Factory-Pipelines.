# Robust Data Ingestion

## Project Overview :

The project involves the development of a data ingestion pipeline using Microsoft Fabric Data Factory, designed to automate the processing of CSV files from a source folder. The pipeline validates the data by checking data types, column names, and duplicate values before ingesting it into a production table in Delta format. If any validation checks fail, an email notification is sent regarding the problematic CSV files. This approach ensures that only correctly formatted data is ingested, enhancing data quality and streamlining the overall data processing workflow.

## Architecture Overview :

In this process, the source files are located in a designated Source folder. A data pipeline triggers a Spark notebook to verify the files for any new or missing columns, invalid data types, or duplicate key values. If the file passes all checks without errors, the pipeline loads the CSV data into a Parquet file and subsequently calls another Spark notebook to transfer the Parquet file into a Delta table within the Lakehouse. Conversely, if any errors are detected in the file, the pipeline sends out a notification email.
