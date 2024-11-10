# Robust Data Ingestion

## 📜 Table of Contents
- [Project Overview](#project-overview-)
- [Architecture Overview](#architecture-overview-)
- [Orchestrator Pipeline](#orchestrator-pipeline)
- [Validation Process](#notebook-activity---validate-the-csv-file-and-load-the-data-validation-data)
- [Copy Data Activity](#copy-data-activity)
- [Upsert Switch Activity](#upsert-switch-activity)
- [Error Notifications](#error-notifications)
- [Semantic Model Refresh](#semantic-model-refresh)
- [Getting Started](#getting-started)



</details>

## Project Overview :

The **Robust Data Ingestion Pipeline** project leverages **Microsoft Fabric Data Factory** to automate the validation and ingestion of CSV files from a source folder. Key benefits include:

- **Data Quality Assurance**: Automatically performs checks on structure, data types, and duplicates.
- **Error Notifications**: Sends timely alerts for failed files.
- **Scalable Data Management**: Supports large data handling with reduced manual processing..

## Architecture Overview :


This pipeline architecture is centered around CSV validation and ingestion, encapsulated within **Spark Notebooks** in Microsoft Fabric Data Factory. Key components include:

1. **Data Storage**: CSV files and associated metadata are organized in designated **Source** folders.
2. **Validation Process**: Utilizes Spark Notebooks to validate incoming CSV files.
3. **Data Processing**: Validated files are converted to **Parquet** format and merged into a **Delta table**.
4. **Error Handling**: Error notifications are generated for any validation failures.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Architecture.png)

## Orchestration Pipeline

### Source Files and Metadata Files

The **Landing Zone** folder consists of:
- **Dataset Folder**: Contains all CSV files set to be processed.
- **Metadata Folder**: Holds metadata files defining the expected data structure.
- **Output Folder**: It contains all the log files of the processed CSV files.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Sources%20Files%20and%20Metadata.png)

The **Landing Zone folder** contains a subfolder named Metadata, which describes the CSV file format used for validating the CSV files in the subsequent stages of the pipeline. For example, the solution includes a table called Product, which consists of the following columns: **Product ID, Product Name, Category, Sub-Category, and Price** . In the format definition file Product_metadata, there is a row for each column of the Product table that provides details such as the column name, column data type, and whether it serves as a primary key. This metadata file is later utilized in the Spark Notebook to ensure that incoming files conform to this specified format.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Metadata%20file%20Format.png)

## Data Pipeline

### Orchestrator Pipeline

The Orchestrator Pipeline automates file processing, iterating through each file and performing parameterized actions based on file structure. Key configurations, including lakehouse paths and file formats, make the pipeline adaptable for any data lakehouse.


![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Setting%20the%20parameters%20for%20Input%20Folder.png)


## For Each Activity to iterate through Files

Files from the Dataset folder are passed into a For Each activity. This  activitiy for validation and data transfer, including:

Copy Data: Copies the file.
**Validation Notebook** : Checks data structure and content.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/For%20Each%20Activity%20has%20two%20switch%20activity%20-%201.png)

## Notebook activity - Validate the CSV file and load the data (Validation Data)

The pipeline includes a **Notebook activity** that validates each CSV file against predefined metadata. Key checks performed include:

- **Column Names and Data Types**
- **Primary Key Constraints**
- **Duplicate Key Values**
Successful files are converted to Parquet format for further processing; otherwise, error details are logged.

```python
lakehousepath = 'abfss://xxxxxx7@onelake.dfs.fabric.microsoft.com/aef0e8dc-7168-48b5-911c-2aff7f69ed87'
filename = "Product.csv"
outputfilename = "Product"
metadatafilename = "Product_metadata.csv"
datasetfolder = "LandingZone/Dataset"
metadatafolder = "LandingZone/Metadata"
outputfolder = "LandingZone/Output"
fileformat = "Product"
# Importing libraries

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Set path variables

inputfilepath = f'{lakehousepath}/Files/{datasetfolder}/'
metadatafilepath = f'{lakehousepath}/Files/{metadatafolder}/'
outputfilepath = f'{lakehousepath}/Files/{outputfolder}/'


# Read text file and metadata file
data = pd.read_csv(f'{inputfilepath}{filename}')
metadata = pd.read_csv(f'{metadatafilepath}{metadatafilename}')


## Only get the column names for the file formatted that was input
metadata = metadata[metadata['formatname'] == fileformat]

## Get any key fields is specified 

keyfields = metadata.loc[metadata['iskeyfield'] == 1 , 'columnname'].tolist()


## Check for the errors in the CSV or not
haserror = 0

# Check if columns are matching or not

if list(data.columns) !=  list(metadata['columnname']):
    result = "Error: Column names do not match "
    haserror = 1

else:
    # Check if data type is matching or not 

    if list(data.dtypes) != list(metadata["datatype"]):
        result = "Error : Data types do not match"
        haserror = 1
    
    else:
        # Checking if files having key fields or not 
        if keyfields != '':
            checkdups = data.groupby(keyfields).size().reset_index(name = 'count')

            if checkdups['count'].max() > 1:
                dups = checkdups[checkdups['count'] > 1]
                haserror = 1
                (dups.to_csv(f'{lakehousepath}/Files/processed/error_duplicate_key_values/duplicaterecord_{filename}', mode = 'w', index = False))
                result = "Error: Duplicate key values"

if haserror == 0:

    df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv(f'{inputfilepath}{filename}')
    
    print(f'File is : {inputfilepath}{filename}')

    display(df)
    df.write.mode("overwrite").format("parquet").save(f"{outputfilepath}{outputfilename}")

    result = f"Data written to parquet successfully. Key fields are {keyfields}"

mssparkutils.notebook.exit(str(result))
```
 The process includes a notebook activity, and upon the successful execution of this activity, it copies the data. Simultaneously, the output message generated by the notebook activity is stored in a variable.


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Activities%20in%20Switch%20Activity%20-%202.png)

### Copy Data Activity

Once validation is successful, the **Copy Data Activity** moves the CSV files to the **Processed** folder in the Lakehouse. If the move is successful, the original files are deleted. 


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Setting%20up%20Copy%20Source%20Activity%20-%203.png)


 **Destination folder** name is derived from the **Notebook exit value  which returns the success or the error message**. 


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Setting%20up%20Destination%20Directory%20-%204.png)

## Upsert Switch Activity 

Determines whether CSV data can be merged into the **Delta table** based on validation results:

- **Successful Validation**: Initiates the upsert to the Delta table.
- **Validation Failure**: Triggers error notifications.
If validated, a Spark Notebook merges the Parquet file into the Delta table, updating existing records and inserting new ones


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Upsert%20Switch%20Activity%20Conditiion%20-%205.png)

The parameters for key fields are obtained from the preceding PySpark notebook and passed to the Upsert Table notebook. The processing flow is as follows:

- **Key Fields Present & Table Exists**: A merge expression is constructed for the PySpark upsert statement, allowing existing records to be updated and new records to be inserted into the Delta table.
  
- **No Key Fields or Table Doesn't Exist**: The data is written to, or the existing Delta table is overwritten with the new information.

This efficient handling ensures optimal data management and helps maintain the integrity of the dataset. 


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Upsert%20Notebook%20-%206.png)



```Python
# create or merge to delta

# input parameters below
lakehousepath = 'abfss://xxxx@onelake.dfs.fabric.microsoft.com/aef0e8dc-7168-48b5-911c-2aff7f69ed87'
inputfolder = 'LandingZone/Output'
filename = "Product"
tablename = "Product"
keyfields="['ProductID', 'ProductName']"

# define paths
outputpath = f'{lakehousepath}/Tables/{tablename}'
inputpath = f'{lakehousepath}/Files/{inputfolder}/{filename}'

# import delta table and sql functions
from delta.tables import *
from pyspark.sql.functions import *


# get list of key values
keylist = eval(keyfields)
print(keylist)

# read input parquet file
df2 = spark.read.parquet(inputpath + '/*.parquet')
# display(df2)

# if there are keyfields define in the table, build the merge key expression
if keyfields != None:
    mergekey = ''
    keycount = 0
    for key in keylist:
        mergekey = mergekey + f't.{key} = s.{key} AND '
    mergeKeyExpr = mergekey.rstrip(' AND')
    print(mergeKeyExpr)


# if table exists and if table should be upserted as indicated by the merge key, do an upsert and return how many rows were inserted and updated; 
# if it does not exist or is a full load, overwrite existing table return how many rows were inserted


if DeltaTable.isDeltaTable(spark,outputpath) and mergeKeyExpr is not None:
    deltaTable = DeltaTable.forPath(spark,outputpath)
    deltaTable.alias("t").merge(
        df2.alias("s"),
        mergeKeyExpr
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    history = deltaTable.history(1).select("operationMetrics")
    operationMetrics = history.collect()[0]["operationMetrics"]
    numInserted = operationMetrics["numTargetRowsInserted"]
    numUpdated = operationMetrics["numTargetRowsUpdated"]
else:
    df2.write.format("delta").mode("overwrite").save(outputpath)
    numInserted = df2.count()
    numUpdated = 0
print(numInserted)

result = "numInserted="+str(numInserted)+  "|numUpdated="+str(numUpdated)
mssparkutils.notebook.exit(str(result))
```
## Error Notifications 

If validation fails, an Outlook Activity sends notifications detailing the issues to enable timely resolution.


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Email%20Notification%20-%207.png)

## Semantic Model Refresh

After successful ingestion, a **Semantic Model Refresh** updates Power BI reports to reflect the newly ingested data.


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Semantic%20Model%20Refresh%20-%208.png)


## Getting Started
### Prerequisites
- Microsoft Azure account
- Microsoft Fabric subscription
- Access to Azure Synapse or related tools

### Installation
Instructions on how to set up the pipeline and run the validations can be found in the associated documentation. Ensure to configure your lakehouse paths and metadata files correctly.

---

Feel free to reach out via [GitHub Issues](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/issues) for any questions or feature requests!


