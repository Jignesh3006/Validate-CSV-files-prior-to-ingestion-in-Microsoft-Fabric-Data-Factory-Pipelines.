# Robust Data Ingestion

## Table of Content - Links

- [Project Overview](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#project-overview)
- [Architecture Overview](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#architecture-overview)
- [Orchestration Pipeline](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#orchestration-pipeline)

<details>
<summary>Data Pipeline</summary>

- [Orchestrator Pipeline](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#data-pipeline)
- [For Each Activity to iterate through Files](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#for-each-activity-to-iterate-through-files)
- [Notebook activity - Validate the CSV file and load the data](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#notebook-activity---validate-the-csv-file-and-load-the-data)
- [Copy Data Activity](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#copy-data-activity)
- [Upsert Switch Activity](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines/blob/main/README.md#upsert-switch-activity)

</details>

## Project Overview :

#### The project involves the development of a data ingestion pipeline using Microsoft Fabric Data Factory, designed to automate the processing of CSV files from a source folder. The pipeline validates the data by checking data types, column names, and duplicate values before ingesting it into a production table in Delta format. If any validation checks fail, an email notification is sent regarding the problematic CSV files. This approach ensures that only correctly formatted data is ingested, enhancing data quality and streamlining the overall data processing workflow.

## Architecture Overview :

#### In this process, the source files are located in a designated Source folder. A data pipeline triggers a Spark notebook to verify the files for any new or missing columns, invalid data types, or duplicate key values. If the file passes all checks without errors, the pipeline loads the CSV data into a Parquet file and subsequently calls another Spark notebook to transfer the Parquet file into a Delta table within the Lakehouse. Conversely, if any errors are detected in the file, the pipeline sends out a notification email.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Architecture.png)

## Orchestration Pipeline

### Source Files and Metadata Files

#### The Landing Zone folder contains a subfolder named Dataset, which houses all the CSV files that are set to be processed. Additionally, the Landing Zone folder includes another subfolder called Metadata, where metadata corresponding to each CSV file in the Dataset is stored.


![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Sources%20Files%20and%20Metadata.png)

#### The Landing Zone folder contains a subfolder named Metadata, which describes the CSV file format used for validating the CSV files in the subsequent stages of the pipeline. For example, the solution includes a table called Product, which consists of the following columns: Product ID, Product Name, Category, Sub-Category, and Price. In the format definition file Product_meta, there is a row for each column of the Product table that provides details such as the column name, column data type, and whether it serves as a primary key. This metadata file is later utilized in the Spark Notebook to ensure that incoming files conform to this specified format.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Metadata%20file%20Format.png)

## Data Pipeline

### Orchestrator Pipeline

#### The orchestrator pipeline is designed to be straightforward. As I am executing my pipeline on a scheduled batch basis, it iterates through the files folder. This involves parameterizing the lakehouse path, the source folders, the destination folder, and the file format. This flexibility enables the same process to be applied to any lakehouse and any file format or table for loading


![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Setting%20the%20parameters%20for%20Input%20Folder.png)


### For Each Activity to iterate through Files

#### The child items of the folder, specifically the files located in the Dataset folder, are retrieved and passed as an array. This array is then processed through a For Each activity to execute subsequent actions in the pipeline. In that for each activity it has two switch activity one switch activity has it Copy the File using the Copy Data Activity and Run the Notebook for validating the files.

![AltText](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/For%20Each%20Activity%20has%20two%20switch%20activity%20-%201.png)

### Notebook activity - Validate the CSV file and load the data

#### Below is the PySpark code  process retrieves the column names and inferred data types from the CSV file, as well as the column names, data types, and key field names from the metadata file. It verifies whether the column names align, if the data types are consistent, if key fields are defined for the file, and checks for any duplicate key values in the incoming file. If duplicate key fields are found, it logs the duplicate key values to a separate file. If the names and data types match and there are no duplicate key values, the file is converted to Parquet format, and the key field names from the metadata file are returned. Otherwise, an appropriate error message is provided.

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
#### The process includes a notebook activity, and upon the successful execution of this activity, it copies the data. Simultaneously, the output message generated by the notebook activity is stored in a variable.


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Activities%20in%20Switch%20Activity%20-%202.png)

### Copy Data Activity

#### This Copy Data activity essentially moves the csv file from the Landing Zone  files folder to a processed folder in the Fabric Lakehouse. In this copy data activity after files moves to the process folder it gets delete to the source position


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Setting%20up%20Copy%20Source%20Activity%20-%203.png)


#### Destination folder name is derived from the Notebook exit value  which returns the success or the error message 


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Setting%20up%20Destination%20Directory%20-%204.png)

### Upsert Switch Activity 

#### In this Switch activity, an Upsert operation is performed to verify whether the CSV file has been successfully validated. If the file validation is successful, the Upsert Notebook activity is executed.


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Upsert%20Switch%20Activity%20Conditiion%20-%205.png)

#### If file validated successfully and there is no errors it will call the spark notebook to merge the parquet file written from the previous pyspark notebook activity which was present in the switch activity 


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Upsert%20Notebook%20-%206.png)

#### Parameters for the key fields are passed in, as previously mentioned. These key fields, derived from the earlier PySpark notebook, are provided to the Upsert Table notebook.The following is the Spark notebook code. If the Delta table already exists and key fields are present, it constructs a string expression for use in the PySpark upsert statement and subsequently performs a merge on the Delta table. If there are no key fields or if the table does not exist, it writes to or overwrites the Delta table.

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
#### If validation gets failed it will operate the Outlook activity 

![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Email%20Notification%20-%207.png)

#### After all the upsert operation it refresh the semantic model where user can see the updated refreshed Power BI Report


![Alt](https://github.com/Jignesh3006/Validate-CSV-files-prior-to-ingestion-in-Microsoft-Fabric-Data-Factory-Pipelines./blob/main/Images/Semantic%20Model%20Refresh%20-%208.png)



