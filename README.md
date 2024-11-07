# Data Ingestion Accelerator

## Overview

The Data Ingestion Accelerator is a Python package designed to
facilitate seamless ingestion of data from various sources into a
specified storage format in a Databricks environment. It supports
ingestion from Azure Blob Storage, AWS S3, and databases via JDBC
connections.

## Installation

To install the Data Ingestion Accelerator, ensure you have access to a
Databricks environment with the necessary permissions. Use the following
command:

• **Upload** .whl **file to Databricks Workspace**:

-   • Go to **Workspace** in Databricks.

-   Select the folder to upload the file.

-   Click **Create \> Library \> Upload** and select the .whl file.

• **Install the Library on the Cluster**:

-   • Go to **Clusters** and select your desired cluster.

-   In the **Libraries** tab, click **Install New \> Workspace**.

-   Choose the uploaded .whl file and click **Install**.

## Getting Started

Before using the functionalities, import the package in your Python
script:

**from Data_Ingestion.data_ingestion_accelerator import \***

## Class Initialization

To use the functionality of the Data Ingestion Accelerator, create an
instance of the class by initializing it with your Spark session:

**data_ingestor = DataIngestionAccelerator(spark)**

## Ingesting Data

### Method: batch_ingest_from_source

This method allows you to ingest data from different sources.

#### Method Signature

**dataframe = data_ingestor.batch_ingest_from_source(source_type,
source_config, output_path, output_format=\'delta\')**

#### Parameters

**(1)** **source_type(string, required)**: Specifies the type of data
source.\
- Accepted Values:\
- \"azure_blob_storage\": For Azure Blob Storage ingestion.\
- \"aws_s3\": For AWS S3 ingestion.\
- \"database\": For database ingestion via JDBC.\
\
**(2)** **source_config (dictionary, required)**: Configuration settings
for the data source, which varies based on the \`source_type\`.\
\
- Expected Keys:\
**(A)** For **Azure Blob Storage**:\
- \`container\` (string): Name of the Azure Blob Storage container.\
- \`storage_account\` (string): Name of the Azure storage account.\
- \`path\` (string): Path to the data file within the container.\
- \`file_format\` (string, optional): Format of the input data file
(default is \'parquet\').\
\
**(B)** For **AWS S3**:\
- \`path\` (string): Full S3 path to the data file.\
- \`file_format\` (string, optional): Format of the input data file
(default is \'parquet\').\
\
**(C)** For **Database (JDBC)**\
- \`url\` (string): JDBC connection URL to the database.\
- \`table\` (string): Name of the table to ingest.\
- \`user\` (string): Username for database authentication.\
- \`password\` (string): Password for database authentication.\
- \`driver\` (string): JDBC driver class name.\
\
**(3)** **output_path** (string, required): The destination path where
the ingested data will be saved in the specified format (Delta, Parquet,
or CSV).\
\
**(4)** **output_format** (string, optional): Format for saving the
ingested data.\
- Accepted Values:\
- \'delta\' (default)\
- \'parquet\'\
- \'csv\'

## Example Usage

### 1. Ingesting from Azure Blob Storage:

azure_config = {\
\'container\': \'my-container\',\
\'storage_account\': \'my-storage-account\',\
\'path\': \'data/my_data_file.parquet\',\
\'file_format\': \'parquet\' \# Optional\
}\
output_path = \'dbfs:/path/to/output/folder\'\
\
**dataframe =
data_ingestor.batch_ingest_from_source(\"azure_blob_storage\",
azure_config, output_path)**

### 2. Ingesting from AWS S3:

aws_s3_config = {\
\'path\': \'s3://my-bucket/path/to/data.parquet\',\
\'file_format\': \'parquet\' \# Optional\
}\
output_path = \'dbfs:/path/to/output/folder\'\
\
**dataframe = data_ingestor.batch_ingest_from_source(\"aws_s3\",
aws_s3_config, output_path)\
**

### 3. Ingesting from a Database:

jdbc_config = {\
\'url\': \'jdbc:mysql://localhost:3306/mydatabase\',\
\'table\': \'my_table\',\
\'user\': \'username\',\
\'password\': \'password\',\
\'driver\': \'com.mysql.cj.jdbc.Driver\'\
}\
output_path = \'dbfs:/path/to/output/folder\'\
\
**dataframe = data_ingestor.batch_ingest_from_source(\"database\",
jdbc_config, output_path)\
**

## Error Handling

### Types of Errors

> **1001 - TypeError**

1.  **Description**: Raised when an operation or function is applied to
    > an object of inappropriate type.

2.  **Example**: Passing a non-dictionary object as source_config when a
    > dictionary is expected.

3.  **Cause**: Mismatch between expected input types.

> **1002 - ValueError**

1.  **Description**: Raised when a function receives an argument of the
    > right type but inappropriate value.

2.  **Example**: If the source_type is set to a value that isn't
    > recognized (e.g., \"ftp\").

3.  **Cause**: Invalid or unsupported parameter values.

> **1003 - KeyError**

1.  **Description**: Raised when a dictionary is accessed with a key
    > that doesn't exist.

2.  **Example**: Attempting to access a key in source_config that is
    > required but not provided (e.g., source_config\[\'url\'\]).

3.  **Cause**: Missing keys in the configuration dictionary.

> **1004 - FileNotFoundError**

1.  **Description**: Raised when a file or directory is requested but
    > cannot be found.

2.  **Example**: When the output_path does not point to a valid
    > location.

3.  **Cause**: Incorrect or inaccessible output path.

> **1005 - PermissionError**

1.  **Description**: Raised when trying to open a file in write mode
    > when the file is not writable or the directory is not accessible.

2.  **Example**: Insufficient permissions to write to the specified
    > output_path.

3.  **Cause**: Lack of write permissions for the output directory.

> **1006 - ConnectionError**

1.  **Description**: Raised when a network connection cannot be
    > established.

2.  **Example**: If the database specified in source_config cannot be
    > reached.

3.  **Cause**: Network issues or incorrect connection parameters.

> **1007 - AuthenticationError**

1.  **Description**: Raised when authentication fails during a
    > connection attempt.

2.  **Example**: Incorrect credentials provided in the source_config for
    > database connections.

3.  **Cause**: Invalid username/password or insufficient permissions.

**1008 - ValueError (Data Validation)**

-   **Description**: Raised when data validation fails, particularly
    > during transformation or loading.

-   **Example**: When trying to convert a string to a number and the
    > string is not numeric.

-   **Cause**: Invalid data types or unexpected data formats.

> **1009 - RuntimeError**

1.  **Description**: Raised when an error is detected that doesn't fall
    > into any of the other categories.

2.  **Example**: A generic error that could occur during runtime, such
    > as an unexpected failure during data ingestion.

3.  **Cause**: Various runtime issues that may not be anticipated.
