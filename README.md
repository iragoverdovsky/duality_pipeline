# duality_pipeline


Generic Data Pipeline for Unknown Sources and Scale
Objective
Build a generic, scalable data pipeline that:
Accepts data from an unknown source (simulated with mock data).
Supports varying data volumes (from small to large).
Transforms the data into a well-defined target schema.
Is testable, observable, and extensible.



 The Setup
You are tasked with building a data ingestion and transformation pipeline that meets the following product constraints:
Requirements:
Data Source: Unknown. Could be a database, flat files (CSV, Parquet), or API.
Simulate at least one data source
Scale: Unknown.
Your solution should work for small scale
Show awareness of scalability.
Organize the data: your pipeline should organize the data in its output according to Duality query which defines various retrieved columns and predicates.
Inputs:
You are provided with the following:
 Source Table (transactions)
A transactional table (can be in a file or a database).
The column names might differ slightly from the target schema (e.g., txn_id instead of transaction_id, cust_id instead of customer_id).
The data volume may vary.
Txn_id is the primary key.
Show awareness for cases where there is no index nor key on the table. 

Example Source Columns:

*txn_id
(numeric)
Cust_id
(numeric)
City_name
(string)
Country_code
(numeric)
Tx_time
(datetime)
1234
8899
Paris
123
2023-08-01 12:34:56








Please use this mock data, click  here for download.
Duality schema


Column Name
Type
Notes
transaction_id
string


customer_id
string


city
string


country
string
ISO 3166 Alpha-2 code
time_of_transaction
timestamp
ISO 8601 format required












Please use this json file for the schema representation, click  here for download.

Duality Query
This is the entity that needs to be reviewed and execute your pipeline  accordingly.
Please use this json file, click  here for download.


Mapping file
This json file will help you to translate the local data as it is in the csv file to the duality schema
Please use this mapping file, click  here for download.






Output: 
The final pipeline output is some persistent storage (database table or files)  that must conform to the well-defined schema as described by Duality schema, and organized in optimized manner according to the Duality Query requirements.



Deliverables
You must deliver the following in a GitHub repo or folder structure:
1. Modular Pipeline Code
Code should be clean, modular, and production-ready.


Organized into components:


Ingestion layer (handles different input sources)


Transformation layer (handles standardization and query logic)


Output layer (writes to Parquet or SQLite)


2. Mock Data
Mock data is provided through csv file and json files in the input paragraph, use it as you feel its most convenient for you to serve as the input for your pipeline. 
3. Tests
Include tests that are running successfully.


Use a testing framework (e.g., pytest).


Clearly show how the pipeline is tested against different inputs and expected outputs.
(You can use subsets of the csv file or change it to your will)


4. Performance Strategy
Briefly describe how you would scale this pipeline for large data:



What happens if there's no index on the source table?


How do you enable parallelism?
Time Allocation
Estimated time: 4–6 hours
Please note: You’re not expected to handle all edge cases perfectly, but we’re looking for clear thinking, awareness of trade-offs, and extensibility.