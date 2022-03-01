# baywheels project

### Stages

download files -> stg -> cln -> transform data

Staging: Load the raw files as is without any transformation

cleansing stage: Apply data quality rules like check date format, null values, invalid records etc

Transform data: Apply data transfoamtion rules, split the data into multiple tables based on the data model

Control tables as part of frame work:
file_load_status:  to track all the files downloaded from web and its import staus
job_status: to track status of each stage 
record_cnt_reconcile: to keep track of no of records loaded at each stage along with table names for data reconciliation

Failure notificatiopns:

