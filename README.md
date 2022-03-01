# baywheels project

### Stages

download files -> stg -> cln -> transform data

- Staging: Load the raw files as is without any transformation

- cleansing stage: Apply data quality rules like check date format, null values, invalid records etc

- Transform data: Apply data transfoamtion rules, split the data into multiple tables based on the data model



#### Data Tables: 
- staging:  tripdata_stg
- Clean STage: tripdata_cln
- dw :  stations_dim. (station_id, station_name, station_code,logintude,lattitude)
        rides_fact.  (ride_id,rideable_type,started_at,ended_at,start_station_id,end_stattion_id,member_casual)

While transforming the data from cln -> dw , first get all the station related data to the dim table.
Lookup the station id while loading rides_fact table

#### Control tables:
- file_load_status:  to track all the files downloaded from web and its import staus
- job_status: to track status of each stage 
- record_cnt_reconcile: to keep track of no of records loaded at each stage along with table names for data reconciliatio
      
### Airflow to schedule workflows

Workflows: Airflow is used to create the workflos and set the dependencies among the tasks
Configured email notifications on failure in the airflow dag
We can set the retreis option to 3 or something to retry the task before terminating especially while downloading files from web.

### Restartbility

Need to create a batch id for each run to keep track of the daily load
Stg & cln tables will be truncate and load for each batch
Add pre-sql statements before starting the load for each stage to clean up the partial data load while rerunning the same batch again.

### Containers
Created separate containers for postgresql and airflow for this project
