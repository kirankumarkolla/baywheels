import psycopg2,logging
import json
from datetime import datetime
import os,pandas as pd
from sqlalchemy import create_engine


def getConn():
    conn = psycopg2.connect(host='192.168.1.151',port='5432', dbname='baywheels',user='admin',password='admin')
    #engine = create_engine('postgresql://admin:admin@192.168.1.151:5432/baywheels')
    return conn
def getEngine():
    engine = create_engine('postgresql://admin:admin@192.168.1.151:5432/baywheels')
    return engine

def getBatchId():
    batchId = (datetime.now()).strftime("%Y%m%d")
    return batchId

def loadStaging():
    conn = getConn()
    cur = conn.cursor()
    batch_id = (datetime.now()).strftime("%Y%m%d")
    csvDir = "/usr/local/airflow/srcFiles/csv"
    load_status = ''
    for filename in os.listdir(csvDir):
        logging.info("Importing file "+ filename  + " to the staging table")
        filepath = os.path.join(csvDir, filename)
        if(filename.endswith('.csv')):
            pd.read_csv(filepath)
            with open(filepath, 'r') as f:
                next(f)
                try:
                    cur.copy_from(f, 'tripdata_stg', sep=',')
                    logging.info("Completed importing the file " + filename +" to the staging table")
                    load_status = 'Completed'
                except Exception as e:
                    load_status = 'Failed'
                    logging.error("error while loading " + filename + " to the staging table")
                    logging.exception(e)
            ctl_query = "insert into load_status_ctl (batch_id,file_name,load_date,load_status) values(" + batch_id + ",'" + filename + "','" + str(datetime.now()) + "','" + load_status +"')" 
            cur.execute(ctl_query)

    cur.execute(ctl_query)
    conn.commit()
    cur.close()
    conn.close()



def sqlToDf(query,conn):
    cur = conn.cursor()
    cur.execute(query)

    engine = create_engine('postgresql://admin@192.168.1.151:5432/baywheels')
    logging.info("Executing query:" + query)
    records = cur.fetchall()
    colNames=[]
    for item in cur.description:
        colNames.append(item[0])    
    logging.info("Fetcged data from the staging table")

    df = pd.DataFrame(records,colNames)
    logging.info("Conerted to data frame")
    cur.close()
    return df

def sqlToDf2(query):
    engine = create_engine('postgresql://admin:admin@192.168.1.151:5432/baywheels')
    logging.info("Executing query:" + query)
    df = pd.read_sql_query(query,con=engine)
    logging.info("Conerted to data frame")
    return df

def dfToSql(df,tgtTable,conn):
    engine = create_engine('postgresql://admin:admin@192.168.1.151:5432/baywheels')
    df.to_sql(tgtTable,engine,if_exists="append",index=False)


def dataCleansing():
    logging.info("Starting load to cln table")
    #stgCols = ["ride_id","rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id","start_lat","start_lng","end_lat","end_lng","member_casual"]
    stgQuery = "select * from tripdata_stg limit 100"
    conn = getEngine()
    stgDf = sqlToDf2(stgQuery)
    logging.info("Created staging dataframe")
    logging.info(stgDf.count)
    trimmedDf = stgDf.applymap(lambda x : x.strip())
    trimmedDf['started_at'] = trimmedDf['started_at'].fillna('NA')
    trimmedDf.replace('','NA',inplace=True)
    batch_id = getBatchId()
    logging.info(str(batch_id))
    trimmedDf['batch_id'] = int(batch_id)
    trimmedDf['load_date'] = str(datetime.now())
    logging.info("Cleaned data frame and added control columns")
    for col in trimmedDf.columns:
        logging.info(col)
    dfToSql(trimmedDf,'tripdata_cln',conn)

def stations_dim_load():
    dimQuery = "insert into stations_dim (station_name,station_code,latitude,longitude) select distinct start_station_name as station_name,start_station_id as staion_code,start_lat as latitude,start_lng as longitude from tripdata_cln"
    conn = getConn()
    cur = conn.cursor()
    cur.execute(dimQuery)
    conn.commit()
    cur.close()
    conn.close()


def main():
    loadStaging


if __name__ == "__main__":
    main()