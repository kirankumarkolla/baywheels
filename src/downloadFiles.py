import requests
from datetime import datetime
from zipfile import ZipFile
from dateutil.relativedelta import relativedelta
import logging

def getYM(dtObj):
    return dtObj.strftime('%Y%m')


def downloadFiles():
    currDate = datetime.now()
    yyyymm = currDate.strftime('%Y%m')
    fileDate = currDate + relativedelta(months=-2)
    #srcFolder = '/Users/kiran.kolla/Projects/Python/Baywheels/srcFiles/'
    srcFolder = '/usr/local/airflow/srcFiles/'
    while fileDate <= currDate:
        fileYM = getYM(fileDate)

        fileURL = 'https://s3.amazonaws.com/baywheels-data/'+ fileYM + '-baywheels-tripdata.csv.zip'
        fileDate = fileDate + relativedelta(months=1)
        zipfileName = fileYM + '-baywheels-tripdata.csv.zip'
        

        print(fileURL)
        #url = 'https://s3.amazonaws.com/baywheels-data/201801-fordgobike-tripdata.csv.zip'
        r = requests.get(fileURL, allow_redirects=True)
        if r.status_code != 404:
            open(srcFolder + zipfileName, 'wb').write(r.content)

            logging.info("Downloaded file : " + zipfileName)
            with ZipFile(srcFolder + zipfileName, 'r') as zipObj:
            # Get a list of all archived file names from the zip
                listOfFileNames = zipObj.namelist()
                # Iterate over the file names
                for fileName in listOfFileNames:
                    # Check filename endswith csv
                    if fileName.endswith('.csv'):
                        # Extract a single file from zip
                        zipObj.extract(fileName, srcFolder + 'csv/')
                        logging.info("Unzipped file " + zipfileName)


def main():
    downloadFiles

if __name__ == "__main__":
    main()