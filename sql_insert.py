from os import listdir
from sqlalchemy import create_engine
import pandas as pd
import urllib
import xlsxwriter
import os, sys
import pyodbc

#reads and returns a dataframe from a single data.csv or multiple output.xlsx
def dataIn(): 
    #srcloc = input("Please enter file name in CURRENT directory where the SOURCE files are: ")
    srcloc = "Source"
    loc = str(os.getcwd()) + "\\" + srcloc
    print(loc)
    filepaths = [f for f in listdir(loc) if f.endswith('xlsx')]
    i=0
    for name in filepaths: 
        filepaths[i] = loc + "\\" + name
        print(filepaths[i])
        i+=1   
    datafr = pd.concat([pd.read_excel(f) for f in filepaths])
    input("Number of rows read " + str(datafr.shape[0]))
    return datafr

#BULK INSERT dataframe into SQL Server DB table
def sqlInsert(datafr):
    trust = "Trusted_Connection=yes"
    m_server = 'K12-SQLM'
    m_database = 'K12Design'
    m_username = "temp"
    m_password = "temp"
    table = 'DesignTenWeb_Temp'
    driver = 'SQL Server Native Client 11.0'

    split = 10000
    countRow = datafr.shape[0]
    counter = int(countRow/split) + (countRow%split > 0)
    input("Number of frames is " + str(counter))

    i=0
    splitfr = []
    while i < counter:
        start = (i*split)
        end = ((i+1)*split-1)
        dataOut =  datafr.iloc[start:end, :]
        splitfr.append(dataOut)
        i+=1

    m_quoted = urllib.parse.quote_plus("DRIVER={%s};SERVER=%s;DATABASE=%s;%s" % (driver, m_server, m_database, trust))
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(m_quoted))

    i=1
    for frames in splitfr: 
        frames.to_sql(table, schema='dbo', con = engine, chunksize=200, method='multi', index=False, if_exists='append')
        print("Frame Number " + str(i) + " Complete!")
        i+=1

    return 0




if __name__ == '__main__':

    # Gets data using dataIn() and bulk inserts data to SQL server DB
    print("Executing BULKINSERT data from files to DB")
    df = dataIn()
    if len(df) == 1 :
        print("No Data Found")
        exit()
    sqlInsert(df)

    print("thanks for using")
