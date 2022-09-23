from multiprocessing import Pool
from os import listdir
from sqlalchemy import create_engine
import pandas as pd
import urllib
import xlsxwriter
import os, sys
import pyodbc

poolLog  = []
parentdir = str(os.path.dirname(os.getcwd()))
resultloc = parentdir + "\\" + "results"
srcloc = ""
destloc = ""

#region Old Functions
def concatData(filetype):
    srcloc = input("Please enter file name in CURRENT directory where the SOURCE files are: ")
    print(srcloc)
    xlsxloc = str(os.getcwd()) + "\\" + srcloc

    filepaths = [f for f in listdir(xlsxloc) if f.endswith('xlsx')]
    
    print(len(filepaths))
    input(filepaths)

    df = pd.concat(map(pd.read_excel, filepaths))
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)

    print(df.head(5))
    print(df.shape[0])
    input(['press enter to continue...'])

    df.to_csv('data.csv', index=False, sep='\t')

#Take and combine all csv files in /csvSource return a single data.csv
def concatCsv():
    srcloc = input("Please enter file name in CURRENT directory where the SOURCE files are: ")
    print(srcloc)

    csvloc = str(os.getcwd()) + "\\" + srcloc
    filepaths = [f for f in listdir(csvloc) if f.endswith('csv')]
    
    print("Number of Files Found: " + str(len(filepaths)))
    input(filepaths)

    df = pd.concat([pd.read_csv(f, sep='\t') for f in filepaths])
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)

    print(df.head(5))
    print("Number of Rows: " + str(df.shape[0]))
    input(['press enter to continue...'])

    df.to_csv('data.csv', index=False, sep='\t')

#endregion

#Take google/youtube xlsx data and reorder them by title-content-url-type
def reorderGoogle(): 
    srcloc = input("Please enter file name in CURRENT directory where the SOURCE files are: ")
    print(srcloc)

    srcloc = str(os.getcwd()) + "\\" + srcloc + "\\"

    google = pd.read_excel(srcloc+'google_wordNature.xlsx') #type 2
    youtube = pd.read_excel(srcloc+'youtube_wordNature.xlsx')#type 3

    googleOut = pd.concat([google['title'], google['text'], google['url']], axis=1)
    googleOut = googleOut.rename(columns={'text': 'content'})
    googleOut = googleOut[['title', 'content', 'url']]
    googleOut['type'] = 2

    youtubeOut = pd.concat([youtube['title'], youtube['text'], youtube['url']], axis=1)
    youtubeOut = youtubeOut.rename(columns={'text': 'content'})
    youtubeOut = youtubeOut[['title', 'content', 'url']]
    youtubeOut['type'] = 3

    print(googleOut.head(5))
    input(youtubeOut.head(5))

    writer = pd.ExcelWriter('api_google.xlsx', engine='xlsxwriter', options={'strings_to_urls': False})
    googleOut.to_excel(writer, index=False)
    writer.save()

    writer2 = pd.ExcelWriter('api_youtube.xlsx', engine='xlsxwriter', options={'strings_to_urls': False})
    youtubeOut.to_excel(writer2, index=False)
    writer2.save()

    return 0

#reads and returns a dataframe from a single data.csv or multiple output.xlsx
def dataIn(filetype): 
    srcloc = input("Please enter file name in CURRENT directory where the SOURCE files are: ")
    print(srcloc)
    loc = str(os.getcwd()) + "\\" + srcloc

    if filetype.startswith('c'): 
        filepaths = [f for f in listdir(loc) if f.endswith('csv')]
        i=0
        for name in filepaths: 
            filepaths[i] = loc + "\\" + name
            print(filepaths[i])
            i+=1
        datafr = pd.concat([pd.read_csv(f, sep='\t') for f in filepaths])

        input("Number of rows read " + str(datafr.shape[0]))
        return datafr
    elif filetype.startswith('x'):
        filepaths = [f for f in listdir(loc) if f.endswith('xlsx')]
        i=0
        for name in filepaths: 
            filepaths[i] = loc + "\\" + name
            print(filepaths[i])
            i+=1
        
        datafr = pd.concat([pd.read_excel(f) for f in filepaths])

        input("Number of rows read " + str(datafr.shape[0]))
        return datafr
    return 0

#exports dataframe to csv or xlsx
def dataOut(datafr, filetype):
    destloc = input("Please enter file name in PARENT directory where the RESULTS are to be stored: ")
    resultloc = parentdir + "\\" + destloc 
    print(resultloc)

    if filetype.startswith('c'): 
        resultloc = resultloc + "\\" + "csvOutput.csv"
        input("Number of rows " + str(datafr.shape[0]))
        datafr.to_csv(resultloc, index=False, sep='\t')
    elif filetype.startswith('x'):
        if datafr.shape[0] > 1010000: 
            per = 1010000
            countRow = datafr.shape[0]
            counter = int(countRow/per) + (countRow%per > 0)
            print(datafr.head(5))
            print("Number of rows is: " + str(countRow))
            inprompt = 'Export to Excel, File count is [' + str(counter) + '] Press Enter to continue, Ctrl+C to exit'
            input(inprompt)
            i=0
            outList = []
            while i < counter:
                start = (i*per)
                end = ((i+1)*per-1)
                dataOut =  datafr.iloc[start:end, :]
                outList.append(dataOut)
                i+=1
            poolprint()
        else:
            resultloc = resultloc + "\\" + "xlsxOutput.xlsx"
            writer = pd.ExcelWriter(resultloc, engine='xlsxwriter', options={'strings_to_urls': False})
            datafr.to_excel(writer, index=False)
            writer.save()

#region Functions for exporting Dataframe to xlsx using MultiProcessing

#exports the dataframe to xlsx
def printout(datafr, num):
    pid = os.getpid()
    outputnum = 'output'+ str(num+1) +'.xlsx'
    outputloc = resultloc + "\\" + outputnum
    writer = pd.ExcelWriter(outputloc, engine='xlsxwriter', options={'strings_to_urls': False})
    datafr.to_excel(writer, index=False)
    writer.save()
    return pid
#Logs PID
def logged(result):
    return poolLog.append(result)
#runs printout() using multiprocessing
def poolprint():
    with Pool() as pool: 
        for num in range(len(outList)):
            pool.apply_async(printout, [outList[num], num], callback= logged)
        pool.close()
        pool.join()
        print('PIDs : ', poolLog)

#endregion

#removes emply rows from excels
def removeNan():
    srcloc = input("Please enter file name in CURRENT directory where the SOURCE files are: ")
    print(srcloc)

    xlsxloc = str(os.getcwd()) + "\\" + srcloc
    filepaths = [f for f in listdir(xlsxloc) if f.endswith('xlsx')]
    print(len(filepaths))
    input(filepaths)

    for file in filepaths:
        fileloc = xlsxloc + "\\" + file
        datafr = pd.read_excel(fileloc)
        #input(datafr.head(5))
        datafr.dropna(inplace=True)
        writer = pd.ExcelWriter(fileloc, engine='xlsxwriter', options={'strings_to_urls': False})
        datafr.to_excel(writer, index=False)
        writer.save()


    return 0

#BULK INSERT dataframe into SQL Server DB table
def sqlInsert(datafr):
    server = 'db'
    database = 'dbname'
    table = 'tbl_name'
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
    input(splitfr[0].head(5))

    quoted = urllib.parse.quote_plus("DRIVER={" + driver + "};SERVER=" + server + ";DATABASE=" + database + ";Trusted_Connection=yes")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    input("waiting")

    i=1
    for frames in splitfr: 
        print("Frame Number" + str(i))
        frames.to_sql(table, schema='dbo', con = engine, chunksize=200, method='multi', index=False, if_exists='append')
        i+=1
        
    #cnxn = pyodbc.connect('DRIVER={'+ driver + '};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes;', )
    #cursor = cnxn.cursor()
    #input(str(cnxn))

    #cursor.execute('SELECT TOP 1 ' + 'Id, Title, Content, Url, Type ' + 'FROM ' + table)

    #datafr2 = pd.read_sql_query('SELECT TOP 20 ' + 'Id, Title, Content, Url, Type ' + 'FROM ' + table, cnxn)
    #input(datafr2.head(5))

    return 0




if __name__ == '__main__':

    print("Enter number for target function, 1 for reorderGoogle(), 2 for removeNan, 3 for merging and exporting  and 4 for BulkInsert to DB")
    execMode = input("Enter Number: ")

    if execMode == "1": # reorder Google/Youtube xlsx files
        print("Executing Mode 1， Reordering Google and Youtube xlsx docs")
        reorderGoogle()
        exit()
    elif execMode == "2": # Remove rows with null cells
        print("Executing Mode 2， Removing empty rows from xlsx files")
        removeNan()
        exit()
    elif execMode == "3": # Import all [xlsx] files in a folder and export as a single [data.csv] file
        print("Executing Mode 3， Removing empty rows from xlsx files")
        
        infile = "xlsx"
        outfile = "csv"

        datafr = dataIn(infile)
        dataOut(datafr, outfile)

        exit()
    elif execMode == "4": # Imports data from [data.csv] or multiple [output#.xlsx], concat and output to multiple xlsx using Multiprocessing
        print("Executing Mode 4， Import csv/xlsx and Export to multiple xlsx")

        filetype = "csv"
        datafr = dataIn(filetype)
        per = 1010000
        countRow = datafr.shape[0]
        counter = int(countRow/per) + (countRow%per > 0)
        print(datafr.head(5))
        print("Number of rows is: " + str(countRow))
        inprompt = 'Step 3: Export to Excel, File count is [' + str(counter) + '] Press Enter to continue, Ctrl+C to exit'
        input(inprompt)
        i=0
        outList = []
        while i < counter:
            start = (i*per)
            end = ((i+1)*per-1)
            dataSec =  datafr.iloc[start:end, :]
            outList.append(dataSec)
            i+=1
        poolprint()
    elif execMode == "5": # Gets data using dataIn() and bulk inserts data to SQL server DB
        print("Executing Mode 5， BULKINSERT data from files to DB")
        filetype = 'xlsx'
        df = dataIn(filetype)
        if len(df) == 1 :
            print("No Data Found")
            exit()
        sqlInsert(df)
    else : # Mode enetered not valid
        print("Not a valid Execute Mode, please rerun the program again")

    print("thanks for using")
