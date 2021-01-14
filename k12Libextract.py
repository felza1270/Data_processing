from multiprocessing import Pool
from os.path import dirname, abspath
import pandas as pd
import xlsxwriter
import os, sys

#Declarations
poolLog  = []
fileCount = 0


parentdir = str(os.path.dirname(os.getcwd()))
sourceloc = str(os.getcwd()) + "\\" + "k12Lib_source"
resultloc = parentdir + "\\" + "k12Lib_results"
names = open("k12lib_fileNames.txt", "r+")
nameList = names.readlines()
names.close()
fileCount = int(nameList[2])


#removing unwanted rows based on filterlist
def filterlibin(datain, flist, fName):
    #fix url format
    urlcheck = datain['url'].str.startswith('http')
    if not urlcheck[5]:
        urlAdd = "https://ustcate0.lib.nctu.edu.tw:443/NTHU:館藏目錄:NTHU_ALEPH"
        datain['url'] = urlAdd + datain['url'].astype(str)
        print(datain['url'].str[30:].head(5))
    

    teststr = flist['urlid'].array
    urlidsta = datain.iloc[0,0].index('H0')


    #print(datain.url.str[urlidsta:].head(5))
    #print(teststr)

    datain = datain[~datain['url'].str.contains('|'.join(teststr), regex=True)]

    loc = resultloc + "\\" + fName
    #print(datain.url.str[urlidsta:].head(5))
    datain.to_csv(loc, index=False)

    return datain

#checking for all isntances of $$alphabet, takes a dataframe and an array/list
def findChktxts(datafr, txtchk) :
    counter = ["" for txt in txtchk]
    i=0
    for txt in txtchk:
        count = datafr.text.str.count(txt).sum()
        counter[i] = txt+ ": "+ str(count)
        i+=1
        #print(txt+ ": "+ str(count))
    return counter

def splitContent(dataIn):

    datafr = pd.DataFrame(dataIn["text"], index=None, dtype='str')
    datafr2 = pd.DataFrame(dataIn["text2"], index=None, dtype='str')
    datafr2 = datafr2.replace({'nan':''}, regex=True)
    datafr['text'] = datafr['text'] + ' ' + datafr2['text2']
    if datafr.iloc[0,0].startswith('text'):
        datafr = datafr.drop([0], axis=0)

    datafr['text'] = datafr['text'].str[3:]

    datafr = datafr.replace({'\$\$a':'_', '\$\$b':'_', '\$\$d':'_', '\$\$e':'_', 
                            '\$\$k':'_', '\$\$n':'_', '\$\$p':' ', '\$\$s':' ', 
                            '\$\$z':'_', '\$\$c':'_', '\$\$f':'_', '\$\$h':'_', '\$\$x':'_'}, regex=True)
    datafr = datafr.replace({'\$\$':'_'}, regex=True)

    tempContent = pd.DataFrame(datafr["text"], index=None, dtype='str')
    tempContent['content'] = datafr['text'].replace({'_':' '}, regex=True)
    tempTitle = pd.DataFrame(datafr["text"], index=None, dtype='str')
    tempTitle[['title', 'content']] = datafr.text.str.split('_', n=1, expand=True)
    dataOut = pd.concat([tempTitle['title'], tempContent['content']], axis=1)

    return dataOut

#exports the dataframe to xlsx
def printout(datafr, num):
    pid = os.getpid()
    outputnum = 'output'+ str(fileCount+num+1) +'.xlsx'
    outputloc = resultloc + "\\" + outputnum
    writer = pd.ExcelWriter(outputloc, engine='xlsxwriter', options={'strings_to_urls': False})
    datafr.to_excel(writer, index=False)
    writer.save()
    return pid

def logged(result):
    return poolLog.append(result)

def poolprint():
    with Pool() as pool: 
        for num in range(len(outList)):
            pool.apply_async(printout, [outList[num], num], callback= logged)
        pool.close()
        pool.join()
        print('PIDs : ', poolLog)
        nameList[2] = str(int(nameList[2])+counter)
        names = open("fileNames.txt", "w")
        names.writelines(nameList)
        names.close()


if __name__ == '__main__':
    #file I/O
    names = open("k12lib_fileNames.txt", "r+")
    nameList = names.readlines()
    names.close()

    fileName = nameList[0].strip()
    filterlist = sourceloc + "\\" + nameList[1].strip()

    filepath = sourceloc + "\\" + fileName

    flistIn = pd.read_csv(filterlist, sep=',', header=None, names=["urlid"], dtype="str")

    #Checking file validity
    if not os.path.isfile(filepath):
        print('cannot find ' + filepath)
        exit()

    inprompt = 'Step 1: Import Data, File is [' + filepath + '] Press Enter to continue, Ctrl+C to exit'
    input(inprompt)

    #checking what operation to perform 
    if fileName.startswith('_'): 
        libIn = pd.read_csv(filepath, sep=',', header=0, names=["url", "num", "text", "text2"], dtype="str")
        print('file: ' + fileName)
    else: 
        libIn = pd.read_csv(filepath, sep=',', header=None, names=["url", "num", "text", "text2"], dtype="str")
        if libIn.iloc[0,0].startswith('Z00R'):
            libIn = libIn.drop([0], axis=0)
        flistIn = flistIn.drop([0], axis=0)
        newfileName = '_' + fileName[:fileName.index(".txt")] + '.csv' + '\n'
        libIn = filterlibin(libIn, flistIn, newfileName)
        nameList[0] = newfileName
        print('Filtering by '+ nameList[1] +' is complete, now exiting', end='\n')
        exit()

    
    print("Step 2: post IO DataFrame Manipulation, beginning", end='\n')
    #post I/O manipulation
    libSplit = splitContent(libIn)
    libOut = pd.concat([libIn['url'], libSplit['title'], libSplit['content']], axis=1)
    libOut = libOut[['title', 'content', 'url']]
    libOut['type'] = 4

    per = 100000
    countRow = libOut.shape[0]
    counter = int(countRow/per) + (countRow%per > 0)


    inprompt = 'Step 3: Export to Excel, File count is [' + str(counter) + '] Press Enter to continue, Ctrl+C to exit'
    input(inprompt)


    i=0
    outList = []
    while i < counter:
        start = (i*per)
        end = ((i+1)*per-1)
        libOuta =  libOut.iloc[start:end, :]
        outList.append(libOuta)
        i+=1


    #export to excel using multiproccesing pools
    poolprint()
