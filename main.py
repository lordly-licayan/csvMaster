import configparser, os, re, datetime, time, xlsxwriter, xlrd
import logging
from pathlib import Path
from os.path import exists, join
from shutil import copyfile

from constants import *
from reporting import *

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

configFileName= 'config.ini'

currentPath= Path(__file__).resolve().parent
config_file = join(currentPath, configFileName)
config = configparser.ConfigParser()
config.read(config_file, encoding='UTF-8')

#function to parse DB schema
def parseSchema(schemaPath):
    tableStartIndicator = config['OTHERS']['TABLE_START']
    tableEndIndicator = config['OTHERS']['TABLE_END']
    tableNameSearchPattern = config['OTHERS']['TABLE_NAME_SEARCH_PATTERN']
    fieldNameExclude = config['OTHERS']['FIELD_NAME_EXCLUDE']
    notNull = config['OTHERS']['NOT_NULL']
    default = config['OTHERS']['DEFAULT']
    primaryKey = config['OTHERS']['PRIMARY_KEY']    
    
    regexTableStart= re.compile(tableStartIndicator, re.IGNORECASE)
    regexTableName= re.compile(tableNameSearchPattern, re.IGNORECASE)
    regexFieldNameExclude = re.compile(fieldNameExclude, re.IGNORECASE)
    regexNotNull = re.compile(notNull, re.IGNORECASE)
    regexDefault = re.compile(default, re.IGNORECASE)
    regexPrimarykey = re.compile(primaryKey, re.IGNORECASE)
    
    with open(schemaPath, 'rt', encoding='utf-8') as fp:
        tableMap = {}
        tableColumnMap = {}
        tableName = ''
        isNewTable= False

        for line in fp:
            line = line.strip()
            #print(f'line: {line}') 
            
            #make sure line is not empty
            if not line or line == '(':
                continue
            
            hasTable = regexTableStart.search(line)
            if hasTable:
                #This is for collecting the table names
                tableName = regexTableName.search(line)
                if tableName:
                    isNewTable= True
                    tableName = tableName.group()
                    tableMap[tableName] = []
                    tableColumnMap[tableName] = []
                    #print(f'tableName: {tableName}')
                    continue

            #Check first if the table structure has been terminated/closed.
            if tableEndIndicator in line:
                isNewTable= False
                
            #Next is we need to collect the table fields
            fieldNameExcluded =  regexFieldNameExclude.search(line)
            if isNewTable and not fieldNameExcluded:
                columnList = tableMap[tableName]
                line = line.replace(',','')
                item = []
                for ele in re.split('\s', line):
                    if ele.strip():
                        item.append(ele)
                
                columnName = item[0]
                dataType = item[1]

                fieldMap= {}
                fieldMap['fieldName'] = columnName
                fieldMap['dataType'] = dataType
                fieldMap['isNotNull'] = False
                #fieldMap['isPrimaryKey'] = False
                
                tableColumnMap[tableName].append(columnName)
                
                isNotNull = regexNotNull.search(line)
                if isNotNull:
                    fieldMap['isNotNull'] = True

                hasDefault = regexDefault.search(line)
                if hasDefault:
                    fieldMap['default'] = item[3]

                hasPrimaryKey = regexPrimarykey.search(line)
                if hasPrimaryKey:
                    fieldMap['isPrimaryKey'] = True

                columnList.append(fieldMap)
    return tableMap, tableColumnMap           


#returns the list of modified tables
def getModifiedTables(tableColumnOrigMap, tableColumnUpdatedMap):
    #Determine what table(s) has been changed
    modifiedTableList= []
    for tableName in tableColumnOrigMap:
        columnsOrig= tableColumnOrigMap[tableName]
        columnsUpdated = tableColumnUpdatedMap[tableName]
        if columnsOrig != columnsUpdated:
            modifiedTableList.append(tableName)
    return modifiedTableList


#updates the current table columns based on the renamed table columns
def updateTableColumns(updatedTableColumnMap, tableColumnRenamedMap, renamedTableList):
    for tableName in renamedTableList:
        updatedTableColumnMap[tableName] = tableColumnRenamedMap[tableName]


#returns the list of files to evaluate
def listFiles(path, fileSearchPattern):
    fileList = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            filePath= join(r, file)
            baseFile= re.search(fileSearchPattern, filePath, re.IGNORECASE)
            
            if baseFile:
                fileName = os.path.basename(filePath)
                fileList.append(filePath)
    return fileList

#convert column names to list with trim. This also ensures that column names are trimmed.
def covertTrimmedStringToList(columnNames):
    newColumnList= []
    if columnNames:
        columnList= columnNames.lower().split(',')
        for columName in columnList:
             newColumnList.append(columName.strip())
    return newColumnList


#try to remove excluded suffixes
def removeExcludedSuffices(columnList, excludedFieldNameList):
    newColumnList= []
    for columnName in columnList:
        columnName = columnName.strip().lower()
        if not columnName in excludedFieldNameList:
            newColumnList.append(columnName)
    return newColumnList


#Try to predict the table structure of the CSV file.
def predictCsvTable(columnList, tableColumnMap, excludedFieldNameList):
    #Algo:
    #1. Loop through all the identified tables
    #2. Per table, loop all the elements of the columnList. Count the matching column names
    #3. Saved the matching percentage per table.
    #4. Once done, identify the suggested table(s) when the score is more than 50%.
    predictivityPercentageThreshold = config['OTHERS']['PREDICTIVITY_PERCENTAGE_THRESHOLD']
    columnList = removeExcludedSuffices(columnList, excludedFieldNameList)

    suggestedTableMap= {}
    suggestedTableName= ''
    noOfColumns = len(columnList)
    
    for tableName in tableColumnMap:
        ctr= 0
        unmatchedColumnList = []
        for columnName in columnList:

            if columnName in tableColumnMap[tableName]:
                ctr += 1
            else:
                unmatchedColumnList.append(columnName)

        matchPercentage = round((ctr/noOfColumns)*100)
        if matchPercentage >= float(predictivityPercentageThreshold):
            suggestedTableMap[tableName]= {}
            suggestedTableMap[tableName]['percentage'] = matchPercentage
            suggestedTableMap[tableName]['unmatchedColumns'] = unmatchedColumnList
            suggestedTableMap[tableName]['lackingColumns'] = []
        
            #in this case, the columns evaluated has lacking columns from the based table structure
            if matchPercentage == 100.0:
                suggestedTableMap[tableName]['lackingColumns'] = [item for item in tableColumnMap[tableName] if (item not in columnList)]
    
    if suggestedTableMap:
        suggestionList= {}
        for tableName in suggestedTableMap:
            suggestionList[tableName]= suggestedTableMap[tableName]['percentage']
        suggestedTableName =  list(dict(sorted(suggestionList.items(), key=lambda i : i[1], reverse=True)))[0]
        
    return suggestedTableMap, suggestedTableName


#returns the column list removing the excluded suffixed column names.
def getExcludedColumnMap(tableColumnMap, excludedFieldNameList):
    if excludedFieldNameList:
        newTableColumnMap= {}
        for tableName in tableColumnMap:
            newTableColumnMap[tableName] = removeExcludedSuffices(tableColumnMap[tableName], excludedFieldNameList)
        return newTableColumnMap
    return tableColumnMap


#returns a map containing the table structure of the csv
def processCsvTableIdentification(fileList, tableColumnMap, excludedFieldNameList):
    csvTableMapping = {}
    for filePath in fileList:
        csvTableName = ''
        suggestedTableName = ''
        suggestedTableNamePercentage = 0
        unmatchedColumns = ''
        lackingColumns = ''
        errorEncountered = ''
        columnList = []
        suggestedTableColumnList = []

        ctr= 0
        try:
            with open(filePath, 'r', encoding='utf-8') as fp:
                for line in fp:
                    line = line.strip()
                    ctr+= 1
                    #extract column names. Excluded columns names are removed to avoid false matching.
                    columnList= removeExcludedSuffices(covertTrimmedStringToList(line), excludedFieldNameList)
                    
                    #identify the table structure used in the csv file
                    for tableName in tableColumnMap:
                        tableColumns = tableColumnMap[tableName]

                        if tableColumns == columnList:
                            #print(f'tableName: {tableName}')
                            csvTableName = tableName
                            break                        

                    if not csvTableName:       
                        suggestedTableMap, suggestedTableName= predictCsvTable(columnList, tableColumnMap, excludedFieldNameList)

                        if suggestedTableMap:
                            suggestedTableInfo = suggestedTableMap[suggestedTableName]
                            suggestedTableColumnList = tableColumnMap[suggestedTableName]
                            suggestedTableNamePercentage = suggestedTableMap[suggestedTableName].get('percentage')
                            unmatchedColumns = ', '.join(item for item in suggestedTableMap[suggestedTableName].get('unmatchedColumns')) 
                            lackingColumns = ', '.join(item for item in suggestedTableMap[suggestedTableName].get('lackingColumns'))
                    #No need to read the succeeding lines
                    break
        except:
            if ctr == 0:
                errorEncountered = 'Can not read file!'
            else:
                errorEncountered = 'Line no. {} can not be read!!!'.format(ctr + 1)
            print('{} => {}'.format(errorEncountered, filePath))
        
        csvTableMapping[filePath] = {}
        csvTableMapping[filePath] ['tableName'] = csvTableName
        csvTableMapping[filePath] ['numberOfColumns'] = len(columnList)
        csvTableMapping[filePath] ['suggestedTableName'] = suggestedTableName
        csvTableMapping[filePath] ['suggestedTableNamePercentage'] = suggestedTableNamePercentage
        csvTableMapping[filePath] ['suggestedTableNumberOfColumns'] = len(suggestedTableColumnList)
        csvTableMapping[filePath] ['unmatchedColumns'] = unmatchedColumns
        csvTableMapping[filePath] ['lackingColumns'] = lackingColumns
        csvTableMapping[filePath] ['error'] = errorEncountered

    return csvTableMapping


#function to detect the new 
def getNewAndDeletedTableList(updatedTableColumnMap, tableColumnRestructuredMap):
    newTableList = []
    deletedTableList = []
    #list all new tables
    if updatedTableColumnMap:
        for tableName in tableColumnRestructuredMap:
            if tableName not in updatedTableColumnMap:
                newTableList.append(tableName)
    
    #list all deleted tables
    if tableColumnRestructuredMap:
        for tableName in updatedTableColumnMap:
            if tableName not in tableColumnRestructuredMap:
                deletedTableList.append(tableName)
    
    return newTableList, deletedTableList


#function to write files
def writeFile(filePath, lineList):
    #write the updated file
    with open(filePath, 'w+', encoding='utf-8') as fp:
        for line in lineList:
            #print(line)
            fp.write(line + NEWLINE)
        fp.close()


#Column index mapping
def mapColumnIndex(tableList, tableMap):
    columnIndexMap = {}
    for tableName in tableList:
        columnsMapping = {}
        for columnName in tableMap[tableName]:
            columnsMapping[columnName] = tableMap[tableName].index(columnName)
        columnIndexMap[tableName] = columnsMapping
    return columnIndexMap


#return the default value
def getDefaultValue(tableMap, columnName):
    value= ''
    isNotNull = False
    for fieldMap in tableMap:
        if columnName == fieldMap.get('fieldName'):
            #If column found in the old but the value is null then try to correct it with the new table structure.
            #Check if it is expected not to be NULL
            isNotNull = fieldMap.get('isNotNull')

            #try to get the default value
            defaultValue = fieldMap.get('default')
            if defaultValue:
                    value = defaultValue
            break
    return value, isNotNull


#function to process csv files for renamed columns and restructed tables.
def process(csvTableMapping, renamedTableList, updatedTableColumnMap, restructuredTableList, tableColumnRestructuredMap, tableRestructuredMap, appendModifiedFile, isTestMode, isAutoFix):
    #get the column indices of the restructured table.
    newColumnIndexMap = mapColumnIndex(restructuredTableList, tableColumnRestructuredMap)
    #print(f'newColumnIndexMap: {newColumnIndexMap}')

    #get the column indices of the updated table.
    oldColumnIndexMap = mapColumnIndex(restructuredTableList, updatedTableColumnMap)
    #print(f'newColumnIndexMap: {newColumnIndexMap}')

    #Mode of all files evaluated.
    processFileModeMap = {}
    
    #loop all the files. Checking if it needs to be modified based on renamed columns or restructured table
    for filePath in csvTableMapping:
        print(f'>> {filePath}')
        
        mode = 0 #For untouched file
        tableName = csvTableMapping[filePath]['tableName']
        

        #Try to associate the csv with the suggested table name in cases where its table name is undefined.
        if isAutoFix  == 'Y' and not tableName:
            #Objective: to find a suitable table structure of the csv file.
            #Check first if there is a suggested table name and there is no lacking columns.
            #Lacking columns are columns defined in the table structure but not present in the csv file.
            if csvTableMapping[filePath]['suggestedTableName'] and not csvTableMapping[filePath]['lackingColumns']:
                #When the percentage is 100.0% and there is no lacking column, it means that the columns are reordered. We will allow the correction of the header name.
                #In this case, to be able to apply the suggested table name, the column numbers of the csv file must match the column number of the suggested table.           
                #There are cases that column numbers are not matched (not safe to assumme the suggested table).                 
                    if csvTableMapping[filePath]['numberOfColumns'] == csvTableMapping[filePath]['suggestedTableNumberOfColumns']:
                        tableName = csvTableMapping[filePath]['suggestedTableName']
                        csvTableMapping[filePath]['tableName']= '{} (Auto applied)'.format(tableName) 
                        mode = 4        

        if tableName in restructuredTableList:
            mode = 2 #for restructured table
            if tableName in renamedTableList:
                mode = 3 #for renamed columns and restructured table
        elif tableName in renamedTableList:
            mode = 1 #for renamed columns

        #save the mode per file evaluated.
        processFileModeMap[filePath] = {'mode': mode }
        
        if mode == 0:
            #No need to further process the file since its associated table is not either renamed or restructured.
            processFileModeMap[filePath]['status'] = '-'
            continue
        else:
            #time to read the file
            with open(filePath, 'r', encoding='utf-8') as fp:
                lineList = []
                ctr = 0
                for line in fp:
                    line = line.strip()
                    ctr+=1
                    if ctr == 1:
                        if mode in (1,4):
                            #just copy the renamed column header
                            line = ','.join(updatedTableColumnMap[tableName])
                        else:
                            #make use of the column header of the restructured table 
                            line = ','.join(tableColumnRestructuredMap[tableName])
                        
                        #append the new column header
                        lineList.append(line)
                        #proceed to the next line
                        continue

                    if mode in (2,3):
                        #convert line to a list
                        columnValueLineList = re.split(',', line)
                        
                        #retrieve the column index of the old table structure
                        oldColumnMap = oldColumnIndexMap[tableName]
                        
                        #retrieve the column index of the restructured table
                        newColumnMap= newColumnIndexMap[tableName]
                        
                        newColumnValueList = []
                        for columnName in newColumnMap:
                            value= ''
                            isFoundInOldTable= False
                            isNotNull = False

                            #check if the columnName exist in the old table. Otherwise, get the default value for the new column
                            if columnName in oldColumnMap:
                                isFoundInOldTable= True
                                value= columnValueLineList[oldColumnMap[columnName]]

                            if (not isFoundInOldTable) or (not value and isFoundInOldTable):
                                #Check if there is a default value for the new table column(s)
                                value, isNotNull = getDefaultValue(tableRestructuredMap[tableName], columnName)
                                #try to determine if the column must not be NULL.
                                if isFoundInOldTable and not isNotNull:
                                    value = ''

                            #insert the new value in the correct index    
                            newColumnValueList.insert(newColumnMap[columnName], value)
                        #recreate line with the renamed columns or restructured table
                        line = ','.join(newColumnValueList)
                    
                    #append new line
                    lineList.append(line)
                #close file
                fp.close()

            if appendModifiedFile:
                fileData = os.path.splitext(filePath)
                processFileModeMap[filePath]['newFilePath'] = fileData[0] + appendModifiedFile + fileData[1]
            
            if isTestMode != 'Y':
                try:
                    #time to rewrite the csv file.
                    newFilePath = processFileModeMap[filePath].get('newFilePath')
                    
                    if not newFilePath:
                        newFilePath = filePath

                    writeFile(newFilePath, lineList)
                    processFileModeMap[filePath]['status'] = SUCCESS
                except:
                    processFileModeMap[filePath]['status'] = FAILED
                    #print(f'Error: Failed writing file => ', filePath)
            else:
                processFileModeMap[filePath]['status'] = 'No csv file written!'               

    return processFileModeMap


if __name__ == "__main__":
    try:
        start = datetime.datetime.now()
        print(f'\nInitializing...\nTime started: {start}')

        #parsing the current schema
        schemaCurrentPath = config['PATH']['SCHEMA_CURRENT']
        tableMap, tableColumnCurrentMap = parseSchema(schemaCurrentPath) 
        #print(f'tableMap: {tableMap}')
        #print(f'tableColumnCurrentMap: {tableColumnCurrentMap}')
        
        #parsing the schema - for renamed columns
        schemaRenamedPath = config['PATH']['SCHEMA_FOR_RENAMING']
        tableRenamedMap, tableColumnRenamedMap = {},{}
        if schemaRenamedPath:
            tableRenamedMap, tableColumnRenamedMap = parseSchema(schemaRenamedPath)
        #print(f'tableColumnRenamedMap: {tableColumnRenamedMap}')

        #Identify the renamed tables
        renamedTableList = []
        if tableColumnRenamedMap:
            renamedTableList = getModifiedTables(tableColumnCurrentMap, tableColumnRenamedMap)
        #print(f'renamedTableList: {renamedTableList}')

        #Identify the restructured table(s)
        schemaRestructuredPath = config['PATH']['SCHEMA_FOR_RESTRUCTURED']
        tableRestructuredMap, tableColumnRestructuredMap = {},{}
        if schemaRestructuredPath:
            tableRestructuredMap, tableColumnRestructuredMap = parseSchema(schemaRestructuredPath)
        #print(f'tableRestructuredMap: {tableRestructuredMap}')
        #print(f'tableColumnRestructuredMap: {tableColumnRestructuredMap}')
        
        #Get the updated renamed table columns.
        updatedTableColumnMap = tableColumnCurrentMap.copy()
        if not renamedTableList:
            print(f'No table column(s) has been renamed!')
        else:
            updateTableColumns(updatedTableColumnMap, tableColumnRenamedMap, renamedTableList)
        #print(f'updatedTableColumnMap: {updatedTableColumnMap}')

        #Identify the restructured tables
        restructuredTableList = []
        if not tableColumnRestructuredMap:
            print(f'No table(s) has been retructured!')
        else:    
            restructuredTableList = getModifiedTables(updatedTableColumnMap, tableColumnRestructuredMap)
        #print(f'restructuredTableList: {restructuredTableList}')

        #list all the csv files to evaluate
        sourcePath= config['PATH']['SOURCE']
        fileSearchPattern = config['OTHERS']['FILES_SEARCH_PATTERN']
        fileList = listFiles(sourcePath, fileSearchPattern)
        #print(f'fileList: {fileList}')

        #Identify the table associated with the Csv file
        excludedFieldNameList = covertTrimmedStringToList(config['OTHERS']['EXCLUDED_SUFFIX_FIELD_NAMES'])
        excludedColumnMap = getExcludedColumnMap(tableColumnCurrentMap, excludedFieldNameList)
        csvTableMapping= processCsvTableIdentification(fileList, excludedColumnMap, excludedFieldNameList)
        #print(f'csvTableMapping: {csvTableMapping}')

        #Time to process the csv files with the restructured tables.
        isTestMode = config['OTHERS']['TEST_MODE']
        isAutoFix = config['OTHERS']['AUTO_FIX']
        appendModifiedFile = config['OTHERS']['APPEND_MODIFIED_FILE']
        
        processedFileResultMap = process(csvTableMapping, renamedTableList, updatedTableColumnMap, restructuredTableList, tableColumnRestructuredMap, tableRestructuredMap, appendModifiedFile, isTestMode, isAutoFix)
        #print(f'processedFileResultMap: {processedFileResultMap}')

        #Identify the new and deleted tables
        newTableList, deletedTableList = getNewAndDeletedTableList(updatedTableColumnMap, tableColumnRestructuredMap)

        #Time to create the report file
        outputDirectory= config['REPORT']['OUTPUT']
        reportFolder= config['REPORT']['FOLDER_NAME']
        reportFileName= config['REPORT']['FILE_NAME']

        if not outputDirectory:
            outputDirectory= join(currentPath, reportFolder)
        outputFileXls= join(outputDirectory, reportFileName)

        reportingMap = {}
        reportingMap['originalTableList'] = tableColumnCurrentMap.keys()
        reportingMap['renamedTableList'] = renamedTableList
        reportingMap['restructuredTableList'] = restructuredTableList
        reportingMap['newTableList'] = newTableList
        reportingMap['deletedTableList'] = deletedTableList
        reportingMap['csvTableMapping'] = csvTableMapping
        reportingMap['processedFileResultMap'] = processedFileResultMap

        outputFileXls= "{}_{}.xlsx".format(outputFileXls, datetime.datetime.now().strftime("%m%d%Y_%H%M%S"))
        createReport(reportingMap, outputFileXls)

        finish = datetime.datetime.now()
        print(f'\nTime elapsed:\n{finish - start}')
    except Exception as err:
        logger.error(str(err), exc_info=True)