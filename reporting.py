
import pandas as pd
import numpy as np
import xlsxwriter

def createReport(reportingMap, outputFile= "output.xlsx"):
    originalTableList= reportingMap['originalTableList']
    renamedTableList = reportingMap['renamedTableList']
    restructuredTableList = reportingMap['restructuredTableList']
    newTableList = reportingMap['newTableList']
    deletedTableList = reportingMap['deletedTableList']

    print("Writing report on ", outputFile)
    workbook = xlsxwriter.Workbook(outputFile)
    worksheetTableInfo = workbook.add_worksheet("Table Info")

    header = workbook.add_format({'border' : 1,'bg_color' : '#5DB067', 'bold': True})
    border = workbook.add_format({'border': 1})
    evaluatedRow = workbook.add_format({'border' : 1,'bg_color' : '#FFFFCC'})
    errorUnknownRow = workbook.add_format({'border' : 1, 'bg_color' : '#FFF2E5'})
    correctedRow = workbook.add_format({'border' : 1,'bg_color' : '#E6FFFF'})
    errorRow = workbook.add_format({'border' : 1, 'font_color' : 'red', 'bold': True})


    worksheetTableInfo.set_column('A:A',5)
    worksheetTableInfo.set_column('B:B',5)
    worksheetTableInfo.set_column('C:C',40)
    worksheetTableInfo.set_column('D:D',70)

    worksheetTableInfo.write('B2','No.', header)
    worksheetTableInfo.write('C2','Table name', header)
    worksheetTableInfo.write('D2','Remarks', header)

    row = 2
    col= 1
    counter = 1
    
    combineList= list(set(renamedTableList + restructuredTableList + newTableList + deletedTableList))
    #print("combineList: ", combineList)

    for tableName in combineList:
        worksheetTableInfo.write_number(row, col, counter, border)
        worksheetTableInfo.write_string(row, col + 1, tableName, border)
        remarks= 'Table columns renamed.'
        if tableName in renamedTableList and tableName in restructuredTableList:
            remarks= 'Table columns were renamed and restructured.'
        elif tableName in restructuredTableList:
            remarks= 'Table has been restructured.'
        elif tableName in newTableList:
            remarks= 'New table has been detected.'
        elif tableName in deletedTableList:
            remarks= 'Deleted table'
        worksheetTableInfo.write_string(row, col + 2, remarks, border)
        row += 1
        counter += 1

    for tableName in originalTableList:
        if tableName not in combineList:
            worksheetTableInfo.write_number(row, col, counter, border)
            worksheetTableInfo.write_string(row, col + 1, tableName, border)
            worksheetTableInfo.write_string(row, col + 2, 'Unchanged', border)
            row += 1
            counter += 1

    #Reporting per csv file listed
    csvTableMapping = reportingMap['csvTableMapping']
    processedFileResultMap = reportingMap['processedFileResultMap']

    #update processedFileResultMap with the associated table of the csv file
    for filePath in processedFileResultMap:
        processedFileResultMap[filePath]['tableName'] = csvTableMapping[filePath]['tableName']
        processedFileResultMap[filePath]['suggestedTableName'] = ''
        if csvTableMapping[filePath]['suggestedTableNamePercentage'] > 0: 
            processedFileResultMap[filePath]['suggestedTableName'] = '{0} ({1}%)'.format(csvTableMapping[filePath]['suggestedTableName'], csvTableMapping[filePath]['suggestedTableNamePercentage'])
        processedFileResultMap[filePath]['unmatchedColumns'] = csvTableMapping[filePath]['unmatchedColumns']
        if csvTableMapping[filePath]['suggestedTableNamePercentage'] == 100.0:
            if not csvTableMapping[filePath]['lackingColumns']:
                processedFileResultMap[filePath]['unmatchedColumns'] = 'Incorrect column ordering detected!'
            else:
                processedFileResultMap[filePath]['unmatchedColumns'] = 'Lacking columns: {}'.format(csvTableMapping[filePath]['lackingColumns'])
        processedFileResultMap[filePath]['error'] = csvTableMapping[filePath]['error']
    #print(f'processedFileResultMap: {processedFileResultMap}')

    worksheetCsvFile = workbook.add_worksheet("Result")
    worksheetCsvFile.set_column('A:A',5)
    worksheetCsvFile.set_column('B:B',160)
    worksheetCsvFile.set_column('C:C',50)
    worksheetCsvFile.set_column('D:D',60)
    worksheetCsvFile.set_column('E:E',100)
    worksheetCsvFile.set_column('F:F',180)
    worksheetCsvFile.set_column('G:G',100)
    worksheetCsvFile.set_column('H:H',30)

    worksheetCsvFile.write('A2','No.', header) 
    worksheetCsvFile.write('B2','File', header)
    worksheetCsvFile.write('C2','Table name', header)
    worksheetCsvFile.write('D2','Suggested table model', header)
    worksheetCsvFile.write('E2','Unmatched column names', header)
    worksheetCsvFile.write('F2','New File Created', header)
    worksheetCsvFile.write('G2','Remarks', header)
    worksheetCsvFile.write('H2','CSV creation status', header)

    row = 2
    col= 0
    counter = 1
    
    for itemName in processedFileResultMap:
        itemMap = processedFileResultMap[itemName]
        mode = itemMap.get('mode')
        tableName = itemMap.get('tableName')
        suggestedTableName = itemMap.get('suggestedTableName')
        unmatchedColumns = itemMap.get('unmatchedColumns')
        newFilePath= itemMap.get('newFilePath')
        status= itemMap.get('status')
        error= itemMap.get('error')
        
        #create remarks based on the mode
        remarks= "---"
        lineAttribute = border
        if mode == 0:
            if error:
                remarks= error
                lineAttribute = errorRow
            else:
                if not tableName:
                    remarks= "Not evaluated. Unknown table structure."
        elif mode == 1:
            remarks= "Renamed table columns detected!"
        elif mode == 2:
            remarks= "Csv table has been restructed!"
        elif mode == 3:
            remarks= "Table columns renamed and restructured!"
        elif mode == 4:
            remarks= "Corrected table header"
            lineAttribute = correctedRow
        else:
            lineAttribute = errorUnknownRow
            remarks = "Error: Unknown!"

        if not newFilePath:
            newFilePath= '-'

        if tableName in combineList:
            lineAttribute = evaluatedRow

        worksheetCsvFile.write_number(row, col, counter, lineAttribute)
        worksheetCsvFile.write_string(row, col + 1, itemName, lineAttribute)
        worksheetCsvFile.write_string(row, col + 2, tableName, lineAttribute)
        worksheetCsvFile.write_string(row, col + 3, suggestedTableName, lineAttribute)
        worksheetCsvFile.write_string(row, col + 4, unmatchedColumns, lineAttribute)
        worksheetCsvFile.write_string(row, col + 5, newFilePath, lineAttribute)
        worksheetCsvFile.write_string(row, col + 6, remarks, lineAttribute)
        worksheetCsvFile.write_string(row, col + 7, status, lineAttribute)
        row += 1
        counter += 1

    workbook.close()
    print("Done writing report.")