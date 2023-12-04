import dao
import pandas as pd
import employees as emp
import locations 
import regions
import departments
import job_history
import jobs
import countries


targetTables = [
"REGIONS",
"JOBS",
"EMPLOYEES",
"LOCATIONS",
"COUNTRIES",
"DEPARTMENTS",
"JOB_HISTORY",
]


# targetTables = [
# "EMPLOYEES"
# ]

def truncateTable(tablename):
    sqlQuery = f"truncate table dbo.ST_{tablename}"
    sql_server_cnxn = dao.getSourceConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()

    sql_server_cursor.execute(sqlQuery)

    sql_server_cursor.close()

    sql_server_cnxn.commit()
    
    sql_server_cursor.close()
    sql_server_cnxn.close()
    print(sqlQuery)



def fatchRecords(tableName):
    sql_server_cnxn = dao.getSourceConnection()

    sqlQuery = f"""
                    select * 
                    from {tableName}
                    where 1=1
                """


    sql_server_cursor  = sql_server_cnxn.cursor()
    sql_server_cursor.execute(sqlQuery)

    hrList = []
    for row in sql_server_cursor:
        hrList.append([elem for elem in row])
        #print('row = %r' % (row,))
        print([elem for elem in row])

    pd.DataFrame(hrList).iloc[:,:].to_csv(f'{tableName}.csv',index=False)

    # Close the SQL Server cursor and connection
    sql_server_cursor.close()

    sql_server_cnxn.close()

def insertRecoards(tableName):

    if (tableName == 'REGIONS'):
        regions.insertIntoRegions(tableName)
    elif (tableName == 'JOBS'):
        jobs.insertIntoJobs(tableName)
    elif (tableName == 'EMPLOYEES'):
        emp.insertIntoEmployees(tableName)
    elif (tableName == 'LOCATIONS'):
        locations.insertIntoLocations(tableName)
    elif (tableName == 'COUNTRIES'):
        countries.insertIntoCountries(tableName)
    elif (tableName == 'DEPARTMENTS'):
        departments.insertIntoDepartments(tableName)
    elif (tableName == 'JOB_HISTORY'):
        job_history.insertIntoJob_history(tableName)



for tablename in targetTables:
    #fatchRecords(tablename)
    truncateTable(tablename)
    insertRecoards(tablename)
    print('Name: ',tablename)