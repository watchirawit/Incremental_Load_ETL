import dao
import pandas as pd
import employees as emp



# targetTables = [
# "REGIONS",
# "JOBS",
# "EMPLOYEES",
# "LOCATIONS",
# "COUNTRIES",
# "DEPARTMENTS",
# "JOB_HISTORY",
# ]


targetTables = [
# "REGIONS",
# "JOBS",
"EMPLOYEES"
# "LOCATIONS",
# "COUNTRIES",
# "DEPARTMENTS",
# "JOB_HISTORY",
]


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
    emp.insertIntoEmployees(tableName)


for tablename in targetTables:
    #fatchRecords(tablename)
    insertRecoards(tablename)
    print('Name: ',tablename)