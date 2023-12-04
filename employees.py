import dao
import pandas as pd


def insertIntoEmployees(tableName):
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    df = pd.read_csv(f'{tableName}.csv')
    df.fillna('0.0',inplace=True)

    for indexs, row in df.iterrows():
        print('0: ',type(row['0']))
        print('1: ',type(row['1']))
        print('2: ',type(row['2']))
        print('3: ',type(row['3']))
        print('4: ',type(row['4']))
        print('5: ',type(row['5']))
        print('6: ',type(row['6']))
        print('7: ',type(row['7']))
        print('8: ',type(row['8']))
        print('9: ',type(row['9']))
        print('10: ',type(row['10']))

        print('8: ',row['8'])
        print('9: ',row['9'])
        sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[ST_EMPLOYEES]
                                    ([EMPLOYEE_ID]
                                    ,[FIRST_NAME]
                                    ,[LAST_NAME]
                                    ,[EMAIL]
                                    ,[PHONE_NUMBER]
                                    ,[HIRE_DATE]
                                    ,[JOB_ID]
                                    ,[SALARY]
                                    ,[COMMISSION_PCT]
                                    ,[MANAGER_ID]
                                    ,[DEPARTMENT_ID] 
                                    )
                              values(?,?,?,?,?,?,?,?,?,?,?)""" 
                              ,row['0'],row['1'],row['2']
                              ,row['3'],row['4'],row['5']
                              ,row['6'],row['7'],float( row['8'])
                              ,float( row['9']),row['10']
                              )
    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()