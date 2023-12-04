import dao
import pandas as pd


def insertIntoDepartments(tableName):
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    df = pd.read_csv(f'{tableName}.csv')
    df.fillna('0',inplace=True)

    for indexs, row in df.iterrows():

        sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[ST_DEPARTMENTS]
                                    ([DEPARTMENT_ID]
                                    ,[DEPARTMENT_NAME]
                                    ,[MANAGER_ID]
                                    ,[LOCATION_ID]

                                    )
                              values(?,?,?,?)""" 
                              ,row['0'],row['1'],float(row['2'])
                              ,row['3']
                              )
    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()