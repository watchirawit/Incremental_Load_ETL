import dao
import pandas as pd


def insertIntoJobs(tableName):
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    df = pd.read_csv(f'{tableName}.csv')
    df.fillna('',inplace=True)

    for indexs, row in df.iterrows():

        sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[ST_JOBS]
                                    ([JOB_ID]
                                    ,[JOB_TITLE]
                                    ,[MIN_SALARY]
                                    ,[MAX_SALARY]

                                    )
                              values(?,?,?,?)""" 
                              ,row['0'],row['1'],row['2']
                              ,row['3']
                              )
    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()