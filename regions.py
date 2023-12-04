import dao
import pandas as pd


def insertIntoRegions(tableName):
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    df = pd.read_csv(f'{tableName}.csv')
    df.fillna('',inplace=True)

    for indexs, row in df.iterrows():

        sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[ST_REGIONS]
                                    ([REGION_ID]
                                    ,[REGION_NAME]
    
                                    )
                              values(?,?)""" 
                              ,row['0'],row['1']
                             
                              )
    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()