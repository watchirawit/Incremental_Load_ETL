import dao
import pandas as pd


def insertIntoLocations(tableName):
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    df = pd.read_csv(f'{tableName}.csv')
    df.fillna('',inplace=True)

    for indexs, row in df.iterrows():

        sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[ST_LOCATIONS]
                                    ([LOCATION_ID]
                                    ,[STREET_ADDRESS]
                                    ,[POSTAL_CODE]
                                    ,[CITY]
                                    ,[STATE_PROVINCE]
                                    ,[COUNTRY_ID]
                                    )
                              values(?,?,?,?,?,?)""" 
                              ,row['0'],row['1'],row['2']
                              ,row['3'],row['4'],row['5']
                              
                              )
    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()