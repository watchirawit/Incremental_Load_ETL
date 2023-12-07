import dao
import pandas as pd
import dw_job_run_summary as JobsRunSummery
from datetime import datetime


def insertIntoCountries(tableName,job_run_id):
    rows_processed = 0
    success = True
    start_date_time = datetime.now()
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    df = pd.read_csv(f'{tableName}.csv')
    df.fillna('',inplace=True)

    for indexs, row in df.iterrows():
        rows_processed += 1
        try:
            sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[ST_COUNTRIES]
                                        ([COUNTRY_ID]
                                        ,[COUNTRY_NAME]
                                        ,[REGION_ID]
        
                                        )
                                values(?,?,?)""" 
                                ,row['0'],row['1'],row['2']
                                
                                )
        


        except Exception as e:
            #print(type(str(e)))
            success = False
            JobsRunSummery.insertIntoJobRunSummary(tableName, start_date_time, datetime.now(), rows_processed, "Fail", (str(e)), row['0'],job_run_id)
    if(success):
        JobsRunSummery.insertIntoJobRunSummary(tableName, start_date_time, datetime.now(), rows_processed, "Success", '', '',job_run_id)

    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()