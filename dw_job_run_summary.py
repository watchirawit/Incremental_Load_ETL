import dao



def insertIntoJobRunSummary(tableName, start_date_time, end_date_time, rows_processed, status, error_message, colid, job_run_id):
    sql_server_cnxn = dao.getTargetConnection()
    sql_server_cursor  = sql_server_cnxn.cursor()




    try:
        sql_server_cursor.execute("""INSERT INTO [DW].[dbo].[dw_job_run_summary]
                                    ([tablename]
                                    ,[start_date_Time]
                                    ,[end_date_Time]
                                    ,[rows_processed]
                                    ,[status]
                                    ,[error_message]
                                    ,[colid]
                                    ,[job_run_id]
                                    
                                    )
                            values(?,?,?,?,?,?,?,?)""" 
                            ,tableName, start_date_time, end_date_time, rows_processed, status, error_message, colid, job_run_id
                            )
        
    except Exception as e:
            print(type(str(e)))
    
    sql_server_cnxn.commit()
    sql_server_cursor.close()

    sql_server_cnxn.close()