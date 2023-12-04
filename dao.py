
import pyodbc
import configparser


def getSourceConnection():
    config = configparser.ConfigParser()  
    with open('config.ini', 'r') as f:
        config.read_file(f)

    DSN = config.get('DB_Source', 'DSN')
    DB  = config.get('DB_Source', 'DB')
    UID = config.get('DB_Source', 'UID')
    PWD = config.get('DB_Source', 'PWD')



def getTargetConnection():
    config = configparser.ConfigParser()  
    with open('config.ini', 'r') as f:
        config.read_file(f)

    DSN = config.get('DB_Target', 'DSN')
    DB  = config.get('DB_Target', 'DB')
    UID = config.get('DB_Target', 'UID')
    PWD = config.get('DB_Target', 'PWD')









    # Connect to the SQL Server database
    sql_server_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                    f'SERVER={DSN};'
                                    f'DATABASE={DB};'
                                    f'UID={UID};'
                                    f'PWD={PWD}')


    # sql_server_cursor = sql_server_conn.cursor()

    return sql_server_conn
    # Close the SQL Server cursor and connection
    # sql_server_cursor.close()
    # sql_server_conn.close()
