import psycopg2
from config import config



# database connection

def DbConn():
    try:
       # import params from config
        params = config()
        # establish db connection
        conn = psycopg2.connect(**params)
    except psycopg2.OperationalError as e:
        raise e
    else:
        print('Connected')
        return conn