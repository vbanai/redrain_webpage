import datetime
import psycopg2
from psycopg2 import sql

def fetch_chat_messages(start_date, end_date):
    
    print(start_date, end_date)
    host = 'flora-ai-chatbot.postgres.database.azure.com'
    dbname = 'ChatProject'
    user = "vbanai"
    password = "Vertigo9@" 
    sslmode = "require"

    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

    try:
        # Connect to the database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # Specify the table name
        table_name = 'chat_messages_latlong'

        # SQL query to select data within the specified period and order by date (ignoring time), user_id, and created_at
        select_query = f"""
        SELECT * 
        FROM {table_name} 
        WHERE created_at BETWEEN %s AND %s
        ORDER BY DATE(created_at), user_id, created_at;
        """

      

        # Execute the SQL query with parameters
        cursor.execute(select_query, (start_date, end_date))

        # Fetch all the rows
        rows = cursor.fetchall()

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        # Exclude the first column (index 0) and the 5th column (index 4) from columns
        columns = [col for idx, col in enumerate(columns[1:]) if idx != 3]
        
        # Exclude the first column (index 0) and the 5th column (index 4) from rows
        rows = [tuple(cell for idx, cell in enumerate(row[1:]) if idx != 3) for row in rows]

        # Close cursor and connection
        cursor.close()
        conn.close()
       
        return rows, columns

    except Exception as e:
        # Log the exception to the console
        print(f"An error occurred while fetching chat messages: {str(e)}")
        # Optionally, you can return an error response to the client
        return None, None
