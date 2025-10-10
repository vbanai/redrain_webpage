import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import requests
import os
import psycopg2

def user_querry_forquickreview():

  host = os.environ.get("HOST_AZURESQL")
  dbname = 'ChatProject'
  user = os.environ.get("username_AZURESQL")
  password = os.environ.get("password_AZURESQL")
  sslmode = "require"

  # Construct connection string
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

  # Connect to the Azure PostgreSQL database
  conn = psycopg2.connect(conn_string) 
  cursor = conn.cursor()

  # Specify the table name
  table_name = 'chat_messages_latlong'

  # SQL query to select all data from the table
  select_query = f"SELECT * FROM {table_name};"

  # Execute the SQL query
  cursor.execute(select_query)

  # Fetch all the rows
  rows = cursor.fetchall()

  # Get column names
  columns = [desc[0] for desc in cursor.description]

  df= pd.DataFrame(rows, columns=columns)
  cursor.close()
  conn.close()
  
  df = df.set_index(pd.to_datetime(df['created_at'])).drop(columns='created_at')

  
  # df=pd.read_excel("chatpluslocation.xlsx", index_col=0, parse_dates=True)

 
  today = datetime.now().replace(microsecond=0)
  today_ = datetime.today().date()
  days_to_subtract = today.weekday()
  previous_monday = today_ - timedelta(days=days_to_subtract)

  
  # Find the date of today's weekday exactly one week ago
  last_week_today = today - timedelta(days=7)

  # Find the date of the previous Monday exactly one week ago
  last_week_previous_monday = previous_monday - timedelta(days=7)

  today_with_time = today
  previous_monday_with_time = datetime.combine(previous_monday, datetime.min.time())
  last_week_today_with_time = last_week_today
  last_week_previous_monday_with_time = datetime.combine(last_week_previous_monday, datetime.min.time())




  

  filtered_df_thisweek = df[(df.index >= previous_monday_with_time) & (df.index <= today_with_time)]
 
  querry_this_week=len(filtered_df_thisweek)

  groupedthisweek = filtered_df_thisweek.groupby(filtered_df_thisweek.index.date)

  # Create a dictionary to hold the unique user_id counts for each day
  unique_user_counts_thisweek = {str(date): group['user_id'].nunique() for date, group in groupedthisweek}

  # Sum the unique user counts
  total_unique_users_thisweek = sum(unique_user_counts_thisweek.values())
  try:
      # Convert input values to Decimal
      querry_value = Decimal(querry_this_week)
      users_value = Decimal(total_unique_users_thisweek)
      
      # Check for zero division
      if users_value == 0:
          querry_onaverage_thisweek = Decimal('Infinity')  # or None, or a fallback value like Decimal('0.0')
      else:
          # Perform the division
          querry_onaverage_thisweek = querry_value / users_value
          
          # Round the result to one decimal place
          querry_onaverage_thisweek = querry_onaverage_thisweek.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        

  except InvalidOperation as e:
      print(f"Invalid operation: {e}")
  except ZeroDivisionError as e:
      print(f"Error: {e}")

  # querry_onaverage_thisweek = Decimal(querry_this_week) / Decimal(total_unique_users_thisweek)
  
  
  
  filtered_df_lastweek = df[(df.index >= last_week_previous_monday_with_time) & (df.index <= last_week_today_with_time)]

  groupedlastweek = filtered_df_lastweek.groupby(filtered_df_lastweek.index.date)

  # Create a dictionary to hold the unique user_id counts for each day
  unique_user_counts_lastweek = {str(date): group['user_id'].nunique() for date, group in groupedlastweek}

  # Sum the unique user counts
  total_unique_users_lastweek = sum(unique_user_counts_lastweek.values())
  
  
  return total_unique_users_lastweek, total_unique_users_thisweek, querry_onaverage_thisweek, today, previous_monday



def locationranking():

  today = datetime.now().replace(microsecond=0)
  today_ = datetime.today().date()
  days_to_subtract = today.weekday()
  previous_monday = today_ - timedelta(days=days_to_subtract)

  today_with_time = today
  previous_monday_with_time = datetime.combine(previous_monday, datetime.min.time())

  host = os.environ.get("HOST_AZURESQL")
  dbname = 'ChatProject'
  user = os.environ.get("username_AZURESQL")
  password = os.environ.get("password_AZURESQL")
  sslmode = "require"

  # Construct connection string
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
  try:
    # Connect to the Azure PostgreSQL database
    conn = psycopg2.connect(conn_string) 
    cursor = conn.cursor()

    # Specify the table name
    table_name = 'chat_messages_latlong'

    # SQL query to select all data from the table
    select_query = f"SELECT * FROM {table_name};"

    # Execute the SQL query
    cursor.execute(select_query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Get column names
    columns = [desc[0] for desc in cursor.description]

    df= pd.DataFrame(rows, columns=columns)
  
  finally:
    cursor.close()
    conn.close()
  
  
  df = df.set_index(pd.to_datetime(df['created_at'])).drop(columns='created_at')

  # df=pd.read_excel("chatpluslocation.xlsx", index_col=0, parse_dates=True)

  filtered_df_thisweek = df[(df.index >= previous_monday_with_time) & (df.index <= today_with_time)]
  filtered_df_thisweek = filtered_df_thisweek.dropna(subset=['location'])
  
  if filtered_df_thisweek.empty:
        return ""
 
  def get_first_location(group):
    # Drop `None` values and return the first valid location
    valid_locations = group['location'].dropna()
    
    if not valid_locations.empty:
      return valid_locations.iloc[0]
    

  
  
  locations = filtered_df_thisweek.groupby([filtered_df_thisweek.index.date, filtered_df_thisweek.user_id]) \
                                    .apply(get_first_location).tolist()
  locations = [loc for loc in locations if loc not in (None, '')]
  
  
  locationranking={}
  for city in locations:
    locationranking[city] = locationranking.get(city , 0) + 1
  locationranking_top3 = sorted(locationranking.items(), key=lambda x: x[1], reverse=True)[:3]
  
  if len(locationranking_top3)==0:
    return ""
  if len(locationranking_top3)==1:
    return (locationranking_top3[0][0],)
  if len(locationranking_top3)==2:
    return locationranking_top3[0][0], locationranking_top3[1][0], 
  if len(locationranking_top3)==3:
    return locationranking_top3[0][0], locationranking_top3[1][0], locationranking_top3[2][0]

  

  
def longitude_latitude():
  today = datetime.now().replace(microsecond=0)
  today_ = datetime.today().date()
  days_to_subtract = today.weekday()
  previous_monday = today_ - timedelta(days=days_to_subtract)

  today_with_time = today
  previous_monday_with_time = datetime.combine(previous_monday, datetime.min.time())

  # df=pd.read_excel("chatpluslocation.xlsx", index_col=0, parse_dates=True)

  host = os.environ.get("HOST_AZURESQL")
  dbname = 'ChatProject'
  user = os.environ.get("username_AZURESQL")
  password = os.environ.get("password_AZURESQL")
  sslmode = "require"

  # Construct connection string
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
  try:
    # Connect to the Azure PostgreSQL database
    conn = psycopg2.connect(conn_string) 
    cursor = conn.cursor()

    # Specify the table name
    table_name = 'chat_messages_latlong'

    # SQL query to select all data from the table
    select_query = f"SELECT * FROM {table_name};"

    # Execute the SQL query
    cursor.execute(select_query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Get column names
    columns = [desc[0] for desc in cursor.description]

    df= pd.DataFrame(rows, columns=columns)
  
  finally:
    cursor.close()
    conn.close()

  df = df.set_index(pd.to_datetime(df['created_at'])).drop(columns='created_at')
  filtered_df_thisweek = df[(df.index >= previous_monday_with_time) & (df.index <= today_with_time)]
 
  longlat_data = []
  for key, group in filtered_df_thisweek.groupby([filtered_df_thisweek.index.date, filtered_df_thisweek.user_id]):
      # Drop rows where latitude or longitude is NaN
      group = group.dropna(subset=['latitude', 'longitude'])
      
      if not group.empty:
          # Extract the first row with non-NaN latitude and longitude
          lat = group['latitude'].iloc[0]
          lng = group['longitude'].iloc[0]
          longlat_data.append({"location": {"lat": lat, "lng": lng}})
      else:
          longlat_data.append({"location": {"lat": None, "lng": None}})
    
  return longlat_data



def longitude_latitude_detailed(year, month, day, hour, minutes, seconds, year_end, month_end, day_end, hour_end, minutes_end, seconds_end):
  host = os.environ.get("HOST_AZURESQL")
  dbname = 'ChatProject'
  user = os.environ.get("username_AZURESQL")
  password = os.environ.get("password_AZURESQL")
  sslmode = "require"

  # Construct connection string
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
  try:
    # Connect to the Azure PostgreSQL database
    conn = psycopg2.connect(conn_string) 
    cursor = conn.cursor()

    # Specify the table name
    table_name = 'chat_messages_latlong'

    # SQL query to select all data from the table
    select_query = f"SELECT * FROM {table_name};"

    # Execute the SQL query
    cursor.execute(select_query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Get column names
    columns = [desc[0] for desc in cursor.description]

    df= pd.DataFrame(rows, columns=columns)
  
  finally:
    cursor.close()
    conn.close()

  df = df.set_index(pd.to_datetime(df['created_at'])).drop(columns='created_at')
  df = df.sort_index()
  
  
  from_date={"year":year, "month":month, "day":day, "hour":hour, "minutes":minutes, "seconds":seconds}
  to_date={"year":year_end, "month":month_end, "day":day_end, "hour":hour_end, "minutes":minutes_end, "seconds":seconds_end}
  
  def create_date_time(date):
    date_time_obj = datetime(year=int(date["year"]), month=int(date["month"]), day=int(date["day"]),
                            hour=int(date["hour"]), minute=int(date["minutes"]), second=int(date["seconds"]))

    # Format the datetime object as a string
    formatted_date_time = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_date_time

  from_=create_date_time(from_date)
  to_=create_date_time(to_date)

  filtered_df_thisweek = df[(df.index >= from_) & (df.index <= to_)]

  longlat_data = []
  for key, group in filtered_df_thisweek.groupby([filtered_df_thisweek.index.date, filtered_df_thisweek.user_id]):
      # Drop rows where latitude or longitude is NaN
      group = group.dropna(subset=['latitude', 'longitude'])
      
      if not group.empty:
          # Extract the first row with non-NaN latitude and longitude
          lat = group['latitude'].iloc[0]
          lng = group['longitude'].iloc[0]
          longlat_data.append({"location": {"lat": lat, "lng": lng}})
      else:
          longlat_data.append({"location": {"lat": None, "lng": None}})

  return longlat_data



  


