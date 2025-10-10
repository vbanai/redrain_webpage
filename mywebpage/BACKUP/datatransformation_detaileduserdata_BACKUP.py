import pandas as pd
import os
import calendar
import re
from collections import Counter
import numpy as np
from collections import defaultdict
import ast
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import copy
from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta
import psycopg2


def datatransformation_for_chartjs_detailed(year, month, day, hour, minutes, seconds, year_end, month_end, day_end, hour_end, minutes_end, seconds_end,frequency, table_name):
  
  ######################################
  #   FETCHING DATA FROM ELEPHANTSQL   #
  ######################################
    
  # database_url = "postgres://omeakqpt:xUUfVIvuZMNPUookJJXGiq4vFAwcShil@flora.db.elephantsql.com/omeakqpt"
  # database_url=database_url.replace('postgres', 'postgresql')
  # engine = create_engine(database_url)
  # sql_query_tracking = sql_text('SELECT * FROM "chat_messages"')
  # with engine.connect() as connection:
  #   result = connection.execute(sql_query_tracking)
  #   rows = result.fetchall()
  # columns = result.keys()

  # Ez Kell:
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
    table_name = table_name

    # SQL query to select all data from the table
    select_query = f"SELECT * FROM {table_name};"

    # Execute the SQL query
    cursor.execute(select_query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Get column names
    columns = [desc[0] for desc in cursor.description]

    df_pandas = pd.DataFrame(rows, columns=columns)
  finally:
    cursor.close()
    conn.close()

  #path="chat_messages.xlsx"
  # path="chatpluslocation.xlsx"
  # df_pandas = pd.read_excel(path)
  def replace_curly_braces(text):
    # Find patterns like {{...}} and replace with [['...']]
    pattern = r'\{\{(.*?)\}\}'
    # Replace with [['...']]
    replaced_text = re.sub(pattern, r"[['\1']]", text)
    return replaced_text

  # Apply the replacement to the 'topic' column
  df_pandas['topic'] = df_pandas['topic'].apply(replace_curly_braces)
  df_pandas['topic'] = df_pandas['topic'].apply(ast.literal_eval)

  
  #######################################################
  #   CREATING AND RESTRUCTURING THE PANDAS DATAFRAME   #
  #######################################################

  # df_pandas = pd.DataFrame(rows, columns=columns)

  # Remove milliseconds from 'created_at' column and set it as index
  df_pandas['created_at'] = pd.to_datetime(df_pandas['created_at']).dt.floor('s')
  
  # Set 'created_at' column as the index
  df_pandas.set_index('created_at', inplace=True)
  #df_pandas.drop(columns=['id'], inplace=True)
  df_pandas.index = pd.to_datetime(df_pandas.index)

  # Using the extracted data from the FORM to get the requested PERIOD 

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
  df_pandas = df_pandas.sort_index()
  df_pandas=df_pandas.loc[from_: to_]
  if df_pandas.empty:
    return [], [], [], [], [],[],[],[]
######################################################################################################
#  df_pandas DATAFRAME CONTAINING THE CHAT MESSAGES IN THIS WAY:                                     #
#                                                                                                    #
#    created_at          user_id             message                                  topic          #
#  2024-03-03 21:07:39  127.0.0.1  USER: Milyen akusztikus gitárok kaphatók? | AS...                 #
#  2024-03-03 21:08:24  127.0.0.1  USER: Milyen ceruzatartók kaphatók? | ASSISTAN...                 #
######################################################################################################

  # Using the requested FREQUENCY (daily, weekly, monthly, yearly) to the following breakdown of the data

  def find_day_of_week(date_string):
      try:
          # Convert the date string to a datetime object
          date_object = datetime.strptime(date_string, '%Y-%m-%d')
          
          # Get the day of the week (Monday is 0 and Sunday is 6)
          day_of_week = date_object.weekday()
          
          # Define a list of days of the week
          days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
          
          # Return the day of the week corresponding to the index
          return days[day_of_week]
      except ValueError:
          return "Please enter a valid date string in the format YYYY-MM-DD"
  
  start_end_date_byfrequency=[]
  timestamp=[]
  breakdown=frequency

  ##############
  #   WEEKLY   #
  ##############

  if breakdown=="weekly":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index)

    # Find the start and end dates of the data
    start_date = df_pandas.index.min().date()  # Extract the date component
    end_date = df_pandas.index.max().date()    # Extract the date component
   

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Define a function to get the start and end dates of a week
    def get_week_boundaries(current_date):
        start_of_week = current_date - pd.DateOffset(days=current_date.weekday())
        end_of_week = start_of_week + pd.DateOffset(days=6)
        return start_of_week.date(), end_of_week.date()  # Convert Timestamp to date
    
    #add previous start and end date of the previous week of the period
  
    days_to_subtract = start_date.weekday()
    previous_monday = start_date - timedelta(days=days_to_subtract)

    last_week_previous_monday = previous_monday - timedelta(days=7)
    start_lastweek=start_date-timedelta(days=7)
    sunday_of_week = last_week_previous_monday + timedelta(days=6)
    week_start, week_end = get_week_boundaries(pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").date()))
    
    if datetime.strptime(to_, "%Y-%m-%d %H:%M:%S").date()>week_end:
      start_end_date_byfrequency.append([datetime.combine(start_lastweek, time(int(hour), int(minutes), int(seconds))).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(sunday_of_week, time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')])
    else:
      start_end_date_byfrequency.append([datetime.combine(start_lastweek, time(int(hour), int(minutes), int(seconds))).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(sunday_of_week, time(int(hour_end), int(minutes_end), int(seconds_end))).strftime('%Y-%m-%d %H:%M:%S')])


    # Iterate over weeks from start to end
  
    current_date = pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")).date()
    last_day = pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")).date()
    while current_date <= last_day:
        week_start, week_end = get_week_boundaries(pd.Timestamp(current_date))
        if current_date==pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")).date() and last_day>week_end:
          from_converted=datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")
          start_end_date_byfrequency.append([from_, datetime.combine(week_end, time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')])
          sub_df = df_pandas[(df_pandas.index >= from_converted) & (df_pandas.index <= datetime.combine(week_end, time(23, 59, 59)))]

          if sub_df.empty:
            start_date = pd.Timestamp(start_date.year, start_date.month, start_date.day, 1, 0, 0)
            sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[start_date])
            sub_df._is_special_df = True
        elif last_day<=week_end and not(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").date()==current_date):
          year_end_int=int(year_end)
          month_end_int=int(month_end)
          day_end_int=int(day_end)
          end_date_obj = datetime(year_end_int, month_end_int, day_end_int)
          

          start_end_date_byfrequency.append([datetime.combine(week_start, time(0, 0, 0)).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(end_date_obj, time(int(hour_end), int(minutes_end), int(seconds_end))).strftime('%Y-%m-%d %H:%M:%S')])
          sub_df = df_pandas[(df_pandas.index.date >= week_start) & (df_pandas.index.date <= end_date)]
          if sub_df.empty:
            week_start = pd.Timestamp(week_start.year, week_start.month, week_start.day, 1, 0, 0)
            sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[week_start])
            sub_df._is_special_df = True
        #When from and to falls between week start and week end date:
        elif datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")< datetime.combine(week_end, time(23, 59, 59)) and datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")>datetime.combine(week_start, time(0, 0, 0)):
          start_end_date_byfrequency.append([from_, to_])
          from_converted=datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")
          to_converted=datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")
          sub_df = df_pandas[(df_pandas.index >= from_converted) & (df_pandas.index <= to_converted)]
          if sub_df.empty:
            week_start = pd.Timestamp(from_converted.year, from_converted.month, from_converted.day, from_converted.hour, from_converted.minute, from_converted.second)
            sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[week_start])
            sub_df._is_special_df = True
        else:
          start_end_date_byfrequency.append([datetime.combine(week_start, time(0, 0, 0)).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(week_end, time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')])
        # Select rows within the current week
          sub_df = df_pandas[(df_pandas.index.date >= week_start) & (df_pandas.index.date <= week_end)]
          if sub_df.empty:
            week_start = pd.Timestamp(week_start.year, week_start.month, week_start.day, 1, 0, 0)
            sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[week_start])
            sub_df._is_special_df = True
        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)
        # Move to the next week
        current_date = (week_end + pd.DateOffset(days=1)).date()  # Ensure current_date is a date object

    # Now sub_dataframes contains sub DataFrames grouped by week
    for i in sub_dataframes:
      timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))
 

 
  ##############
  #   DAILY    #
  ##############

 
  if breakdown=="daily":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index)

    # Find the start and end dates of the data
    start_date = df_pandas.index.min().date()  # Extract the date component
    end_date = df_pandas.index.max().date()    # Extract the date component

    start_datetime = datetime.combine(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").date() - timedelta(days=1), time(int(hour), int(minutes), int(seconds)))
    if datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")< datetime.combine(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").date(), time(23, 59, 59)):
      end_datetime = datetime.combine(start_datetime.date(), time(int(hour_end), int(minutes_end), int(seconds_end)))
    else:
      end_datetime = datetime.combine(start_datetime.date(), time(23,59,59))

    start_end_date_byfrequency.append([
    start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
    end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    ])
    
    # Initialize a list to store sub DataFrames
    sub_dataframes = []
    
    # Iterate over each day from start to end
    from_date=pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")).date()
    current_date = from_date
    last_day = pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")).date()
    while current_date <= last_day:
      if current_date==from_date and pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))<=datetime.combine(current_date, time(23, 59, 59)):
        start_end_date_byfrequency.append([from_, to_])
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))]
        if sub_df.empty:
          start_date = pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[start_date])
          sub_df._is_special_df = True
      elif current_date==from_date and pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))>=datetime.combine(current_date, time(23, 59, 59)):
        start_end_date_byfrequency.append([from_, datetime.combine(current_date, time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')])
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= datetime.combine(current_date, time(23, 59, 59)))]
        if sub_df.empty:
          start_date = pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[start_date])
          sub_df._is_special_df = True
      elif pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))<=datetime.combine(current_date, time(23, 59, 59)) and current_date!=from_date:
        start_end_date_byfrequency.append([datetime.combine(current_date, time(0, 0, 0)).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(current_date, time(int(hour_end), int(minutes_end), int(seconds_end))).strftime('%Y-%m-%d %H:%M:%S')])
        sub_df = df_pandas[(df_pandas.index >= datetime.combine(current_date, time(0, 0, 0))) & (df_pandas.index <= datetime.combine(current_date, time(int(hour_end), int(minutes_end), int(seconds_end))))]
        if sub_df.empty:
          week_start = pd.Timestamp(current_date, time(0, 0, 0))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[week_start])
          sub_df._is_special_df = True
      #When from and to falls between week start and week end date:
      else:
        start_end_date_byfrequency.append([datetime.combine(current_date, time(0, 0, 0)).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(current_date, time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')])
      # Select rows within the current week
        sub_df = df_pandas[(df_pandas.index >= datetime.combine(current_date, time(0, 0, 0))) & (df_pandas.index <= datetime.combine(current_date, time(23, 59, 59)))]
        if sub_df.empty:
          week_start = pd.Timestamp(current_date, time(0, 0, 0))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[week_start])
          sub_df._is_special_df = True
      # Append the sub DataFrame to the list
      sub_dataframes.append(sub_df)
      # Move to the next day    time(int(hour_end), int(minutes_end), int(seconds_end))
   
      current_date += pd.Timedelta(days=1)

    # Now sub_dataframes contains sub DataFrames grouped by each individual day
    for i in sub_dataframes:
      timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))

  ##############
  #   YEARLY   #
  ##############

  if breakdown=="yearly":
    start_datetime = datetime.combine(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").date() - timedelta(days=365), time(int(hour), int(minutes), int(seconds)))
    year_current = datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").year
    yearend_current= datetime.strptime(f"{year_current}-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
    if datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")< yearend_current:
      end_datetime = datetime.combine(start_datetime.date(), time(int(hour_end), int(minutes_end), int(seconds_end)))
    else:
      end_datetime = datetime.strptime(f"{start_datetime.date().year}-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
    
    start_end_date_byfrequency.append([
    start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
    end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    ])

    
    # Initialize a list to store sub DataFrames
    sub_dataframes = []
    from_date=datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").year
    current_date=datetime.strptime(from_, "%Y-%m-%d %H:%M:%S").year
    last_day=datetime.strptime(to_, "%Y-%m-%d %H:%M:%S").year
    while current_date <= last_day:
      yearend_updatedcurrent= datetime.strptime(f"{current_date}-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
      if current_date==from_date and pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))<=yearend_current:
        start_end_date_byfrequency.append([from_, to_])
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))]
        if sub_df.empty:
          start_date = pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[start_date])
          sub_df._is_special_df = True
      elif current_date==from_date and pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))>=yearend_current:
        start_end_date_byfrequency.append([from_, yearend_current.strftime('%Y-%m-%d %H:%M:%S')])
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= yearend_current)]
        if sub_df.empty:
          start_date = pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[start_date])
          sub_df._is_special_df = True
      elif pd.Timestamp(datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))<=yearend_updatedcurrent and current_date!=from_date:
        start_end_date_byfrequency.append([datetime.strptime(f"{current_date}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), to_])
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(f"{current_date}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= datetime.strptime(to_, "%Y-%m-%d %H:%M:%S"))]
        if sub_df.empty:
          year_start = pd.Timestamp(datetime.strptime(f"{current_date}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[year_start])
          sub_df._is_special_df = True
      #When from and to falls between week start and week end date:
      else:
        start_end_date_byfrequency.append([datetime.strptime(f"{current_date}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'), datetime.strptime(f"{current_date}-12-31 23:59:59", "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')])
      # Select rows within the current week
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(f"{current_date}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= datetime.strptime(f"{current_date}-12-31 23:59:59", "%Y-%m-%d %H:%M:%S"))]
        if sub_df.empty:
          year_start = pd.Timestamp(datetime.strptime(f"{current_date}-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[year_start])
          sub_df._is_special_df = True
      # Append the sub DataFrame to the list
      sub_dataframes.append(sub_df)
      # Move to the next day  
      
      current_date += 1


    # Now sub_dataframes contains sub DataFrames grouped by each individual year
    for i in sub_dataframes:
          timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))


  ###############
  #   MONTHLY   #
  ###############

  if breakdown=="monthly":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index)

    # ---------------------------------------------------------------------------
    # PREVIOUS MONTH
    from_date=datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")
    from_date_year=from_date.date().year
    from_date_month=from_date.date().month
   
    to_date=datetime.strptime(to_, "%Y-%m-%d %H:%M:%S")
    to_date_day=to_date.date().day

    from_date_previous_month = (from_date - relativedelta(months=1))  
    from_date_year_previous=from_date_previous_month.date().year
    from_date_month_previous=from_date_previous_month.date().month

    def last_day_of_month(year, month):
      # Get the last day of the month
      last_day = calendar.monthrange(year, month)[1]
      # Create a datetime object for the last day of the month
      last_day_date = datetime(year, month, last_day)
      return last_day_date
    
    last_day_of_month_= last_day_of_month(from_date_year, from_date_month)
    last_day_of_month_previous= last_day_of_month(from_date_year_previous, from_date_month_previous)
    date_ofLastday_of_month=datetime.strptime(f"{from_date_year}-{from_date_month}-{last_day_of_month_.date().day} 23:59:59", "%Y-%m-%d %H:%M:%S")

    if to_date< date_ofLastday_of_month:
      datetime_str = f"{from_date_year_previous}-{from_date_month_previous}-{to_date_day} {hour_end}:{minutes_end}:{seconds_end}"
      end_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    else:
      datetime_str = f"{from_date_year_previous}-{from_date_month_previous}-{last_day_of_month_previous.date().day} 23:59:59"
      end_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

    start_end_date_byfrequency.append([
    from_date_previous_month.strftime('%Y-%m-%d %H:%M:%S'),
    end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    ])

    
    
    from_date00=datetime.strptime(start_end_date_byfrequency[0][0], "%Y-%m-%d %H:%M:%S")
    to_date00=datetime.strptime(start_end_date_byfrequency[0][1], "%Y-%m-%d %H:%M:%S")
    sub_df00 = df_pandas[(df_pandas.index >= from_date00) & (df_pandas.index <= to_date00)]
    
    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Iterate over each month from start to end
    current_date = pd.Timestamp(year=from_date.date().year, month=from_date.date().month, day=1)
    end_date_timestamp=pd.Timestamp(year=to_date.date().year, month=to_date.date().month, day=1)
    while current_date <= end_date_timestamp:
      start_of_month = pd.Timestamp(year=current_date.year, month=current_date.month, day=1)
      end_of_month = start_of_month + pd.offsets.MonthEnd(0)
      end_of_month_with_time = end_of_month.replace(hour=23, minute=59, second=59)
      
      if current_date.year==from_date.date().year and current_date.month==from_date.date().month and end_of_month_with_time>to_date:
        start_end_date_byfrequency.append([from_, to_])
        # Select rows for the current month
        sub_df = df_pandas[(df_pandas.index >= from_date) & (df_pandas.index <= to_date)]
        if sub_df.empty:
          date = from_date
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[date])
          sub_df._is_special_df = True

      elif current_date.year==from_date.date().year and current_date.month==from_date.date().month and end_of_month_with_time<to_date:
        start_end_date_byfrequency.append([from_, end_of_month.strftime('%Y-%m-%d %H:%M:%S')])
        sub_df = df_pandas[(df_pandas.index >= datetime.strptime(from_, "%Y-%m-%d %H:%M:%S")) & (df_pandas.index <= end_of_month_with_time)]
        if sub_df.empty:
          start_date = pd.Timestamp(datetime.strptime(from_, "%Y-%m-%d %H:%M:%S"))
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[start_date])
          sub_df._is_special_df = True
      elif current_date.month!=from_date.date().month and end_of_month_with_time>to_date:
      
        start_end_date_byfrequency.append([start_of_month.strftime('%Y-%m-%d %H:%M:%S'), to_])
        sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= to_date)]
        if sub_df.empty:
          date = start_of_month
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[date])
          sub_df._is_special_df = True
      
      else:
        start_end_date_byfrequency.append([datetime.combine(start_of_month, time(0, 0, 0)).strftime('%Y-%m-%d %H:%M:%S'), datetime.combine(end_of_month, time(23, 59, 59)).strftime('%Y-%m-%d %H:%M:%S')])
        # Select rows for the current month
        sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= end_of_month)]
        if sub_df.empty:
          date = pd.Timestamp(start_of_month.year, start_of_month.month, start_of_month.day, 1, 0, 0)
          sub_df = pd.DataFrame({'user_id': '', 'message': '', 'topic': [[]], 'latitude':'', 'longitude':'', 'location':''}, index=[date])
          sub_df._is_special_df = True
      # Append the sub DataFrame to the list
      sub_dataframes.append(sub_df)
      # Move to the next month
      current_date = end_of_month + pd.Timedelta(days=1)
      

    # Now sub_dataframes contains sub DataFrames grouped by each individual month

    for i in sub_dataframes:
        timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))
  
  # usernumber/changesinusernumber, querry list, location list ------------------------------------
  usernumber=[]
  querry_on_average=[]
  changesinusernumber=[]
  locations=[]
  counter=0
  for i in sub_dataframes:
    
    if hasattr(i, '_is_special_df') and i._is_special_df==True:
        querry_this_period= 0
    else:
      querry_this_period=len(i)

    groupedthisperiod = i.groupby(i.index.date)

    
    if hasattr(i, '_is_special_df') and i._is_special_df==True:
        total_unique_users_thisperiod=0
        usernumber.append(total_unique_users_thisperiod)
    else:
     
    
      #Create a dictionary to hold the unique user_id counts for each day
      unique_user_counts_thisperiod = {str(date): group['user_id'].dropna().nunique() for date, group in groupedthisperiod}
      
      total_unique_users_thisperiod = sum(unique_user_counts_thisperiod.values())
      usernumber.append(total_unique_users_thisperiod)
   
    if querry_this_period== 0:
      querry_onaverage_thisperiod=0
  
      querry_on_average.append(querry_onaverage_thisperiod)
      
    else:

      try:
          # Convert input values to Decimal
          querry_value = Decimal(querry_this_period)
          users_value = Decimal(total_unique_users_thisperiod)
          
          # Check for zero division
          if users_value == 0:
              querry_onaverage_thisperiod = Decimal('Infinity')  # or None, or a fallback value like Decimal('0.0')
              querry_on_average.append(querry_onaverage_thisperiod)
          else:
              # Perform the division
              querry_onaverage_thisperiod = querry_value / users_value
              
              # Round the result to one decimal place
              querry_onaverage_thisperiod = querry_onaverage_thisperiod.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
              querry_on_average.append(querry_onaverage_thisperiod)

      except InvalidOperation as e:
          print(f"Invalid operation: {e}")
      except ZeroDivisionError as e:
          print(f"Error: {e}")
    
    #creating changesinusernumberList
    if counter==0:
      from_date00=datetime.strptime(start_end_date_byfrequency[0][0], "%Y-%m-%d %H:%M:%S")
      to_date00=datetime.strptime(start_end_date_byfrequency[0][1], "%Y-%m-%d %H:%M:%S")
      sub_df00 = df_pandas[(df_pandas.index >= from_date00) & (df_pandas.index <= to_date00)]
      groupedthisperiod00 = sub_df00.groupby(sub_df00.index.date)
      unique_user_counts_thisperiod00 = {str(date): group['user_id'].nunique() for date, group in groupedthisperiod00}
      total_unique_users_thisperiod00 = sum(unique_user_counts_thisperiod00.values())

  
      # Convert input values to Decimal
      total_unique_users_current = Decimal(usernumber[0])
      total_unique_users_thisperiod00 = Decimal(total_unique_users_thisperiod00)
      
      if total_unique_users_thisperiod00 == 0 and total_unique_users_current!=0:
          querry_onaverage_thisperiod = Decimal('Infinity')  # or None, or a fallback value like Decimal('0.0')
          changesinusernumber.append("100%")
      elif total_unique_users_thisperiod00 == 0 and total_unique_users_current==0:
        changesinusernumber.append("0%")
      else:
          # Perform the division
          changeinusernumber_value=((total_unique_users_thisperiod00 - total_unique_users_current) / total_unique_users_thisperiod00)*-100
          
          
          # Round the result to one decimal place
          changeinusernumber_value = changeinusernumber_value.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
          changesinusernumber.append(str(changeinusernumber_value)+"%")

    if counter>0:
      total_unique_users_current = Decimal(usernumber[counter])
      total_unique_users_thisperiod00 = Decimal(usernumber[counter-1])
     
      
      if total_unique_users_thisperiod00 == 0 and total_unique_users_current!=0:
          querry_onaverage_thisperiod = Decimal('Infinity')  # or None, or a fallback value like Decimal('0.0')
          changesinusernumber.append("100%")
      elif total_unique_users_thisperiod00 == 0 and total_unique_users_current==0:
        changesinusernumber.append("0%")
      else:
        # Perform the division
        changeinusernumber_value=((total_unique_users_thisperiod00 - total_unique_users_current) / total_unique_users_thisperiod00)
        if changeinusernumber_value==0:
          pass
        else:
          changeinusernumber_value=changeinusernumber_value * -100
        
        # Round the result to one decimal place
        changeinusernumber_value = changeinusernumber_value.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        changesinusernumber.append(str(changeinusernumber_value)+"%")


    counter +=1

#LOCATIONS
    
    # def get_first_location(group):
    #   if 'location' in group.columns:
    #       return group['location'].iloc[0]
    #   else:
    #       raise KeyError('location column is missing in the group')
      

    def get_first_location(group):
      if 'location' in group.columns:
          valid_locations = group['location'].dropna()
          if not valid_locations.empty:
              return valid_locations.iloc[0]
          return ""
      else:
          raise KeyError('location column is missing in the group')
      

   
    if hasattr(i, '_is_special_df') and i._is_special_df==True:
        locations_ = []
    elif i.empty:
        locations_ = []
    else:
            
        try:
          
            locations_ = i.groupby([i.index.date, i.user_id]).apply(get_first_location).tolist()
            locations_ = [loc for loc in locations_ if loc not in (None, '')]
          
        except KeyError as e:
            print(f"KeyError: {e}")
    

    locationranking = {}
    for city in locations_:
        if isinstance(city, str):  # Ensure the city is a string before using it as a key
            locationranking[city] = locationranking.get(city, 0) + 1
        else:
            print(f"Unexpected type: {type(city)}, value: {city}")
    
    locationranking_top3 = sorted(locationranking.items(), key=lambda x: x[1], reverse=True)[:3]

    if len(locationranking_top3)==0:
      locations.append(["-","-","-"])
    if len(locationranking_top3)==1:
      locations.append([locationranking_top3[0][0],"-","-"])
    if len(locationranking_top3)==2:
      locations.append([locationranking_top3[0][0], locationranking_top3[1][0],"-"]) 
    if len(locationranking_top3)==3:
      locations.append([locationranking_top3[0][0], locationranking_top3[1][0], locationranking_top3[2][0]])
    
 




    
  # ------------------------------------

  # database_url = os.environ.get('DATABASE_URL')   
  data_to_transform=[]
  for df_pandas in sub_dataframes: # sub_dataframes contains each period a week, month etc.
    list_for_4items=[]
    for index, row in df_pandas.iterrows():
      for p in row['topic']:
        if len(p)==4:
          list_for_4items.append(p)

    grouped_dict = {}

    for sublist in list_for_4items:
        key = sublist[0]
        if key in grouped_dict.keys():
            grouped_dict[key].append(sublist)
        else:
            grouped_dict[key] = [sublist]


    # Convert the dictionary values to a list
    grouped_list = list(grouped_dict.values())

   

    #collection will hold the 4 item productlines
    collection = []


    for x in grouped_list:
        summary = [defaultdict(int) for _ in range(len(grouped_list[0][0]))]
        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        final_result = []
        for s in summary:
            result = []
            for key, value in s.items():
                result.append({key: value})
            final_result.append(result)
        collection.append(final_result)

    
    list_for_3items=[]
    for index, row in df_pandas.iterrows():
      for p in row['topic']:
        if len(p)==3:
          list_for_3items.append(p)


    list_for_2items=[]
    for index, row in df_pandas.iterrows():
      for p in row['topic']:
        if len(p)==2:
          list_for_2items.append(p)

    list_for_1items=[]
    for index, row in df_pandas.iterrows():
      for p in row['topic']:
        if len(p)==1:
          list_for_1items.append(p)
    

    #####################################################################
    #          MANAGING 3 ITEM LIST FOR THE data_to_transform LIST      #
    #####################################################################

    new_3_items=[]
  
    
    # y:[['zongora', 'akusztikus zongora', 'yamaha'],['zongora', 'akusztikus zongora', 'petrof']]

    for p in list_for_3items:  # MÉG NESTED LISTÁVAL CSINÁLOM, LEHETNE NÉLKÜLE
      # for p in y:
        # IDENTIFY NEW 3 ITEM PRODUCT LINE, nincs benne az eddigi 4 itemes listába
      q=[]
      for k in range(len(collection)):
        if p[0] not in collection[k][0][0].keys():
          q.append("no")
        else:
          q.append("yes")
            # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 1.ROOT ITEMS
          collection[k][0][0][list(collection[k][0][0].keys())[0]]+=1
          # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 2. ITEMS
          temp=[]
          for counter2 in range(len(collection[k][1])):
            if p[1] in collection[k][1][counter2].keys():
              collection[k][1][counter2][list(collection[k][1][counter2].keys())[0]]+=1

          # IDENTIFY AND ADD NEW DICTIONARY IN THE COLLECTION LIST 2. ITEMS
              temp.append("yes")
            else:
              temp.append("no")
          if all(element == 'no' for element in temp):
            collection[k][1].append({p[1]:1})

          # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 3. ITEMS
          temp=[]
          for counter2 in range(len(collection[k][2])):
            if p[2] in collection[k][2][counter2].keys():
              collection[k][2][counter2][list(collection[k][2][counter2].keys())[0]]+=1

          # IDENTIFY AND ADD NEW DICTIONARY IN THE COLLECTION LIST 3. ITEMS
              temp.append("yes")
            else:
              temp.append("no")
          if all(element == 'no' for element in temp):
            collection[k][2].append({p[2]:1})



    # ------------- Adding the new 3 item collection to the main collection

      if all(element == 'no' for element in q):
        new_3_items.append(p)

    grouped_dict_new_3_items = {}

    for sublist in new_3_items:
        key = sublist[0]
        if key in grouped_dict_new_3_items.keys():
            grouped_dict_new_3_items[key].append(sublist)
        else:
            grouped_dict_new_3_items[key] = [sublist]


    # Convert the dictionary values to a list
    grouped_list_new_3_item = list(grouped_dict_new_3_items.values())

    collection_new_3_item = []


    for x in grouped_list_new_3_item:
        summary = [defaultdict(int) for _ in range(len(grouped_list_new_3_item[0][0]))]
        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        final_result = []
        for s in summary:
            result = []
            for key, value in s.items():
                result.append({key: value})
            final_result.append(result)

        collection_new_3_item.append(final_result)

    for i in collection_new_3_item:
      collection.append(i)

    ######################################
    #         TWO ITEMED LIST            #
    ######################################
    new_2_items=[]   # [['gitár', 'elektromos'], ['effektpedál', 'effektpedál']]
    for p in list_for_2items:  # MÉG NESTED LISTÁVAL CSINÁLOM, LEHETNE NÉLKÜLE
      # for p in y:
        # IDENTIFY NEW 3 ITEM PRODUCT LINE, nincs benne az eddigi 4 itemes listába
      q=[]
      for k in range(len(collection)):
        if p[0] not in collection[k][0][0].keys():
          q.append("no")
        else:
          q.append("yes")
            # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 1.ROOT ITEMS
          collection[k][0][0][list(collection[k][0][0].keys())[0]]+=1
          # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 2. ITEMS
          temp=[]
          for counter2 in range(len(collection[k][1])):
            if p[1] in collection[k][1][counter2].keys():
              collection[k][1][counter2][list(collection[k][1][counter2].keys())[0]]+=1

          # IDENTIFY AND ADD NEW DICTIONARY IN THE COLLECTION LIST 2. ITEMS
              temp.append("yes")
            else:
              temp.append("no")
          if all(element == 'no' for element in temp):
            collection[k][1].append({p[1]:1})
    
    # ------------- Adding the new 2 item collection to the main collection

      if all(element == 'no' for element in q):
        new_2_items.append(p)

    grouped_dict_new_2_items = {}

    for sublist in new_2_items:
        key = sublist[0]
        if key in grouped_dict_new_2_items.keys():
            grouped_dict_new_2_items[key].append(sublist)
        else:
            grouped_dict_new_2_items[key] = [sublist]


    # Convert the dictionary values to a list
    grouped_list_new_2_item = list(grouped_dict_new_2_items.values())

    collection_new_2_item = []


    for x in grouped_list_new_2_item:
        summary = [defaultdict(int) for _ in range(len(grouped_list_new_2_item[0][0]))]
        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        final_result = []
        for s in summary:
            result = []
            for key, value in s.items():
                result.append({key: value})
            final_result.append(result)

        collection_new_2_item.append(final_result)

    for i in collection_new_2_item:
      collection.append(i)



    ######################################
    #         ONE ITEM LIST            #
    ######################################
    new_1_items=[]
    # y: [['erősítő'], ['erősítő']]
    for p in list_for_1items:  # MÉG NESTED LISTÁVAL CSINÁLOM, LEHETNE NÉLKÜLE
      # for p in y:
        # IDENTIFY NEW 3 ITEM PRODUCT LINE, nincs benne az eddigi 4 itemes listába
      q=[]
      for k in range(len(collection)):
        if p[0] not in collection[k][0][0].keys():
          q.append("no")
        else:
          q.append("yes")
            # INCREMENT EXISTING DICTIONARIES IN THE COLLECTION LIST 1.ROOT ITEMS
          collection[k][0][0][list(collection[k][0][0].keys())[0]]+=1


      if all(element == 'no' for element in q):
        new_1_items.append(p)

    grouped_dict_new_1_items = {}


    for sublist in new_1_items:
        key = sublist[0]
        if key in grouped_dict_new_1_items.keys():
            grouped_dict_new_1_items[key].append(sublist)
        else:
            grouped_dict_new_1_items[key] = [sublist]

    # Convert the dictionary values to a list
    grouped_list_new_1_item = list(grouped_dict_new_1_items.values())

    collection_new_1_item = []


    for x in grouped_list_new_1_item:
        summary = [defaultdict(int) for _ in range(len(grouped_list_new_1_item[0][0]))]
        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        final_result = []
        for s in summary:
            result = []
            for key, value in s.items():
                result.append({key: value})
            final_result.append(result)

        collection_new_1_item.append(final_result)

    for i in collection_new_1_item:
      collection.append(i)

    data_to_transform.append(collection)
    # if len(sub_dataframes)>1:
    #   data_to_transform.append(collection)
    # else:
    #   data_to_transform=collection

  # print(data_to_transform)

  #######################################################################################################################################################
  #  CREATING THE final_transformed_data FOR THE CHART.JS PAGE (WE SHOULD HANDLE ONE AND MORE PERIOD LISTS DIFFERENTLY BASED ON THE DEPTH OF THE LISTS) #
  #######################################################################################################################################################
  
  c = ['gyártó', 'márka']

  def calculate_depth(lst):
      if isinstance(lst, list):
          if lst:
              return 1 + max(calculate_depth(item) for item in lst)
          else:
              return 1
      else:
          return 0

  
  depth_of_data=calculate_depth(data_to_transform)
 
  final_transformed_data = []
  if depth_of_data==3:
    if len(data_to_transform)==1 and len(data_to_transform[0])==0:
        product_data="There was no chat activity in this period"
        final_transformed_data.append(product_data)
    else:
      for item in data_to_transform:
        product_data = {}
        product_data['label'] = list(item[0][0].keys())[0]
        main_chart_data = [{'x': timestamp[timestamp_index], 'y': list(entry.values())[0]} for entry in item[0]]
        product_data['mainChartData'] = main_chart_data
        if len(item)>1:
          secondaryChartData = [list(entry.values())[0] for entry in item[1]]
          product_data['x_secondary'] = [key for entry in item[1] for key in entry.keys()]
          product_data['secondaryChartData'] = [secondaryChartData]
        if len(item)>2:
          secondaryChartData_b = [list(entry.values())[0] for entry in item[2]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = [key for entry in item[2] for key in entry.keys()]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
        if len(item)>3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[3]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = [key for entry in item[3] for key in entry.keys()]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]

        if len(item)==1:
          secondaryChartData = [list(entry.values())[0] for entry in item[0]]
          product_data['x_secondary'] = ["Típusról nem folyt beszélgetés"]
          product_data['secondaryChartData'] = [secondaryChartData]
          secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==2:
          secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        final_transformed_data.append(product_data)

  def longest_mainChartData(consolidated_list):
    max_length = 0
    max_item = None

    # Iterate over each dictionary in the list
    for item in consolidated_list:
        # Get the length of the mainChartData list
        length = len(item.get('mainChartData', []))

        # Check if the length is longer than the current maximum
        if length > max_length:
            max_length = length
            max_item = item
    return max_length, max_item

  # Initialize a new list to store the consolidated items
  data_for_final_transformation=[]  
  consolidated_list = []

  if depth_of_data==4:
    timestamp_index=0
    for period in data_to_transform:
      period_to_add_to_finaltransformation=[]
      for item in period:
        product_data = {}
        product_data['label'] = list(item[0][0].keys())[0]
        main_chart_data = [{'x': timestamp[timestamp_index], 'y': list(entry.values())[0]} for entry in item[0]]
        product_data['mainChartData'] = main_chart_data
        if len(item)>1:
          secondaryChartData = [list(entry.values())[0] for entry in item[1]]
          product_data['x_secondary'] = [key for entry in item[1] for key in entry.keys()]
          product_data['secondaryChartData'] = [secondaryChartData]
        if len(item)>2:
          secondaryChartData_b = [list(entry.values())[0] for entry in item[2]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = [key for entry in item[2] for key in entry.keys()]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
        if len(item)>3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[3]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = [key for entry in item[3] for key in entry.keys()]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]

        if len(item)==1:
          secondaryChartData = [list(entry.values())[0] for entry in item[0]]
          product_data['x_secondary'] = ["Típusról nem folyt beszélgetés"]
          product_data['secondaryChartData'] = [secondaryChartData]
          secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==2:
          secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = ["Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        period_to_add_to_finaltransformation.append(product_data)
    
      timestamp_index+=1
      data_for_final_transformation.append(period_to_add_to_finaltransformation)
    
    # for main_itemxx in data_for_final_transformation:
    #   print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    #   print(main_itemxx)
    #   print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    
    data_for_final_transformation_copy = copy.deepcopy(data_for_final_transformation)

    for main_item in data_for_final_transformation:
      if len(consolidated_list)==0 and type(main_item)!=str:
        consolidated_list=main_item
        continue
      if len(consolidated_list)==0 and type(main_item)==str:
        consolidated_list.append(main_item)
        continue
      if all(isinstance(item, str) for item in consolidated_list) and type(main_item)==str:
        consolidated_list.append(main_item)
        continue
      # All items are list in the consolidated_list and the item is string (csak dátumot tartalmaz)   
      if all(not isinstance(item, str) for item in consolidated_list) and type(main_item)==str:
        for item in consolidated_list:
            # Append the copied last element to item['mainChartData']
          item['mainChartData'].append({'x': main_item, 'y':0})
          result_init=[0] * len(item['x_secondary'])
          item['secondaryChartData'].append(result_init)
          if 'x_secondary_b' in item:
            result_init=[0] * len(item['x_secondary_b'])
            item['secondaryChartData_b'].append(result_init)
          if 'x_secondary_c' in item:
            result_init=[0] * len(item['x_secondary_c'])
            item['secondaryChartData_c'].append(result_init)



     
      #HANDELING THOSE ITEMS WHICH HAS COMMON LABEL IN DIFFERENT PERIODS
 
      for item_a in main_item:
        # All items are string in the consolidated_list(végső) and the item to be consolidated is list
        if all(isinstance(item, str) for item in consolidated_list) and type(item_a)!=str:
          consolidated_item = {'label': item_a['label']}
          previous_mainChart=[]
          x_secondary_data_prev=[]
          x_secondary_data_prev_b=[]
          x_secondary_data_prev_c=[]
        
          for i in range(len(consolidated_list)):
            previous_mainChart.append({'x': consolidated_list[i], 'y':0})
            x_secondary_data_prev.append([0] * len(item_a['x_secondary']))
            x_secondary_data_prev_b.append([0] * len(item_a['x_secondary_b']))
            x_secondary_data_prev_c.append([0] * len(item_a['x_secondary_c']))
          consolidated_item['mainChartData']=previous_mainChart + item_a['mainChartData']
          consolidated_item['x_secondary'] = item_a['x_secondary']
          consolidated_item['secondaryChartData']=x_secondary_data_prev+item_a['secondaryChartData']
          consolidated_item['x_secondary_b'] = item_a['x_secondary_b']
          consolidated_item['secondaryChartData_b']=x_secondary_data_prev_b+item_a['secondaryChartData_b']
          consolidated_item['x_secondary_c'] = item_a['x_secondary_c']
          consolidated_item['secondaryChartData_c']=x_secondary_data_prev_c+item_a['secondaryChartData_c']

        
          consolidated_list=[]
          
        
        # All items are list in the consolidated_list and the item to be consolidated is list
        if all(not isinstance(item, str) for item in consolidated_list) and type(item_a)!=str and len(consolidated_list)!=0:
        # Check if the item's label exists in list b
          if any(item_a['label'] == item_b['label'] for item_b in consolidated_list):
            # Find the corresponding item in list b
            item_b = next(item_b for item_b in consolidated_list if item_b['label'] == item_a['label'])

            # Create a new item to store the consolidated data
            consolidated_item = {'label': item_a['label']}
            b_time=item_b['mainChartData'][-1]['x']
            a_time=item_a['mainChartData'][0]['x']
            b_time = datetime.strptime(b_time, "%Y-%m-%d %H:%M:%S")
            a_time = datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S")
          
            # Consolidate mainChartData

            # if a_time < b_time:
            #     consolidated_item['mainChartData'] = item_a['mainChartData'] + item_b['mainChartData']

            if a_time > b_time:
              consolidated_item['mainChartData'] = item_b['mainChartData'] + item_a['mainChartData']

            # Consolidate x_secondary
            consolidated_item['x_secondary'] = list(set(item_a['x_secondary'] + item_b['x_secondary']))
            
            # Consolidate secondaryChartData
            result_a = [0] * len(consolidated_item['x_secondary'])
            for index_a,value_a in enumerate(item_a['x_secondary']):
              for index_cons, value_cons in enumerate(consolidated_item['x_secondary']):
                if value_a ==value_cons:
                  result_a[index_cons] = item_a['secondaryChartData'][0][index_a]
              result_b=[]
              for x in range(len(item_b['mainChartData'])):
                result_init=[0] * len(consolidated_item['x_secondary'])
                for index_b,value_b in enumerate(item_b['x_secondary']):
                  for index_cons, value_cons in enumerate(consolidated_item['x_secondary']):
                    if value_b ==value_cons:
                      result_init[index_cons] = item_b['secondaryChartData'][x][index_b]
                result_b.append(result_init)
              if a_time < b_time:
                consolidated_item['secondaryChartData'] = [result_a]+ result_b
              elif a_time > b_time:
                consolidated_item['secondaryChartData'] = result_b+ [result_a]


            # Consolidate label_b x_secondary_b and Consolidate secondaryChartData_b
            consolidated_item['label_b'] = 'gyártó'
            if 'x_secondary_b' not in item_a and 'x_secondary_b' not in item_b:
              consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
              result_b=[]
              for x in item_b['mainChartData']:
                result_init=[0]
                result_b.append(result_init)
              consolidated_item['secondaryChartData_b'] = result_b + [[0]]



            if 'x_secondary_b' in item_a and 'x_secondary_b' not in item_b:
              consolidated_item['x_secondary_b']=item_a['x_secondary_b']
              result_b=[]
              for x in item_b['mainChartData']:
                result_init=[0] * len(consolidated_item['x_secondary_b'])
                result_b.append(result_init)
              if a_time < b_time:
                consolidated_item['secondaryChartData_b'] = item_a['secondaryChartData_b']+ result_b
              elif a_time > b_time:
                consolidated_item['secondaryChartData_b'] = result_b+ item_a['secondaryChartData_b']

            if 'x_secondary_b' in item_b and 'x_secondary_b' not in item_a:
              consolidated_item['x_secondary_b']=item_b['x_secondary_b']
              if a_time < b_time:
                consolidated_item['secondaryChartData_b'] = item_a['secondaryChartData_b']+ [[0]]
              elif a_time > b_time:
                consolidated_item['secondaryChartData_b'] = [[0]]+ item_a['secondaryChartData_b']

            if 'x_secondary_b' in item_b and 'x_secondary_b'  in item_a:
              consolidated_item['x_secondary_b'] = list(set(item_a['x_secondary_b'] + item_b['x_secondary_b']))
              result_a = [0] * len(consolidated_item['x_secondary_b'])
              for index_a,value_a in enumerate(item_a['x_secondary_b']):
                for index_cons, value_cons in enumerate(consolidated_item['x_secondary_b']):
                  if value_a ==value_cons:
                    result_a[index_cons] = item_a['secondaryChartData_b'][0][index_a]
                result_b=[]
                for x in range(len(item_b['mainChartData'])):
                  result_init=[0] * len(consolidated_item['x_secondary_b'])
                  for index_b,value_b in enumerate(item_b['x_secondary_b']):
                    for index_cons, value_cons in enumerate(consolidated_item['x_secondary_b']):
                      if value_b ==value_cons:
                        result_init[index_cons] = item_b['secondaryChartData_b'][x][index_b]
                  result_b.append(result_init)
                if a_time < b_time:
                  consolidated_item['secondaryChartData_b'] = [result_a]+ result_b
                elif a_time > b_time:
                  consolidated_item['secondaryChartData_b'] = result_b+ [result_a]

            # Consolidate label_c  x_secondary_c secondaryChartData_c
            consolidated_item['label_c'] = 'márka'
            if 'x_secondary_c' not in item_a and 'x_secondary_c' not in item_b:
              consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
              result_b=[]
              for x in item_b['mainChartData']:
                result_init=[0]
                result_b.append(result_init)
              consolidated_item['secondaryChartData_c'] = result_b + [[0]]



            if 'x_secondary_c' in item_a and 'x_secondary_c' not in item_b:
              consolidated_item['x_secondary_c']=item_a['x_secondary_c']
              result_b=[]
              for x in item_b['mainChartData']:
                result_init=[0] * len(consolidated_item['x_secondary_c'])
                result_b.append(result_init)
              if a_time < b_time:
                consolidated_item['secondaryChartData_c'] = item_a['secondaryChartData_c']+ result_b
              elif a_time > b_time:
                consolidated_item['secondaryChartData_c'] = result_b+ item_a['secondaryChartData_c']

            if 'x_secondary_c' in item_b and 'x_secondary_c' not in item_a:
              consolidated_item['x_secondary_c']=item_b['x_secondary_c']
              if a_time < b_time:
                consolidated_item['secondaryChartData_c'] = item_a['secondaryChartData_c']+ [[0]]
              elif a_time > b_time:
                consolidated_item['secondaryChartData_c'] = [[0]]+ item_a['secondaryChartData_c']


            if 'x_secondary_c' in item_b and 'x_secondary_c'  in item_a:
              consolidated_item['x_secondary_c'] = list(set(item_a['x_secondary_c'] + item_b['x_secondary_c']))
              result_a = [0] * len(consolidated_item['x_secondary_c'])
              for index_a,value_a in enumerate(item_a['x_secondary_c']):
                for index_cons, value_cons in enumerate(consolidated_item['x_secondary_c']):
                  if value_a ==value_cons:
                    result_a[index_cons] = item_a['secondaryChartData_c'][0][index_a]
                result_b=[]
                for x in range(len(item_b['mainChartData'])):
                  result_init=[0] * len(consolidated_item['x_secondary_c'])
                  for index_b,value_b in enumerate(item_b['x_secondary_c']):
                    for index_cons, value_cons in enumerate(consolidated_item['x_secondary_c']):
                      if value_b ==value_cons:
                        result_init[index_cons] = item_b['secondaryChartData_c'][x][index_b]
                  result_b.append(result_init)
                if a_time < b_time:
                  consolidated_item['secondaryChartData_c'] = [result_a]+ result_b
                elif a_time > b_time:
                  consolidated_item['secondaryChartData_c'] = result_b+ [result_a]

            
            for index, item in enumerate(consolidated_list):
                  if item == item_b:

                      consolidated_list [index] = consolidated_item


          

          #HANDELING THOSE ITEMS WHICH DON'T HAVE COMMON LABEL IN DIFFERENT PERIODS

          # Determine all unique label values in lists a and b
          all_labels = set(sublist['label'] for sublist in consolidated_list)

      


          if item_a['label'] not in all_labels:

            # Complete the mainChartData with missing months
            max_length, max_item=longest_mainChartData(consolidated_list)

            consolidated_item = {'label': item_a['label']}

            b_time= max_item['mainChartData'][-1]['x']
            a_time=item_a['mainChartData'][0]['x']
            b_time = datetime.strptime(b_time, "%Y-%m-%d %H:%M:%S")
            a_time = datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S")

            # Consolidate mainChartData
            if a_time > b_time:
              consolidated_item = {'label': item_a['label']}
              max_length, max_item=longest_mainChartData(consolidated_list)
              chartdata=[]
              for r in range(len(max_item['mainChartData'])):
                x=max_item['mainChartData'][r]['x']
                chartdata.append({'x': x, 'y': 0})
              chartdata.append(item_a['mainChartData'][0])
              consolidated_item['mainChartData']=chartdata

        



              consolidated_item['x_secondary']=item_a['x_secondary']
              result_b=[]
              consolidated_item['label_b'] = 'gyártó'
              for x in max_item['mainChartData']:
                result_init=[0] * len(item_a['x_secondary'])
                result_b.append(result_init)
              consolidated_item['secondaryChartData'] = result_b + item_a['secondaryChartData']
              if 'x_secondary_b' in item_a:
                consolidated_item['x_secondary_b']=item_a['x_secondary_b']
                result_b=[]
                for x in  max_item['mainChartData']:
                  result_init=[0] * len(item_a['x_secondary_b'])
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_b'] = result_b + item_a['secondaryChartData_b']
              if 'x_secondary_b' not in item_a:
                consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
                result_b=[]
                for x in max_item['mainChartData']:
                  result_init=[0]
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_b'] = result_b + [[0]]
              consolidated_item['label_c'] = 'márka'
              if 'x_secondary_c' in item_a:
                consolidated_item['x_secondary_c']=item_a['x_secondary_c']
                result_b=[]
                for x in max_item['mainChartData']:
                  result_init=[0] * len(item_a['x_secondary_c'])
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_c'] = result_b + item_a['secondaryChartData_c']
              if 'x_secondary_c' not in item_a:
                consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
                result_b=[]
                for x in max_item['mainChartData']:
                  result_init=[0]
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_c'] = result_b + [[0]]


            if a_time < b_time:
              consolidated_item = {'label': item_a['label']}
              max_length, max_item=longest_mainChartData(consolidated_list)
              consolidated_item['mainChartData'] = max_item['mainChartData'] + item_a['mainChartData']
              consolidated_item['x_secondary']=item_a['x_secondary']
              consolidated_item['label_b'] = 'gyártó'
              result_b=[]
              for x in  max_item['mainChartData']:
                result_init=[0] * len(item_a['x_secondary'])
                result_b.append(result_init)
              consolidated_item['secondaryChartData'] = item_a['secondaryChartData'] +  result_b
              if 'x_secondary_b' in item_a:
                consolidated_item['x_secondary_b']=item_a['x_secondary_b']
                result_b=[]
                for x in  max_item['mainChartData']:
                  result_init=[0] * len(item_a['x_secondary_b'])
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_b'] = item_a['secondaryChartData_b']+result_b
                if 'x_secondary_b' not in item_a:
                  consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
                result_b=[]
                for x in max_item['mainChartData']:
                  result_init=[0]
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_b'] =  [[0]] + result_b
              consolidated_item['label_c'] = 'márka'
              if 'x_secondary_c' in item_a:
                consolidated_item['x_secondary_c']=item_a['x_secondary_c']
                result_b=[]
                for x in  max_item['mainChartData']:
                  result_init=[0] * len(item_a['x_secondary_c'])
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_c'] =  item_a['secondaryChartData_c'] + result_b
              if 'x_secondary_c' not in item_a:
                consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
                result_b=[]
                for x in max_item['mainChartData']:
                  result_init=[0]
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_c'] =  [[0]] + result_b

            if a_time == b_time:
              consolidated_item = {'label': item_a['label']}
              max_length, max_item=longest_mainChartData(consolidated_list)

              chartdata=[]
              for r in range(len(max_item['mainChartData'])-1):
                x=max_item['mainChartData'][r]['x']
                chartdata.append({'x': x, 'y': 0})
              chartdata.append(item_a['mainChartData'][0])
              consolidated_item['mainChartData']=chartdata




              consolidated_item['x_secondary']=item_a['x_secondary']
              result_b=[]
              consolidated_item['label_b'] = 'gyártó'
              for x in  range(len(max_item['mainChartData'][:-1])):
                result_init=[0] * len(item_a['x_secondary'])
                result_b.append(result_init)
              consolidated_item['secondaryChartData'] = result_b + item_a['secondaryChartData']
              if 'x_secondary_b' in item_a:
                consolidated_item['x_secondary_b']=item_a['x_secondary_b']
                result_b=[]
                for x in  range(len(max_item['mainChartData'][:-1])):
                  result_init=[0] * len(item_a['x_secondary_b'])
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_b'] = result_b + item_a['secondaryChartData_b']
              if 'x_secondary_b' not in item_a:
                consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
                result_b=[]
                for x in  range(len(max_item['mainChartData'][:-1])):
                  result_init=[0]
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_b'] = result_b + [[0]]
              consolidated_item['label_c'] = 'márka'
              if 'x_secondary_c' in item_a:
                consolidated_item['x_secondary_c']=item_a['x_secondary_c']
                result_b=[]
                for x in  range(len(max_item['mainChartData'][:-1])):
                  result_init=[0] * len(item_a['x_secondary_c'])
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_c'] = result_b + item_a['secondaryChartData_c']
              if 'x_secondary_c' not in item_a:
                consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
                result_b=[]
                for x in  range(len(max_item['mainChartData'][:-1])):
                  result_init=[0]
                  result_b.append(result_init)
                consolidated_item['secondaryChartData_c'] = result_b + [[0]]


            consolidated_list.append(consolidated_item)

        if len(consolidated_list)==0:
          consolidated_list.append(consolidated_item)
      
      # UPDATE ALL ITEMS IN CONSOLIDATED LIST IF NEEDED TO HAVE THE SAME LENGTH REGARDING MAINCHARTDATA
     
      max_length, max_item=longest_mainChartData(consolidated_list)

      for item in consolidated_list:
        
        if len(item['mainChartData'])<max_length:


          x=max_item['mainChartData'][-1]['x']


            # Append the copied last element to item['mainChartData']
          item['mainChartData'].append({'x': x, 'y': 0})

          result_init=[0] * len(item['x_secondary'])
          item['secondaryChartData'].append(result_init)
          if 'x_secondary_b' in item:
            result_init=[0] * len(item['x_secondary_b'])
            item['secondaryChartData_b'].append(result_init)
          if 'x_secondary_c' in item:
            result_init=[0] * len(item['x_secondary_c'])
            item['secondaryChartData_c'].append(result_init)

   
    final_transformed_data=consolidated_list
 
  return final_transformed_data, data_for_final_transformation_copy, timestamp, start_end_date_byfrequency, usernumber, querry_on_average, changesinusernumber, locations
  
