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
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session
from contextlib import contextmanager
from sqlalchemy.exc import OperationalError
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from mywebpage.mainpulation_weeklyreport import get_or_create_weekly_df
import pytz
import asyncio

from mywebpage.concurrency import run_cpu_task





async def datatransformation_for_chartjs_weekly(client_id, cpu_pool, cpu_sem):
    # Fetch dataframe (already async + handles CPU heavy parts internally)
    df = await get_or_create_weekly_df(client_id, "chat_messages")
    if df.empty:
        return ""

    # Push Pandas filtering/grouping into process pool
    return await run_cpu_task(datatransformation_for_chartjs_weekly_cpu, df,
        cpu_pool=cpu_pool,
        cpu_sem=cpu_sem)





#def datatransformation_for_chartjs(year, month, day, hour, minutes, seconds, year_end, month_end, day_end, hour_end, minutes_end, seconds_end,frequency, table_name):
def datatransformation_for_chartjs_weekly_cpu(df_pandas: pd.DataFrame, frequency="weekly"):
  ######################################
  #   FETCHING DATA FROM THE DATABASE  #
  ######################################




  
  # Create a function to check if literal_eval will work  !!!!!!!!!!!  DE Ez CSAK IDEIGLENES!!!!
  def replace_curly_braces(text):
    # Find patterns like {{...}} and replace with [['...']]
    pattern = r'\{\{(.*?)\}\}'
    # Replace with [['...']]
    replaced_text = re.sub(pattern, r"[['\1']]", text)
    return replaced_text
  
  def safe_literal_eval(text):
    try:
        # Replace curly braces with proper literal syntax
        text = replace_curly_braces(text)
        return ast.literal_eval(text)
    except (ValueError, SyntaxError) as e:
        # If literal_eval fails, handle the exception gracefully
        #print(f"Error parsing text: {text}. Error: {e}")
        return []  # Or handle it differently, depending on your needs


  # Apply the replacement to the 'topic' column
  #df_pandas['topic'] = df_pandas['topic'].apply(replace_curly_braces)
  print(df_pandas.head())       # shows the first 5 rows
  print("COL: ", df_pandas.columns)
  df_pandas['topic'] = df_pandas['topic'].apply(safe_literal_eval)

  
  
  #######################################################
  #   CREATING AND RESTRUCTURING THE PANDAS DATAFRAME   #
  #######################################################

  # df_pandas = pd.DataFrame(rows, columns=columns)

  # Remove milliseconds from 'created_at' column and set it as index
  df_pandas['created_at'] = pd.to_datetime(df_pandas['created_at'], utc=True, errors='coerce').dt.floor('s')
  # Set 'created_at' column as the index
  df_pandas.set_index('created_at', inplace=True)
  df_pandas.drop(columns=['id'], inplace=True)
  #df_pandas['user_id'] = df_pandas['user_id'].apply(lambda x: x.split('_')[0])
  df_pandas.index = pd.to_datetime(df_pandas.index)
  df_pandas.sort_index(ascending=True, inplace=True)



  # THIS IS NOT NEEDED IF WE GET THE DATA FROM REDIS ############
  # # Using the extracted data from the FORM to get the requested PERIOD 

  # from_date={"year":year, "month":month, "day":day, "hour":hour, "minutes":minutes, "seconds":seconds}
  # to_date={"year":year_end, "month":month_end, "day":day_end, "hour":hour_end, "minutes":minutes_end, "seconds":seconds_end}
  
  # def create_date_time(date):
  #   date_time_obj = datetime(year=int(date["year"]), month=int(date["month"]), day=int(date["day"]),
  #                           hour=int(date["hour"]), minute=int(date["minutes"]), second=int(date["seconds"]))

  #   # Format the datetime object as a string
  #   formatted_date_time = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
  #   return formatted_date_time

  # from_=create_date_time(from_date)
  # to_=create_date_time(to_date)
  utc = pytz.UTC

  now = datetime.now(utc).replace(microsecond=0)
  today_date = now.date()
  days_to_subtract = today_date.weekday()
  previous_monday_date = today_date - timedelta(days=days_to_subtract)
  previous_monday_start = datetime.combine(previous_monday_date, datetime.min.time()).replace(tzinfo=utc)

  #df_pandas = df_pandas.loc[previous_monday_start:now]
  from_=previous_monday_start
  to_=now
  
  df_pandas = df_pandas.sort_index()
  df_pandas=df_pandas.loc[from_: to_]
  if df_pandas.empty:
    return []
  ####################################################

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
      
  timestamp=[]
  breakdown=frequency

  ##############
  #   WEEKLY   #
  ##############

  if breakdown=="weekly":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)
    df_pandas = df_pandas.sort_index()

    # Get the full datetime range
    start_dt = df_pandas.index.min()
    end_dt = df_pandas.index.max()

    # Truncate to date only for looping
    start_date = start_dt.date()
    end_date = end_dt.date()

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Function to get timezone-aware weekly boundaries
    def get_week_boundaries(date, tz):
        start_of_week = datetime.combine(date - timedelta(days=date.weekday()), datetime.min.time()).replace(tzinfo=tz)
        end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return start_of_week, end_of_week  # Convert Timestamp to date

    # Iterate over weeks from start to end
    current_date = start_date
    while current_date <= end_date:
        # Get the start and end dates of the current week
        week_start, week_end = get_week_boundaries(current_date, tz=start_dt.tzinfo)
        # Select rows within the current week
        sub_df = df_pandas[(df_pandas.index >= week_start) & (df_pandas.index <= week_end)]
        if sub_df.empty:
          dummy_index = week_start + timedelta(hours=1)
          sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[dummy_index])
        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)
        # Move to the next week
        current_date = week_end.date() + timedelta(days=1)  # Ensure current_date is a date object

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

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Iterate over each day from start to end
    current_date = start_date
    while current_date <= end_date:
        # Select rows for the current day
        sub_df = df_pandas[df_pandas.index.date == current_date]
        if sub_df.empty:
          sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[current_date])

        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)
        # Move to the next day
        current_date += pd.Timedelta(days=1)

    # Now sub_dataframes contains sub DataFrames grouped by each individual day
    for i in sub_dataframes:
      timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))

  ##############
  #   YEARLY   #
  ##############

  if breakdown=="yearly":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index)

    # Find the start and end dates of the data
    start_year = df_pandas.index.min().year  # Extract the year component
    end_year = df_pandas.index.max().year    # Extract the year component
    actual_year = start_year

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Iterate over each year from start to end
    for year in range(start_year, end_year + 1):

        # Select rows for the current year
        sub_df = df_pandas[df_pandas.index.year == year]
        if sub_df.empty:
          actual_year=year
          date = pd.Timestamp(actual_year, 1, 1, 1, 0, 0)
          sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[date])
        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)

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

    # Find the start and end dates of the data
    start_date = df_pandas.index.min().date()  # Extract the date component
    end_date = df_pandas.index.max().date()    # Extract the date component

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Convert end_date to Timestamp for comparison
    end_date_timestamp = pd.Timestamp(end_date)

    # Iterate over each month from start to end
    current_date = pd.Timestamp(start_date)  # Convert current_date to Timestamp
    while current_date <= end_date_timestamp:
        # Get the start and end dates of the current month
        start_of_month = pd.Timestamp(year=current_date.year, month=current_date.month, day=1)
        end_of_month = start_of_month + pd.offsets.MonthEnd(0)
        # Select rows for the current month
        sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= end_of_month)]
        if sub_df.empty:
          date = pd.Timestamp(start_of_month.year, start_of_month.month, start_of_month.day, 1, 0, 0)
          sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[date])
        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)
        # Move to the next month
        current_date = end_of_month + pd.Timedelta(days=1)

    # Now sub_dataframes contains sub DataFrames grouped by each individual month

    for i in sub_dataframes:
        timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))
  
  
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
  print("***********")
  print(final_transformed_data)
  print("***********")
  return final_transformed_data#, data_for_final_transformation_copy, timestamp






      






   