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
from dateutil.relativedelta import relativedelta2
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session
from contextlib import contextmanager
from sqlalchemy.exc import OperationalError
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from mywebpage import session_scope
from mywebpage.datatransformation_detaileduserdata import get_or_create_detailed_df, run_cpu_task
import pytz 
from mywebpage import run_cpu_task, stream_chunks_from_redis, fetch_partitions_for_table_chunks, fetch_partition_rows_in_chunks_gen, store_chunk_in_redis, find_gaps_in_range, async_session_scope, list_blob_names_for_range, load_blob_in_chunks
import json
from functools import reduce
from copy import deepcopy


# Helper for datatransformation_for_chartjs

def merge_two_lists(list1, list2, breakdown="weekly"):
    """
    Merge two structured lists (A, B) depending on daily, weekly, monthly, or yearly breakdown.
    Merges per label if both lists contain data from the same period.
    """
    def parse_date(x):
        if isinstance(x, list):
            x = x[0]
        return datetime.strptime(x, "%Y-%m-%d %H:%M:%S")

    def get_first_last_date(lst):
        all_dates = []
        for sub in lst:
            if isinstance(sub, list):
                for el in sub:
                    if isinstance(el, dict) and 'mainChartData' in el:
                        all_dates.append(parse_date(el['mainChartData'][0]['x']))
        if not all_dates:
            return None, None
        return min(all_dates), max(all_dates)

    # --- Determine chronological order ---
    start1, end1 = get_first_last_date(list1)
    start2, end2 = get_first_last_date(list2)
    if start1 is None or start2 is None:
        return deepcopy(list1) + deepcopy(list2)
    if start2 < start1:
        list1, list2 = list2, list1
        start1, end1, start2, end2 = start2, end2, start1, end1

    # --- Find last and first data blocks ---
    def find_last_block(lst):
        for sub in reversed(lst):
            if isinstance(sub, list) and sub and isinstance(sub[0], dict):
                return sub
        return []

    def find_first_block(lst):
        for sub in lst:
            if isinstance(sub, list) and sub and isinstance(sub[0], dict):
                return sub
        return []

    last_block = find_last_block(list1)
    first_block = find_first_block(list2)
    if not last_block or not first_block:
        return deepcopy(list1) + deepcopy(list2)

    # --- Compare periods ---
    last_date = parse_date(last_block[0]['mainChartData'][0]['x'])
    first_date = parse_date(first_block[0]['mainChartData'][0]['x'])

    if breakdown == "daily":
        same_period = (last_date.date() == first_date.date())
    elif breakdown == "weekly":
        same_period = (last_date.isocalendar()[:2] == first_date.isocalendar()[:2])
    elif breakdown == "monthly":
        same_period = (last_date.year == first_date.year and last_date.month == first_date.month)
    elif breakdown == "yearly":
        same_period = (last_date.year == first_date.year)
    else:
        raise ValueError("breakdown must be 'daily', 'weekly', 'monthly', or 'yearly'")

    if not same_period:
        return deepcopy(list1) + deepcopy(list2)

    # --- Merge by label ---
    merged_block = []
    labels_last = {d['label']: d for d in last_block}
    labels_first = {d['label']: d for d in first_block}
    all_labels = set(labels_last) | set(labels_first)
    later_date_str = max(last_date, first_date).strftime("%Y-%m-%d %H:%M:%S")

    def merge_dicts(d1, d2):
        merged = deepcopy(d1)
        merged['mainChartData'][0]['x'] = later_date_str
        merged['mainChartData'][0]['y'] = d1['mainChartData'][0]['y'] + d2['mainChartData'][0]['y']

        def merge_secondary(x1, y1, x2, y2):
            merged_x = list(dict.fromkeys(x1 + x2))
            merged_y = [[0]*len(merged_x)]
            for i, lbl in enumerate(merged_x):
                val1 = y1[0][x1.index(lbl)] if lbl in x1 else 0
                val2 = y2[0][x2.index(lbl)] if lbl in x2 else 0
                merged_y[0][i] = val1 + val2
            return merged_x, merged_y

        merged['x_secondary'], merged['secondaryChartData'] = merge_secondary(
            d1['x_secondary'], d1['secondaryChartData'],
            d2['x_secondary'], d2['secondaryChartData']
        )
        merged['x_secondary_b'], merged['secondaryChartData_b'] = merge_secondary(
            d1['x_secondary_b'], d1['secondaryChartData_b'],
            d2['x_secondary_b'], d2['secondaryChartData_b']
        )
        merged['x_secondary_c'], merged['secondaryChartData_c'] = merge_secondary(
            d1['x_secondary_c'], d1['secondaryChartData_c'],
            d2['x_secondary_c'], d2['secondaryChartData_c']
        )
        return merged

    for lbl in all_labels:
        if lbl in labels_last and lbl in labels_first:
            merged_block.append(merge_dicts(labels_last[lbl], labels_first[lbl]))
        elif lbl in labels_last:
            d = deepcopy(labels_last[lbl])
            d['mainChartData'][0]['x'] = later_date_str
            merged_block.append(d)
        else:
            d = deepcopy(labels_first[lbl])
            d['mainChartData'][0]['x'] = later_date_str
            merged_block.append(d)

    # --- Build final combined list ---
    merged_list = deepcopy(list1[:-1]) + [merged_block] + deepcopy(list2[1:])
    return merged_list


# Helpers for datatransformation_for_chartjs_CPU 

def merge_new_date_into_consolidated_list(consolidated_list, new_date):
    """
    Merge a single new date (given as a list with one string) into an existing consolidated_list,
    keeping mainChartData and secondaryChartData arrays aligned.
    """
    # Extract string from list if needed
    if isinstance(new_date, list) and len(new_date) == 1:
        new_date = new_date[0]

    for item in consolidated_list:
        main_data = item['mainChartData']

        # Build main_dates safely
        main_dates = [entry['x'] for entry in main_data]

        if new_date not in main_dates:
            # Find the correct insertion index (chronological order)
            insert_pos = 0
            while insert_pos < len(main_data):
                entry_x = main_data[insert_pos]['x'][0] if isinstance(main_data[insert_pos]['x'], list) else main_data[insert_pos]['x']
                if datetime.strptime(entry_x, "%Y-%m-%d %H:%M:%S") < datetime.strptime(new_date, "%Y-%m-%d %H:%M:%S"):
                    insert_pos += 1
                else:
                    break

            # Insert new date with y=0
            main_data.insert(insert_pos, {'x': new_date, 'y': 0})

            # Insert zeros into secondary arrays
            for key in ['secondaryChartData', 'secondaryChartData_b', 'secondaryChartData_c']:
                if key in item:
                    length = len(item[key][0]) if item[key] and item[key][0] else 0
                    item[key].insert(insert_pos, [0]*length)

        # Sort mainChartData chronologically
        main_data.sort(key=lambda e: datetime.strptime(e['x'][0] if isinstance(e['x'], list) else e['x'], "%Y-%m-%d %H:%M:%S"))

    return consolidated_list




def merge_consolidated(main_items, consolidated_list):
    # Flatten and sort consolidated dates
    consolidated_dates = sorted(
        [d for d in consolidated_list if isinstance(d, str)],
        key=lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
    )

    for item in main_items:
        # Current mainChartData
        main_data = item['mainChartData']

        # Build a list of all dates in mainChartData
        main_dates = [entry['x'] for entry in main_data]

        for date in consolidated_dates:
            # Only add missing dates
            if date not in main_dates:
                # Find the position to insert (keep chronological order)
                insert_pos = 0
                while insert_pos < len(main_data) and datetime.strptime(main_data[insert_pos]['x'], "%Y-%m-%d %H:%M:%S") < datetime.strptime(date, "%Y-%m-%d %H:%M:%S"):
                    insert_pos += 1

                # Insert into mainChartData
                main_data.insert(insert_pos, {'x': date, 'y': 0})

                # Insert corresponding 0s into secondary datasets
                for key in ['secondaryChartData', 'secondaryChartData_b', 'secondaryChartData_c']:
                    if key in item:
                        # Determine the length of each row in that dataset
                        length = len(item[key][0]) if item[key] and item[key][0] else 0
                        # Insert at the same position
                        item[key].insert(insert_pos, [0]*length)

        # Sort mainChartData again just in case
        main_data.sort(key=lambda e: datetime.strptime(e['x'], "%Y-%m-%d %H:%M:%S"))

    # Clear old consolidated_list and replace with updated main_items
    consolidated_list.clear()
    consolidated_list.extend(main_items)

    return main_items





async def datatransformation_for_chartjs(client_id, year, month, day, hour, minutes, seconds, year_end, month_end, day_end, hour_end, minutes_end, seconds_end,frequency, table_name, redis, topic: str | None = None):
    utc = pytz.UTC
    start_dt = utc.localize(datetime(int(year), int(month), int(day), int(hour), int(minutes), int(seconds)))
    end_dt = utc.localize(datetime(int(year_end), int(month_end), int(day_end), int(hour_end), int(minutes_end), int(seconds_end)))

    all_transformed = []
    covered_ranges = []

    # 1. Stream cached chart chunks
    async for df_chunk, (chunk_start, chunk_end) in stream_chunks_from_redis(client_id, start_dt, end_dt, redis):
        transformed = await run_cpu_task(datatransformation_for_chartjs_cpu, df_chunk, start_dt, end_dt, frequency)
        all_transformed.extend(transformed)
        covered_ranges.append((chunk_start, chunk_end))

    # 2. Compute missing ranges
    missing_intervals = find_gaps_in_range(start_dt, end_dt, covered_ranges)
    if not missing_intervals:
        if not all_transformed:
            return []

        def merge_all():
            from functools import reduce
            return reduce(lambda acc, lst: merge_two_lists(acc, lst, breakdown=frequency), deepcopy(all_transformed))

        return await run_cpu_task(merge_all)

    # 3. For each missing interval → get data from blob/db, run transformation per chunk
    async with async_session_scope() as session:
        for gap_start, gap_end in missing_intervals:
            # Example: blob processing
            blob_names = await list_blob_names_for_range(client_id, gap_start, gap_end)
            for blob_name in blob_names:
                def process_blob():
                    return list(load_blob_in_chunks(client_id, blob_name, chunk_size=10_000))
                blob_chunks = await run_cpu_task(process_blob)

                for df_chunk in blob_chunks:
                    df_filtered = df_chunk[
                        (df_chunk["created_at"] >= gap_start) &
                        (df_chunk["created_at"] <= gap_end)
                    ]
                    if df_filtered.empty:
                        continue

                    transformed = await run_cpu_task(datatransformation_for_chartjs_cpu, df_filtered, start_dt, end_dt, frequency, topic)
                    all_transformed.extend(transformed)

                    await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)

            # Example: db partitions
            partitions = await fetch_partitions_for_table_chunks(session, "chat_messages", gap_start, gap_end)
            for part in partitions:
                async for df_chunk in fetch_partition_rows_in_chunks_gen(session, client_id, part, gap_start, gap_end):
                    df_filtered = df_chunk[
                        (df_chunk["created_at"] >= gap_start) &
                        (df_chunk["created_at"] <= gap_end)
                    ]
                    if df_filtered.empty:
                        continue

                    transformed = await run_cpu_task(datatransformation_for_chartjs_cpu, df_filtered, start_dt, end_dt, frequency, topic)
                    all_transformed.extend(transformed)

                    await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)

    
    # 4. Final merge of all chunks
    if not all_transformed:
        return []

    # Merge everything using merge_two_lists
    def merge_all():
        from functools import reduce
        return reduce(lambda acc, lst: merge_two_lists(acc, lst, breakdown=frequency), deepcopy(all_transformed))

    final_result = await run_cpu_task(merge_all)
    return final_result




def datatransformation_for_chartjs_cpu(df_pandas, start_dt, end_dt, frequency, topic: str | None = None):
  
  if df_pandas.empty:
     return []
  utc = pytz.UTC

  # --- Topic filtering  ---
  if topic and "topic_classification" in df_pandas.columns and topic.lower!="összes":
      df_pandas["topic_classification"] = df_pandas["topic_classification"].fillna("")
      df_pandas = df_pandas[
          df_pandas["topic_classification"].str.lower() == topic.lower()
      ]

    

 
  
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


  # Apply vectorized with Pandas .apply (still row-wise but faster than ast.literal_eval)
  df_pandas['topic'] = df_pandas['topic'].apply(safe_literal_eval)
  

 
  
  
  #######################################################
  #   CREATING AND RESTRUCTURING THE PANDAS DATAFRAME   #
  #######################################################

  # df_pandas = pd.DataFrame(rows, columns=columns)

  # Remove milliseconds from 'created_at' column and set it as index
  df_pandas['created_at'] = pd.to_datetime(df_pandas['created_at'], utc=True).dt.floor('s')
  
  # Set 'created_at' column as the index
  df_pandas.set_index('created_at', inplace=True)
  df_pandas.sort_index(ascending=True, inplace=True)

  # 6. Drop columns if needed
  if 'id' in df_pandas.columns:
      df_pandas.drop(columns=['id'], inplace=True)





  if df_pandas.empty:
    return []

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

    # # Get the full datetime range
    # start_dt = df_pandas.index.min()
    # end_dt = df_pandas.index.max()
    
    # Truncate to date only for looping, DE EZ MÁR UTCBEN VAN EREDETILEG IS
    start_dt = start_dt.replace(tzinfo=pytz.UTC)
    end_dt = end_dt.replace(tzinfo=pytz.UTC)

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Function to get timezone-aware weekly boundaries
    def get_week_boundaries(date, tz):
        start_of_week = datetime.combine(date - timedelta(days=date.weekday()), datetime.min.time()).replace(tzinfo=tz)
        end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return start_of_week, end_of_week  # Convert Timestamp to date

    # Iterate over weeks from start to end
    current_date = start_dt
    while current_date <= end_dt:
        # Get the start and end dates of the current week
        week_start, week_end = get_week_boundaries(current_date, tz=start_dt.tzinfo)
        # Adjust first week start and end
        if current_date == start_dt:
            week_start = start_dt
            week_end = min(week_end, end_dt)

        # Adjust last week end
        if week_end > end_dt:
            week_end = end_dt
     
        # Select rows within the current week
        sub_df = df_pandas[(df_pandas.index >= week_start) & (df_pandas.index <= week_end)]
        if sub_df.empty:
          dummy_index = week_start + timedelta(seconds=1)
          sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[dummy_index])
        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)
        # Move to the next week
        current_date = week_end + timedelta(seconds=1)  # Ensure current_date is a date object

    # Now sub_dataframes contains sub DataFrames grouped by week
    for i in sub_dataframes:
      timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))


   
  ##############
  #   DAILY    #
  ##############

 
  if breakdown=="daily":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)

    # Get start and end datetime
    start_dt = start_dt.replace(tzinfo=pytz.UTC)
    end_dt = end_dt.replace(tzinfo=pytz.UTC)

  

    # Initialize a list to store sub DataFrames
    sub_dataframes = []

    # Iterate over each day from start to end
    current_dt = start_dt
    while current_dt <= end_dt:
        # Select rows for the current day
        day_start = datetime.combine(current_dt, datetime.min.time()).replace(tzinfo=start_dt.tzinfo)
        day_end = min(datetime.combine(current_dt.date(), datetime.max.time()).replace(tzinfo=current_dt.tzinfo), end_dt)


        sub_df = df_pandas[(df_pandas.index >= day_start) & (df_pandas.index <= day_end)]
        if sub_df.empty:
            dummy_index = day_start + timedelta(seconds=1)
            sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[dummy_index])

        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)
        # Move to the next day
        current_dt += pd.Timedelta(days=1)

    # Now sub_dataframes contains sub DataFrames grouped by each individual day
    for i in sub_dataframes:
      timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))


  ##############
  #   YEARLY   #
  ##############

  if breakdown=="yearly":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)

    # Make start and end UTC-aware
    start_dt = start_dt.replace(tzinfo=pytz.UTC)
    end_dt = end_dt.replace(tzinfo=pytz.UTC)

    sub_dataframes = []

    current_year = start_dt.year
    last_year = end_dt.year

    while current_year <= last_year:
        
        # Start and end of the current year
        year_start = pd.Timestamp(current_year, 1, 1, 0, 0, 0, tz='UTC')
        year_end = pd.Timestamp(current_year, 12, 31, 23, 59, 59, tz='UTC')

        # Adjust first and last year boundaries to client start/end
        if current_year == start_dt.year:
            year_start = start_dt
        if current_year == end_dt.year:
            year_end = end_dt

        # Select rows in this year
        sub_df = df_pandas[(df_pandas.index >= year_start) & (df_pandas.index <= year_end)]

        # If no data, create dummy row
        if sub_df.empty:
            dummy_index = year_start + pd.Timedelta(hours=1)
            sub_df = pd.DataFrame({'id': '', 'user_id': '', 'message': '', 'topic': [[]]}, index=[dummy_index])

        sub_dataframes.append(sub_df)
        current_year += 1


    # Now sub_dataframes contains sub DataFrames grouped by each individual year
    for i in sub_dataframes:
          timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))
    

  ###############
  #   MONTHLY   #
  ###############

  if breakdown=="monthly":
    # Assuming df_pandas is your DataFrame
    # Convert index to datetime if it's not already
    df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)

    # Find the start and end dates of the data
    start_dt = start_dt.replace(tzinfo=pytz.UTC)
    end_dt = end_dt.replace(tzinfo=pytz.UTC)

    sub_dataframes = []

    # Current month loop
    current_dt = start_dt
    while current_dt <= end_dt:
        # Start and end of the current month (UTC-aware)
        start_of_month = pd.Timestamp(current_dt.year, current_dt.month, 1, 0, 0, 0, tz='UTC')
        # MonthEnd(1) gives you the last day of that month
        end_of_month = (start_of_month + pd.offsets.MonthEnd(1)).replace(hour=23, minute=59, second=59)

        # Adjust boundaries for first and last months
        if current_dt == start_dt:
            start_of_month = start_dt
        if end_of_month > end_dt:
            end_of_month = end_dt

        # Select rows for the current month
        sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= end_of_month)]

        # Handle empty month → create dummy row
        if sub_df.empty:
            dummy_index = start_of_month + pd.Timedelta(hours=1)
            sub_df = pd.DataFrame(
                {'id': '', 'user_id': '', 'message': '', 'topic': [[]]},
                index=[dummy_index]
            )

        # Append the sub DataFrame to the list
        sub_dataframes.append(sub_df)

        # Move to the first day of the next month
        current_dt = (start_of_month + pd.offsets.MonthBegin(1)).replace(hour=0, minute=0, second=0)


    # Now sub_dataframes contains sub DataFrames grouped by each individual month

    for i in sub_dataframes:
        timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))
  
 
  # database_url = os.environ.get('DATABASE_URL')   
  data_to_transform=[]
  
  for df_pandas in sub_dataframes:  # each period: week, month, etc.
    # --- 4-item topics ---

    list_for_4items = [p for row in df_pandas['topic'] for p in row if len(p) == 4]
 
    # Group by first element using defaultdict
    grouped_dict = defaultdict(list)
    for sublist in list_for_4items:
        grouped_dict[sublist[0]].append(sublist)

    grouped_list = list(grouped_dict.values())

    # Build collection with counts per level
    collection = []
    for x in grouped_list:
        summary = [defaultdict(int) for _ in range(len(x[0]))]  # one defaultdict per level
        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        # Convert to list-of-dicts per level (for chartjs)
        final_result = [[{k: v} for k, v in s.items()] for s in summary]
        collection.append(final_result)
    
   

    # --- 3-, 2-, 1-item topics ---
    list_for_3items = [p for row in df_pandas['topic'] for p in row if len(p) == 3]
    list_for_2items = [p for row in df_pandas['topic'] for p in row if len(p) == 2]
    list_for_1items = [p for row in df_pandas['topic'] for p in row if len(p) == 1]
  
        

    #####################################################################
    #          MANAGING 3 ITEM LIST FOR THE data_to_transform LIST      #
    #####################################################################

    new_3_items=[]

    for p in list_for_3items:
        is_new_collection = True

        for k, coll in enumerate(collection):
            root_key = list(coll[0][0].keys())[0]

            # Level 1: check root
            if p[0] == root_key:
                is_new_collection = False
                coll[0][0][root_key] += 1

                # Build lookup for level 2
                level2_lookup = {list(d.keys())[0]: d for d in coll[1]}
                if p[1] in level2_lookup:
                    level2_lookup[p[1]][p[1]] += 1
                else:
                    coll[1].append({p[1]: 1})

                # Build lookup for level 3
                level3_lookup = {list(d.keys())[0]: d for d in coll[2]}
                if p[2] in level3_lookup:
                    level3_lookup[p[2]][p[2]] += 1
                else:
                    coll[2].append({p[2]: 1})

        if is_new_collection:
            new_3_items.append(p)

    # Group new 3-item rows by root key using defaultdict
    grouped_dict_new_3_items = defaultdict(list)
    for sublist in new_3_items:
        key = sublist[0]
        grouped_dict_new_3_items[key].append(sublist)

    # Convert the dictionary values to a list (same as your original grouped_list_new_3_item)
    grouped_list_new_3_item = list(grouped_dict_new_3_items.values())

    # Build collection_new_3_item
    collection_new_3_item = []

    for x in grouped_list_new_3_item:
        # Initialize counters for each level in the 3-item sublists
        summary = [defaultdict(int) for _ in range(len(x[0]))]

        # Count occurrences at each level
        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        # Convert summary to the same nested list-of-dicts format
        final_result = [[{key: value} for key, value in s.items()] for s in summary]

        collection_new_3_item.append(final_result)

    # Merge into main collection
    for i in collection_new_3_item:
        collection.append(i)

    ######################################
    #         TWO ITEMED LIST            #
    ######################################


    # Identify new 2-item collections
    new_2_items = []

    for p in list_for_2items:
        is_new_collection = True

        for coll in collection:
            root_key = list(coll[0][0].keys())[0]

            # Level 1: check root
            if p[0] == root_key:
                is_new_collection = False
                coll[0][0][root_key] += 1

                # Build lookup for level 2
                level2_lookup = {list(d.keys())[0]: d for d in coll[1]}
                if p[1] in level2_lookup:
                    level2_lookup[p[1]][p[1]] += 1
                else:
                    coll[1].append({p[1]: 1})

        if is_new_collection:
            new_2_items.append(p)

    # Group new 2-item rows by root key using defaultdict
    grouped_dict_new_2_items = defaultdict(list)
    for sublist in new_2_items:
        key = sublist[0]
        grouped_dict_new_2_items[key].append(sublist)

    # Convert the dictionary values to a list
    grouped_list_new_2_item = list(grouped_dict_new_2_items.values())

    # Build collection_new_2_item
    collection_new_2_item = []

    for x in grouped_list_new_2_item:
        summary = [defaultdict(int) for _ in range(len(x[0]))]

        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        final_result = [[{key: value} for key, value in s.items()] for s in summary]
        collection_new_2_item.append(final_result)

    # Merge into main collection
    for i in collection_new_2_item:
        collection.append(i)



    ######################################
    #         ONE ITEM LIST              #
    ######################################

    
    # Identify new 1-item collections
    new_1_items = []

    for p in list_for_1items:
        is_new_collection = True

        for coll in collection:
            root_key = list(coll[0][0].keys())[0]

            # Level 1: check root
            if p[0] == root_key:
                is_new_collection = False
                coll[0][0][root_key] += 1

        if is_new_collection:
            new_1_items.append(p)

    # Group new 1-item rows by root key using defaultdict
    grouped_dict_new_1_items = defaultdict(list)
    for sublist in new_1_items:
        key = sublist[0]
        grouped_dict_new_1_items[key].append(sublist)

    # Convert the dictionary values to a list
    grouped_list_new_1_item = list(grouped_dict_new_1_items.values())

    # Build collection_new_1_item
    collection_new_1_item = []

    for x in grouped_list_new_1_item:
        summary = [defaultdict(int) for _ in range(len(x[0]))]

        for sublist in x:
            for i, item in enumerate(sublist):
                summary[i][item] += 1

        final_result = [[{key: value} for key, value in s.items()] for s in summary]
        collection_new_1_item.append(final_result)

    # Merge into main collection
    for i in collection_new_1_item:
        collection.append(i)

    # Append to data_to_transform
    data_to_transform.append(collection)

 
 
  #data_to_transform
  # több periódus: [[], [[[{'gitár': 8}], [{'akusztikus gitár': 2}, {'balkezes akusztikus gitár': 2}, {'balkezes elektro-akusztikus gitár': 2}, {'elektro-akusztikus gitár': 2}], [{'cort': 4}]]]]
  # egy periódus:  [[[[{'gitár': 8}], [{'akusztikus gitár': 2}, {'balkezes akusztikus gitár': 2}, {'balkezes elektro-akusztikus gitár': 2}, {'elektro-akusztikus gitár': 2}], [{'cort': 4}]]]]
  
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
  if len(data_to_transform)==1:
    if len(data_to_transform)==1 and len(data_to_transform[0])==0:
        
        product_data="There was no chat activity in this period"
        final_transformed_data.append(product_data)
    else:
     
      for item0 in data_to_transform:
        for item in item0:
          product_data = {}
          product_data['label'] = list(item[0][0].keys())[0]
          main_chart_data = [{'x': timestamp[0], 'y': list(entry.values())[0]} for entry in item[0]]
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
            product_data['x_secondary'] = ["**Típusról nem folyt beszélgetés"]
            product_data['secondaryChartData'] = [secondaryChartData]
            secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
            product_data['label_b'] = c[0]
            product_data['x_secondary_b'] = ["**Gyártóról nem folyt beszélgetés"]
            product_data['secondaryChartData_b'] = [secondaryChartData_b]
            secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
            product_data['label_c'] = c[1]
            product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
            product_data['secondaryChartData_c'] = [secondaryChartData_c]
          if len(item)==2:
            secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
            product_data['label_b'] = c[0]
            product_data['x_secondary_b'] = ["**Típusról nem folyt beszélgetés"]
            product_data['secondaryChartData_b'] = [secondaryChartData_b]
            secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
            product_data['label_c'] = c[1]
            product_data['x_secondary_c'] = ["**Gyártóról nem folyt beszélgetés"]
            product_data['secondaryChartData_c'] = [secondaryChartData_c]
          if len(item)==3:
            secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
            product_data['label_c'] = c[1]
            product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
            product_data['secondaryChartData_c'] = [secondaryChartData_c]
          final_transformed_data.append(product_data)


  print("FINALT: ", len(final_transformed_data))  
  #FINALT:  [{'label': 'gitár', 'mainChartData': [{'x': '2025-01-30 20:38:09', 'y': 321}], 'x_secondary': ['akusztikus gitár', 'balkezes akusztikus gitár', 'balkezes elektro-akusztikus gitár', 'elektro-akusztikus gitár', 'típust nem említett'], 'secondaryChartData': [[84, 50, 47, 51, 88]], 'label_b': 'gyártó', 'x_secondary_b': ['cort', 'admira', 'gewa', 'alhambra'], 'secondaryChartData_b': [[145, 44, 31, 44]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[321]]}, {'label': 'basszusgitár', 'mainChartData': [{'x': '2025-01-30 20:38:09', 'y': 1}], 'x_secondary': ['basszusgitár'], 'secondaryChartData': [[1]], 'label_b': 'gyártó', 'x_secondary_b': ['cort'], 'secondaryChartData_b': [[1]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[1]]}]
  
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

  if len(data_to_transform)>1:
    
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
          product_data['x_secondary'] = ["**Típusról nem folyt beszélgetés"]
          product_data['secondaryChartData'] = [secondaryChartData]
          secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = ["**Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==2:
          
          secondaryChartData_b = [list(entry.values())[0] for entry in item[0]]
          product_data['label_b'] = c[0]
          product_data['x_secondary_b'] = ["**Típusról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        

        
        period_to_add_to_finaltransformation.append(product_data)
        
      if not period:
        period_to_add_to_finaltransformation.append(timestamp[timestamp_index])
          
      timestamp_index+=1
      data_for_final_transformation.append(period_to_add_to_finaltransformation)

    # for i in data_for_final_transformation:
    #   print("ˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇ")
    #   print(i)
    #  ˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇ
    # ['2025-01-13 00:00:01']
    # ˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇˇ
    # [{'label': 'gitár', 'mainChartData': [{'x': '2025-01-23 18:46:54', 'y': 174}], 'x_secondary': ['akusztikus gitár', 'elektro-akusztikus gitár', 'balkezes akusztikus gitár', 'típust nem említett', 'balkezes elektro-akusztikus gitár'], 'secondaryChartData': [[45, 27, 27, 50, 24]], 'label_b': 'gyártó', 'x_secondary_b': ['cort', 'admira', 'gewa', 'alhambra'], 'secondaryChartData_b': [[71, 25, 16, 25]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[174]]}, {'label': 'basszusgitár', 'mainChartData': [{'x': '2025-01-23 18:46:54', 'y': 1}], 'x_secondary': ['basszusgitár'], 'secondaryChartData': [[1]], 'label_b': 'gyártó', 'x_secondary_b': ['cort'], 'secondaryChartData_b': [[1]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[1]]}]
    print("++++++++++++++++++++++++++++++++")      
    print(data_for_final_transformation)
    print("++++++++++++++++++++++++++++++++")
    for main_item in data_for_final_transformation:
      
      
      #Nothing we have in the consolidated list, and add chartjs data
      if len(consolidated_list)==0 and type(main_item[0])!=str:
        consolidated_list=main_item
        continue

      
      #Nothing we have in the consolidated list, and add timestamp
      if len(consolidated_list)==0 and type(main_item[0])==str:
        consolidated_list.append(main_item[0])
        continue

      #if we have timestamps only in consolidated list and add chartjs strucure
      if all(isinstance(item, str) for item in consolidated_list) and type(main_item[0])==str:
        consolidated_list.append(main_item[0])
        continue

      #if we have timestamps only in consolidated list and add chartjs strucure
      if all(isinstance(item, str) for item in consolidated_list) and type(main_item[0])!=str:
        consolidated_list=merge_consolidated(main_item, consolidated_list)
        continue

      # All items are chartjs structure in consolidated_list and the item is string (csak dátumot tartalmaz)   
      if all(not isinstance(item, str) for item in consolidated_list) and type(main_item[0])==str:
        consolidated_list=merge_new_date_into_consolidated_list(consolidated_list, main_item)
        continue

      
      # ilyen adatstruktúrák lehetnek:
      # main_item: [{'label': 'gitár', 'mainChartData': [{'x': '2025-01-23 18:46:54', 'y': 174}], 'x_secondary': ['akusztikus gitár', 'elektro-akusztikus gitár', 'balkezes akusztikus gitár', 'típust nem említett', 'balkezes elektro-akusztikus gitár'], 'secondaryChartData': [[45, 27, 27, 50, 24]], 'label_b': 'gyártó', 'x_secondary_b': ['cort', 'admira', 'gewa', 'alhambra'], 'secondaryChartData_b': [[71, 25, 16, 25]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[174]]}, {'label': 'basszusgitár', 'mainChartData': [{'x': '2025-01-23 18:46:54', 'y': 1}], 'x_secondary': ['basszusgitár'], 'secondaryChartData': [[1]], 'label_b': 'gyártó', 'x_secondary_b': ['cort'], 'secondaryChartData_b': [[1]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[1]]}]
      # item_a: {'label': 'basszusgitár', 'mainChartData': [{'x': '2025-01-23 18:46:54', 'y': 1}], 'x_secondary': ['basszusgitár'], 'secondaryChartData': [[1]], 'label_b': 'gyártó', 'x_secondary_b': ['cort'], 'secondaryChartData_b': [[1]], 'label_c': 'márka', 'x_secondary_c': ['Márkatípusról nem folyt beszélgetés'], 'secondaryChartData_c': [[1]]}
      
     
      if all(not isinstance(item, str) for item in consolidated_list) and type(main_item[0])!=str and len(consolidated_list)!=0:
      
        for item_a in main_item:
       
      
          
         #HANDELING THOSE ITEMS WHICH HAS COMMON LABEL IN DIFFERENT PERIODS

        # All items are list in the consolidated_list and the item to be consolidated is list
        
        
        # Check if the item's label exists in list b
          if any(item_a['label'] == item_b['label'] for item_b in consolidated_list):
           
            # Find the corresponding item in list b
            item_b = next(item_b for item_b in consolidated_list if item_b['label'] == item_a['label'])
           
            # Create a new item to store the consolidated data
            consolidated_item = {'label': item_a['label']}



            a_time=item_a['mainChartData'][0]['x']
            b_times = [d['x'] for d in item_b['mainChartData']]
            if a_time in b_times:

              index_in_b = b_times.index(a_time)
              # 1.) Consolidate mainChartData
              y_value_b_plus_value_a = item_b['mainChartData'][index_in_b]['y'] + item_a['mainChartData'][0]['y']
              consolidated_item['mainChartData'] = item_b['mainChartData']
              consolidated_item['mainChartData'][index_in_b]['y']=y_value_b_plus_value_a
              # 2.) Consolidate x_secondary
              consolidated_item['x_secondary'] = list(set(item_a['x_secondary'] + item_b['x_secondary']))
              # 3.) Consolidate secondaryChartData
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
              result_b[index_in_b]=[x + y for x, y in zip(result_b[index_in_b], result_a)]       
              consolidated_item['secondaryChartData'] = result_b

              #4 Consolidate Secondary B data
              consolidated_item['label_b'] = 'gyártó'
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

                result_b[index_in_b]=[x + y for x, y in zip(result_b[index_in_b], result_a)]       
                consolidated_item['secondaryChartData_b'] = result_b

              # 5 Consolidate Secondary C data
              consolidated_item['label_c'] = 'márka'
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
                result_b[index_in_b]=[x + y for x, y in zip(result_b[index_in_b], result_a)]       
                consolidated_item['secondaryChartData_c'] = result_b

           


            else:

              b_time=item_b['mainChartData'][-1]['x']
              
              
              
              b_time = datetime.strptime(b_time, "%Y-%m-%d %H:%M:%S")
              a_time = datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S")
    
              # 1.) Consolidate mainChartData

              if a_time < b_time:
                consolidated_item['mainChartData'] = item_a['mainChartData'] + item_b['mainChartData']

              if a_time > b_time:
                consolidated_item['mainChartData'] = item_b['mainChartData'] + item_a['mainChartData']

              
              # 2.) Consolidate x_secondary
              consolidated_item['x_secondary'] = list(set(item_a['x_secondary'] + item_b['x_secondary']))
              
              # 3.) Consolidate secondaryChartData
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


            a_time=item_a['mainChartData'][0]['x']
            b_times = [d['x'] for d in max_item['mainChartData']]

            if a_time in b_times:
            

              index_in_b = b_times.index(a_time)
              # 1.) Consolidate mainChartData
            
              chartdata=[]
              for r in range(len(max_item['mainChartData'])):
                x=max_item['mainChartData'][r]['x']
                chartdata.append({'x': x, 'y': 0})
              chartdata[index_in_b] = item_a['mainChartData'][0]
              consolidated_item['mainChartData']=chartdata


              consolidated_item['x_secondary']=item_a['x_secondary']
              result_b=[]
              
              for x in  range(len(max_item['mainChartData'])):
                result_init=[0] * len(item_a['x_secondary'])
                result_b.append(result_init)
              result_b[index_in_b] = item_a['secondaryChartData'][0]
              consolidated_item['secondaryChartData'] = result_b 
              
              consolidated_item['label_b'] = 'gyártó'
              if 'x_secondary_b' in item_a:
                consolidated_item['x_secondary_b']=item_a['x_secondary_b']
                result_b=[]
                for x in  range(len(max_item['mainChartData'])):
                  result_init=[0] * len(item_a['x_secondary_b'])
                  result_b.append(result_init)

                result_b[index_in_b] = item_a['secondaryChartData_b'][0]
                consolidated_item['secondaryChartData_b'] = result_b 


              consolidated_item['label_c'] = 'márka'
              if 'x_secondary_c' in item_a:
                consolidated_item['x_secondary_c']=item_a['x_secondary_c']
                result_b=[]
                for x in  range(len(max_item['mainChartData'])):
                  result_init=[0] * len(item_a['x_secondary_c'])
                  result_b.append(result_init)
                result_b[index_in_b] = item_a['secondaryChartData_c'][0]
                consolidated_item['secondaryChartData_c'] = result_b 
          

            else:
    
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

                # if 'x_secondary_b' not in item_a:
                #   consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
                #   result_b=[]
                #   for x in max_item['mainChartData']:
                #     result_init=[0]
                #     result_b.append(result_init)
                #   consolidated_item['secondaryChartData_b'] = result_b + [[0]]

                consolidated_item['label_c'] = 'márka'
                if 'x_secondary_c' in item_a:
                  consolidated_item['x_secondary_c']=item_a['x_secondary_c']
                  result_b=[]
                  for x in max_item['mainChartData']:
                    result_init=[0] * len(item_a['x_secondary_c'])
                    result_b.append(result_init)
                  consolidated_item['secondaryChartData_c'] = result_b + item_a['secondaryChartData_c']
                # if 'x_secondary_c' not in item_a:
                #   consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
                #   result_b=[]
                #   for x in max_item['mainChartData']:
                #     result_init=[0]
                #     result_b.append(result_init)
                #   consolidated_item['secondaryChartData_c'] = result_b + [[0]]
             

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
                  # if 'x_secondary_b' not in item_a:
                  #   consolidated_item['x_secondary_b']=['Gyártóval kapcsolatban nem történt beszélgetés']
                  # result_b=[]
                  # for x in max_item['mainChartData']:
                  #   result_init=[0]
                  #   result_b.append(result_init)
                  # consolidated_item['secondaryChartData_b'] =  [[0]] + result_b
                consolidated_item['label_c'] = 'márka'
                if 'x_secondary_c' in item_a:
                  consolidated_item['x_secondary_c']=item_a['x_secondary_c']
                  result_b=[]
                  for x in  max_item['mainChartData']:
                    result_init=[0] * len(item_a['x_secondary_c'])
                    result_b.append(result_init)
                  consolidated_item['secondaryChartData_c'] =  item_a['secondaryChartData_c'] + result_b
                # if 'x_secondary_c' not in item_a:
                #   consolidated_item['x_secondary_c']=['Márkával kapcsolatban nem történt beszélgetés']
                #   result_b=[]
                #   for x in max_item['mainChartData']:
                #     result_init=[0]
                #     result_b.append(result_init)
                #   consolidated_item['secondaryChartData_c'] =  [[0]] + result_b

 
            consolidated_list.append(consolidated_item)

            # if len(consolidated_list)==0:
            #   consolidated_list.append(consolidated_item)

          # UPDATE ALL ITEMS IN CONSOLIDATED LIST IF NEEDED TO HAVE THE SAME LENGTH REGARDING MAINCHARTDATA
     
          max_length, max_item=longest_mainChartData(consolidated_list)
          for i, item in enumerate(consolidated_list):
            if 'mainChartData' not in item:
                continue  # skip to next item
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

  return final_transformed_data#, data_for_final_transformation_copy, timestamp

    




      






   