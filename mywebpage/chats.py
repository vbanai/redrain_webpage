import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import requests
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session
from contextlib import contextmanager
from mywebpage import Base, session_scope
from mywebpage.datatransformation_detaileduserdata import get_or_create_detailed_df
import pytz
from mywebpage import run_cpu_task, stream_chunks_from_redis, fetch_partitions_for_table_chunks, fetch_partition_rows_in_chunks_gen, store_chunk_in_redis, find_gaps_in_range, async_session_scope, list_blob_names_for_range, load_blob_in_chunks


async def fetch_chat_messages(start_dt, end_dt, client_id, timezone_str, frequency, redis, table_name="chat_messages"):
    """
    Fetch chat messages in a memory-efficient, streaming + caching way.
    """
    chat_rows = []
    columns = None
    covered_ranges = []

    # Stream cached chunks from Redis
    async for df_chunk, (chunk_start, chunk_end) in stream_chunks_from_redis(client_id, start_dt, end_dt, redis):
        rows, cols = await run_cpu_task(fetch_chat_messages_cpu, df_chunk, start_dt, end_dt, timezone_str)
        chat_rows.extend(rows)
        if columns is None:
            columns = cols
        covered_ranges.append((chunk_start, chunk_end))

    # Compute missing intervals
    missing_intervals = find_gaps_in_range(start_dt, end_dt, covered_ranges)

    if not missing_intervals:
        return chat_rows, columns or []

    # Process missing intervals: blobs first, then DB partitions
    chunk_index = 0
    async with async_session_scope() as session:
        for gap_start, gap_end in missing_intervals:

            # 3a. Blobs
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

                    # Process CPU-heavy transformations
                    rows, cols = await run_cpu_task(fetch_chat_messages_cpu, df_filtered, gap_start, gap_end, timezone_str)
                    chat_rows.extend(rows)
                    if columns is None:
                        columns = cols

                    # Cache chunk in Redis
                    await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)
                    chunk_index += 1

            # 3b. DB partitions
            partitions = await fetch_partitions_for_table_chunks(session, table_name, gap_start, gap_end)
            for part in partitions:
                async for df_chunk in fetch_partition_rows_in_chunks_gen(session, client_id, part, gap_start, gap_end):
                    df_filtered = df_chunk[
                        (df_chunk["created_at"] >= gap_start) &
                        (df_chunk["created_at"] <= gap_end)
                    ]
                    if df_filtered.empty:
                        continue

                    # Process CPU-heavy transformations
                    rows, cols = await run_cpu_task(fetch_chat_messages_cpu, df_filtered, gap_start, gap_end, timezone_str)
                    chat_rows.extend(rows)
                    if columns is None:
                        columns = cols

                    # Cache chunk in Redis
                    await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)
                    chunk_index += 1

    return chat_rows, columns or []



def fetch_chat_messages_cpu(df_chunk, start_dt, end_dt, timezone_str):
    df_chunk["created_at"] = pd.to_datetime(df_chunk["created_at"], utc=True)

    # Filter to requested interval
    mask = (df_chunk["created_at"] >= start_dt) & (df_chunk["created_at"] <= end_dt)
    df_filtered = df_chunk.loc[mask].copy()
    if df_filtered.empty:
        return [], []

    # Sort
    df_filtered.sort_values(by=["created_at", "user_id"], inplace=True)

    # Columns logic (reuse from your original)
    excluded_column_indices = {0, 2, 6, 10, 11, 12, 13, 14}
    columns = [col for idx, col in enumerate(df_filtered.columns) if idx not in excluded_column_indices]
    
    rows = []
    user_tz = pytz.timezone(timezone_str)
    for _, row in df_filtered.iterrows():
        new_row = []
        for idx, col in enumerate(df_filtered.columns):
            if idx in excluded_column_indices:
                continue
            if idx == 4:  # message column merge
                merged_message = f"USER: {row[4] or ''} | ASSISTANT: {row[5] or ''}"
                new_row.append(merged_message)
            elif col == "user_id":
                new_row.append(str(row[col])[:8])
            elif col == "created_at":
                ts = row[col]
                local_dt = ts.astimezone(user_tz)
                new_row.append(local_dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S"))
            else:
                new_row.append(row[col])
        rows.append(tuple(new_row))
    return rows, columns



# async def fetch_chat_messages(start_date, end_date, client_id, timezone_str, frequency, redis, table_name="chat_messages"):
#     # Fetch dataframe (already async + handles CPU heavy parts internally)
#     df = await get_or_create_detailed_df(client_id,
#                                 start_date,
#                                 end_date,
#                                 frequency,
#                                 table_name,
#                                 redis)
#     if df.empty:
#         return [], []

#     # Push Pandas filtering/grouping into process pool
#     return await run_cpu_task(fetch_chat_messages_cpu, df, start_date, end_date, timezone_str)



# def fetch_chat_messages_cpu(df, start_date, end_date, timezone_str):
#     # 1. Load cached or freshly built weekly DataFrame from Redis
#      # 1. Convert to datetime objects
 

#     # 2. Ensure datetime and filter by date range
#     df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    

#     mask = (df["created_at"] >= start_date) & (df["created_at"] <= end_date)
#     df_filtered = df.loc[mask].copy()

#     # 3. Sort as before
#     df_filtered.sort_values(by=["created_at", "user_id"], inplace=True)

#     # 4. Exclude and merge columns — just like in your original logic
#     # Define columns to exclude
#     excluded_column_indices = {0, 2, 6, 10, 11, 12, 13, 14}

#     # Get ordered list of columns
#     columns = list(df_filtered.columns)

#     new_columns = []
#     for idx, col in enumerate(columns):
#         if idx in excluded_column_indices:
#             continue
#         if idx == 4:
#             new_columns.append("message")
#         elif idx == 5:
#             continue
#         else:
#             new_columns.append(col)

#     # Build new rows
#     new_rows = []
#     user_tz = pytz.timezone(timezone_str)  
#     for _, row in df_filtered.iterrows():
#         new_row = []
#         for idx, col in enumerate(columns):
#             if idx in excluded_column_indices:
#                 continue
#             if idx == 4:
#                 merged_message = f"USER: {row[4] or ''} | ASSISTANT: {row[5] or ''}"
#                 new_row.append(merged_message)
#             elif idx == 5:
#                 continue
#             elif col == "user_id":
#                 short_client_id = str(row[col])[:8]
#                 new_row.append(short_client_id)
#             elif col == "created_at":
#                 ts = row[col]
#                 if ts.tzinfo is None:
#                     ts = ts.tz_localize('UTC')
#                 local_dt = ts.astimezone(user_tz)
#                 new_row.append(local_dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S"))
#             else:
#                 new_row.append(row[col])
#         new_rows.append(tuple(new_row))

#     return new_rows, new_columns


