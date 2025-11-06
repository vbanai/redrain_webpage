import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import os
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import time
from mywebpage import redis_client 
import gzip
from mywebpage import session_scope
import pickle
import base64
import pytz 
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import html
from mywebpage import async_session_scope, run_cpu_task
import asyncio

# SQLITE ############################################
# # SQLite database path (adjust as needed)
# db_path = r"C:\Users\vbanai\Documents\Programming\Dezsi porject\ChatFrontEnd\NiceLogin_Entra\mywebpage\AIChatBot.db"
# DATABASE_URI = f"sqlite:///{db_path}"

# # Create SQLAlchemy engine and session
# engine = create_engine(DATABASE_URI, connect_args={"check_same_thread": False})
# # Create a session factory
# Session = sessionmaker(bind=engine)

# # Create a scoped session
# Session_scope = scoped_session(Session)
###########################################################





################    HELPER FOR REDIS    ##############################

def compress_df(df):
    compressed = gzip.compress(pickle.dumps(df))
    return base64.b64encode(compressed).decode('utf-8')

async def store_df_in_redis(client_id: str, df: pd.DataFrame, redis, ttl_seconds=900):
    key = f"weekly_df:{client_id}"
    # compress may be CPU-heavy → run in pool
    compressed = await run_cpu_task(compress_df, df)
    await redis.setex(key, ttl_seconds, compressed)

def decompress_df(encoded_str: str) -> pd.DataFrame:
    compressed = base64.b64decode(encoded_str.encode('utf-8'))
    return pickle.loads(gzip.decompress(compressed))


def save_data_with_lock(client_id: str, df: pd.DataFrame, redis_client, ttl_seconds=900):
    lock_key = f"lock:weekly_df:{client_id}"
    # Try to acquire lock using NX (only set if not exists) with 30 sec expiry
    lock_acquired = redis_client.set(lock_key, "1", nx=True, ex=30)
    
    if lock_acquired:
        try:
            store_df_in_redis(client_id, df, ttl_seconds)
        finally:
            redis_client.delete(lock_key)  # Always release the lock
    else:
        print(f"Lock not acquired for client {client_id}, skipping save.")


def load_blobs_for_client(client_id, start_dt, end_dt) -> pd.DataFrame:
    BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    df_archived = pd.DataFrame()
    prefix = "chat_messages_"
    archived_blobs = container_client.list_blobs(name_starts_with=prefix)

    for blob in archived_blobs:
        try:
            name = os.path.basename(blob.name)
            if not name.endswith(".jsonl"):
                continue

            date_part = name.replace("chat_messages_", "").replace(".jsonl", "")
            if "_to_" in date_part:
                start_str, end_str = date_part.split("_to_")
                blob_start_date = pytz.UTC.localize(datetime.strptime(start_str, "%Y_%m_%d"))
                blob_end_date = pytz.UTC.localize(datetime.strptime(end_str, "%Y_%m_%d"))
            else:
                blob_start_date = blob_end_date = pytz.UTC.localize(datetime.strptime(date_part, "%Y_%m_%d"))

            if blob_end_date < start_dt or blob_start_date > end_dt:
                continue

            blob_client = container_client.get_blob_client(blob)
            stream = BytesIO()
            blob_data = blob_client.download_blob()
            blob_data.readinto(stream)
            stream.seek(0)
            df_blob = pd.read_json(stream, lines=True)
            df_blob = df_blob[df_blob['client_id'] == client_id]

            df_archived = pd.concat([df_archived, df_blob], ignore_index=True)
        except Exception as e:
            print(f"Skipped blob {blob.name} due to error: {e}")

    return df_archived

async def fetch_partitions_for_table(table_name: str, start_dt, end_dt) -> list[str]:
    async with async_session_scope() as session:
        sql = """
            SELECT child.relname AS partition_name
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.relname = :parent_table
            ORDER BY partition_name;
        """
        result = await session.execute(text(sql), {"parent_table": table_name})
        partitions = [row[0] for row in result.fetchall()]

        # filter by date from partition name
        def extract_date_from_partition(name: str):
            try:
                date_str = name.replace(f'{table_name}_', '')
                return datetime.strptime(date_str, "%Y_%m_%d")
            except Exception:
                return datetime.min

        return [p for p in partitions if start_dt <= extract_date_from_partition(p) <= end_dt]


async def fetch_partition_rows(client_id: int, part_table: str, start_dt, end_dt) -> pd.DataFrame:
    async with async_session_scope() as session:
        sql = f"""
            SELECT * FROM {part_table}
            WHERE client_id = :client_id
              AND created_at BETWEEN :start_dt AND :end_dt
        """
        result = await session.execute(text(sql), {"client_id": client_id, "start_dt": start_dt, "end_dt": end_dt})
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns)


async def build_weekly_df_from_sources(client_id: int, table_name: str) -> pd.DataFrame:
    # Compute weekly range
    today = datetime.now().replace(microsecond=0)
    days_to_subtract = today.weekday()
    start_dt = (today - timedelta(days=7 + days_to_subtract)).replace(hour=0, minute=0, second=0)
    end_dt = today

    # 1️Load blob data (CPU-heavy) in process pool
    df_blobs = await run_cpu_task(load_blobs_for_client, client_id, start_dt, end_dt)

    # Fetch partition names async
    partitions = await fetch_partitions_for_table(table_name, start_dt, end_dt)

    # Fetch each partition rows async and merge
    # df_parts_list = []
    # for part_table in partitions:
    #     df_part = await fetch_partition_rows(client_id, part_table, start_dt, end_dt)
    #     df_parts_list.append(df_part)
    # a fenti helyett mert ott loopolom és többször hívom meg ugyanazt a functiont
    df_parts_list = await asyncio.gather(*[
        fetch_partition_rows(client_id, part, start_dt, end_dt) for part in partitions
    ])


    # Combine blobs + partitions (CPU-heavy) in process pool
    df_archived = await run_cpu_task(pd.concat, [df_blobs] + df_parts_list, ignore_index=True)


    return df_archived






async def get_or_create_weekly_df(client_id: str, table_name: str, redis, ttl_seconds=900) -> pd.DataFrame:
    redis_key = f"weekly_df:{client_id}"

    cached = await redis.get(redis_key)
    if cached:
        print("Loaded from Redis")
        ttl_left = await redis.ttl(redis_key)
        if ttl_left is not None and ttl_left < 60:
            await redis.expire(redis_key, ttl_seconds)
            print(f"Refreshed TTL for {redis_key} (was {ttl_left}s)")
        # decompression may be CPU heavy → run in process pool
        return await run_cpu_task(decompress_df, cached)

    print("No cache. Acquiring lock...")
    lock_key = f"lock:weekly_df:{client_id}"
    lock_acquired = await redis.set(lock_key, "1", nx=True, ex=30)

    if lock_acquired:
        try:
            cached_after_lock = await redis.get(redis_key)
            if cached_after_lock:
                print("Cache appeared while waiting. Using that.")
                return await run_cpu_task(decompress_df, cached_after_lock)

            # heavy: build df
            df = await build_weekly_df_from_sources(client_id, table_name)
            await store_df_in_redis(client_id, df, redis)
            return df
        finally:
            await redis.delete(lock_key)
    else:
        print("Another process is saving. Waiting...")
        for _ in range(30):
            await asyncio.sleep(0.1)
            cached_retry = await redis.get(redis_key)
            if cached_retry:
                print("Loaded from cache after waiting")
                return await run_cpu_task(decompress_df, cached_retry)

        raise Exception("Timeout: Cache not available and lock not released.")


############################### END OF HELPER FOR REDIS ################################




async def user_querry_forquickreview(client_id):
    # Await the async DataFrame getter
    df = await get_or_create_weekly_df(client_id, "chat_messages")
    if df.empty:
        return Decimal("0"), Decimal("0"), Decimal("0.0"), datetime.now(pytz.UTC), datetime.now(pytz.UTC).date()

    # Push heavy Pandas work into process pool
    return await run_cpu_task(_user_querry_forquickreview_cpu, df)



def _user_querry_forquickreview_cpu(df: pd.DataFrame):

  utc = pytz.UTC
  today = datetime.now(utc).replace(microsecond=0)
  today_date = today.date()

  days_to_subtract = today_date.weekday()
  previous_monday_date = today_date - timedelta(days=days_to_subtract)

  
  # Find the date of today's weekday exactly one week ago
  last_week_today = today - timedelta(days=7)

  # Find the date of the previous Monday exactly one week ago
  last_week_previous_monday_date = previous_monday_date - timedelta(days=7)

  today_with_time = today
  previous_monday_with_time = datetime.combine(previous_monday_date, datetime.min.time()).replace(tzinfo=utc)
  last_week_previous_monday_with_time = datetime.combine(last_week_previous_monday_date, datetime.min.time()).replace(tzinfo=utc)

  last_week_today_with_time = last_week_today
  
  df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
  df = df.set_index('created_at')
 

  

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
          querry_onaverage_thisweek = 0  # or None, or a fallback value like Decimal('0.0')
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
  
  
  return total_unique_users_lastweek, total_unique_users_thisweek, querry_onaverage_thisweek, today, previous_monday_date





async def locationranking(client_id):
    # Fetch dataframe (already async + handles CPU heavy parts internally)
    df = await get_or_create_weekly_df(client_id, "chat_messages")
    if df.empty:
        return ""

    # Push Pandas filtering/grouping into process pool
    return await run_cpu_task(_locationranking_cpu, df)


def _locationranking_cpu(df: pd.DataFrame):

  import pytz
  utc = pytz.UTC
  today = datetime.now(utc).replace(microsecond=0)
  today_ = today.date()

  days_to_subtract = today.weekday()
  previous_monday = today_ - timedelta(days=days_to_subtract)

  today_with_time = today
  previous_monday_with_time = datetime.combine(previous_monday, datetime.min.time()).replace(tzinfo=utc)
  
  df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
  df = df.set_index('created_at')
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

  

  
async def longitude_latitude(client_id: str, redis) -> list[dict]:
    utc = pytz.UTC
    today = datetime.now(utc).replace(microsecond=0)
    today_ = today.date()
    days_to_subtract = today_.weekday()
    previous_monday = today_ - timedelta(days=days_to_subtract)

    today_with_time = today
    previous_monday_with_time = datetime.combine(previous_monday, datetime.min.time()).replace(tzinfo=utc)

    # This DB/Redis fetch is I/O-bound → OK to call directly
    df = await run_cpu_task(get_or_create_weekly_df, client_id, "chat_messages", redis)
    if df.empty:
        return []

    # Define CPU-heavy part (pandas filtering & grouping) in a pure function
    def process_dataframe(df):
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        df = df.set_index("created_at")
        filtered_df = df[(df.index >= previous_monday_with_time) & (df.index <= today_with_time)]

        longlat_data = []

        filtered_df = filtered_df.dropna(subset=["latitude", "longitude"])
        for _, group in filtered_df.groupby([filtered_df.index.date, filtered_df.user_id]):
            if not group.empty:
                lat = group["latitude"].iloc[0]
                lng = group["longitude"].iloc[0]
                longlat_data.append({"location": {"lat": lat, "lng": lng}})
            else:
                longlat_data.append({"location": {"lat": None, "lng": None}})
        return longlat_data

    # Run the pandas computation in the process pool
    longlat_data = await run_cpu_task(process_dataframe, df)
    return longlat_data



# --- CPU-bound part (sync) ---
def process_longitude_latitude_detailed(rows, columns):
    df = pd.DataFrame(rows, columns=columns)
    df = df.dropna(subset=["latitude", "longitude"])
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df = df.set_index("created_at").sort_index()

    df["date"] = df.index.date
    df_first = df.drop_duplicates(subset=["date", "user_id"], keep="first")

    return [{"location": {"lat": lat, "lng": lng}}
            for lat, lng in zip(df_first["latitude"], df_first["longitude"])]





# --- Wrapper (async) ---
async def longitude_latitude_detailed(
    year, month, day, hour, minutes, seconds,
    year_end, month_end, day_end, hour_end, minutes_end, seconds_end
):
    from_ = datetime(year, month, day, hour, minutes, seconds, tzinfo=pytz.UTC)
    to_ = datetime(year_end, month_end, day_end, hour_end, minutes_end, seconds_end, tzinfo=pytz.UTC)

    query = text("""
        SELECT created_at, user_id, latitude, longitude
        FROM chat_messages
        WHERE created_at BETWEEN :from_ AND :to_
    """)

    async with async_session_scope() as db_session:
        result = await db_session.execute(query, {"from_": from_, "to_": to_})
        rows = result.all()
        columns = result.keys()

    # Run heavy part in process pool
    return await run_cpu_task(
        process_longitude_latitude_detailed,
        rows, columns
    )
 

async def fetch_chat_messages_weekly(start_date, end_date, client_id, timezone_str):
    # Fetch dataframe (already async + handles CPU heavy parts internally)
    df = await get_or_create_weekly_df(client_id, "chat_messages")
    if df.empty:
        return [], []

    # Push Pandas filtering/grouping into process pool
    return await run_cpu_task(fetch_chat_messages_weekly_cpu, df, start_date, end_date, timezone_str)


def fetch_chat_messages_weekly_cpu(df, start_date, end_date, timezone_str):


    # 2. Ensure datetime and filter by date range
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

    # Localize naive start_date and end_date as UTC
    if start_date.tzinfo is None:
        start_date = pytz.UTC.localize(start_date)
    if end_date.tzinfo is None:
        end_date = pytz.UTC.localize(end_date)

 
    mask = (df["created_at"] >= start_date) & (df["created_at"] <= end_date)
    df_filtered = df.loc[mask].copy()

    # 3. Sort as before
    df_filtered.sort_values(by=["created_at", "user_id"], inplace=True)

    # 4. Exclude and merge columns — just like in your original logic
    # Define columns to exclude
    excluded_column_indices = {0, 2, 6, 10, 11, 12, 13, 14}

    # Get ordered list of columns
    columns = list(df_filtered.columns)

    new_columns = []
    for idx, col in enumerate(columns):
        if idx in excluded_column_indices:
            continue
        if idx == 4:
            new_columns.append("message")
        elif idx == 5:
            continue
        else:
            new_columns.append(col)

    # Build new rows
    new_rows = []
    for _, row in df_filtered.iterrows():
        new_row = []
        print("All columns in df_filtered: ..................")
        for idx, col in enumerate(columns):
            print(f"Index {idx}: {col}")
            if idx in excluded_column_indices:
                continue
            if idx == 4:
                user_msg = html.escape(row[4] or "")
                assistant_msg = html.escape(row[5] or "")
                agent_name = html.escape(row.get("agent", "ASSISTANT") or "ASSISTANT")
                merged_message = (
                    f"<span class='user-label' data-role='user'>USER</span>: {user_msg} | "
                    f"<span class='assistant-label' data-role='assistant'>{agent_name}</span>: {assistant_msg}"
                )
                new_row.append(merged_message)
            elif idx == 5:
                continue
            elif col == "user_id":
                short_client_id = str(row[col])[:8]
                new_row.append(short_client_id)
            elif col == "created_at":
                ts = row.iloc[idx]  # safer indexing
                if ts.tzinfo is None:
                    ts = pytz.UTC.localize(ts)
                    #ts = ts.tz_localize('UTC')
                local_dt = ts.astimezone(pytz.timezone(timezone_str))
                new_row.append(local_dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S"))
            else:
                new_row.append(row[idx])
        new_rows.append(tuple(new_row))

    return new_rows, new_columns



  


