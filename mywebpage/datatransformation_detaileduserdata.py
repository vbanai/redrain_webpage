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
from mywebpage import redis_client 
import gzip
from mywebpage import session_scope
import pickle
import base64
import pytz 
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from mywebpage import run_cpu_task, async_session_scope, fetch_partition_rows, fetch_partitions_for_table, load_blobs_for_client
import asyncio
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import pytz
from datetime import datetime
from copy import deepcopy
from functools import reduce


def generate_detailed_key(client_id: str, start_dt: datetime, end_dt: datetime):
    return f"detailed_df:{client_id}:{start_dt.isoformat()}:{end_dt.isoformat()}"

def generate_detailed_lock_key(client_id: str, start_dt: datetime, end_dt: datetime):
    data_key = generate_detailed_key(client_id, start_dt, end_dt)
    return f"lock:{data_key}"

def generate_chunk_key(client_id: str, start_dt: datetime, end_dt: datetime, chunk_index: int) -> str:
    """
    Store a chunk in Redis covering start_dt -> end_dt.
    chunk_index allows multiple chunks per same interval (if split further).
    """
    start_s = start_dt.strftime("%Y%m%d%H%M%S")
    end_s = end_dt.strftime("%Y%m%d%H%M%S")
    return f"detailed:{client_id}:{start_s}-{end_s}:chunk-{chunk_index}"


def compress_df(df):
    compressed = gzip.compress(pickle.dumps(df))
    return base64.b64encode(compressed).decode('utf-8')

def decompress_df(encoded_str: str) -> pd.DataFrame:
    compressed = base64.b64decode(encoded_str.encode('utf-8'))
    return pickle.loads(gzip.decompress(compressed))

def store_detailed_df_in_redis(redis_key: str, df: pd.DataFrame, ttl_seconds=900):
    compressed = compress_df(df)
    redis_client.setex(redis_key, ttl_seconds, compressed)



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






#---------------------------------
#    Stream blobs individually     
#---------------------------------

def load_blob_in_chunks(client_id: str, blob_name: str, chunk_size: int = 10_000):
    """
    Load a blob and yield DataFrame chunks of up to `chunk_size` rows.
    Runs in a CPU-bound worker (call via run_cpu_task).
    """
    BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    blob_client = container_client.get_blob_client(blob_name)
    stream = BytesIO()
    blob_data = blob_client.download_blob()
    blob_data.readinto(stream)
    stream.seek(0)

    # Instead of loading full DataFrame, stream in chunks
    with BytesIO() as stream:
        blob_data.readinto(stream)
        stream.seek(0)
        reader = pd.read_json(stream, lines=True, chunksize=chunk_size)
        for df_chunk in reader:
            df_chunk = df_chunk[df_chunk['client_id'] == client_id]
            if not df_chunk.empty:
                yield df_chunk






async def fetch_partition_rows_in_chunks_gen(
    session: AsyncSession,
    client_id: str,
    partition_name: str,
    start_dt: datetime,
    end_dt: datetime,
    chunk_size: int = 10_000
):
    """
    Async generator that yields DataFrame chunks from a single partition table.
    Each chunk contains up to `chunk_size` rows for the given client_id and date range.
    """

    offset = 0

    sql = f"""
        SELECT *
        FROM {partition_name}
        WHERE client_id = :client_id
          AND timestamp BETWEEN :start_dt AND :end_dt
        ORDER BY timestamp
        LIMIT :limit OFFSET :offset;
    """

    while True:
        result = await session.execute(
            text(sql),
            {
                "client_id": client_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "limit": chunk_size,
                "offset": offset,
            },
        )
        rows = result.fetchall()
        if not rows:
            break

        # Convert to DataFrame and yield immediately
        df_chunk = pd.DataFrame(rows, columns=result.keys())
        yield df_chunk

        offset += chunk_size



async def fetch_partitions_for_table_chunks(
    session: AsyncSession,
    table_name: str,
    start_dt: datetime,
    end_dt: datetime,
) -> list[str]:
    """
    Fetch partition table names for `table_name` that overlap with [start_dt, end_dt].
    Returns a list of partition names (strings).
    """

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

    def extract_date_from_partition(name: str) -> datetime:
        try:
            # partition names look like: chat_messages_2025_09_29
            date_str = name.replace(f"{table_name}_", "")
            return datetime.strptime(date_str, "%Y_%m_%d")
        except Exception:
            return datetime.min

    return [
        p
        for p in partitions
        if start_dt <= extract_date_from_partition(p) <= end_dt
    ]


async def list_blob_names_for_range(
    client_id: str,
    start_dt: datetime,
    end_dt: datetime,
    prefix: str = "chat_messages_"
) -> list[str]:
    """
    Return blob names in the container that fall within [start_dt, end_dt].
    Only returns names, not blob content.
    """

    BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    blob_names: list[str] = []
    start_dt = start_dt.astimezone(pytz.UTC)
    end_dt = end_dt.astimezone(pytz.UTC)

    async for blob in container_client.list_blobs(name_starts_with=prefix):
        name = os.path.basename(blob.name)

        if not name.endswith(".jsonl"):
            continue

        date_part = name.replace(prefix, "").replace(".jsonl", "")

        if "_to_" in date_part:
            start_str, end_str = date_part.split("_to_")
            blob_start_date = pytz.UTC.localize(datetime.strptime(start_str, "%Y_%m_%d"))
            blob_end_date = pytz.UTC.localize(datetime.strptime(end_str, "%Y_%m_%d"))
        else:
            blob_start_date = blob_end_date = pytz.UTC.localize(
                datetime.strptime(date_part, "%Y_%m_%d")
            )

        # Only include blobs that overlap with requested range
        if blob_end_date >= start_dt and blob_start_date <= end_dt:
            blob_names.append(blob.name)

    return blob_names



async def store_chunk_in_redis(
    client_id: str,
    chunk_start: datetime,
    chunk_end: datetime,
    df_chunk: pd.DataFrame,
    redis,
    ttl_seconds: int = 3600
):
    """
    Compress and store a single chunk of DataFrame in Redis with a key
    that encodes the chunk's date range.
    """
    start_s = chunk_start.strftime("%Y%m%d%H%M")
    end_s = chunk_end.strftime("%Y%m%d%H%M")
    key = f"detailed:{client_id}:{start_s}-{end_s}"

    compressed = await run_cpu_task(compress_df, df_chunk)
    await redis.setex(key, ttl_seconds, compressed)
    return key

def parse_chunk_key(key: str) -> tuple[datetime, datetime]:
    # key format: detailed:clientid:20250101-20250110
    parts = key.split(":")
    date_range = parts[2]
    start_s, end_s = date_range.split("-")
    start = datetime.strptime(start_s, "%Y%m%d%H%M")
    end = datetime.strptime(end_s, "%Y%m%d%H%M")
    return start, end

def overlaps(req_start: datetime, req_end: datetime, chunk_start: datetime, chunk_end: datetime) -> bool:
    return not (req_end <= chunk_start or req_start >= chunk_end)


async def stream_chunks_from_redis(client_id: str, req_start: datetime, req_end: datetime, redis):
    """
    Async generator that yields DataFrame chunks stored in Redis
    if they overlap with [req_start, req_end].
    Uses SCAN-based iteration (non-blocking, memory-efficient).
    """
    pattern = f"detailed:{client_id}:*"

    async for k in redis.scan_iter(match=pattern):  # non-blocking scan
        chunk_start, chunk_end = parse_chunk_key(k)
        if overlaps(req_start, req_end, chunk_start, chunk_end):
            encoded = await redis.get(k)
            if encoded:
                df = await run_cpu_task(decompress_df, encoded)
                yield df, (chunk_start, chunk_end)




def find_gaps_in_range(start_dt: datetime, end_dt: datetime, covered_ranges: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """
    Given [start_dt, end_dt] and a list of covered intervals, return the missing intervals.
    """
    if not covered_ranges:
        return [(start_dt, end_dt)]

    covered_ranges.sort(key=lambda x: x[0])  # sort by start
    gaps = []
    current = start_dt

    for (s, e) in covered_ranges:
        if s > current:
            gaps.append((current, s))
        current = max(current, e)
    if current < end_dt:
        gaps.append((current, end_dt))
    return gaps


#------------------------------------
#    Stream blobs individually end   
#------------------------------------







#-----------------------------------------------------
#    Heart of Coordination retrieval for detailed page
#-----------------------------------------------------


async def build_coords_from_sources_async(
    client_id: str,
    start_dt: datetime,
    end_dt: datetime,
    table_name: str,
    redis,
    chunk_size: int = 10_000,
    ttl_seconds: int = 3600
) -> list[dict]:
    """
    Build a list of coordinates from Redis, blobs, and DB partitions
    in a memory-efficient way. Streams chunks instead of holding them all.
    """
    coords: list[dict] = []
    covered_ranges: list[tuple[datetime, datetime]] = []

    # --- 1. Stream chunks from Redis ---
    async for df_chunk, (chunk_start, chunk_end) in stream_chunks_from_redis(client_id, start_dt, end_dt, redis):
        covered_ranges.append((chunk_start, chunk_end))
        for lat, lng in zip(df_chunk["latitude"], df_chunk["longitude"]):
            if pd.notna(lat) and pd.notna(lng):
                coords.append({"location": {"lat": lat, "lng": lng}})

    # --- 2. Find missing ranges ---
    missing_intervals = find_gaps_in_range(start_dt, end_dt, covered_ranges)

    # --- 3. Process each missing interval ---
    chunk_index = 0
    for gap_start, gap_end in missing_intervals:

        # 3a. Blobs in the gap
        blob_names = await list_blob_names_for_range(client_id, gap_start, gap_end)
        for blob_name in blob_names:
            def process_blob():
                return list(load_blob_in_chunks(client_id, blob_name, chunk_size))
            blob_chunks = await run_cpu_task(process_blob)

            for df_chunk in blob_chunks:
                df_filtered = df_chunk[
                    (df_chunk["timestamp"] >= gap_start) &
                    (df_chunk["timestamp"] <= gap_end)
                ]
                if df_filtered.empty:
                    continue

                # Process coordinates
                for lat, lng in zip(df_filtered["latitude"], df_filtered["longitude"]):
                    if pd.notna(lat) and pd.notna(lng):
                        coords.append({"location": {"lat": lat, "lng": lng}})

                # Cache in Redis
                await store_chunk_in_redis(
                    client_id, gap_start, gap_end, chunk_index,
                    df_filtered.to_dict(orient="records"),
                    redis, ttl_seconds
                )
                chunk_index += 1

        # 3b. DB partitions in the gap
        async with async_session_scope() as session:
            partitions = await fetch_partitions_for_table_chunks(session, table_name, gap_start, gap_end)
            for part in partitions:
                async for df_chunk in fetch_partition_rows_in_chunks_gen(
                    session, client_id, part, gap_start, gap_end, chunk_size=chunk_size
                ):
                    if df_chunk.empty:
                        continue

                    # Process coordinates
                    for lat, lng in zip(df_chunk["latitude"], df_chunk["longitude"]):
                        if pd.notna(lat) and pd.notna(lng):
                            coords.append({"location": {"lat": lat, "lng": lng}})

                    # Cache in Redis
                    await store_chunk_in_redis(
                        client_id, gap_start, gap_end, chunk_index,
                        df_chunk.to_dict(orient="records"),
                        redis, ttl_seconds
                    )
                    chunk_index += 1

    return coords


#---------------------------------------------------------------
#    END ....  Heart of Coordination retrieval for detailed page
#---------------------------------------------------------------


async def build_df_from_sources_async(client_id: str,
                                start_dt: datetime,
                                end_dt: datetime,
                                table_name: str) -> pd.DataFrame:
    """
    Build a detailed DataFrame for a client between start_dt and end_dt
    by combining blob data + SQL partition rows.
    """

    # 1. Load blob data in process pool (CPU heavy)
    df_blobs = await run_cpu_task(load_blobs_for_client, client_id, start_dt, end_dt)

    # 2. Fetch partition names (async)
    partitions = await fetch_partitions_for_table(table_name, start_dt, end_dt)

    # 3. Fetch each partition’s rows concurrently
    df_parts_list = await asyncio.gather(*[
        fetch_partition_rows(client_id, part, start_dt, end_dt) for part in partitions
    ])

    # 4. Merge blobs + partitions (CPU heavy)
    df_archived = await run_cpu_task(pd.concat, [df_blobs] + df_parts_list, ignore_index=True)

    return df_archived



async def store_df_in_redis_customdate(
    client_id: str,
    start_dt: datetime,
    end_dt: datetime,
    df: pd.DataFrame,
    redis,
    ttl_seconds: int = 900
):
    """
    Store a DataFrame in Redis for a custom date range (detailed data).
    Uses a key generated from client_id, start, end, and frequency.
    """
    key = generate_detailed_key(client_id, start_dt, end_dt)
    compressed = await run_cpu_task(compress_df, df)
    await redis.setex(key, ttl_seconds, compressed)


async def get_or_create_detailed_df(
    client_id: str,
    start_dt: datetime,
    end_dt: datetime,
    table_name: str,
    redis,
    ttl_seconds=900
) -> pd.DataFrame:
    
    redis_key = generate_detailed_key(client_id, start_dt, end_dt)
    lock_key = generate_detailed_lock_key(client_id, start_dt, end_dt)

    # Try reading from cache
    cached = await redis.get(redis_key)
    if cached:
        print(f"Loaded from Redis for key {redis_key}")
        ttl_left = await redis.ttl(redis_key)
        refresh_threshold = min(60, ttl_seconds // 5)
        if ttl_left is not None and ttl_left < refresh_threshold:
            await redis.expire(redis_key, ttl_seconds)
            print(f"Refreshed TTL for {redis_key} (was {ttl_left}s)")
        return await run_cpu_task(decompress_df, cached)

    
    for attempt in range(2):
        print(f"No cache for key {redis_key}. Acquiring lock...")
        lock_acquired = await redis.set(lock_key, "1", nx=True, ex=30)
        if lock_acquired:
            try:
                cached_after_lock = await redis.get(redis_key)
                if cached_after_lock:
                    print("Cache appeared after acquiring lock. Using that.")
                    return await run_cpu_task(decompress_df, cached_after_lock)

                # Build DF asynchronously (this function must be async)
                df = await build_df_from_sources_async(client_id, start_dt, end_dt, table_name)
                await store_df_in_redis_customdate(
                    client_id, start_dt, end_dt, df, redis, ttl_seconds
                )
                return df
            except Exception as e:
                print(f"Failed to build DF on attempt {attempt+1} for {redis_key}: {e}")
                if attempt == 0:
                    print("Retrying once...")
                    await asyncio.sleep(1)  # small delay before retry
                else:
                    raise
            finally:
                await redis.delete(lock_key)
        else:
            # Another process is building → wait for cache
            print(f"Another process is building {redis_key}, waiting...")
            deadline = asyncio.get_event_loop().time() + 30  # wait up to 30s
            while asyncio.get_event_loop().time() < deadline:
                cached_retry = await redis.get(redis_key)
                if cached_retry:
                    print("Loaded from Redis after waiting.")
                    return await run_cpu_task(decompress_df, cached_retry)
                await asyncio.sleep(0.1)

            if attempt == 0:
                print("Timeout waiting for cache, retrying to acquire lock...")
                await asyncio.sleep(1)  # small delay before retry
            else:
                raise Exception("Timeout: Cache not available and lock not released.")




# Helper: determine previous period by frequency
def get_previous_period(start_dt, frequency):
    if frequency == "daily":
        return start_dt - timedelta(days=1), start_dt - timedelta(seconds=1)
    elif frequency == "weekly":
        return start_dt - timedelta(weeks=1), start_dt - timedelta(seconds=1)
    elif frequency == "monthly":
        prev_month_end = start_dt - timedelta(seconds=1)
        prev_month_start = (prev_month_end.replace(day=1) - timedelta(days=1)).replace(day=1)
        return prev_month_start, prev_month_end
    elif frequency == "yearly":
        prev_year_start = datetime(start_dt.year - 1, 1, 1)  # January 1st of the previous year
        prev_year_end = datetime(start_dt.year - 1, 12, 31, 23, 59, 59)  # December 31st, 11:59:59 PM of the previous year
        return prev_year_start, prev_year_end
    else:
        return start_dt - timedelta(days=1), start_dt - timedelta(seconds=1)




async def datatransformation_for_chartjs_detailed(
    client_id,
    year, month, day, hour, minutes, seconds,
    year_end, month_end, day_end, hour_end, minutes_end, seconds_end,
    frequency,
    table_name,
    redis, topic: str | None = None
):
    """
    Detailed async version of chart data transformation with additional datasets and
    usernumber change recalculation (including fetching the previous period).
    """

    utc = pytz.UTC
    start_dt = utc.localize(datetime(int(year), int(month), int(day), int(hour), int(minutes), int(seconds)))
    end_dt = utc.localize(datetime(int(year_end), int(month_end), int(day_end), int(hour_end), int(minutes_end), int(seconds_end)))

    all_final_data = []
    all_data_for_final_copy = []
    all_timestamps = []
    all_start_end = []
    all_usernumbers = []
    all_querry_avg = []
    all_changes_usernum = []
    all_locations = []
    covered_ranges = []

    # ----------------------------------------
    # STREAM FROM REDIS CACHE
    # ----------------------------------------
    async for df_chunk, (chunk_start, chunk_end) in stream_chunks_from_redis(client_id, start_dt, end_dt, redis):
        (
            final_transformed_data,
            data_for_final_transformation_copy,
            timestamp,
            start_end_date_byfrequency,
            usernumber,
            querry_on_average,
            changesinusernumber,
            locations
        ) = await run_cpu_task(
            datatransformation_for_chartjs_detailed_cpu,
            df_chunk, start_dt, end_dt, client_id, year, month, day, hour,
            minutes, seconds, year_end, month_end, day_end, hour_end,
            minutes_end, seconds_end, frequency, table_name, topic
        )

        all_final_data.extend(final_transformed_data)
        all_data_for_final_copy.extend(data_for_final_transformation_copy)
        all_timestamps.extend(timestamp)
        all_start_end.extend(start_end_date_byfrequency)
        all_usernumbers.extend(usernumber)
        all_querry_avg.extend(querry_on_average)
        all_changes_usernum.extend(changesinusernumber)
        all_locations.extend(locations)

        covered_ranges.append((chunk_start, chunk_end))

    # ----------------------------------------
    # FIND MISSING INTERVALS
    # ----------------------------------------
    missing_intervals = find_gaps_in_range(start_dt, end_dt, covered_ranges)

    if not missing_intervals:
        # If we have everything from cache
        return _merge_detailed_results(all_final_data,
                                       all_data_for_final_copy,
                                       all_timestamps,
                                       all_start_end,
                                       all_usernumbers,
                                       all_querry_avg,
                                       all_changes_usernum,
                                       all_locations,
                                       frequency)

    # ----------------------------------------
    # PROCESS MISSING RANGES (from blobs + DB)
    # ----------------------------------------
    async with async_session_scope() as session:
        for gap_start, gap_end in missing_intervals:
            # Blobs
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

                    result = await run_cpu_task(
                        datatransformation_for_chartjs_detailed_cpu,
                        df_filtered, start_dt, end_dt, client_id, year, month, day, hour,
                        minutes, seconds, year_end, month_end, day_end, hour_end,
                        minutes_end, seconds_end, frequency, table_name, topic
                    )

                    (
                        final_transformed_data,
                        data_for_final_transformation_copy,
                        timestamp,
                        start_end_date_byfrequency,
                        usernumber,
                        querry_on_average,
                        changesinusernumber,
                        locations
                    ) = result

                    all_final_data.extend(final_transformed_data)
                    all_data_for_final_copy.extend(data_for_final_transformation_copy)
                    all_timestamps.extend(timestamp)
                    all_start_end.extend(start_end_date_byfrequency)
                    all_usernumbers.extend(usernumber)
                    all_querry_avg.extend(querry_on_average)
                    all_changes_usernum.extend(changesinusernumber)
                    all_locations.extend(locations)

                    await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)

            # DB partitions
            partitions = await fetch_partitions_for_table_chunks(session, table_name, gap_start, gap_end)
            for part in partitions:
                async for df_chunk in fetch_partition_rows_in_chunks_gen(session, client_id, part, gap_start, gap_end):
                    df_filtered = df_chunk[
                        (df_chunk["created_at"] >= gap_start) &
                        (df_chunk["created_at"] <= gap_end)
                    ]
                    if df_filtered.empty:
                        continue

                    result = await run_cpu_task(
                        datatransformation_for_chartjs_detailed_cpu,
                        df_filtered, start_dt, end_dt, client_id, year, month, day, hour,
                        minutes, seconds, year_end, month_end, day_end, hour_end,
                        minutes_end, seconds_end, frequency, table_name, topic
                    )

                    (
                        final_transformed_data,
                        data_for_final_transformation_copy,
                        timestamp,
                        start_end_date_byfrequency,
                        usernumber,
                        querry_on_average,
                        changesinusernumber,
                        locations
                    ) = result

                    all_final_data.extend(final_transformed_data)
                    all_data_for_final_copy.extend(data_for_final_transformation_copy)
                    all_timestamps.extend(timestamp)
                    all_start_end.extend(start_end_date_byfrequency)
                    all_usernumbers.extend(usernumber)
                    all_querry_avg.extend(querry_on_average)
                    all_changes_usernum.extend(changesinusernumber)
                    all_locations.extend(locations)

                    await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)

    # ----------------------------------------
    # FETCH PREVIOUS PERIOD — ONLY USER_ID COUNTS
    # ----------------------------------------
   
    prev_start, prev_end = get_previous_period(start_dt, frequency)
    prev_usercount = 0
    collected_user_ids = set()
    covered_prev = []

    async def extract_user_ids(df_chunk):
        """Helper to safely extract unique user_ids from a dataframe chunk."""
        if "user_id" not in df_chunk.columns:
            return set()
        return set(df_chunk["user_id"].dropna().unique())

    # ---- (1) Try from Redis first
    async for df_chunk, (chunk_start, chunk_end) in stream_chunks_from_redis(client_id, prev_start, prev_end, redis):
        user_ids = await run_cpu_task(extract_user_ids, df_chunk)
        collected_user_ids |= user_ids
        covered_prev.append((chunk_start, chunk_end))

    # ---- (2) If some gaps exist, fill from blobs and DB
    missing_prev = find_gaps_in_range(prev_start, prev_end, covered_prev)

    if missing_prev:
        async with async_session_scope() as session:
            for gap_start, gap_end in missing_prev:

                # BLOBS
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

                        user_ids = await run_cpu_task(extract_user_ids, df_filtered)
                        collected_user_ids |= user_ids
                        await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)

                # DB PARTITIONS
                partitions = await fetch_partitions_for_table_chunks(session, table_name, gap_start, gap_end)
                for part in partitions:
                    async for df_chunk in fetch_partition_rows_in_chunks_gen(session, client_id, part, gap_start, gap_end):
                        df_filtered = df_chunk[
                            (df_chunk["created_at"] >= gap_start) &
                            (df_chunk["created_at"] <= gap_end)
                        ]
                        if df_filtered.empty:
                            continue

                        user_ids = await run_cpu_task(extract_user_ids, df_filtered)
                        collected_user_ids |= user_ids
                        await store_chunk_in_redis(client_id, gap_start, gap_end, df_filtered, redis)

    # ---- (3) Compute total previous user count
    prev_usercount = len(collected_user_ids)

    # ---- (4) Compute percentage change for the first current period
    if all_usernumbers:
        first_current = all_usernumbers[0]
        last_previous = prev_usercount  # 0 if nothing found

        if last_previous == 0:
            pct_change = 0
        else:
            pct_change = ((first_current - last_previous) / last_previous) * 100

        all_changes_usernum[0] = f"{pct_change:.1f}%"


    # ----------------------------------------
    # FINAL MERGE OF ALL LISTS
    # ----------------------------------------
    return _merge_detailed_results(all_final_data,
                                   all_data_for_final_copy,
                                   all_timestamps,
                                   all_start_end,
                                   all_usernumbers,
                                   all_querry_avg,
                                   all_changes_usernum,
                                   all_locations,
                                   frequency)


# Helper to merge lists in parallel
def _merge_detailed_results(
    all_final_data,
    all_data_for_final_copy,
    all_timestamps,
    all_start_end,
    all_usernumbers,
    all_querry_avg,
    all_changes_usernum,
    all_locations,
    frequency
):
    def merge_main():
        return reduce(lambda acc, lst: merge_two_lists(acc, lst, breakdown=frequency), deepcopy(all_final_data))

    final_merged = merge_main()
    return (
        final_merged,
        all_data_for_final_copy,
        all_timestamps,
        all_start_end,
        all_usernumbers,
        all_querry_avg,
        all_changes_usernum,
        all_locations,
    )



def datatransformation_for_chartjs_detailed_cpu(df_pandas, start_dt, end_dt, client_id, year, month, day, hour, minutes, seconds, year_end, month_end, day_end, hour_end, minutes_end, seconds_end,frequency, table_name, topic):
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

  # Apply the replacement to the 'topic' column
  #df_pandas['topic'] = df_pandas['topic'].apply(replace_curly_braces)
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

  #Drop columns if needed
  if 'id' in df_pandas.columns:
      df_pandas.drop(columns=['id'], inplace=True)



  from_ = start_dt
  to_ = end_dt
 
  df_pandas = df_pandas.sort_index()
  # df_pandas=df_pandas.loc[from_: to_]
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
    df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)

    # Find the start and end dates of the data
    start_date = df_pandas.index.min().date()  # Extract the date component
    end_date = df_pandas.index.max().date()    # Extract the date component
     

    # Define a function to get the start and end dates of a week
    def get_week_boundaries(current_date):
        start_of_week = current_date - timedelta(days=current_date.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        return start_of_week, end_of_week
    
    #add previous start and end date of the previous week of the period
  
    days_to_subtract = start_date.weekday()
    previous_monday = start_date - timedelta(days=days_to_subtract)

    last_week_previous_monday = previous_monday - timedelta(days=7)
    start_lastweek=start_date-timedelta(days=7)
    sunday_of_week = last_week_previous_monday + timedelta(days=6)
    week_start, week_end = get_week_boundaries(from_.date())
    
    if to_.date() > week_end:
        start_end_date_byfrequency.append([
            datetime.combine(start_lastweek, time(from_.hour, from_.minute, from_.second)).replace(tzinfo=utc).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.combine(sunday_of_week, time(23, 59, 59)).replace(tzinfo=utc).strftime('%Y-%m-%d %H:%M:%S')
        ])
    else:
        start_end_date_byfrequency.append([
            datetime.combine(start_lastweek, time(from_.hour, from_.minute, from_.second)).replace(tzinfo=utc).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.combine(sunday_of_week, time(to_.hour, to_.minute, to_.second)).replace(tzinfo=utc).strftime('%Y-%m-%d %H:%M:%S')
        ])


    # Iterate over weeks from start to end

     # Initialize a list to store sub DataFrames
    current_date = from_.date()
    last_day = to_.date()
    sub_dataframes = []
    while current_date <= last_day:
        week_start, week_end = get_week_boundaries(current_date)
        week_start_dt = utc.localize(datetime.combine(week_start, time(0, 0, 0)))
        week_end_dt = utc.localize(datetime.combine(week_end, time(23, 59, 59)))

        if current_date == from_.date() and last_day > week_end:
            start_end_date_byfrequency.append([
                from_.strftime('%Y-%m-%d %H:%M:%S'),
                week_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= from_) & (df_pandas.index <= week_end_dt)]
            if sub_df.empty:
                fallback_index = from_
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[fallback_index])
                sub_df._is_special_df = True

        elif last_day <= week_end and current_date != from_.date():
            to_dt_trimmed = to_.replace(hour=int(hour_end), minute=int(minutes_end), second=int(seconds_end))
            start_end_date_byfrequency.append([
                week_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                to_dt_trimmed.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= week_start_dt) & (df_pandas.index <= to_dt_trimmed)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[week_start_dt])
                sub_df._is_special_df = True

        #When from and to falls between week start and week end date:
        elif to_ < week_end_dt and from_ > week_start_dt:
            start_end_date_byfrequency.append([
                from_.strftime('%Y-%m-%d %H:%M:%S'),
                to_.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= from_) & (df_pandas.index <= to_)]
            if sub_df.empty:
                fallback_index = from_
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[fallback_index])
                sub_df._is_special_df = True

        else:
            start_end_date_byfrequency.append([
                week_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                week_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= week_start_dt) & (df_pandas.index <= week_end_dt)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[week_start_dt])
                sub_df._is_special_df = True

        sub_dataframes.append(sub_df)
        current_date = pd.Timestamp(week_end_dt + timedelta(seconds=1)).date()



    # Now sub_dataframes contains sub DataFrames grouped by week
    for i in sub_dataframes:
      timestamp.append(i.index[-1].strftime('%Y-%m-%d %H:%M:%S'))
 

 
  ##############
  #   DAILY    #
  ##############

 
  if breakdown == "daily":
    df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)

    start_date = df_pandas.index.min().date()
    end_date = df_pandas.index.max().date()
    prev_day = start_date - timedelta(days=1)

    # Assuming from_ and to_ are aware datetimes already, otherwise parse and localize here once
    start_datetime = datetime.combine(from_.date() - timedelta(days=1), time(int(hour), int(minutes), int(seconds))).replace(tzinfo=utc)

    if to_ < datetime.combine(prev_day, time(23, 59, 59)).replace(tzinfo=utc):
        end_datetime = datetime.combine(prev_day, time(int(hour_end), int(minutes_end), int(seconds_end))).replace(tzinfo=utc)
    else:
        end_datetime = datetime.combine(prev_day, time(23, 59, 59)).replace(tzinfo=utc)

    start_end_date_byfrequency.append([
        start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    ])

    sub_dataframes = []

    current_date = from_.date()
    last_day = to_.date()

    while current_date <= last_day:
        day_start_dt = datetime.combine(current_date, time(0, 0, 0)).replace(tzinfo=utc)
        day_end_dt = datetime.combine(current_date, time(23, 59, 59)).replace(tzinfo=utc)

        if current_date == from_.date() and to_ <= day_end_dt:
            start_end_date_byfrequency.append([
                from_.strftime('%Y-%m-%d %H:%M:%S'),
                to_.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= from_) & (df_pandas.index <= to_)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[from_])
                sub_df._is_special_df = True

        elif current_date == from_.date() and to_ > day_end_dt:
            start_end_date_byfrequency.append([
                from_.strftime('%Y-%m-%d %H:%M:%S'),
                day_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= from_) & (df_pandas.index <= day_end_dt)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[from_])
                sub_df._is_special_df = True

        elif to_ <= day_end_dt and current_date != from_.date():
            sub_end = datetime.combine(current_date, time(to_.hour, to_.minute, to_.second)).replace(tzinfo=utc)
            start_end_date_byfrequency.append([
                day_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                sub_end.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= day_start_dt) & (df_pandas.index <= sub_end)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[day_start_dt])
                sub_df._is_special_df = True

        else:
            start_end_date_byfrequency.append([
                day_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                day_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= day_start_dt) & (df_pandas.index <= day_end_dt)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[day_start_dt])
                sub_df._is_special_df = True

        sub_dataframes.append(sub_df)
        current_date += timedelta(days=1)

    for df in sub_dataframes:
        timestamp.append(df.index[-1].strftime('%Y-%m-%d %H:%M:%S'))


  ##############
  #   YEARLY   #
  ##############

  if breakdown == "yearly":
    start_datetime = datetime.combine(
        (from_.date() - timedelta(days=365)),
        time(int(hour), int(minutes), int(seconds))
    ).replace(tzinfo=utc)

    year_current = from_.year
    yearend_current = datetime(year_current, 12, 31, 23, 59, 59, tzinfo=utc)

    if to_ < yearend_current:
        end_datetime = datetime.combine(
            start_datetime.date(),
            time(int(hour_end), int(minutes_end), int(seconds_end))
        ).replace(tzinfo=utc)
    else:
        end_datetime = datetime(year_current, 12, 31, 23, 59, 59, tzinfo=utc)

    start_end_date_byfrequency.append([
        start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    ])

    sub_dataframes = []
    from_year = from_.year
    last_year = to_.year
    current_year = from_year

    while current_year <= last_year:
        year_start_dt = datetime(current_year, 1, 1, 0, 0, 0, tzinfo=utc)
        year_end_dt = datetime(current_year, 12, 31, 23, 59, 59, tzinfo=utc)

        if current_year == from_year and to_ <= year_end_dt:
            # from_ and to_ within same year
            start_end_date_byfrequency.append([
                from_.strftime('%Y-%m-%d %H:%M:%S'),
                to_.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= from_) & (df_pandas.index <= to_)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[from_])
                sub_df._is_special_df = True

        elif current_year == from_year and to_ > year_end_dt:
            # first year partial
            start_end_date_byfrequency.append([
                from_.strftime('%Y-%m-%d %H:%M:%S'),
                year_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= from_) & (df_pandas.index <= year_end_dt)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[from_])
                sub_df._is_special_df = True

        elif current_year == last_year:
            # last year partial
            start_end_date_byfrequency.append([
                year_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                to_.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= year_start_dt) & (df_pandas.index <= to_)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[year_start_dt])
                sub_df._is_special_df = True

        else:
            # full year
            start_end_date_byfrequency.append([
                year_start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                year_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            sub_df = df_pandas[(df_pandas.index >= year_start_dt) & (df_pandas.index <= year_end_dt)]
            if sub_df.empty:
                sub_df = pd.DataFrame({
                    'user_id': '', 'message': '', 'topic': [[]],
                    'latitude': '', 'longitude': '', 'location': ''
                }, index=[year_start_dt])
                sub_df._is_special_df = True

        sub_dataframes.append(sub_df)
        current_year += 1

    for df in sub_dataframes:
        timestamp.append(df.index[-1].strftime('%Y-%m-%d %H:%M:%S'))


  ###############
  #   MONTHLY   #
  ###############

  if breakdown == "monthly":
      df_pandas.index = pd.to_datetime(df_pandas.index, utc=True)

      # ---------------------------------------------------------------------------
      # PREVIOUS MONTH
      from_date = from_
      print("++!+!+!+!+!+!")
      from_date_year = from_date.date().year
      from_date_month = from_date.date().month

      to_date = to_
      to_date_day = to_date.date().day

      from_date_previous_month = (from_date - relativedelta(months=1))
      from_date_year_previous = from_date_previous_month.date().year
      from_date_month_previous = from_date_previous_month.date().month

      def last_day_of_month(year, month):
          last_day = calendar.monthrange(year, month)[1]
          return datetime(year, month, last_day, 23, 59, 59, tzinfo=utc)

      last_day_of_month_ = last_day_of_month(from_date_year, from_date_month)
      last_day_of_month_previous = last_day_of_month(from_date_year_previous, from_date_month_previous)

      date_ofLastday_of_month = last_day_of_month_

      if to_date < date_ofLastday_of_month:
          datetime_str = f"{from_date_year_previous}-{from_date_month_previous}-{to_date_day} {hour_end}:{minutes_end}:{seconds_end}"
          end_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
      else:
          end_datetime = last_day_of_month_previous

      start_end_date_byfrequency.append([
          from_date_previous_month.strftime('%Y-%m-%d %H:%M:%S'),
          end_datetime.strftime('%Y-%m-%d %H:%M:%S')
      ])

      from_date00 = pd.to_datetime(start_end_date_byfrequency[0][0]).tz_localize(utc) if pd.to_datetime(start_end_date_byfrequency[0][0]).tzinfo is None else pd.to_datetime(start_end_date_byfrequency[0][0])
      to_date00 = pd.to_datetime(start_end_date_byfrequency[0][1]).tz_localize(utc) if pd.to_datetime(start_end_date_byfrequency[0][1]).tzinfo is None else pd.to_datetime(start_end_date_byfrequency[0][1])
      sub_df00 = df_pandas[(df_pandas.index >= from_date00) & (df_pandas.index <= to_date00)]

      sub_dataframes = []

      current_date = pd.Timestamp(from_date.year, from_date.month, 1, tz=utc)
      end_date_timestamp = pd.Timestamp(to_date.year, to_date.month, 1, tz=utc)

      while current_date <= end_date_timestamp:
          start_of_month = pd.Timestamp(current_date.year, current_date.month, 1, tz=utc)
          end_of_month = (start_of_month + pd.offsets.MonthEnd(0)).replace(tzinfo=utc)
          end_of_month_with_time = end_of_month.replace(hour=23, minute=59, second=59)

          if current_date.year == from_date.year and current_date.month == from_date.month and end_of_month_with_time > to_date:
              start_end_date_byfrequency.append([
                  from_date.strftime('%Y-%m-%d %H:%M:%S'),
                  to_date.strftime('%Y-%m-%d %H:%M:%S')
              ])
              sub_df = df_pandas[(df_pandas.index >= from_date) & (df_pandas.index <= to_date)]
              if sub_df.empty:
                  sub_df = pd.DataFrame({
                      'user_id': '', 'message': '', 'topic': [[]],
                      'latitude': '', 'longitude': '', 'location': ''
                  }, index=[from_date])
                  sub_df._is_special_df = True

          elif current_date.year == from_date.year and current_date.month == from_date.month and end_of_month_with_time < to_date:
              start_end_date_byfrequency.append([
                  from_date.strftime('%Y-%m-%d %H:%M:%S'),
                  end_of_month.strftime('%Y-%m-%d %H:%M:%S')
              ])
              sub_df = df_pandas[(df_pandas.index >= from_date) & (df_pandas.index <= end_of_month_with_time)]
              if sub_df.empty:
                  start_date = from_date
                  sub_df = pd.DataFrame({
                      'user_id': '', 'message': '', 'topic': [[]],
                      'latitude': '', 'longitude': '', 'location': ''
                  }, index=[start_date])
                  sub_df._is_special_df = True

          elif current_date.month != from_date.month and end_of_month_with_time > to_date:
              start_end_date_byfrequency.append([
                  start_of_month.strftime('%Y-%m-%d %H:%M:%S'),
                  to_date.strftime('%Y-%m-%d %H:%M:%S')
              ])
              sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= to_date)]
              if sub_df.empty:
                  sub_df = pd.DataFrame({
                      'user_id': '', 'message': '', 'topic': [[]],
                      'latitude': '', 'longitude': '', 'location': ''
                  }, index=[start_of_month])
                  sub_df._is_special_df = True

          else:
              start_end_date_byfrequency.append([
                  start_of_month.strftime('%Y-%m-%d %H:%M:%S'),
                  end_of_month_with_time.strftime('%Y-%m-%d %H:%M:%S')
              ])
              sub_df = df_pandas[(df_pandas.index >= start_of_month) & (df_pandas.index <= end_of_month_with_time)]
              if sub_df.empty:
                  sub_df = pd.DataFrame({
                      'user_id': '', 'message': '', 'topic': [[]],
                      'latitude': '', 'longitude': '', 'location': ''
                  }, index=[start_of_month])
                  sub_df._is_special_df = True

          sub_dataframes.append(sub_df)
          current_date = end_of_month + pd.Timedelta(days=1)

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
      from_date00 = datetime.strptime(start_end_date_byfrequency[0][0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
      to_date00 = datetime.strptime(start_end_date_byfrequency[0][1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc)
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
  if len(data_to_transform)==1:
    if len(data_to_transform)==1 and len(data_to_transform[0])==0:
        product_data="There was no chat activity in this period"
        final_transformed_data.append(product_data)
    else:
      for item in data_to_transform:
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
          product_data['x_secondary_b'] = ["**Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
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
          product_data['x_secondary_b'] = ["**Gyártóról nem folyt beszélgetés"]
          product_data['secondaryChartData_b'] = [secondaryChartData_b]
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
          product_data['secondaryChartData_c'] = [secondaryChartData_c]
        if len(item)==3:
          secondaryChartData_c = [list(entry.values())[0] for entry in item[0]]
          product_data['label_c'] = c[1]
          product_data['x_secondary_c'] = ["**Márkatípusról nem folyt beszélgetés"]
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
  


