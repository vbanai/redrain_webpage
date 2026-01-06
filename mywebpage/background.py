import asyncio
from datetime import datetime, timedelta
import aioredis
import json
from mywebpage.socketio_app import sio
from mywebpage.db import async_session_scope
from sqlalchemy import text, select, desc
from mywebpage.elephantsql import Client, ChatHistory, OrgEventLog
from azure.storage.blob.aio import BlobServiceClient
import os
from datetime import datetime, timezone
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import OperationalError  #catch db op error



INACTIVITY_TIMEOUT_SECONDS = 30
BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")



# Helpers





def normalize_timestamp(ts: str) -> str:
    """
    Convert any timestamp string into ISO8601 UTC format without microseconds:
    '2025-08-17 17:42:59' -> '2025-08-17T17:42:59Z'
    '2025-08-17T17:42:59.104581+00:00' -> '2025-08-17T17:42:59Z'
    """
    if not ts:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    
    # Try to parse ISO or space-separated datetime
    try:
        # Replace space with T if needed
        ts_fixed = ts.replace(' ', 'T') if ' ' in ts and 'T' not in ts else ts
        dt = datetime.fromisoformat(ts_fixed.replace('Z', '+00:00'))
        # Force UTC and remove microseconds
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt.isoformat().replace('+00:00', 'Z')
    except Exception as e:
        # Fallback to current UTC if parsing fails
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')





async def classify_topic(input_text, fastapi_app):

     # WAIT until model is loaded
    await fastapi_app.state.models_loaded_event.wait()


    minilm_model = fastapi_app.state.minilm_model_encoder_for_clf_classifier
    lr_classifier = fastapi_app.state.lr_classifier

    if minilm_model is None or lr_classifier is None:
        raise RuntimeError("Models not available")

    label_reverse = {
    0: "Termékérdeklődés",
    1: "Vásárlási szándék",
    2: "Ár és promóció",
    3: "Panaszok és problémák",
    4: "Szolgáltatás",
    5: "Egyéb"
    }
    emb = await asyncio.to_thread(minilm_model.encode, [input_text])
    pred = await asyncio.to_thread(lr_classifier.predict, [emb[0]])
    return label_reverse[pred]





async def classify_and_save(payload, fastapi_app):
    """
    Wait for chatbot DB insert, then classify topic and update topic_classification field.
    """
    try:
        message = payload["message"]
        user_id = message["user_id"]
        org_id = message["org_id"]
        standalone_prediction = message["standalone_prediction"]
        user_message = message["user_message"]
        non_standalone_input = message["input_for_not_standalone_topic_classification"]
        bot_message = message["bot_message"]
        saved_flag_key = message.get("saved_flag_key")
        created_at_str = message["timestamp"]

        #we are converting the time str got from redis it back into a datetime
        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))

        # --- Wait until chatbot side finishes DB insert ---
        if saved_flag_key:
            print(f"Waiting for Redis save flag: {saved_flag_key}")
            for _ in range(60):  # wait up to 30 seconds
                exists = await fastapi_app.state.redis_client.exists(saved_flag_key)
                if exists:
                    print(f"Redis save flag detected: {saved_flag_key}")
                    break
                await asyncio.sleep(0.5)
            else:
                print(f"Timeout waiting for DB save flag for {saved_flag_key}")
                return
        else:
            print("No saved_flag_key provided in payload; proceeding anyway.")

        # --- Choose input for topic classification ---
        classification_input = (
            user_message if standalone_prediction == 0 else non_standalone_input
        )

        # --- Run model inference (non-blocking) ---
        try:
            topic = await classify_topic(classification_input, fastapi_app)
            print(f"Predicted topic: {topic}")
        except Exception as e:
            print("Topic classification failed")
            topic = "unknown"

        # --- Update the database record ---
        async with async_session_scope() as session:
            try:
                stmt = (
                    update(ChatHistory)
                    .where(
                        ChatHistory.user_id == user_id,
                        ChatHistory.client_id == org_id,
                        ChatHistory.message == user_message,
                        ChatHistory.response == bot_message,
                        ChatHistory.created_at.between(created_at - timedelta(seconds=1), created_at + timedelta(seconds=1))
                    )
                    .values(topic_classification=topic)
                    .execution_options(synchronize_session="fetch")
                )
                result = await session.execute(stmt)
              
              

                if result.rowcount == 0:
                    print(
                        f"No chat record found for "
                        f"user_id={user_id}, org_id={org_id}, message={user_message[:50]}"
                    )
                else:
                    print(f"Topic classification saved for {result.rowcount} record(s).")

            except SQLAlchemyError as e:
                await session.rollback()
                print(f"Database update failed: {e}")
                raise

    except Exception as e:
        print(f"classify_and_save() failed: {e}")




async def fetch_last_msgs_pipeline(org_id: int, redis_client: aioredis.Redis, batch_size: int = 10000):
    """
    Fetch last 4 messages per user for a tenant (org_id) from DB + blobs.
    Update Redis incrementally in batches, stop early for users who already have 4 messages.
    """

    user_msgs = {}  # in-memory per-user message collection

    # --- Step 1: Fetch from DB ---
    async with async_session_scope(org_id=org_id) as session:
        stmt = (
            select(ChatHistory)
            .where(ChatHistory.client_id == org_id)
            .order_by(desc(ChatHistory.created_at))
        )
        stream = await session.stream(stmt)
        count = 0

        async for row in stream:
            msg: ChatHistory = row[0]
            uid = msg.user_id

            if uid not in user_msgs:
                user_msgs[uid] = []

            if len(user_msgs[uid]) < 4:
                user_msgs[uid].append({
                    "timestamp": msg.created_at.isoformat(),
                    "user_message": msg.message,
                    "bot_message": msg.response,
                    "agent": msg.agent or "bot",       # 'bot' for automatic, admin name for manual
                    "mode": msg.mode or "automatic"
                })

            count += 1
            if count % batch_size == 0:
                await _update_redis_batch(org_id, user_msgs, redis_client)
                # Keep only users who still need more messages
                user_msgs = {uid: msgs for uid, msgs in user_msgs.items() if len(msgs) < 4}

            # Stop early if all users have 4 messages
            if user_msgs and all(len(v) >= 4 for v in user_msgs.values()):
                break

        if user_msgs:
            await _update_redis_batch(org_id, user_msgs, redis_client)

    # --- Step 2: Fetch from Blobs ---

    async with BlobServiceClient.from_connection_string(BLOB_CONN_STR) as blob_service:
        container_client = blob_service.get_container_client(CONTAINER_NAME)

        async for blob in container_client.list_blobs(name_starts_with="chat_messages_"):
            blob_client = container_client.get_blob_client(blob)
            stream = await blob_client.download_blob()
            data = await stream.readall()
            lines = [json.loads(line) for line in data.decode("utf-8").splitlines()]

            await _process_blob_lines_incremental_early(lines, org_id, redis_client, batch_size)


async def _update_redis_batch(org_id: int, user_msgs: dict, redis_client: aioredis.Redis):
    """
    Write batch of user messages to Redis, merge with existing data, keep latest 4.
    """
    for uid, msgs in user_msgs.items():
        if not msgs:
            continue

        key = f"tenant:{org_id}:user:{uid}:recent_msgs"
        existing_raw = await redis_client.lrange(key, 0, 3)
        existing_msgs = [json.loads(m) for m in existing_raw]

        # Merge DB messages with existing Redis messages
        merged = msgs + existing_msgs
        merged.sort(key=lambda m: m["timestamp"], reverse=True)
        merged = merged[:4]

        pipe = redis_client.pipeline()
        pipe.delete(key)
        for m in merged:
            pipe.rpush(key, json.dumps(m))
        await pipe.execute()


async def _process_blob_lines_incremental_early(lines, org_id: int, redis_client: aioredis.Redis, batch_size: int):
    """
    Process blob batch with early stopping for users who already have 4 messages in Redis.
    """
    lines_buffer = []

    for line in reversed(lines):  # newest first
        lines_buffer.append(line)
        if len(lines_buffer) >= batch_size:
            await _process_blob_lines_buffer_early(lines_buffer, org_id, redis_client)
            lines_buffer = []

    if lines_buffer:
        await _process_blob_lines_buffer_early(lines_buffer, org_id, redis_client)


async def _process_blob_lines_buffer_early(lines, org_id: int, redis_client: aioredis.Redis):
    """
    Merge blob messages with Redis, skip users who already have 4 messages.
    """
    batch_user_msgs = {}

    # Collect messages per user
    for msg in lines:
        if msg["client_id"] != org_id:
            continue
        uid = msg["user_id"]

        # Check existing Redis messages first
        key = f"tenant:{org_id}:user:{uid}:recent_msgs"
        existing_raw = await redis_client.lrange(key, 0, 3)
        existing_msgs = [json.loads(m) for m in existing_raw]

        # Skip users who already have 4 messages
        if len(existing_msgs) >= 4:
            continue

        if uid not in batch_user_msgs:
            batch_user_msgs[uid] = []

        batch_user_msgs[uid].append({
            "timestamp": msg["created_at"],
            "user_message": msg["message"],
            "bot_message": msg["response"],
            "agent": msg.get("agent", "bot"),
            "mode": msg.get("mode", "automatic")
        })

    # Merge and save back
    for uid, new_msgs in batch_user_msgs.items():
        key = f"tenant:{org_id}:user:{uid}:recent_msgs"
        existing_raw = await redis_client.lrange(key, 0, 3)
        existing_msgs = [json.loads(m) for m in existing_raw]

        merged = new_msgs + existing_msgs
        merged.sort(key=lambda m: m["timestamp"], reverse=True)
        merged = merged[:4]

        pipe = redis_client.pipeline()
        pipe.delete(key)
        for m in merged:
            pipe.rpush(key, json.dumps(m))
        await pipe.execute()



async def log_event(org_id, event_type, data, frontend_time=None):
    try:
        # Determine timestamp (frontend or fallback)
        timestamp = frontend_time or data.get("message", {}).get("timestamp")
        if timestamp:
            # Handle ISO 8601 with 'Z' (UTC) properly
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        else:
            timestamp = datetime.now(timezone.utc)

        async with async_session_scope() as session:
            event = OrgEventLog(
                org_id=org_id,
                event_type=event_type,
                data=data,
                timestamp=timestamp,
            )
            session.add(event)

            # Flush writes pending INSERT, commit persists permanently
          

            return {
                "org_id": event.org_id,
                "event_type": event.event_type,
                "data": event.data,
                "timestamp": event.timestamp,
            }

    except Exception as e:
        # Log the exception with traceback if needed
        print(f"Error logging event for org {org_id}: {e}")
        return None


async def get_client_mode(org_id: str) -> str:
    """
    Retrieve the mode for a specific organization from the clients table.
    If the organization is not found, return 'automatic' as the default mode.
    """
    async with async_session_scope() as db_session:
        try:
            result = await db_session.execute(
                select(Client.mode).where(Client.id == org_id)
            )
            mode = result.scalar_one_or_none()

            if mode:
                print(f"Retrieved mode for org_id={org_id}: {mode}")
                return mode
            else:
                print(f"No mode found for org_id={org_id}. Defaulting to 'automatic'.")
                return "automatic"

        except Exception as e:
            print(f"Error retrieving client mode: {e}")
            raise





























async def redis_listener(fastapi_app):
    redis_client = fastapi_app.state.redis_client
    pubsub = None
    while True:  # Outer loop -> reconnects after inactivity or errors
        
        try:
            pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe("chatbot:messages")
            print("Chatbot subscribed to Redis channel 'chatbot:messages'")

            last_activity = datetime.utcnow()

            while True:  # Inner loop -> process messages
                now = datetime.utcnow()

                # inactivity check
                if (now - last_activity) > timedelta(seconds=INACTIVITY_TIMEOUT_SECONDS):
                    print(f"[WARNING] No message or heartbeat in {INACTIVITY_TIMEOUT_SECONDS} seconds.")
                    break  # break inner loop → reconnect in outer loop

                # Poll for message (non-blocking with timeout)
                message = await pubsub.get_message(timeout=1.0)

                if message is None:
                    await asyncio.sleep(0.1)  # small pause when idle
                    continue

                if message["type"] != "message":
                    continue

                try:
                    data = json.loads(message["data"])
                    last_activity = datetime.utcnow()  # reset inactivity timer

                    # Handle heartbeat
                    if data.get("type") in ("heartbeat", "admin_heartbeat"):
                        print(f"[Heartbeat] Received at {data.get('timestamp')}")
                        continue

                    print("MESSAGE FROM REDIS:", data)

                    org_id = str(data.get("message", {}).get("org_id"))
                    msg = data.get("message", {})
                    is_recurrent = msg.get("is_recurrent", {})

                    if is_recurrent:
                        # First-time setup for this tenant/user
                        await fetch_last_msgs_pipeline(org_id, redis_client)



                    user_id=msg.get("user_id")

                    if "timestamp" in msg:
                        msg["timestamp"] = normalize_timestamp(msg["timestamp"])

                    event = await log_event(org_id, "new_message", msg)
                    mode = await get_client_mode(org_id)

                    print("event!!!", event)

                    #  HANDLING RECURRENT USER LOGIC

                    user_cache_key = f"tenant:{org_id}:user:{user_id}:recent_msgs"

                    # --- Step 1: Get current history ---
                    # Fetch last 4 messages from Redis BEFORE adding the new one
                    recent_msgs_raw = await redis_client.lrange(user_cache_key, 0, 3)
                    recent_history = [json.loads(m) for m in recent_msgs_raw]
    

                    if event and "data" in event:
                        event["data"]["recent_history"] = recent_history

                    # Step 2: Push the new message for next round
                    new_msg = {
                        "timestamp": msg.get("timestamp"),
                        "user_message": msg.get("user_message"),
                        "bot_message": msg.get("bot_message"),
                    }
                    await redis_client.lpush(user_cache_key, json.dumps(new_msg))
                    await redis_client.ltrim(user_cache_key, 0, 3)

             

                    # === AUTOMATIC MODE ===
                    if mode == "automatic":
                        user_text = msg.get("user_message", "")
                        if user_text.strip().lower() in ["ügyintézőt kérek.", "please connect me to a colleague."]:
                            pass

                        
                        # -------- Emit message to all connected admins in the org --------

                        try:
                            org_key = f"org:{org_id}:connections"
                            sids_bytes = await redis_client.smembers(org_key)  # returns set of bytes
                            sids = [s.decode() for s in sids_bytes]
                            #sids = [row[0] for row in result.fetchall()]
                            for sid in sids:
                                try:
                                    await sio.emit(
                                        "new_message_FirstUser",
                                        {"messages": [event["data"]]
                                            },
                                        to=sid,
                                    )
                                except Exception as emit_err:
                                    print(f"Emit error to SID {sid}: {emit_err}")
                        except OperationalError as db_err:
                            print(f"[DB ERROR - Automatic] OperationalError: {db_err}")
                        except Exception as db_err:
                            print(f"[DB ERROR - Automatic] General DB error: {db_err}")

                        # --- Run classification & saving in background ---
                        asyncio.create_task(
                        classify_and_save(data, fastapi_app)
)
                    
                    # === MANUAL MODE ===
                    elif mode == "manual":
                        try:
                            async with async_session_scope() as session:
                                result = await session.execute(
                                    select(Client.last_manualmode_triggered_by).where(
                                        Client.id == org_id, Client.is_active.is_(True)
                                    )
                                )
                                admin_user_id = result.scalar()

                                if admin_user_id:
                        

                                    # Get all active sids for this org
                                    org_connections_key = f"org:{org_id}:connections"
                                    sids_bytes = await redis_client.smembers(org_connections_key)
                                    sids = [s.decode() for s in sids_bytes]

                                    # Find a connection that belongs to the admin_user_id
                                    admin_sid = None
                                    for sid in sids:
                                        conn_data = await redis_client.hgetall(f"connection:{sid}")
                                        if conn_data.get(b"user_id") and int(conn_data[b"user_id"]) == int(admin_user_id):
                                            admin_sid = sid
                                            break

                                    if admin_sid:
                                        await sio.emit(
                                            "new_message_FirstUser",
                                            {"messages": [event["data"]]
                                             },
                                            to=admin_sid,
                                        )
                                        print(f"Emitted to SID {admin_sid} for user {admin_user_id}")
                                    else:
                                        print(f"No active SID found for user_id {admin_user_id} in org {org_id}")
                                else:
                                    print(f"No admin user_id found for org_id {org_id}")
                        except OperationalError as e:
                            print(f"[DB ERROR - Manual] OperationalError: {e}")
                        except Exception as e:
                            print(f"[DB ERROR - Manual] General DB error: {e}")

                        # --- Run classification & saving in background ---
                        asyncio.create_task(
                        classify_and_save(data, fastapi_app)
                        )
                except Exception as e:
                    print(f"Error processing message from Redis: {e}")

        except Exception as outer_e:
            print(f"[Redis Listener] Connection lost, retrying in 5s... Error: {outer_e}")
            await asyncio.sleep(5)
        finally:
            try:
                if pubsub:
                    await pubsub.close()
            except:
                pass


async def send_admin_heartbeat(fastapi_app):
    redis_client = fastapi_app.state.redis_client
    while True:
        try:
            heartbeat_data = {
                "type": "admin_heartbeat",
                "timestamp": datetime.utcnow().isoformat()
            }
            await redis_client.publish("chatbot:admin_heartbeat", json.dumps(heartbeat_data))
            print(f"[Admin Heartbeat] Sent at {heartbeat_data['timestamp']}")
            await asyncio.sleep(10)  # normal interval
        except (aioredis.exceptions.ConnectionError, asyncio.TimeoutError) as e:
            # transient failure → retry sooner
            print(f"[Admin Heartbeat Transient Error] {e}, retrying in 2s")
            await asyncio.sleep(2)
        except Exception as e:
            # permanent / unknown failure → log and wait longer
            print(f"[Admin Heartbeat Fatal Error] {e}, retrying in 30s")
            await asyncio.sleep(30)



