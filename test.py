import redis

# Try localhost first
r = redis.Redis(host="127.0.0.1", port=6379)
print("PING localhost:", r.ping())