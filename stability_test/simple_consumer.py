from smax import SmaxRedisClient
import csv

# Initialize smax client.
smax_client = SmaxRedisClient("localhost")

# Subscribe to a struct using a wildcard '*' at the end of the table name.
smax_client.smax_subscribe("test:simple")

for i in range(100000):
    result = smax_client.smax_wait_on_any_subscribed()
    print(result)
