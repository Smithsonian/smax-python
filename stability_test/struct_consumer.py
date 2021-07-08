from smax import SmaxRedisClient
import logging
import sys

logging.getLogger("smax").setLevel(logging.DEBUG)


# Initialize smax client.
smax_client = SmaxRedisClient("localhost")

# Subscribe to a struct using a wildcard '*' at the end of the table name.
smax_client.smax_subscribe("test:swarm*")

i = 0
while True:
    # print(i)
    result = smax_client.smax_wait_on_subscribed("test:swarm*")
    # print(result)
    i = i + 1
