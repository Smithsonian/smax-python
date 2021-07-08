from smax import SmaxRedisClient
import logging
import sys
import csv
# csv_fields = ['Roach2', 'Temp', 'Firmware', 'Bengine_gains', 'Glitch', 'MemoryMB', 'Timing']
#
# with open('eggs.csv', 'w', newline='') as csvfile:
#     csvwriter = csv.writer(csvfile)
#     csvwriter.writerow(fields)
#     csvwriter.writerows(rows)

logging.getLogger("smax").setLevel(logging.DEBUG)


# Initialize smax client.
smax_client = SmaxRedisClient("localhost")

# Subscribe to a struct using a wildcard '*' at the end of the table name.
smax_client.smax_subscribe("test:swarm*")

i = 0
while True:
    print(i)
    result = smax_client.smax_wait_on_subscribed("test:swarm*")
    print(sys.getsizeof(result))
    i = i + 1
