from smax import SmaxRedisClient
import csv
import logging
import time
import psutil
import os

logging.getLogger("smax").setLevel(logging.DEBUG)

csv_fields = ['Index', 'Memory_MB', 'Timing']
csv_filename = 'simple_consumer_python.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(csv_fields)

# Initialize smax client.
smax_client = SmaxRedisClient("localhost")

# Subscribe to a struct using a wildcard '*' at the end of the table name.
smax_client.smax_subscribe("test:simple")

for i in range(10000):

    start = time.process_time()
    result = smax_client.smax_wait_on_any_subscribed()
    end = time.process_time() - start
    memory_MB = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
    with open(csv_filename, 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([i+1, memory_MB, end])
