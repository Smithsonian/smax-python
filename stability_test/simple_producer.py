from smax import SmaxRedisClient
import time
import os
import psutil
import csv

smax_client = SmaxRedisClient("localhost")

csv_fields = ['Index', 'Memory_MB', 'Timing']

with open('simple_producer_python.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(csv_fields)

for i in range(10000):
    start = time.process_time()
    smax_client.smax_share("test", "simple", [float(i)] * 3)
    end = time.process_time() - start
    memory_MB = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2

    with open('simple_producer_python.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([i, memory_MB, end])

