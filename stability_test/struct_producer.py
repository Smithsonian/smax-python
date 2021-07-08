from smax import SmaxRedisClient
import time
import os
import psutil
import csv
import logging

logger = logging.getLogger("smax")
logger.setLevel(logging.DEBUG)

smax_client = SmaxRedisClient("localhost")

csv_fields = ['Index', 'Memory_MB', 'Timing']
csv_filename = 'struct_producer_python.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(csv_fields)


for i in range(10):
    swarm_dict = {}
    for j in range(6):
        for k in range(8):
            roach2_name = f"roach2-{j}{k + 1}"
            if roach2_name not in swarm_dict:
                swarm_dict[roach2_name] = {}
            swarm_dict[roach2_name]["temp"] = i
            swarm_dict[roach2_name]["firmware"] = str(i)
            swarm_dict[roach2_name]["bengine-gains"] = [float(i)] * 3
            swarm_dict[roach2_name]["glitch"] = [[i]*3 for blah in range(3)]

    start = time.process_time()
    smax_client.smax_share("test", "swarm", swarm_dict)
    end = time.process_time() - start
    memory_MB = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2

    with open(csv_filename, 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([i, memory_MB, end])

smax_client.smax_disconnect()
