import argparse
import datetime

from .smax_redis_client import SmaxRedisClient, _TYPE_MAP

desc = """
A simple Python command line utility to share or push SMA-X values.
"""

default_server = "localhost"
default_port = 6379
default_db = 0

def print_tree(d, verbose):
    for k, i in d.items():
        if type(i) is dict:
            print(k)
            print_tree(i, verbose)
        else:
            print_smax(i, verbose)
            
def print_smax(smax_value, verbose):
    if verbose:
        print(f"SMA-X value {smax_value.origin}:")
        print(f"    data   : {smax_value.data}")
        print(f"    type   : {smax_value.type}")
        print(f"    dim    : {smax_value.dim}")
        print(f"    date   : {datetime.datetime.utcfromtimestamp(smax_value.date)}")
        print(f"    source : {smax_value.source}")
        print(f"    seq    : {smax_value.seq}")
    else:
        print(f"SMA-X value {smax_value.origin} : {smax_value.data}")
    

def main():
    parser = argparse.ArgumentParser(description=desc)
    
    parser.add_argument("--redis_ip", "-i", help="Host name or IP of SMA-X Redis host", default=default_server)
    parser.add_argument("--port", "-p", help="Port number of the SMA-X Redis server", default=default_port)
    parser.add_argument("--db", "-d", help="Database number of the SMA-X Redis database", default=default_db)
    
    parser.add_argument("--table", "-t", help="SMA-X table to address", required=True)
    parser.add_argument("--key", "-k", help="SMA-X key to address", required=True)
    
    parser.add_argument("--type", "-T", help="Type for the value to be set.", choices=list(_TYPE_MAP.keys()))
    
    parser.add_argument("--set", "-s", help="SMA-X value to set", default=None)
    
    parser.add_argument("--verbose", "-v", action='store_true', help="Show all SMA-X metadata associated with the key", default=False)
    
    args = parser.parse_args()
    
    with SmaxRedisClient(redis_ip=args.redis_ip, redis_port=args.port, redis_db=args.db, program_name="smax-cli") as smax_client:

        # If set is set, set the value
        if args.set is not None:
            if args.type is None:
                # Try to convert set vale to the current type of the SMA-X variable
                smax_type = smax_client.smax_pull(args.table, args.key).type
            else:
                smax_type = _TYPE_MAP[args.type]
            try:
                    val = smax_type(args.set)
            except ValueError:
                val = args.set
            smax_client.smax_share(args.table, args.key, val)
        # Set not given, so return value
        else:
            result = smax_client.smax_pull(args.table, args.key)
            
            if type(result) is dict:
                # We have a struct of SMA-X values and need to walk through the values
                print_tree(result, args.verbose)
            else:
                print_smax(result, args.verbose)
        smax_client.smax_disconnect()
        
if __name__ == "__main__":
    main()