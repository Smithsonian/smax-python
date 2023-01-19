import argparse
import datetime

from .smax_redis_client import SmaxRedisClient, _TYPE_MAP, _REVERSE_TYPE_MAP, ConnectionError, TimeoutError

desc = """
A simple Python command line utility to share or push SMA-X values.
"""

default_server = "localhost"
default_port = 6379
default_db = 0

def print_tree(d, verbose):
    """Walk through a tree of SMA-X values, printing the leaf nodes"""
    for k, i in d.items():
        if type(i) is dict:
            print(k)
            print_tree(i, verbose)
        else:
            print_smax(i, verbose)
            
def print_smax(smax_value, verbose):
    """Print a SMA-X value"""
    print(f"SMA-X value {smax_value.smaxname}:")
    print(f"    data   :", smax_value.data)
    if verbose:
        print(f"    type   : {_REVERSE_TYPE_MAP[smax_value.type]}")
        print(f"    dim    : {smax_value.dim}")
        print(f"    date   : {datetime.datetime.utcfromtimestamp(smax_value.date)}")
        print(f"    origin : {smax_value.origin}")
        print(f"    seq    : {smax_value.seq}")
    

def main():
    parser = argparse.ArgumentParser(description=desc)
    
    parser.add_argument("--redis_ip", "-i", help="Host name or IP of SMA-X Redis host", default=default_server)
    parser.add_argument("--port", "-p", help="Port number of the SMA-X Redis server", default=default_port)
    parser.add_argument("--db", "-d", help="Database number of the SMA-X Redis database", default=default_db)

    parser.add_argument("--table", "-t", help="SMA-X table to address")
    parser.add_argument("--key", "-k", help="SMA-X key to address")
    
    parser.add_argument("--type", "-T", help="Type for the value to be set.", choices=list(_TYPE_MAP.keys()))
    
    parser.add_argument("--set", "-s", help="SMA-X value to set", default=None)
    
    parser.add_argument("--search", "-S", help="Search for SMA-X keys in Redis, and return matching keys", default=None)
    parser.add_argument("--list", action='store_true', default=False, help="List all SMA-X keys in the Redis database")
    
    parser.add_argument("--verbose", "-v", action='store_true', help="Show all SMA-X metadata associated with the key", default=False)
    
    args = parser.parse_args()
    
    with SmaxRedisClient(redis_ip=args.redis_ip, redis_port=args.port, redis_db=args.db, program_name="smax-cli") as smax_client:

        # If search is set, search for the string
        if args.search is not None:
            # Get all the keys matching the string
            search_str = f"*{args.search}*"
            keys = smax_client._client.execute_command(f"keys {search_str}")
            for k in keys:
                key = k.decode("UTF-8")
                if key[0] == "<" or key == "scripts":
                    continue
                else:
                    print(key)
        elif args.list:
            keys = smax_client._client.execute_command(f"keys *")
            for k in keys:
                key = k.decode("UTF-8")
                if key[0] == "<" or key == "scripts":
                    continue
                else:
                    print(key)
        # If set is set, set the value
        elif args.set is not None:
            if args.type is None:
                # Try to convert set value to the current type of the SMA-X variable
                try:
                    smax_type = smax_client.smax_pull(args.table, args.key).type
                except RuntimeError:
                    # Value isn't in Redis yet
                    print(f"Type not given and {args.table}:{args.key} not yet in Redis. Assuming string for type")
                    smax_type = str
                except (TimeoutError, ConnectionError):
                    print("Could not connect to Redis")
            else:
                smax_type = _TYPE_MAP[args.type]
            try:
                    val = smax_type(args.set)
            except ValueError:
                val = args.set
            smax_client.smax_share(args.table, args.key, val)
        # Set not given, so return value
        else:
            try:
                result = smax_client.smax_pull(args.table, args.key)
            except RuntimeError:
                result = None
            except (TimeoutError, ConnectionError):
                print("Could not connect to Redis")
        
            if result is None:
                print(f"Can't find {args.table}:{args.key} in Redis")
            else:
                if type(result) is dict:
                    # We have a struct of SMA-X values and need to walk through the values
                    print_tree(result, args.verbose)
                else:
                    print_smax(result, args.verbose)
        smax_client.smax_disconnect()
        
if __name__ == "__main__":
    main()