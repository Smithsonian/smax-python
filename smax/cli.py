import argparse
import datetime

from .smax_redis_client import SmaxRedisClient

desc = """
A simple Python command line utility to share or push SMA-X values.
"""

default_server = "localhost"
default_port = 6379
default_db = 0

def main():
    parser = argparse.ArgumentParser(description=desc)
    
    parser.add_argument("--redis_ip", "-i", help="Host name or IP of SMA-X Redis host", default=default_server)
    parser.add_argument("--port", "-p", help="Port number of the SMA-X Redis server", default=default_port)
    parser.add_argument("--db", "-d", help="Database number of the SMA-X Redis database", default=default_db)
    
    parser.add_argument("--table", "-t", help="SMA-X table to address", required=True)
    parser.add_argument("--key", "-k", help="SMA-X key to address", required=True)
    
    parser.add_argument("--set", "-s", help="SMA-X value to set", default=None)
    
    parser.add_argument("--verbose", "-v", action='store_true', help="Show all SMA-X metadata associated with the key", default=False)
    
    args = parser.parse_args()
    
    with SmaxRedisClient(redis_ip=args.redis_ip, redis_port=args.port, redis_db=args.db, program_name="smax-cli") as smax_client:

        # If set is set, set the value
        if args.set is not None:
            # Try to convert set value to the current type of the SMA-X variable
            smax_type = smax_client.smax_pull(args.table, args.key).type
            try:
                val = smax_type(args.set)
            except ValueError:
                val = args.set
            smax_client.smax_share(args.table, args.key, val)
        # Set not given, so return value
        else:
            result = smax_client.smax_pull(args.table, args.key)
            
            if result is dict:
                # We have a struct of SMA-X values and need to walk through the values
                raise NotImplemented("We have not implemented pulling of structs in this CLI yet.")
            
            else:
                if args.verbose:
                    print(f"SMA-X value {args.table}:{args.key}:")
                    print(f"    data   : {result.data}")
                    print(f"    type   : {result.type}")
                    print(f"    dim    : {result.dim}")
                    print(f"    date   : {datetime.datetime.utcfromtimestamp(result.date)}")
                    print(f"    source : {result.source}")
                    print(f"    seq    : {result.seq}")
                    print(f"    origin : {result.origin}")
                else:
                    print(f"SMA-X value {args.table}:{args.key} : {result.data}")
                    
        smax_client.smax_disconnect()
        
if __name__ == "__main__":
    main()