import argparse
import datetime

from .smax_redis_client import SmaxRedisClient, _TYPE_MAP, _REVERSE_TYPE_MAP, ConnectionError, TimeoutError

desc = """
A simple Python command line utility to share or push SMA-X values.
"""

default_server = "localhost"
default_port = 6379
default_db = 0

def print_tree(d, verbose, indent=0):
    """Walk through a tree of SMA-X values, printing the leaf nodes"""
    if indent != 0:
        indent_str = " "*(indent)
    else:
        indent_str = ""
    for k, i in d.items():
        if type(i) is dict:
            print(indent_str, k, sep="")
            print_tree(i, verbose, indent + 4)
        else:
            print_smax(i, verbose, indent)
            
def print_smax(smax_value, verbose, indent=0):
    """Print a SMA-X value"""
    if indent != 0:
        indent_str = " "*(indent)
    else:
        indent_str = ""
    if verbose:
        print(indent_str, f"SMA-X value {smax_value.smaxname} :", sep="")
        prefix = "    data   : "
    else:
        prefix = f"{smax_value.smaxname} : "
    if smax_value.type == str:
        if smax_value.dim == 1:
            print(indent_str, prefix, smax_value.data, sep="")
        else:
            for l in smax_value.data:
                print(indent_str, prefix, l, sep="")
                prefix = " "*len(prefix)
    else:
        for l in str(smax_value.data).splitlines():
            print(indent_str, prefix, l, sep="")
            prefix = " "*len(prefix)
    if verbose:
        print(indent_str, f"    type   : {_REVERSE_TYPE_MAP[smax_value.type]}", sep="")
        print(indent_str, f"    dim    : {smax_value.dim}", sep="")
        print(indent_str, f"    date   : {datetime.datetime.utcfromtimestamp(smax_value.date)}", sep="")
        print(indent_str, f"    origin : {smax_value.origin}", sep="")
        print(indent_str, f"    seq    : {smax_value.seq}", sep="")
    

def main():
    parser = argparse.ArgumentParser(description=desc)
    
    parser.add_argument("--redis_ip", "-i", help="Host name or IP of SMA-X Redis host", default=default_server)
    parser.add_argument("--port", "-p", help="Port number of the SMA-X Redis server", default=default_port)
    parser.add_argument("--db", "-d", help="Database number of the SMA-X Redis database", default=default_db)

    parser.add_argument("--table", "-t", help="SMA-X table to address")
    parser.add_argument("--key", "-k", help="SMA-X key to address")
    
    parser.add_argument("--type", "-T", help="Type for the value to be set.", choices=list(_TYPE_MAP.keys()), default=None)
    
    parser.add_argument("--set", "-s", help="SMA-X value to set", default=None)
    
    parser.add_argument("--search", "-S", 
                        help="Search a string in SMA-X keys in Redis, and return matching keys. Wildcards can be included within the string, and are automatically prepended and appended", default=None)
    
    parser.add_argument("--purge", action='store_true', 
                        help='Purge the specified key from table, or an entire table if no key is given. Wildcards can be included. This is a dangerous operation.')
    parser.add_argument("--purge-volatile", action='store_true', help='Purge everything except the "persistent" branch.  An _EXTREMELY_ dangerous option')
    parser.add_argument("--list", action='store_true', help="List all SMA-X keys in the Redis database. This is an expensive operation.")
    
    
    parser.add_argument("--verbose", "-v", action='store_true', help="Show all SMA-X metadata associated with the key")
    
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
        elif args.purge:
            if args.key is None:
                pattern = args.table
            else:
                pattern = f"{args.table}:{args.key}"
            confirm = input(f"Really purge everything matching {pattern} [yes/NO] ")
            if confirm.lower() == "yes":
                if pattern is not None:
                    fields_del = smax_client.smax_purge(f"{args.table}:{args.key}")
                    print(f'{fields_del} keys deleted.')   
                else:
                    print("Table or pattern to delete must be specified")
            else:
                print('Not confirmed - exiting.')    
        elif args.purge_volatile:
            confirm = input("Really purge everything except the 'persistent' branch? [yes/NO] ")
            if confirm.lower() == "yes":
                smax_client.smax_purge_volatile()
            else:
                print('Not confirmed - exiting.')
        # If set is set, set the value
        elif args.table is not None and args.key is not None:
            if args.set is not None:
                if args.type is None:
                    # Try to convert set value to the current type of the SMA-X variable
                    try:
                        smax_type = _REVERSE_TYPE_MAP[smax_client.smax_pull(args.table, args.key).type]
                    except RuntimeError:
                        smax_type = None
                    except (TimeoutError, ConnectionError):
                        smax_type = None
                        print("Could not connect to Redis")
                    
                    if smax_type is None:
                        # Value isn't in Redis yet
                        print(f"Type not given and {args.table}:{args.key} not yet in Redis. Trying to determine best type from int, float, str")
                        try:
                            v = float(args.set)
                            smax_type = "float"
                            if v.is_integer():
                                smax_type = "integer"
                        except ValueError:
                            smax_type = "str"
                            
                        print(f"Using type {smax_type}")
                else:
                    smax_type = args.type      
                
                python_type = _TYPE_MAP[smax_type]
                
                try:
                    val = python_type(args.set)
                except ValueError:
                    try:
                        # Python can't convert a float type string representing an integer to an int.
                        val = python_type(float(args.set))
                    except ValueError:
                        val = args.set
                        print(f"Could not convert {args.set} to {smax_type}, falling back to str.")
                    
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
        else:
            parser.print_help()
        smax_client.smax_disconnect()
        
if __name__ == "__main__":
    main()