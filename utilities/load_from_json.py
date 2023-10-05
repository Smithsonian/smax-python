import json

import smax

smax_redis_ip = "localhost"

smax_tree_file = "smax_tree.json"

# table to load the values to.
table = "_testing_"


def recurse_json_to_struct(node):
    """Recurse through a smax_tree json object, converting to a struct suitable for writing to SMAX
    
    Arguments:
        tree_json (json.Json) : JSON object loaded by `json.load`.
        
    Returns:
        dict : dictionary of Python values ready to be loaded to SMAX"""
    if type(node) is dict:
        # Detect if this is a leaf SMAX node
        if "data" in node.keys() and "dim" in node.keys() and "type" in node.keys():
            return node["data"]
        else:
            ret_dict = {}
            for key in node:
                dict_val = recurse_json_to_struct(node[key])
                ret_dict[key] = dict_val
            return ret_dict
    elif type(node) is list:
        ret_list = []
        for l in node:
            ret_list.append(recurse_json_to_struct(l))
        return ret_list
    else:
        # Shouldn't get here.
        return node
    

if __name__ == "__main__":
    with open(smax_tree_file) as f:
        smax_tree_json = json.load(f)
    
    smax_tree_struct = recurse_json_to_struct(smax_tree_json)
    
    with smax.SmaxRedisClient(smax_redis_ip) as smax_client:
    
        for key in smax_tree_struct.keys():
            smax_client.smax_share(table, key, smax_tree_struct[key])