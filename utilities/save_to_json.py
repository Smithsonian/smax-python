import json
from collections import namedtuple

import smax

smax_redis_ip = "localhost"

smax_tree_file = "smax_dump.json"

# table to load the values to.
table = "_testing_"
key = "SMAX"


def recurse_struct_to_json(node):
    """Recurse through a smax struct object, converting the leaf nodes to dicts so that `json.dump()` can be used
    
    Arguments:
        node (dict or NameTuple) : subtree of the smax struct
        
    Returns:
        dict : dictionary ready to be dumped by `json.dump()` or `json.dumps()`"""
    if type(node) is dict:
        ret_dict = {}
        for key in node:
            dict_val = recurse_struct_to_json(node[key])
            ret_dict[key] = dict_val
        return ret_dict
    elif type(node) is list:
        ret_list = []
        for l in node:
            ret_list.append(recurse_struct_to_json(l))
        return ret_list
    else: # type(node) is namedtuple:
        # We have a named tuple - SMAX leaf
        ret_leaf = node._asdict()
        if 'type' in ret_leaf.keys():
            ret_leaf['type'] = str(ret_leaf['type'])
            
        return ret_leaf
    

if __name__ == "__main__":
    with smax.SmaxRedisClient(smax_redis_ip) as smax_client:
        smax_tree_struct = smax_client.smax_pull(table, key)
    
    smax_tree_dict = recurse_struct_to_json(smax_tree_struct)
    
    with open(smax_tree_file, "w") as f:
        json.dump(smax_tree_dict, f, indent=4)