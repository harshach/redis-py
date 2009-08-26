
from __future__ import generators
import zlib

POINTS_PER_SERVER=5

def binary_search(ary,value):
    upper=len(ary) - 1
    lower=0
    idx=0
    while (lower <= upper):
        idx = (lower+upper)/2
        if (ary[idx] == value):
            return idx
        elif ary[idx] > value:
            upper = idx - 1
        else:
            lower = idx + 1
    return upper

class HashRing(object):
    
    def __init__(self,nodes,replicas=POINTS_PER_SERVER):
        self.replicas = replicas
        self.ring = {}
        self.nodes = []
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)
    

    def add_node(self,node):
        self.nodes.append(node)
        for i in xrange(0,self.replicas):
            key = zlib.crc32("%s:%d" % (node,i))
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def remove_node(self,node):
        for i in xrange(0,self.replicas):
            key = zlib.crc32("%s:%d" % (node,i))
            del self.ring[key]
            self.sorted_keys.remove(k)
    
    def get_node(self,key):
        return self.get_node_pos(key)[0]
    
    def get_node_pos(self,key):
        if not self.ring:
            return [None,None]
        crc = zlib.crc32(key)
        idx = binary_search(self.sorted_keys,crc)
        return [self.ring[self.sorted_keys[idx]],idx]

    def iter_nodes(self,key):
        if self.ring.size == 0:
            yield [None,None]
        pos = get_node_post(key)[1]
        for key in self.sorted_keys[pos:]:
            yield self.ring[key]


