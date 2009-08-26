#!/usr/local/bin/python2.6
from hash_ring import HashRing
from redis import Redis
import re


class MethodMissing(object):
    def method_missing(self, attr, *args, **kwargs):
        raise AttributeError("Missing method %s called." %attr)

    def __getattr__(self, attr):
        def callable(*args, **kwargs):
            return self.method_missing(attr, *args, **kwargs)
        return callable

class DistRedis(MethodMissing):
    def __init__(self,hosts):
        self.hosts=[]
        for h in hosts:
            host,port = h.split(':')
            self.hosts.append(Redis(host,int(port)))
        self.ring = HashRing(self.hosts)

    def add_server(self,server):
        server,port = server.split(':')
        r= Redis(server,port)
        self.ring.add_node(r)

    def save(self):
        for redis in self.ring:
            redis.save()
    
    def bgsave(self):
        for redis in self.ring:
            redis.save(True)

    def delete_cloud(self):
        for redis in self.ring:
            for key in self.ring.keys("*"):
                redis.delete(key)
    
    def quit(self):
        for redis in self.ring:
            redis.quit

    def node_for_key(self,key):
        if re.match("/\{(.*)\?\}/",key):
            l=re.split("/\{(.*)\?\}/",key)
            key = l[0]
        return self.ring.get_node(key)

    def method_missing(self, attr, *args, **kwargs):
        redis = self.node_for_key(args[0])
        if redis != None:
            return redis.__getattribute__(attr)(*args,**kwargs)


    '''def node_for_key(self,key):
        if re.match("\{(.*)?\}",key):
            key'''
        
if __name__ == '__main__':
    r = DistRedis(['localhost:6379','localhost:6380','localhost:6381'])
    print r.get('new')
    r.set('new','bye',True)
    r.set('hi','hello')
    print r.get('cool')
    print r.get('hi')
    r.push('l','a')
    r.push('l','b')
    r.push('l','y',True)
    print r.pop('l',True)
    print r.pop('l')
