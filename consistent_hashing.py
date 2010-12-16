#!/bin/env python

# TODO: documentation
#       class CacheSet
#       class HashFunction with subclasses
#             NaiveHashFunction and ConsistentHashFunction
#       means to test removing a Cache
#       command line argument for max_key and ConsistentHashTable.factor

import sys
from optparse import OptionParser
import random

max_key = 200000

def naive_hash(key, extent):
    return key % extent

def naive_add_cache(cache):
    pass

class ConsistentHashTable():
    """A table for implementing consistent hashing."""
    def __init__(self, factor = 2000):
        """`size` is the number of possible entries in the table.
        `factor` is the number of times to insert one node.  Optimal
        factor size is a function of the number of caches, which
        varies."""
        self.table = {}
        self.factor = factor
    def add(self, node):
        """Adds a possible node for caching."""
        # XXX this doesn't handle:
        #     random number collisions
        #     too many adds
        for i in range(0, self.factor):
            rand = random.randint(0, max_key - 1)
            self.table[rand] = node
    def find(self, key):
        """Returns the node to use for this key."""
        while not self.table.has_key(key):
            if key == max_key:
                key = 0
            else:
                key += 1
        return self.table[key]

the_consistent_hash_table = ConsistentHashTable()

def consistent_hash(key, extent):
    return the_consistent_hash_table.find(key)

def consistent_add_cache(cache):
    the_consistent_hash_table.add(cache)

class Database:
    """A Database is a set of name value pairs.

    This implementation all happens in core, as this program is
    pedagogical."""
    def __init__(self):
        self.db = {}
    def lookup(self, key):
        if self.db.has_key(key):
            return self.db[key]
        else:
            return None
    def insert(self, key, value):
        self.db[key] = value

def hit_ratio(hits, misses):
    if hits + misses <= 0:
        return 0.0
    else:
        return (float(hits) / float(hits + misses))

class Cache:
    """A Cache is a set of name value pairs meant to be a fast and
    shardable cache of a Database."""
    def __init__(self, src):
        self.cache = {}
        self.source = src
        self.hits = 0
        self.misses = 0
    def lookup(self, key):
        if self.cache.has_key(key):
            result = self.cache[key]
            self.hits += 1;
        else:
            result = self.source.lookup(key)
            if result != None:
                self.misses +=1;
                self.insert(key, result);
        return result
    def insert(self, key, value):
        # This implementation is entirely naive, and assumes an
        # infinitely large cache.  This is fine though, as upstream we
        # want to measure only the misses from adding a new Cache to a
        # set of Caches for a particular Database, we're not so
        # concerned with Cache misses that occur, for example, because
        # the Cache is full and a FIFO.
        self.cache[key] = value
    def reset_counters(self):
        self.hits = 0;
        self.misses = 0;
    def hit_ratio(self):
        return hit_ratio(self.hits, self.misses)
    def print_stats(self):
        print "hits      %d" % self.hits
        print "misses    %d" % self.misses
        print "hit ratio %f" % self.hit_ratio()

def smoke_test():
    d = Database()
    d.insert(1, 1)
    d.insert(2, 2)
    c = Cache(d)
    assert c.lookup(1) == 1
    assert c.lookup(1) == 1
    assert c.lookup(1) == 1
    assert c.lookup(1) == 1
    assert c.lookup(1) == 1
    assert c.misses == 1
    assert c.hits == 4

options = {}
    
def debug_print(message):
    global options
    if options.debug:
        print message

def debug_print_caches(caches):
    global options
    if options.debug:
        for i in range(0, len(caches)):
            print "cache[%d] ----------" % i
            caches[i].print_stats()
            print ""
        
def average_hit_ratio_test(hash_function, add_cache_function):
    # We make the database just the identiy relation of key value
    # pairs, so for any k inserted into the database db[k] == k.
    # There are obviously far more efficient ways to simulate such a
    # database, but because we might want to change this code later to
    # investigate some other aspect of hashing or caching, we
    # implement it poorly and pretend.
    db = Database()
    debug_print("populating database with %d entries" % options.dbsize)
    for i in range(0, options.dbsize):
        db.insert(i, i)
    n_caches = options.start_caches
    debug_print("creating %d empty caches" % n_caches)
    caches = []
    for i in range(0, n_caches):
        add_cache_function(i)
        caches.append(Cache(db))

    debug_print("populating caches to cover database")
    debug_print("(this should show a hit ratio of 0.0)")
    for i in range(0, options.dbsize):
        assert caches[hash_function(i, n_caches)].lookup(i) == i
    debug_print("")
    debug_print_caches(caches)
    debug_print("resetting cache counts")
    for i in range(0, n_caches):
        caches[i].reset_counters()

    debug_print("%d random lookups over %d caches"
                % (options.lookups, n_caches))
    debug_print("(this should show a hit ratio of 1.0)")
    for i in range(0, options.lookups):
        rand = random.randint(0, options.dbsize - 1)
        assert caches[hash_function(rand, n_caches)].lookup(rand) == rand
    debug_print("")
    debug_print_caches(caches)
    debug_print("resetting cache counts")
    for i in range(0, n_caches):
        caches[i].reset_counters()

    debug_print("add %d cache or caches" % options.add_caches)
    for i in range(0, options.add_caches):
        add_cache_function(i)
        caches.append(Cache(db))
    n_caches += options.add_caches
    debug_print("")

    debug_print("%d random lookups over %d caches, naive cache selection"
                % (options.lookups, n_caches))
    debug_print("(this shows the hit ratios we're interested in knowing)")
    for i in range(0, options.lookups):
        rand = random.randint(0, options.dbsize - 1)
        assert caches[hash_function(rand, n_caches)].lookup(rand) == rand
    debug_print("")
    debug_print_caches(caches)

    total_hits = 0
    total_misses = 0
    for i in range(0, n_caches - options.add_caches):
        total_hits += caches[i].hits
        total_misses += caches[i].misses
    if options.debug:
        print "average hit ratio %.3f" % hit_ratio(total_hits, total_misses)
    else:
        print "%.3f" % hit_ratio(total_hits, total_misses)

def main():
    global max_key
    global options

    smoke_test()

    command_parser = OptionParser()
    command_parser.add_option('-d', '--debug',
                              action = 'store_true',
                              dest = "debug")
    command_parser.add_option('-B', '--database-size',
                              type = 'int',
                              dest = "dbsize",
                              default = 100000)
    command_parser.add_option('-S', '--start-caches',
                              type = 'int',
                              dest = "start_caches",
                              default = 3)
    command_parser.add_option('-A', '--add-caches',
                              type = 'int',
                              dest = "add_caches",
                              default = 1)
    command_parser.add_option('-L', '--lookups',
                              type = 'int',
                              dest = "lookups",
                              default = 50)
    (options, args) = command_parser.parse_args()
    if options.dbsize > max_key:
        print "Dbsize must be less than %d." % max_key
        sys.exit(0)

    # averge_hit_ratio_test() gets a lot of info out of options.foo
    debug_print("NAIVE HASHING ---------------------------------")
    average_hit_ratio_test(naive_hash, naive_add_cache)
    debug_print("CONSISTENT HASHING-----------------------------")
    average_hit_ratio_test(consistent_hash, consistent_add_cache)

if __name__ == '__main__':
    main()
