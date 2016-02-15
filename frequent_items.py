__author__ = 'shilpagulati'

import MapReduce
import sys
import collections
import itertools
mr=MapReduce.MapReduce()

def mapper(record):

    totalP=itertools.combinations(record,2)
    for pair in totalP:
        mr.emit_intermediate(pair,1)

def reducer(key,List_of_values):
    total=0
    for value in List_of_values:
        total+=value
    if total>=100:

        mr.emit((key))



if __name__=='__main__':

    input=open(sys.argv[1])
    mr.execute(input,mapper,reducer)




