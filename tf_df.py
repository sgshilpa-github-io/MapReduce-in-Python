from collections import Counter

__author__ = 'shilpagulati'
import MapReduce
import re
import sys
import string

mr=MapReduce.MapReduce()

def mapper(record):
    key = record[0]
    value = record[1]

    value=re.sub('[\+\-\.\,\!\@\#\$\^\&\*\(\)\;\\\/\|\<\>\"\'\?\!\:\~\[\]\}\{]','',value)
    words = value.split()
    words = [element.lower() for element in words]
    wordL = ' '.join(words)

    counted=[]
    for word in words:

        if word in counted:
            continue

        count= len(re.findall(r'\b%s\b' % word, wordL))
        counted.append(word)
        mr.emit_intermediate(word,[key,count])




def reducer(key,list_of_values):
    total = 0
    for v in list_of_values:

         total += v[1]

    mr.emit((key, len(list_of_values), list_of_values))




if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)







