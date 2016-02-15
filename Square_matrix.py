__author__ = 'shilpagulati'
import MapReduce
import sys
import re

mr=MapReduce.MapReduce()


def mapper(line):
        # print line[2]
        # line=re.sub('[" "\[\]]','',line)
        # line= line.split(',')

        for i in range(0,5):

            if line[0] == i:
                for k in range(0,5):
                    mr.emit_intermediate((i,k),['A',line])
            if line[1]==i:
                 for k in range(0,5):
                    mr.emit_intermediate((k,i),['B',line])


def reducer(key,list_of_values):
        elemA=[]
        elemB=[]
        sum=0
        for v in list_of_values:
            if v[0]=='A':
                elemA.append((v[1][0],v[1][1],v[1][2]))

            if v[0]=='B':
                elemB.append((v[1][0],v[1][1],v[1][2]))
        for eachA in elemA:
            for eachB in elemB:
                if eachA[1]==eachB[0]:
                    sum+=eachA[2]*eachB[2]
        mr.emit((key,sum))





if __name__ == '__main__':

    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

