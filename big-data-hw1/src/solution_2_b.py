#!/usr/bin/python3

import math
from mrjob.job import MRJob


class MRStandardDeviation(MRJob):
    def mapper(self, _, line):
        (i, j, v) = line.split(',')

        # yield with key=column and value=value
        yield int(j), float(v)

    def reducer(self, key, values):
        values_list = list(values)
        summation = sum(values_list)

        average = summation / 100

        variance = 0
        for value in values_list:
            variance += (value - average) ** 2
        
        for i in range(100 - len(values_list)):
            variance += average ** 2

        yield key, math.sqrt(variance / 100)


if __name__ == '__main__':
    MRStandardDeviation.run()
