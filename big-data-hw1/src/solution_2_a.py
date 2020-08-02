#!/usr/bin/python3

from mrjob.job import MRJob


class MRColumnAverage(MRJob):
    def mapper(self, _, line):
        (i, j, v) = line.split(',')

        # yield with key=column and value=value
        yield int(j), float(v)

    def reducer(self, key, values):
        summation = sum(values)
        yield key, (summation / 100)


if __name__ == '__main__':
    MRColumnAverage.run()
