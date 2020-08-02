#!/usr/bin/python3

import os
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMultiplication(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
        ]

    def mapper_1(self, _, line):
        (i, j, v) = line.split(',')
        
        file_name = os.environ.get('map_input_file', None)

        if 'mat1.csv' in file_name:
            yield int(j), (1, int(i), float(v))
        elif 'mat2.csv' in file_name:
            yield int(i), (2, int(j), float(v))

    def reducer_1(self, key, values):
        values_list = list(values)

        # separate values by the matrix they came from
        tp1_values = []
        tp2_values = []

        for value in values_list:
            if value[0] == 1:
                tp1_values.append(value)
            else:
                tp2_values.append(value)

        # create key value pairs for multiplications
        for tp1_value in tp1_values:
            for tp2_value in tp2_values:
                v = tp1_value[2] * tp2_value[2]
                if v != 0:
                    yield (tp1_value[1], tp2_value[1]), v

    def reducer_2(self, key, values):
        v = sum(values)

        # if value is zero => do not print it to output
        if v != 0:
            yield key, v


if __name__ == '__main__':
    MRMultiplication.run()
