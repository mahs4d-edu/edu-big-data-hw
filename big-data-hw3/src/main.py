import hashlib
import random
from abc import ABC, abstractmethod
from os import path

import pandas as pd


class StreamAlgorithm(ABC):
    @abstractmethod
    def feed(self, data):
        pass

    @abstractmethod
    def get_result(self, *args, **kwargs):
        pass

    def feed_dataset(self, dataset):
        for _, data in dataset.iterrows():
            self.feed(data)


class FlajoletMartinAlgorithm(StreamAlgorithm):
    def __init__(self, column, k):
        # column specifies which column to hash
        self.column = column
        # k is the number different hashes to use
        self.k = k
        # max_zeros is used to save maximum number of zeros found on tail of specific hash salt
        self.max_zeros = [0] * k

    def _hash_zero_count(self, value, salt=0):
        # prefix salt to the value to generate different hashes for different salts
        # sha1 does not support direct salting
        mvalue = str(salt) + str(value)

        # hash with sha1
        a = hashlib.sha1(mvalue.encode()).hexdigest()

        # find and return number of zeros on the tail of hash
        zeros = 0
        for d in a[len(a)::-1]:
            bitarray = bin(int(d, 16))[2:]

            for bit in bitarray[len(bitarray)::-1]:
                if bit == '1':
                    break

                zeros += 1

            if d != '0':
                break

        return zeros

    def feed(self, data):
        # in this function we hash input data column with k different hash functions and 
        # save the maximum zeros found in the max_zeros array
        value = data[self.column]
        for i in range(self.k):
            zeros = self._hash_zero_count(value, i)
            self.max_zeros[i] = max(self.max_zeros[i], zeros)

    def get_result(self, f=5):
        # splits max_zeros array to "f" buckets and takes average of them
        bin_averages = [0] * f
        bin_sizes = [0] * f
        for i in range(self.k):
            bin_index = i % f
            bin_sizes[bin_index] += 1
            bin_averages[bin_index] = (bin_averages[bin_index] * (bin_sizes[bin_index] - 1) + (
                    2 ** self.max_zeros[i])) / bin_sizes[bin_index]

        # sorts the averages and return the median
        sorted(bin_averages)
        return bin_averages[int(f / 2)]


class AlonMatiasSzegedyAlgorithm(StreamAlgorithm):
    def __init__(self, column, s, stream_detector):
        self.column = column
        # s: maximum number of samples we are allowed to have
        self.s = s
        # stream_detector: a function to generate different streams from a single stream
        self.stream_detector = stream_detector
        # tracked: a dictionary containing an array for each stream (stream is the key) the array \
        # contains tracked (element, value) tuples in which algorithm is working with
        self.tracked = {}
        # n: is a dictionary containing number of arrived items for each stream separately
        self.n = {}

    def _add_tracked_item(self, stream, data):
        target_element = data[self.column]
        self.tracked[stream].append([target_element, 0])

    def _remove_tracked_item(self, stream, index):
        del self.tracked[stream][index]

    def _update_tracked_items(self, stream, data):
        # this function gets a data and increments value of all the tracked items with same element
        target_element = data[self.column]
        for i, tracked_item in enumerate(self.tracked[stream]):
            element, value = tracked_item
            if element == target_element:
                self.tracked[stream][i][1] += 1

    def _get_tracked_items(self, stream):
        if stream not in self.tracked:
            self.tracked[stream] = []
            self.n[stream] = 0

        return self.tracked[stream]

    def feed(self, data):
        # detect the stream
        stream = self.stream_detector(data)

        # check if you should pick this specific data
        tracked_items = self._get_tracked_items(stream)
        # if we haven't filled s positions for this stream, pick it
        if len(tracked_items) < self.s:
            self._add_tracked_item(stream, data)
        else:
            # if we are at max (s items tracked) select it with probability of s/n+1
            selection_coin = random.randrange(0, self.n[stream] + 1)
            if selection_coin < self.s:
                # now remove one of the previous items with probability of 1/s
                removed_item = random.randrange(0, self.s)
                self._remove_tracked_item(stream, removed_item)
                self._add_tracked_item(stream, data)

        self.n[stream] += 1
        self._update_tracked_items(stream, data)

    def get_result(self, m):
        # returns "m"th moment of all the streams
        results = {}
        # compute v^m - (v-1)^m for all streams
        for stream in self.tracked:
            results[stream] = 0
            for element, value in self.tracked[stream]:
                results[stream] += (value ** m) - ((value - 1) ** m)

            results[stream] *= self.n[stream]

        return results


file_path = path.join(path.abspath(path.dirname(__file__)), '../data/data.csv')
dataset = pd.read_csv(file_path, index_col=False)

# question 1
f = FlajoletMartinAlgorithm('amount', 20)
f.feed_dataset(dataset)
n_distinct_persons = f.get_result(5)
print('Flajolet Martin Result on Person Field: {0}'.format(n_distinct_persons))

# question 2
ams1 = AlonMatiasSzegedyAlgorithm('goods', 10, lambda x: x['region'])
ams1.feed_dataset(dataset)
momend_2_per_region = ams1.get_result(2)
print('AlonMatiasSzegedy Algorithm Result (2nd Moment Per Region): {0}'.format(momend_2_per_region))

# question 3
ams2 = AlonMatiasSzegedyAlgorithm('goods', 10, lambda x: x['region'] + ':' + x['date'].split(' ')[1])
ams2.feed_dataset(dataset)
momend_2_per_region_month = ams2.get_result(2)
print('AlonMatiasSzegedy Algorithm Result (2nd Moment Per Region and Month): {0}'.format(momend_2_per_region_month))
