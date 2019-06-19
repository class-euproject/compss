#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import sys
import time
from collections import defaultdict, deque

from pycompss.api.api import compss_wait_on

from pycompss.dds import DDS
from pycompss.dds.tasks import gen_fragment


def to_list(a): return [a]


def append(a, b):
    a.append(b)
    return a


def extender(a, b):
    a.extend(b)
    return a


def _sum(a, b): return a + b


def _finished(a):
    if len(a[1]) > 2:
        return True


def inside(_):
    import random
    x, y = random.random(), random.random()
    if (x * x) + (y * y) < 1:
        return True


def reduce_example():
    test = DDS(range(100), 50).reduce((lambda b, a: b + a), initial=100,
                                      arity=3, collect=False) \
        .map(lambda a: a + 1).collect()
    print(test)


def word_count():
    path_file = sys.argv[1]
    size_block = int(sys.argv[3])

    start = time.time()
    local_results = DDS().load_file(path_file, chunk_size=size_block, worker_read=True) \
        .map_partitions(split_and_dict).collect(future_objects=True)

    local_results = deque(local_results)
    arity = 2
    branch = list()
    while local_results:
        while local_results and len(branch) < arity:
            temp = local_results.popleft()
            branch.append(temp)

        if len(branch) == 1:
            break

        # branch = compss_wait_on(branch)
        reduce_dicts(branch[0], branch[1])
        local_results.append(branch[0])
        branch = []

    results = compss_wait_on(branch[0])
    print("Elapsed Time: ", time.time() - start)
    return


def split_and_dict(partition):
    from collections import defaultdict
    ret = defaultdict(int)
    for item in partition[0].split():
        ret[item] += 1

    return ret


def pi_estimation():
    """
    Example is taken from: https://spark.apache.org/examples.html
    """
    print("Estimating Pi by 'throwing darts' algorithm.")
    tries = 100000
    print("Number of tries: {}".format(tries))

    count = DDS().load(range(0, tries), 10) \
        .filter(inside).count()
    print("Pi is roughly %f" % (4.0 * count / tries))
    print('______________END OF THE EXAMPLE________________\n')


def example_1():
    print("Creating a DDS with range(10) and 5 partitions:")
    dds = DDS(range(10), 5)
    print("Elements of the DDS:")
    print(dds.collect())

    print("Elements & Partitions of the DDS:")
    dds = DDS(range(10), 5)
    print(dds.collect(True))
    print('______________END OF THE EXAMPLE________________\n')


def example_2():
    # EXAMPLE 3
    print("Occurrences of letters in different strings are as following:")
    occurrences = [
        ("a", 1), ("b", 1), ("c", 2), ("d", 7), ("a", 2), ("b", 7), ("b", 6),
        ("a", 2), ("c", 7), ("d", 6), ("e", 2), ("n", 7), ("m", 2), ("n", 6),
        ("e", 2), ("e", 12)]
    print(occurrences)
    print("Retrieve the letters that have more than 5 occurrences in total:")

    dds = DDS(occurrences)
    print(dds.reduce_by_key(_sum).filter(lambda x: x[1] > 5).keys().collect())
    print('______________END OF THE EXAMPLE________________\n')


def example_3():
    print("Given: ID's of players and their points from different tries are:")
    results = [(1, 10), (2, 5), (3, 8), (1, 7), (2, 6), (3, 15), (1, 5), (2, 6)]
    print(results)
    print("Knowing that maximum tries is 3, show the results of the players "
          "who have finished the game :)")
    dds = DDS(results)
    completed = dds.combine_by_key(to_list, append, extender).filter(
        _finished).collect()
    for k, v in completed:
        print("Player ID: ", k)
        print("Points: ", v)
    print('______________END OF THE EXAMPLE________________\n')


def example_4():
    """
    Just a crazy example to try as many methods as possible
    :return:
    """
    print("Some words:")
    words = ["This is a test", "This is an example.", "H o l a", "Lorem ipsum.",
             "Foo, bar", "Examples of some DDS methods..."]
    print(words)

    print("Some letters:")
    letters = "someletterswilloccurmorethanonceandthisisreallycool! \n"
    print(letters)

    # Extract single letter words from 'words' and count them
    dds_words = DDS().load(words, 10).map_and_flatten(lambda x: x.split(" ")) \
        .filter(lambda x: len(x) == 1).count_by_value()

    # Extract letters from 'letters' and count them
    dds_letters = DDS().load(letters, 5).map_and_flatten(lambda x: list(x)) \
        .count_by_value()
    print()
    print("Amongst single letter words and letters, the highest occurrence is:")
    # Join, group by letter and show the highest occurrence
    print(dds_words.union(dds_letters).reduce_by_key(_sum).max(lambda x: x[1]))
    print('______________END OF THE EXAMPLE________________\n')


def example_5():
    print("WordCount for lines containing '#' (sharp) in a file.")

    file_name = 'test.txt'
    f = open(file_name, 'w')
    for i in range(1000):
        f.write("This is a line with # \n")
        f.write("This one doesn't have a sharp {}\n")
    f.close()

    results = DDS().load_text_file(file_name, chunk_size=100) \
        .filter(lambda line: '#' in line) \
        .map_and_flatten(lambda line: line.split(" ")) \
        .count_by_value() \
        .filter(lambda x: len(x[0]) > 2) \
        .collect_as_dict()

    print("Words of lines containing '#':")
    print(results)

    import os
    os.remove(file_name)
    print("______________END OF THE EXAMPLE________________\n")


def terasort(num_fragments, num_entries, num_buckets, seed):
    """
    ----------------------
    Terasort main program
    ----------------------
    This application generates a set of fragments that contain randomly
    generated key, value tuples and sorts them all considering the key of
    each tuple.

    :param num_fragments: Number of fragments to generate
    :param num_entries: Number of entries (k,v tuples) within each fragment
    :param num_buckets: Number of buckets to consider.
    :param seed: Initial seed for the random number generator.
    """

    dataset = [gen_fragment(num_entries, seed + i) for i in range(num_fragments)]
    dds = DDS(dataset, -1).sort_by_key().collect(True)
    for i in range(len(dds)):
        if dds[i][0] < dds[i-1][0] and i > 0:
            print("Failed:", i)
            break


def run_terasort():
    arg1 = sys.argv[1] if len(sys.argv) > 1 else 16
    arg2 = sys.argv[2] if len(sys.argv) > 2 else 50

    num_fragments = int(arg1)  # Default: 16
    num_entries = int(arg2)  # Default: 50
    # be very careful with the following argument (since it is in a decorator)
    num_buckets = 10  # int(sys.argv[3])
    seed = 5

    start_time = time.time()
    terasort(num_fragments, num_entries, num_buckets, seed)
    print("Elapsed Time {} (s)".format(time.time() - start_time))


def load_n_map_example():

    fayl = 'test.txt'
    test = open(fayl, 'w')
    for number in range(100):
        test.write("This is line # {} \n".format(number))
    test.close()

    def sum_line_numbers(partition, initial=0):
        """
        Doesn't return a list, but a single value...
        """
        sum = initial
        for line in partition:
            sum += int(line.split()[-1])
        return sum

    result = DDS().load_and_map_partitions(fayl, sum_line_numbers, initial=9).collect()
    import os
    os.remove(fayl)
    print(result)


def main_program():
    print("________RUNNING EXAMPLES_________")
    # example_1()
    # example_2()
    # example_3()
    # example_4()
    # example_5()
    # pi_estimation()
    # See 'launch.sh' for WordCount example.
    word_count()
    # reduce_example()
    # load_n_map_example()
    run_terasort()


if __name__ == '__main__':
    main_program()
