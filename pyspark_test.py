from pyspark.sql import SparkSession
from time import time
import random
import json

sc = SparkSession.builder.getOrCreate().sparkContext


def test_word_count(filepath):
    st = time()
    words = sc.textFile(filepath).flatMap(lambda line: line.split(" "))
    result = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()
    et = time()
    return result, et - st


def test_pi_estimation(num_of_samples):
    def inside(_):
        x, y = random.random(), random.random()
        return x * x + y * y < 1
    st = time()
    count = sc.parallelize(range(0, num_of_samples)).filter(inside).count()
    result = 4.0 * (count / num_of_samples)
    et = time()
    return result, et - st


def test_intersection_reduce(data):
    st = time()
    rdd = sc.parallelize(data)
    rdd = rdd.map(lambda x: x * 2)
    rdd = rdd.filter(lambda x: x >= 6)
    result = rdd.first()
    et = time()
    return result, et - st


def test_union_group_by_value(data):
    st = time()
    rdd = sc.parallelize(data)
    rdd = rdd.map(lambda x: x * 2)
    rdd = rdd.filter(lambda x: x >= 6)
    result = rdd.first()
    et = time()
    return result, et - st


def test_distinct_take_ordered(data):
    st = time()
    rdd = sc.parallelize(data)
    rdd = rdd.map(lambda x: x * 2)
    rdd = rdd.filter(lambda x: x >= 6)
    result = rdd.first()
    et = time()
    return result, et - st


def test_sort(data):
    print("Data before sort:")
    print(data)
    st = time()
    rdd = sc.parallelize(data)
    result = rdd.sortBy(lambda x: x).collect()
    et = time()
    return result, et - st


def run_all_benchmarks(datasets):
    results = {"word_count": (test_word_count(datasets["word_count"])),
               "pi_estimation": (test_pi_estimation(datasets["pi_estimation"])),
               # "intersection_reduce": (test_intersection_reduce(datasets["intersection_reduce"])),
               # "union_group_by_value": (test_union_group_by_value(datasets["union_group_by_value"])),
               # "distinct_take_ordered": (test_distinct_take_ordered(datasets["distinct_take_ordered"])),
               "sort": (test_sort(datasets["sort"]))}

    return results


def generate_data_to_sort():
    result = []
    for _ in range(1000):
        result.append(random.randint(0, 333))

    with open("test_cases/generated_unsorted_data.json", "w") as f1:
        f1.write(json.dumps(result))

    return result


datasets_input = {
    "word_count": "./test_cases/words.txt",
    "pi_estimation": 1000,
    # "intersection_reduce": [[], []],
    # "union_group_by_value": [[], []],
    # "distinct_take_ordered": [],
    "sort": generate_data_to_sort()
}

if __name__ == "__main__":
    benchmark_results = run_all_benchmarks(datasets_input)

    print(benchmark_results)

    with open("test_results/pyspark_benchmark_result.json", "w") as f:
        f.write(json.dumps(benchmark_results))


# wiecej scenariuszy!
# porownac z innym frameworkiem
# umozliwic konfiguracje np batchow i uwzglednic w scenariuszach
# zautomatyzowac wykonanie scenariuszow dla roznych konfiguracji i automatycznie wygenerowac raport
