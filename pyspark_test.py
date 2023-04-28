import pyspark
from time import time
import random
import json
import matplotlib.pyplot as plt

sc = pyspark.SparkContext('local[*]')


def generate_data_to_sort(size, n, m):
    result = []
    for _ in range(size):
        result.append(random.randint(n, m))

    with open("test_cases/generated_unsorted_data.json", "w") as f1:
        f1.write(json.dumps(result))

    return result


datasets_input = {
    "intersection_reduce": [
        [59, 207, 89, 248, 45, 307, 70, 55, 12, 156, 89, 225, 204, 71, 251, 30, 72, 145, 101, 62, 181, 286, 234, 198,
         300, 220, 54, 189, 163, 289, 268, 66, 155, 157, 188, 330, 200, 160, 290, 182, 280, 250, 174, 96, 222, 10, 26,
         211, 202, 135, 64, 183, 165, 16, 192, 257, 212, 59, 311, 280, 312, 274, 318, 2, 118, 195, 40, 299, 254, 106,
         16, 296, 316, 48, 152, 186, 270, 1, 272, 271, 62, 264, 183, 177, 317, 23, 280, 277, 9, 262, 213, 226, 226, 28,
         199, 196, 52, 13, 184, 173, 211, 282, 256, 3, 103, 119, 271, 103, 207, 107, 310, 329, 241, 124, 325, 228, 63,
         1, 311, 142, 225, 242, 53, 302, 205, 124, 142, 244, 217, 203, 229, 18, 13, 166, 191, 107, 6, 83, 212, 171, 41,
         89, 60, 304, 215, 124, 35, 78, 120, 54, 304, 99, 44, 272, 311, 213, 115, 288, 221, 85, 101, 22, 144, 323, 119,
         39, 68, 175, 246, 73, 45, 270, 209, 275, 115, 192, 285, 1, 93, 259, 243, 50, 57, 195, 34, 40, 158, 261, 29,
         108, 73, 133, 99, 222, 79, 299, 104, 260, 23, 235, 283, 92, 1, 101, 36, 94, 212, 262, 24, 228, 185, 108, 30,
         122, 266, 66, 9, 131, 58, 192, 132, 56, 189, 22, 75, 288, 281, 60, 148, 238, 265, 248, 103, 200, 275, 47, 173,
         185, 168, 45, 22, 200, 268, 298, 22, 2, 234, 122, 303, 223, 305, 284, 207, 154, 36, 331, 22, 207, 226, 327, 50,
         29, 289, 113, 32, 328, 193, 45, 114, 283, 92, 161, 28, 147, 75, 9, 23, 109, 310, 15, 270, 74, 107, 225, 216,
         54, 103, 133, 27, 2, 171, 204, 23, 97, 12, 310, 166, 267, 276, 106, 149, 140, 207, 141, 166, 228, 299, 227,
         220, 317, 1, 95, 254, 64, 302, 47, 68, 310, 148, 103, 115, 218, 17, 43, 92, 247, 315, 129, 55, 277, 321, 6,
         244, 160, 149, 36, 314, 172, 120, 51, 151, 77, 268, 94, 95, 34, 40, 276, 254, 146, 86, 230, 218, 277, 23, 125,
         284, 308, 169, 234, 59, 168, 42, 60, 6, 88, 314, 294, 294, 300, 55, 1, 168, 199, 37, 182, 168, 225, 11, 185,
         158, 18, 253, 222, 308, 180, 304, 1, 233, 41, 65, 178, 199, 176, 312, 254, 215, 118, 62, 55, 89, 328, 190, 50,
         252, 60, 284, 151, 55, 103, 143, 151, 252, 306, 72, 47, 109, 15, 161, 94, 52, 267, 325, 78, 170, 305, 231, 272,
         188, 92, 83, 152, 139, 69, 19, 281, 113, 241, 146, 27, 9, 287, 264, 31, 194, 293, 321, 173, 81, 0, 20, 2, 196,
         174, 308, 185, 72, 88, 212, 71, 45, 256, 97, 232, 321, 40, 75, 128, 120, 181, 176, 243, 298, 139, 209, 230, 61,
         41, 261, 272, 278, 246, 29, 277, 329, 268, 154, 154, 294, 243, 246, 150, 169, 272, 255, 136, 234, 266, 189,
         119, 67],
        [214, 248, 139, 205, 82, 19, 41, 43, 210, 287, 305, 309, 27, 62, 135, 269, 100, 277, 40, 71, 91, 81, 34, 131,
         237, 26, 308, 320, 30, 321, 237, 64, 127, 286, 279, 276, 225, 318, 44, 82, 226, 212, 11, 38, 11, 220, 69, 28,
         38, 132, 271, 48, 81, 313, 208, 62, 221, 251, 26, 235, 241, 157, 21, 166, 244, 62, 293, 167, 157, 3, 166, 170,
         281, 304, 265, 65, 144, 148, 27, 280, 268, 142, 84, 29, 307, 13, 259, 122, 40, 225, 277, 52, 237, 31, 156, 217,
         204, 70, 227, 235, 71, 310, 231, 206, 241, 93, 227, 83, 70, 54, 62, 37, 211, 229, 199, 49, 209, 297, 70, 133,
         38, 205, 325, 111, 9, 321, 196, 223, 113, 240, 262, 183, 314, 146, 91, 138, 120, 10, 62, 252, 149, 260, 96,
         281, 297, 50, 107, 128, 40, 63, 173, 206, 155, 230, 82, 38, 89, 28, 107, 63, 141, 238, 34, 243, 112, 79, 292,
         252, 153, 303, 273, 178, 272, 114, 16, 284, 0, 310, 88, 53, 208, 29, 235, 259, 30, 196, 98, 29, 23, 81, 322,
         19, 207, 95, 28, 157, 176, 131, 327, 170, 243, 304, 332, 176, 50, 222, 33, 59, 149, 154, 228, 253, 276, 112,
         131, 287, 28, 42, 0, 221, 324, 200, 174, 179, 293, 290, 54, 244, 271, 294, 97, 135, 325, 9, 173, 128, 166, 81,
         326, 163, 241, 272, 98, 104, 274, 262, 17, 6, 22, 257, 266, 215, 328, 165, 4, 222, 239, 183, 271, 106, 87, 218,
         272, 294, 230, 320, 209, 83, 167, 203, 295, 106, 235, 118, 98, 190, 108, 100, 299, 58, 285, 135, 249, 243, 92,
         14, 274, 139, 245, 177, 204, 192, 25, 0, 142, 272, 116, 10, 38, 12, 54, 67, 332, 63, 201, 272, 79, 104, 95,
         221, 100, 191, 238, 201, 287, 101, 190, 229, 58, 43, 209, 250, 267, 119, 34, 139, 59, 146, 228, 130, 73, 159,
         99, 43, 203, 68, 37, 264, 147, 312, 228, 74, 50, 67, 319, 98, 301, 99, 143, 11, 190, 110, 83, 142, 119, 291,
         262, 76, 304, 39, 250, 296, 122, 326, 247, 272, 149, 134, 203, 249, 109, 20, 276, 184, 168, 255, 192, 109, 154,
         133, 268, 115, 26, 221, 121, 204, 130, 197, 143, 1, 108, 314, 114, 276, 15, 80, 45, 333, 98, 137, 69, 98, 20,
         322, 261, 260, 85, 77, 212, 223, 33, 316, 63, 133, 216, 111, 23, 263, 117, 233, 222, 102, 79, 17, 168, 248, 96,
         66, 222, 85, 103, 209, 137, 192, 259, 108, 205, 194, 51, 292, 234, 190, 271, 22, 330, 80, 195, 100, 317, 109,
         70, 228, 243, 193, 331, 205, 151, 293, 1, 85, 294, 88, 177, 98, 40, 121, 126, 301, 10, 322, 143, 116, 251, 282,
         64, 227, 49, 275, 123, 188, 163, 213, 144, 226, 14, 54, 55, 78, 302, 199, 157, 2, 330, 331, 145, 43, 55, 71,
         207]
    ],
    "union_group_by_value": [[
        [
            "A",
            0
        ],
        [
            "B",
            1
        ],
        [
            "C",
            0
        ],
        [
            "D",
            2
        ],
        [
            "E",
            2
        ]
    ],
        [
            [
                "F",
                0
            ],
            [
                "G",
                1
            ],
            [
                "H",
                0
            ],
            [
                "I",
                2
            ],
            [
                "J",
                2
            ]
        ]],
    "distinct_take_ordered": generate_data_to_sort(1000, 1, 100)
}
batched_datasets_input = {
    "word_count": "./test_cases/words.txt",
    "pi_estimation": 1000,
    "sort": generate_data_to_sort(1000, 0, 333),
}


def test_word_count(filepath, num_partitions):
    st = time()
    words = sc.textFile(filepath, num_partitions).flatMap(lambda line: line.split(" "))
    result = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()
    et = time()
    return result, et - st


def test_pi_estimation(num_of_samples, num_partitions):
    def inside(_):
        x, y = random.random(), random.random()
        return x * x + y * y < 1

    st = time()
    count = sc.parallelize(range(0, num_of_samples), num_partitions).filter(inside).count()
    result = 4.0 * (count / num_of_samples)
    et = time()
    return result, et - st


def test_intersection_reduce(data):
    st = time()
    rdd = sc.parallelize(data[0], 1)
    rdd2 = sc.parallelize(data[1], 1)
    rdd = rdd.intersection(rdd2)
    result = rdd.reduce(lambda a, b: a + b)
    et = time()
    return result, et - st


def test_union_group_by_value(data):
    st = time()
    rdd = sc.parallelize(data[0], 1)
    rdd2 = sc.parallelize(data[1], 1)
    rdd = rdd.union(rdd2)
    result = rdd.groupBy(lambda a: a[1]).mapValues(list).collect()
    et = time()
    return result, et - st


def test_distinct_take_ordered(data):
    st = time()
    rdd = sc.parallelize(data, 1)
    rdd = rdd.distinct()
    result = rdd.takeOrdered(5)
    et = time()
    return result, et - st


def test_sort(data, num_partitions):
    print("Data before sort:")
    st = time()
    rdd = sc.parallelize(data, num_partitions)
    result = rdd.sortBy(lambda x: x).collect()
    et = time()
    return result, et - st


def run_all_benchmarks(datasets):
    results = {
        "intersection_reduce": (test_intersection_reduce(datasets["intersection_reduce"])),
        "union_group_by_value": (test_union_group_by_value(datasets["union_group_by_value"])),
        "distinct_take_ordered": (test_distinct_take_ordered(datasets["distinct_take_ordered"]))
    }

    with open("test_results/pyspark_benchmark_result_non_batched.json", "w") as f:
        f.write(json.dumps(results))


def generate_plots(batches, word_count_times, pi_estimation_times, sort_times):
    generate_plot(batches, word_count_times, "word_count", 1)
    generate_plot(batches, pi_estimation_times, "pi_estimation", 2)
    generate_plot(batches, sort_times, "sort", 3)
    plt.show()

def generate_plot(batches, times, title, number):
    f = plt.figure(num=number)

    plt.bar(batches, times, color='coral')
    plt.xlabel("number of batches")
    plt.ylabel("time (s)")
    plt.title(title)
    f.show()
    f.savefig("plots/pyspark_" + title + ".png")

def run_all_benchmarks_batched(datasets, min_batches, max_batches):
    batches = list(range(min_batches, max_batches))
    word_count_times = []
    pi_estimation_times = []
    sort_times = []
    for num_partitions in range(min_batches, max_batches):
        results = {
            "word_count": (test_word_count(datasets["word_count"], num_partitions)),
            "pi_estimation": (test_pi_estimation(datasets["pi_estimation"], num_partitions)),
            "sort": (test_sort(datasets["sort"], num_partitions))
        }
        word_count_times.append(results["word_count"][1])
        pi_estimation_times.append(results["pi_estimation"][1])
        sort_times.append(results["sort"][1])
        with open("./test_results/pyspark_benchmark_result_batch_num_" + str(num_partitions) + ".json", "w") as f:
            f.write(json.dumps(results))

    generate_plots(batches, word_count_times, pi_estimation_times, sort_times)


if __name__ == "__main__":
    run_all_benchmarks(datasets_input)
    run_all_benchmarks_batched(batched_datasets_input, 1, 15)
