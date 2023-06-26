from aws import invoke_coordinator
import json
import matplotlib.pyplot as plt

scenarios = {}


def run_all_benchmarks(data):
    results = {}
    for k, v in data.items():
        results[k] = invoke_coordinator(data[k])
    print()
    return results


def read_file(file_path):
    with open(file_path) as f:
        return f.read()


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
    f.savefig("plots/aws_" + title + ".png")


def run_batched_scenarios(scenarios_dict, min_batches, max_batches):
    batches = list(range(min_batches, max_batches))
    word_count_times = []
    pi_estimation_times = []
    sort_times = []
    for i in range(min_batches, max_batches):
        print("running tests for num of batches = " + str(i))
        for k, v in scenarios_dict.items():
            scenarios_dict[k]["num_of_batches"] = i
        datasets_input = scenarios_dict.copy()
        for k, v in scenarios_dict.items():
            datasets_input[k] = json.dumps(datasets_input[k])
        benchmark_results = run_all_benchmarks(datasets_input)
        word_count_times.append(benchmark_results["word_count"][1])
        pi_estimation_times.append(benchmark_results["pi_estimation"][1])
        sort_times.append(benchmark_results["sort"][1])
        with open("./test_results/aws_benchmark_result_batch_num_" + str(i) + ".json", "w") as f:
            f.write(json.dumps(benchmark_results))

    generate_plots(batches, word_count_times, pi_estimation_times, sort_times)


def run_scenarios(scenarios_dict):
    datasets_input = scenarios_dict.copy()
    for k, v in scenarios_dict.items():
        datasets_input[k] = json.dumps(datasets_input[k])
    benchmark_results = run_all_benchmarks(datasets_input)
    with open("./test_results/aws_benchmark_result_non_batched.json", "w") as f:
        f.write(json.dumps(benchmark_results))


if __name__ == "__main__":
    batched_scenarios = {
        "word_count": json.loads(read_file("./test_cases/test_word_count.json")),
        "pi_estimation": json.loads(read_file("./test_cases/test_pi_estimation.json")),
        "sort": json.loads(read_file("./test_cases/test_sort.json"))
    }
    scenarios = {
        "union_group_by_value": json.loads(read_file("./test_cases/test_union_group_by_value.json")),
        "distinct_take_ordered": json.loads(read_file("./test_cases/test_distinct_take_ordered.json")),
        "test_union_group_by_value": json.loads(read_file("./test_cases/test_union_group_by_value.json"))
    }

    run_scenarios(scenarios)
    run_batched_scenarios(batched_scenarios, 1, 20)
