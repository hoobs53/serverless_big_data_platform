from aws import invoke_coordinator
import json
import matplotlib.pyplot as plt


scenarios = {}


def run_all_benchmarks(data):
    results = {"word_count": (invoke_coordinator(data["word_count"])),
               "pi_estimation": (invoke_coordinator(data["pi_estimation"])),
               "sort": (invoke_coordinator(data["sort"]))}
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
    f.savefig("plots/" + title + ".png")


def run_all_scenarios(min_batches, max_batches):
    batches = list(range(min_batches, max_batches))
    word_count_times = []
    pi_estimation_times = []
    sort_times = []
    for i in range(min_batches, max_batches):
        print("running tests for num of batches = " + str(i))
        for k, v in scenarios.items():
            scenarios[k]["num_of_batches"] = i
        datasets_input = scenarios.copy()
        for k, v in scenarios.items():
            datasets_input[k] = json.dumps(datasets_input[k])
        benchmark_results = run_all_benchmarks(datasets_input)
        word_count_times.append(benchmark_results["word_count"][1])
        pi_estimation_times.append(benchmark_results["pi_estimation"][1])
        sort_times.append(benchmark_results["sort"][1])
        with open("./test_results/aws_benchmark_result_batch_num_" + str(i) + ".json", "w") as f:
            f.write(json.dumps(benchmark_results))

    generate_plots(batches, word_count_times, pi_estimation_times, sort_times)


if __name__ == "__main__":
    scenarios = {
        "word_count": json.loads(read_file("./test_cases/test_word_count.json")),
        "pi_estimation": json.loads(read_file("./test_cases/test_pi_estimation.json")),
        "sort": json.loads(read_file("./test_cases/test_sort.json"))
    }

    run_all_scenarios(1, 11)
