import time
from typing import Optional

import requests
import pandas as pd
from tqdm import tqdm
import click

from ray import serve
from ray.serve.constants import DEFAULT_HTTP_ADDRESS


def block_until_ready(url):
    while requests.get(url).status_code == 404:
        time.sleep(1)
        print("Waiting for noop route to showup.")


def run_http_benchmark(url):
    latency = []
    for _ in tqdm(range(5200)):
        start = time.perf_counter()
        resp = requests.get(url)
        end = time.perf_counter()
        latency.append(end - start)

    # Remove initial samples
    latency = latency[200:]

    series = pd.Series(latency) * 1000
    print("Latency for single noop backend (ms)")
    print(series.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))


@click.command()
@click.option("--blocking", is_flag=True, required=False, help="Block forever")
@click.option("--num-replicas", type=int, default=1)
@click.option("--max-concurrent-queries", type=int, required=False)
def main(num_replicas: int, max_concurrent_queries: Optional[int],
         blocking: bool):
    serve.init()

    def noop(_):
        return "hello world"

    config = {
        "num_replicas": num_replicas,
        "max_concurrent_queries": max_concurrent_queries
    }
    print("Using config", config)
    serve.create_backend("noop", noop, config=config)
    serve.create_endpoint("noop", backend="noop", route="/noop")

    url = "{}/noop".format(DEFAULT_HTTP_ADDRESS)
    block_until_ready(url)

    if blocking:
        print("Endpoint {} is ready.".format(url))
        while True:
            time.sleep(5)
    else:
        run_http_benchmark(url)


if __name__ == "__main__":
    main()
