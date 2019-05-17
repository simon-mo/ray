from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import pytest
import requests

import ray
from ray.experimental.serve import DeadlineAwareRouter
from ray.experimental.serve.example_actors import TimesThree
from ray.experimental.serve.frontend import HTTPFrontendActor
from ray.experimental.serve.router import start_router

ROUTER_NAME = "DefaultRouter"
NUMBER_OF_TRIES = 5


@pytest.fixture(scope="module")
def get_router():
    # We need this many workers so resource are not oversubscribed
    ray.init(num_cpus=4)
    router = start_router(DeadlineAwareRouter, ROUTER_NAME)
    yield router
    ray.shutdown()


def test_http_basic(get_router):
    router = get_router
    a = HTTPFrontendActor.remote(router=ROUTER_NAME)
    frontend_crashed = a.start.remote()

    router.register_actor.remote("TimesThree", TimesThree)

    for _ in range(NUMBER_OF_TRIES):
        try:
            url = "http://0.0.0.0:8090/TimesThree"
            payload = {"input": {"inp": 3}, "slo_ms": 1000}
            resp = requests.request("POST", url, json=payload)
        except Exception:
            # it is possible that the actor is not yet instantiated
            time.sleep(1)

    print(resp.text)
    assert resp.json() == {"success": True, "actor": "TimesThree", "result": 9}

    url = "http://0.0.0.0:8090/"
    resp = requests.request("POST", url)
    print(resp.text)
    assert resp.json() == {"actors": ["TimesThree"]}
