import time
import pytest
import ray
import requests


def test_simple():
    ray.init(num_cpus=4)
    import ray.experimental.serve as srv
    from ray.experimental.serve.example_actors import TimesThree

    actor_name = "Times3"
    assert srv.register_actor(
        actor_name, TimesThree
    ) == "http://0.0.0.0:8090/{}".format(actor_name)

    srv.set_replica(actor_name, 2)

    assert srv.call(actor_name, 3) == 9
    assert ray.get(srv.enqueue(actor_name, 4, time.time() + 10)) == 12

    url = "http://0.0.0.0:8090/Times3"
    payload = {"input": {"inp": 5}, "slo_ms": 1000}
    resp = requests.request("POST", url, json=payload)
    assert resp.json() == {"success": True, "actor": "Times3", "result": 15}

    ray.shutdown()
