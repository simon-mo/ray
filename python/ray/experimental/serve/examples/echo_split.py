"""
Example of traffic splitting. We will first use echo:v1. Then v1 and v2
will split the incoming traffic evenly.
"""
import time
from pprint import pprint

import requests

from ray.experimental import serve


def echo_v1(context):
    return "v1"


def echo_v2(context):
    return "v2"


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(echo_v1, "echo:v1")
serve.link("my_endpoint", "echo:v1")

for _ in range(3):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

serve.create_backend(echo_v2, "echo:v2")
serve.split("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})
while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
