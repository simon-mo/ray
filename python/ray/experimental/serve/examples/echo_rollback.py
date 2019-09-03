"""
Example rollback action in ray serve. Here we first deploy v1, and then
a 50/50 deployment was set, lastly we rollback to v1
"""
import time

import requests

from ray.experimental import serve
from utils import pprint_color_json


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
    pprint_color_json(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

serve.create_backend(echo_v2, "echo:v2")
serve.split("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})

for _ in range(6):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint_color_json(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

serve.rollback("my_endpoint")
for _ in range(6):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint_color_json(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
