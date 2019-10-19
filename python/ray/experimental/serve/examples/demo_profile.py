"""
Example service that prints out http context.
"""

import time

import requests

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json


def echo(flask_request):
    time.sleep(0.1) # sleep to 0.1 seconds
    return "done"


serve.init(blocking=True)
serve.create_endpoint("my_endpoint", "/sleep", blocking=True)
serve.create_backend(echo, "echo:v1")
serve.link("my_endpoint", "echo:v1")
serve.scale("echo:v1", 4)

while True:
    time.sleep(1000)
