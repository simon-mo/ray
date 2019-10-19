"""
Let this file run.
And in a new terminal, run ApacheBench
   ab -c 20 -n 60 http://127.0.0.1:8000/sleep

Ploting code

import pandas as pd
import matplotlib.pyplot as plt
df = pd.read_json('/tmp/serve_profile.jsonl', lines=True)
for i, row in df.iterrows():
    plt.plot([row['start'], row['end']], [i,i])
plt.xlabel("UNIX Timestamp")
plt.ylabel("Query ID")
plt.title("Handling ab -c 20 -n 60 http://127.0.0.1:8000/sleep")
plt.savefig("profile.png")
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
