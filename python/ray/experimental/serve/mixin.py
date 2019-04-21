from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import traceback
from typing import List
import time
import sys

import ray
from ray.experimental.serve import SingleQuery


def batched_input(func):
    """Decorator to mark an actor method as accepting only a single input.

    By default methods accept a batch.
    """
    func.ray_serve_batched_input = True
    return func


def _execute_and_seal_error(method, arg, method_name):
    """Execute method with arg and return the result.

    If the method fails, return a RayTaskError so it can be sealed in the
    resultOID and retried by user.
    """
    try:
        return method(arg)
    except Exception as e:
        print(e)
        return ray.worker.RayTaskError(method_name, traceback.format_exc())


class RayServeMixin:
    """Enable a ray actor to interact with ray.serve

    Usage:
    ```
        @ray.remote
        class MyActor(RayServeMixin):
            # This is optional, by default it is "__call__"
            serve_method = 'my_method'

            def my_method(self, arg):
                ...
    ```
    """

    serve_method = "__call__"
    put_timing_data_instead = False

    def _sleep(self, nap_time):
        time.sleep(nap_time)

    @ray.method(num_return_vals=0)
    def _dispatch(self, input_batch: List[SingleQuery]):
        """Helper method to dispatch a batch of input to self.serve_method."""
        method = getattr(self, self.serve_method)
        is_batched = hasattr(method, "ray_serve_batched_input")
        assert not is_batched

        begin_ray_get = time.time()

        if self.put_timing_data_instead:
            assert not is_batched, "Put timing data assumes single input"

        assert len(input_batch) == 1, "Assuming batchsize 1"
        inp = input_batch[0]

        data = ray.get(ray.ObjectID.from_binary(inp["data"]))
        end_ray_get = time.time()

        result_object_id = ray.ObjectID.from_binary(inp["result_object_id"])

        if is_batched:
            batch = [inp.data for inp in input_batch]
            result = _execute_and_seal_error(method, batch, self.serve_method)
            for res, inp in zip(result, input_batch):
                ray.worker.global_worker.put_object(inp.result_object_id, res)
        else:
            for inp in input_batch:
                # We are in this case only
                result = _execute_and_seal_error(method, data, self.serve_method)
                if self.put_timing_data_instead:
                    inp["model_rcvd"] = begin_ray_get
                    inp["model_ray_get"] = end_ray_get
                    inp["model_done"] = time.time()
                inp.pop("data")
                inp.pop("result_object_id")
                inp.pop("deadline")
                result = inp
                ray.worker.global_worker.put_object(result_object_id, result)
