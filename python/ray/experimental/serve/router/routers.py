from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
from functools import total_ordering
from typing import Callable, Dict, List, Set, Tuple
import time
from random import random

import msgpack

import ray
from ray.experimental.serve.object_id import get_new_oid
from ray.experimental.serve.utils.priority_queue import FIFOQueue

ACTOR_NOT_REGISTERED_MSG: Callable = (
    lambda name: (
        "Actor {} is not registered with this router. Please use "
        "'router.register_actor.remote(...)' "
        "to register it."
    ).format(name)
)


# Use @total_ordering so we can sort SingleQuery
@total_ordering
class SingleQuery:
    """A data container for a query.

    Attributes:
        data: The request data.
        result_object_id: The result object ID.
        deadline: The deadline in seconds.
    """

    def __init__(
        self, data, result_object_id: ray.ObjectID, deadline_s: float, req_id, send_time
    ):
        self.data = data
        self.result_object_id = result_object_id
        self.deadline = deadline_s
        self.req_id = req_id
        self.send_time = send_time

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __eq__(self, other):
        return self.deadline == other.deadline


@ray.remote(num_cpus=1, resources={"host": 1})
class DeadlineAwareRouter:
    """DeadlineAwareRouter is a router that is aware of deadlines.

    It takes into consideration the deadline attached to each query. It will
    reorder incoming query based on their deadlines.
    """

    def __init__(self, router_name, load_filename):
        # Runtime Data
        self.query_queues: Dict[str, FIFOQueue] = defaultdict(FIFOQueue)
        self.running_queries: Dict[ray.ObjectID, ray.actor.ActorHandle] = {}
        self.actor_handles: Dict[str, List[ray.actor.ActorHandle]] = (defaultdict(list))

        # Actor Metadata
        self.managed_actors: Dict[str, ray.actor.ActorClass] = {}
        self.actor_init_arguments: Dict[str, Tuple[List, Dict]] = {}
        self.max_batch_size: Dict[str, int] = {}

        # map resource_bundle_id -> (actor_name: str, idx_in_lst: int)
        self.resource_id_to_actors: Dict[int, tuple] = {}
        self.warmup_actors: Dict[int, Tuple[ray.actor.ActorHandle, str]] = {}

        # Router Metadata
        self.name = router_name
        self.handle = None

        # Fractional actor
        self.fractional_actor_handle = None
        self.fractional_actor_frac = None
        self.fractional_actor_sleep = None

        # Metis specific
        self.metis_model = None
        self.metis_load_file_unpacker = msgpack.Unpacker(open(load_filename, "rb"))
        self.next_load_item = None
        self.plasma_client = ray.worker.global_worker.plasma_client.orig_obj
        self.total_queries = 0

    @ray.method(num_return_vals=0)
    def start(self):
        """Kick off the router loop"""

        # Note: This is meant for hiding the complexity for a user
        #       facing method.
        #       Because the `loop` api can be hard to understand.
        self.handle = ray.experimental.get_actor(self.name)
        self.handle.loop.remote()

    def register_actor(
        self,
        actor_name: str,
        actor_class: ray.actor.ActorClass,
        init_args: List = [],
        init_kwargs: dict = {},
        num_replicas: int = 1,
        max_batch_size: int = -1,  # Unbounded batch size
    ):
        """Register a new managed actor.
        """
        self.managed_actors[actor_name] = actor_class
        self.actor_init_arguments[actor_name] = (init_args, init_kwargs)
        self.max_batch_size[actor_name] = max_batch_size

        self.metis_model = actor_name

    @ray.method(num_return_vals=0)
    def add_replica_handle(self, actor_name, actor_handle, bundle_id):
        self.resource_id_to_actors[bundle_id] = (actor_name, actor_handle)
        # promote new actors to the front of the queue
        self.actor_handles[actor_name].insert(0, actor_handle)

    @ray.method(num_return_vals=0)
    def mark_replica_fractional(self, bundle_id, frac, sleep_time):
        self.fractional_actor_handle = self.resource_id_to_actors[bundle_id][1]
        self.fractional_actor_frac = frac
        self.fractional_actor_sleep = sleep_time

    # this function is called in a blocking maneer
    # so we can't decorate with num_return_vals=0
    def remove_replica(self, resource_bundle_id):
        actor_name, actor_handle = self.resource_id_to_actors.pop(resource_bundle_id)
        self.actor_handles[actor_name].remove(actor_handle)
        # del will be handled by controller
        # del actor_handle

    def get_metric(self, model_name):
        num_replicas = len(self.actor_handles[model_name])
        if self.fractional_actor_handle:
            num_replicas -= 1 - self.fractional_actor_frac
        return {
            "q_len": len(self.query_queues[model_name]),
            "num_replicas": num_replicas,
        }

    # @ray.method(num_return_vals=0)
    # def call(self, actor_name, data, result_object_id, deadline_s, req_id, send_time):
    #     """Enqueue a request to one of the actor managed by this router.

    #     Returns:
    #         List[ray.ObjectID] with length 1, the object ID wrapped inside is
    #             the result object ID when the query is executed.
    #     """
    #     assert actor_name in self.managed_actors, ACTOR_NOT_REGISTERED_MSG(actor_name)

    #     # result_object_id = get_new_oid()

    #     # Here, 'data_object_id' is either an ObjectID or an actual object.
    #     # When it is an ObjectID, this is an optimization to avoid creating
    #     # an extra copy of 'data' in the object store.
    #     # data_object_id = ray.worker.global_worker._current_task.arguments()[1]

    #     for obj in [data, result_object_id]:
    #         assert isinstance(obj, bytes), "data is type {}: {}".format(type(obj), obj)
    #     # assert isinstance(data, list) and len(data) == 1 and isinstance(data[0], ray.ObjectID)
    #     data_object_id = data

    #     self.query_queues[actor_name].push(
    #         SingleQuery(data_object_id, result_object_id, deadline_s, req_id, send_time)
    #     )

    def _check_send_signal_once(self):
        if self.next_load_item == None:
            try:
                self.next_load_item = next(self.metis_load_file_unpacker)
            except StopIteration:
                return False

        oid_bytes = self.next_load_item[b"send_signal"]
        arrow_oid = ray.pyarrow.plasma.ObjectID(oid_bytes)
        if self.plasma_client.contains(arrow_oid):
            sg = SingleQuery(
                b"", #self.next_load_item[b"inp"],
                self.next_load_item[b"out"],
                1.0,
                self.next_load_item[b"id"],
                self.plasma_client.get(arrow_oid),
            )
            self.query_queues[self.metis_model].push(sg)

            self.next_load_item = None
            return True
        else:
            return False

    def _check_send_signal(self):
        count = 0
        while count < 400 and self._check_send_signal_once():
            count += 1
            pass

    @ray.method(num_return_vals=0)
    def loop(self):
        """Main loop for router. It will does the following things:

        1. Check which running actors finished.
        2. Iterate over free actors and request queues, dispatch requests batch
           to free actors.
        3. Tail recursively schedule itself.
        """
        # 0. Receive from query frontend
        self._check_send_signal()

        # 1. Check which running actors finished.
        ready_oids, _ = ray.wait(
            object_ids=list(self.running_queries.keys()),
            num_returns=len(self.running_queries),
            timeout=0.003, # 3ms
        )

        for ready_oid in ready_oids:
            self.running_queries.pop(ready_oid)
        busy_actors: Set[ray.actor.ActorHandle] = set(self.running_queries.values())

        # 2. Iterate over free actors and request queues, dispatch requests
        #    batch to free actors.

        for actor_name, queue in self.query_queues.items():
            # try to drain the queue
            for actor_handle in self.actor_handles[actor_name]:
                if len(queue) == 0:
                    break
                if actor_handle in busy_actors:
                    continue

                if (
                    actor_handle == self.fractional_actor_handle
                    and random() > self.fractional_actor_frac
                ):
                    result_oid = actor_handle._sleep.remote(self.fractional_actor_sleep)
                    self._mark_running(result_oid, actor_handle)
                    continue

                batch = self._get_next_batch(actor_name)
                assert len(batch) == 1, len(batch)

                actor_handle._dispatch.remote(batch)
                result_oid = ray.ObjectID.from_binary(batch[0]["result_object_id"])
                self._mark_running(result_oid, actor_handle)

        # 3. Tail recursively schedule itself.
        self.handle.loop.remote()

    def _get_next_batch(self, actor_name: str) -> List[dict]:
        """Get next batch of request for the actor whose name is provided."""
        assert actor_name in self.query_queues, ACTOR_NOT_REGISTERED_MSG(actor_name)

        inputs = []
        batch_size = self.max_batch_size[actor_name]
        if batch_size == -1:
            inp = self.query_queues[actor_name].try_pop()
            while inp:
                inputs.append(self._simplify_single_query(inp))
                inp = self.query_queues[actor_name].try_pop()
        else:
            for _ in range(batch_size):
                inp = self.query_queues[actor_name].try_pop()
                if inp:
                    inputs.append(self._simplify_single_query(inp))
                else:
                    break

        return inputs

    def _simplify_single_query(self, q: SingleQuery):
        d = q.__dict__
        d["dequeue_time"] = time.time()
        return d

    def _mark_running(
        self, batch_oid: ray.ObjectID, actor_handle: ray.actor.ActorHandle
    ):
        """Mark actor_handle as running identified by batch_oid.

        This means that if batch_oid is fullfilled, then actor_handle must be
        free.
        """
        self.running_queries[batch_oid] = actor_handle
