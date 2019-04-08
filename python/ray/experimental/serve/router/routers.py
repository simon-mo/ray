from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
from functools import total_ordering
from typing import Callable, Dict, List, Set, Tuple
import time

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

    def __init__(self, data, result_object_id: ray.ObjectID, deadline_s: float,
                req_id, send_time
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


@ray.remote
class DeadlineAwareRouter:
    """DeadlineAwareRouter is a router that is aware of deadlines.

    It takes into consideration the deadline attached to each query. It will
    reorder incoming query based on their deadlines.
    """

    def __init__(self, router_name, debug=False):
        # Runtime Data
        self.query_queues: Dict[str, PriorityQueue] = defaultdict(FIFOQueue)
        self.running_queries: Dict[ray.ObjectID, ray.actor.ActorHandle] = {}
        self.actor_handles: Dict[str, List[ray.actor.ActorHandle]] = (defaultdict(list))

        # Actor Metadata
        self.managed_actors: Dict[str, ray.actor.ActorClass] = {}
        self.actor_init_arguments: Dict[str, Tuple[List, Dict]] = {}
        self.max_batch_size: Dict[str, int] = {}

        # map resource_bundle_id -> (actor_name: str, idx_in_lst: int)
        self.resource_id_to_actors: Dict[int, tuple] = {}

        # Router Metadata
        self.name = router_name
        self.handle = None
        self.debug = debug

    @ray.method(num_return_vals=0)
    def start(self):
        """Kick off the router loop"""

        # Note: This is meant for hiding the complexity for a user
        #       facing method.
        #       Because the `loop` api can be hard to understand.
        self.handle = ray.experimental.get_actor(self.name)
        self.handle.loop.remote()

    @ray.method(num_return_vals=0)
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

        # ray.experimental.get_actor(self.name).set_replica.remote(
        #     actor_name, num_replicas
        # )

    # def set_replica(self, actor_name, new_replica_count):
    #     """Scale a managed actor according to new_replica_count."""
    #     assert actor_name in self.managed_actors, ACTOR_NOT_REGISTERED_MSG(actor_name)

    #     current_replicas = len(self.actor_handles[actor_name])

    #     # Increase the number of replicas
    #     if new_replica_count > current_replicas:
    #         for _ in range(new_replica_count - current_replicas):
    #             args = self.actor_init_arguments[actor_name][0]
    #             kwargs = self.actor_init_arguments[actor_name][1]
    #             new_actor_handle = self.managed_actors[actor_name].remote(
    #                 *args, **kwargs
    #             )
    #             self.actor_handles[actor_name].append(new_actor_handle)

    #     # Decrease the number of replicas
    #     if new_replica_count < current_replicas:
    #         for _ in range(current_replicas - new_replica_count):
    #             # Note actor destructor will be called after all remaining
    #             # calls finish. Therefore it's safe to call del here.
    #             del self.actor_handles[actor_name][-1]
    
    @ray.method(num_return_vals=0)
    def add_replica(self, 
        actor_name, 
        num_cpus,
        resource_vector, 
        resource_bundle_id,
        init_args=None, 
        init_kwargs=None
        ):
        if init_args == None: 
            init_args = []
        if init_kwargs == None: 
            init_kwargs = dict()
        
        new_actor_handle = self.managed_actors[actor_name]._remote(
            args=init_args, 
            kwargs=init_kwargs,
            num_cpus=num_cpus,
            resources=resource_vector
        )

        idx = len(self.actor_handles)
        self.actor_handles[actor_name].append(new_actor_handle)
        self.resource_id_to_actors[resource_bundle_id] = (actor_name, idx)

    @ray.method(num_return_vals=0)
    def remove_replica(self, resource_bundle_id):
        actor_name, idx = self.resource_id_to_actors.pop(resource_bundle_id)
        actor_found = self.actor_handles[actor_name].pop(idx)
        del actor_found
        

    @ray.method(num_return_vals=0)
    def call(self, actor_name, data, result_object_id, deadline_s, req_id, send_time):
        """Enqueue a request to one of the actor managed by this router.

        Returns:
            List[ray.ObjectID] with length 1, the object ID wrapped inside is
                the result object ID when the query is executed.
        """
        assert actor_name in self.managed_actors, ACTOR_NOT_REGISTERED_MSG(actor_name)

        # result_object_id = get_new_oid()

        # Here, 'data_object_id' is either an ObjectID or an actual object.
        # When it is an ObjectID, this is an optimization to avoid creating
        # an extra copy of 'data' in the object store.
        # data_object_id = ray.worker.global_worker._current_task.arguments()[1]

        for obj in [data, result_object_id]:
            assert isinstance(obj, bytes), "data is type {}: {}".format(type(obj), obj)
        # assert isinstance(data, list) and len(data) == 1 and isinstance(data[0], ray.ObjectID)
        data_object_id = data

        self.query_queues[actor_name].push(
            SingleQuery(data_object_id, result_object_id, deadline_s, req_id, send_time)
        )

    @ray.method(num_return_vals=0)
    def loop(self):
        """Main loop for router. It will does the following things:

        1. Check which running actors finished.
        2. Iterate over free actors and request queues, dispatch requests batch
           to free actors.
        3. Tail recursively schedule itself.
        """

        # 1. Check which running actors finished.
        ready_oids, _ = ray.wait(
            object_ids=list(self.running_queries.keys()),
            num_returns=len(self.running_queries),
            timeout=0,
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
        d['dequeue_time'] = time.time()
        return d

    def _mark_running(
        self, batch_oid: ray.ObjectID, actor_handle: ray.actor.ActorHandle
    ):
        """Mark actor_handle as running identified by batch_oid.

        This means that if batch_oid is fullfilled, then actor_handle must be
        free.
        """
        self.running_queries[batch_oid] = actor_handle
