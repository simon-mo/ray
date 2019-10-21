from collections import defaultdict, deque

import numpy as np

import ray
from ray.experimental.serve.utils import logger


class Query:
    def __init__(self,
                 request_args,
                 request_kwargs,
                 request_context,
                 result_object_id=None):
        self.request_args = request_args
        self.request_kwargs = request_kwargs
        self.request_context = request_context

        if result_object_id is None:
            self.result_object_id = ray.ObjectID.from_random()
        else:
            self.result_object_id = result_object_id


class WorkIntent:
    def __init__(self, replica_handle):
        self.replica_handle = replica_handle


class CentralizedQueues:
    """A router that routes request to available workers.

    Router aceepts each request from the `enqueue_request` method and enqueues
    it. It also accepts worker request to work (called work_intention in code)
    from workers via the `dequeue_request` method. The traffic policy is used
    to match requests with their corresponding workers.

    Behavior:
        >>> # psuedo-code
        >>> queue = CentralizedQueues()
        >>> queue.enqueue_request(
            "service-name", request_args, request_kwargs, request_context)
        # nothing happens, request is queued.
        # returns result ObjectID, which will contains the final result
        >>> queue.dequeue_request('backend-1')
        # nothing happens, work intention is queued.
        # return work ObjectID, which will contains the future request payload
        >>> queue.link('service-name', 'backend-1')
        # here the enqueue_requester is matched with worker, request
        # data is put into work ObjectID, and the worker processes the request
        # and store the result into result ObjectID

    Traffic policy splits the traffic among different workers
    probabilistically:

    1. When all backends are ready to receive traffic, we will randomly
       choose a backend based on the weights assigned by the traffic policy
       dictionary.

    2. When more than 1 but not all backends are ready, we will normalize the
       weights of the ready backends to 1 and choose a backend via sampling.

    3. When there is only 1 backend ready, we will only use that backend.
    """

    def __init__(self):
        # service_name -> request queue
        self.queues = defaultdict(deque)

        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)

        # backend_name -> worker request queue
        self.workers = defaultdict(deque)

        # backend_name -> worker payload queue
        self.buffer_queues = defaultdict(deque)
        # replica_actor_id -> replica_actor_handle
        self.replica_handles = dict()

    def is_ready(self):
        return True

    def _serve_metric(self):
        return {
            "backend_{}_queue_size".format(backend_name): {
                "value": len(queue),
                "type": "counter",
            }
            for backend_name, queue in self.buffer_queues.items()
        }

    def enqueue_request(self, service, request_args, request_kwargs,
                        request_context):
        query = Query(request_args, request_kwargs, request_context)
        self.queues[service].append(query)
        self.flush()
        return query.result_object_id.binary()

    def dequeue_request(self, backend, replica_handle):
        intention = WorkIntent(replica_handle)
        self.workers[backend].append(intention)
        self.flush()

    def link(self, service, backend):
        logger.debug("Link %s with %s", service, backend)
        self.traffic[service][backend] = 1.0
        self.flush()

    def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        self.flush()

    def flush(self):
        """In the default case, flush calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        self._flush()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = {
            backend
            for backend, queues in self.workers.items() if len(queues) > 0
        }
        return list(backends_in_policy.intersection(available_workers))

    def _flush(self):
        # perform traffic splitting for requests
        for service, queue in self.queues.items():
            # while there are incoming requests and there are backends
            while len(queue) and len(self.traffic[service]):
                backend_names = list(self.traffic[service].keys())
                backend_weights = list(self.traffic[service].values())
                chosen_backend = np.random.choice(
                    backend_names, p=backend_weights).squeeze()

                request = queue.popleft()
                self.buffer_queues[chosen_backend].append(request)

        # distach buffer queues to work queues
        for service in self.queues.keys():
            ready_backends = self._get_available_backends(service)
            for backend in ready_backends:
                # no work available
                if len(self.buffer_queues[backend]) == 0:
                    continue

                buffer_queue = self.buffer_queues[backend]
                work_queue = self.workers[backend]
                while len(buffer_queue) and len(work_queue):
                    request, work = (
                        buffer_queue.popleft(),
                        work_queue.popleft(),
                    )
                    ray.worker.global_worker.put_object(
                        request, work.work_object_id)


@ray.remote
class CentralizedQueuesActor(CentralizedQueues):
    self_handle = None

    def register_self_handle(self, handle_to_this_actor):
        self.self_handle = handle_to_this_actor

    def flush(self):
        if self.self_handle:
            self.self_handle._flush.remote()
        else:
            self._flush()
