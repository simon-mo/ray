from ray.experimental.serve.frontend import HTTPFrontendActor
from ray.experimental.serve.router import DeadlineAwareRouter, start_router
from ray.experimental.serve.object_id import unwrap
from ray.experimental import named_actors
import ray

import time
from typing import List

_default_router = None
_init_called = False
_default_frontend_handle = None


def init_if_not_ready():
    global _init_called, _default_router, _default_frontend_handle
    if _init_called:
        return
    _init_called = True
    _default_router = start_router(DeadlineAwareRouter, "DefaultRouter")
    _default_frontend_handle = HTTPFrontendActor.remote()
    _default_frontend_handle.start.remote()

    named_actors.register_actor("DefaultHTTPFrontendActor", _default_frontend_handle)


def register_actor(
    actor_name: str,
    actor_class: ray.actor.ActorClass,
    init_args: List = [],
    init_kwargs: dict = {},
    num_replicas: int = 1,
    max_batch_size: int = -1,  # Unbounded batch size
    annotation: dict = {},
):
    """Register a new managed actor.
    """
    assert isinstance(actor_class, ray.actor.ActorClass)
    init_if_not_ready()

    inner_cls = actor_class._modified_class
    annotation.update(getattr(inner_cls, inner_cls.serve_method).__annotations__)
    ray.get(
        _default_router.register_actor.remote(
            actor_name,
            actor_class,
            init_args,
            init_kwargs,
            num_replicas,
            max_batch_size,
            annotation,
        )
    )
    return "http://0.0.0.0:8090/{}".format(actor_name)


def set_replica(actor_name, new_replica_count):
    """Scale a managed actor according to new_replica_count."""
    init_if_not_ready()

    ray.get(_default_router.set_replica.remote(actor_name, new_replica_count))


def call(actor_name, data, deadline_s=None):
    """Enqueue a request to one of the actor managed by this router.

    Returns the result. If you want the ObjectID to be returned immediately,
    use enqueue
    """
    init_if_not_ready()

    if deadline_s is None:
        deadline_s = time.time() + 10
    return ray.get(enqueue(actor_name, data, deadline_s))


def enqueue(actor_name, data, deadline_s=None):
    """Enqueue a request to one of the actor managed by this router.

    Returns:
        ray.ObjectID, the result object ID when the query is executed.
    """
    init_if_not_ready()

    if deadline_s is None:
        deadline_s = time.time() + 10
    return unwrap(_default_router.call.remote(actor_name, data, deadline_s))
