"""A module for serving from actors.

The ray.experimental.serve module is a module for publishing your actors to
interact with the outside world.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

assert sys.version_info >= (3, 5), "ray.experimental.serve is a python3 only library"

from ray.experimental.serve.router import (
    DeadlineAwareRouter,
    SingleQuery,
    start_router,
)  # noqa: E402
from ray.experimental.serve.frontend import HTTPFrontendActor  # noqa: E402
from ray.experimental.serve.mixin import (
    RayServeMixin,
    RayServable,
    batched_input,
)  # noqa: E402
from ray.experimental.serve.defaults import enqueue, call, register_actor, set_replica

__all__ = [
    "DeadlineAwareRouter",
    "SingleQuery",
    "HTTPFrontendActor",
    "RayServeMixin",
    "batched_input",
    "enqueue",
    "call",
    "register_actor",
    "set_replica",
]
