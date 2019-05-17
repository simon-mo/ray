from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from aiohttp import web
from jsonschema import validate, ValidationError

import ray
from ray.experimental.serve.frontend.util import (
    async_get,
    async_unwrap,
    annotation_to_json_schema,
    get_base_schema,
)
from ray.experimental import async_api


@ray.remote
class HTTPFrontendActor:
    """HTTP API for an Actor. This exposes /{actor_name} endpoint for query.

    Request:
        GET /{actor_name} or POST /{actor_name}
        Content-type: application/json
        {
            "slo_ms": float,
            "input": any
        }
    Response:
        Content-type: application/json
        {
            "success": bool,
            "actor": str,
            "result": any
        }
    """

    def __init__(self, ip="0.0.0.0", port=8090, router="DefaultRouter"):
        self.ip = ip
        self.port = port
        self.router = ray.experimental.named_actors.get_actor(router)

        async_api.init()

    def start(self):
        app = web.Application()
        routes = web.RouteTableDef()

        @routes.post("/{actor_name:[^{}/]*}")
        async def hello(request: web.Request):
            # Make sure model name exists
            model_name = request.match_info["actor_name"]
            if len(model_name) == 0:
                all_actors = ray.get(self.router.get_actors.remote())
                return web.json_response({"actors": all_actors})

            # Get the annotation
            try:
                model_annotation = await async_get(
                    self.router.get_annotation.remote(model_name)
                )
            except ray.worker.RayTaskError as e:
                return web.Response(text=str(e), status=400)

            data = await request.json()

            # Validate Schema
            schema = get_base_schema()
            schema["properties"]["input"]["properties"] = annotation_to_json_schema(
                model_annotation
            )
            print(schema)
            try:
                validate(data, schema)
            except ValidationError as e:
                return web.Response(text=e.__unicode__(), status=400)

            # Prepare input
            inp = data.pop("input")
            slo_seconds = data.pop("slo_ms") / 1000
            deadline = time.perf_counter() + slo_seconds
            result_future = await async_unwrap(
                self.router.call.remote(model_name, inp, deadline)
            )
            result = await async_get(result_future)

            return web.json_response(
                {"success": True, "actor": model_name, "result": result}
            )

        app.add_routes(routes)
        web.run_app(app, host=self.ip, port=self.port)
