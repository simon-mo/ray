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
        from prometheus_client import Histogram, Counter, Gauge, start_http_server

        app = web.Application()
        routes = web.RouteTableDef()
        buckets =  (.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 
                    15.0, 20.0, 25.0, 50.0, 100.0, 150.0, 200.0, float("inf"))
        metric_hist = Histogram('response_latency_seconds', 'Response latency (seconds)', buckets=buckets)
        metric_guage = Gauge('in_progress_requests', 'Requests in progress')
        metric_counter_rcv = Counter('request_recv', 'Total requests received')
        metric_counter_done = Counter('request_done', 'Total requests finished')

        @routes.get("/")
        async def list_actors(request: web.Request):
            all_actors = await async_get(self.router.get_actors.remote())
            return web.json_response({"actors": all_actors})

        @routes.get("/{actor_name}")
        async def get_schema(request: web.Request):
            model_name = request.match_info["actor_name"]
            model_annotation = await async_get(
                self.router.get_annotation.remote(model_name)
            )
            schema = get_base_schema()
            schema["properties"]["input"]["properties"] = annotation_to_json_schema(
                model_annotation
            )
            return web.json_response(schema)

        @routes.post("/{actor_name}")
        async def call_actor(request: web.Request):
            metric_counter_rcv.inc()

            model_name = request.match_info["actor_name"]

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

            try:
                validate(data, schema)
            except ValidationError as e:
                return web.Response(text=e.__unicode__(), status=400)

            # Prepare input
            inp = data.pop("input")
            slo_seconds = data.pop("slo_ms") / 1000
            deadline = time.perf_counter() + slo_seconds
            with metric_hist.time():
                with metric_guage.track_inprogress():
                    result_future = await async_unwrap(
                        self.router.call.remote(model_name, inp, deadline)
                    )
                    result = await async_get(result_future)

            metric_counter_done.inc()

            if isinstance(result, ray.worker.RayTaskError):
                return web.json_response(
                    {"success": False, "actor": model_name, "error": str(result)}
                )
            return web.json_response(
                {"success": True, "actor": model_name, "result": result}
            )

        app.add_routes(routes)
        start_http_server(8000)
        web.run_app(app, host=self.ip, port=self.port)
