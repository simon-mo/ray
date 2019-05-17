from ray.experimental import async_api
import ray


async def async_get(future: ray.ObjectID):
    result = await async_api.as_future(future)
    return result


async def async_unwrap(future: ray.ObjectID):
    """Unwrap the result from ray.experimental.server router.
    Router returns a list of object ids when you call them.
    """
    result = await async_get(future)
    return result[0]


def get_base_schema():
    return {
        "type": "object",
        "properties": {
            "slo_ms": {
                "type": "number",
                "description": "Service Level Objective (SLO) for this query, in ms",
            },
            "input": {
                "type": "object",
                "description": "The input to the model. Should be JSON object with key as keyword argument.",
            },
        },
    }


def annotation_to_json_schema(a):
    mapping = {int: "integer", float: "number", str: "string", bool: "boolean"}
    return {k: {"type": mapping[v]} for k, v in a.items()}
