import logging
import time
from typing import Dict


logger = logging.getLogger("httpx.access")
logger.setLevel(logging.DEBUG)
STARTS: Dict[int, float] = {}


async def on_request(request):
    try:
        STARTS[id(request)] = time.perf_counter()
    except Exception:
        pass


async def on_response(response):
    try:
        start = STARTS.pop(id(response.request), None)
        dur_ms = (time.perf_counter() - start) * 1000 if start is not None else None
        size = response.headers.get("content-length")
        logger.info(
            "%s %s %s %s %s",
            response.request.method,
            str(response.request.url),
            response.status_code,
            str(size) if size is not None else "-",
            f"{dur_ms:.1f}ms" if dur_ms is not None else "-",
        )
    except Exception as e:
        logger.info(e)
