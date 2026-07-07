"""A tiny fake service that appends log lines to a file — the source for the shipper-agent
sidecar demo (see the `sidecar` compose profile)."""

import os
import random
import time

_LEVELS = ["INFO"] * 7 + ["WARNING", "ERROR"]
_MSGS = [
    "request completed", "cache miss", "db query slow", "user login ok",
    "payment failed", "retry after timeout", "connection reset", "healthcheck ok",
]


def main() -> None:
    path = os.getenv("DEMO_LOG", "/shared/app.log")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    while True:
        line = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {random.choice(_LEVELS)} {random.choice(_MSGS)}\n"
        with open(path, "a") as f:
            f.write(line)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
