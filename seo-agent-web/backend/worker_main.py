from __future__ import annotations

import os
import signal
import time


def main() -> int:
    # Ensure the in-app worker is enabled in this process.
    os.environ["SEO_AGENT_DISABLE_WORKER"] = "false"

    # Importing `backend.app` registers all worker helpers and initializes DB/env.
    from backend import app as app_module  # type: ignore

    def _stop(*_args) -> None:
        try:
            app_module._WORKER_STOP.set()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    print("[WORKER_MAIN] starting job worker…")
    app_module._start_job_worker()
    app_module._start_retention()

    while not app_module._WORKER_STOP.is_set():
        time.sleep(1.0)

    print("[WORKER_MAIN] stopping.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
