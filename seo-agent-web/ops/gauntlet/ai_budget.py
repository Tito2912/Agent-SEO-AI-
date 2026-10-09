"""A process-local, thread-safe limit on actual Claude calls in fixture benches."""

from __future__ import annotations

from threading import Lock


class ClaudeBudget:
    def __init__(self, limit: int):
        if not 0 <= limit <= 100:
            raise ValueError("Claude call limit must be between 0 and 100.")
        self.limit = limit
        self.attempted = 0
        self.denied = 0
        self._lock = Lock()

    def install(self, backend) -> None:
        original = backend._anthropic_messages_text

        def bounded(**kwargs):
            with self._lock:
                if self.attempted >= self.limit:
                    self.denied += 1
                    raise RuntimeError("Fixture Claude call budget exhausted.")
                self.attempted += 1
            return original(**kwargs)

        def no_fallback(**kwargs):
            raise RuntimeError("Fixture bench allows Claude only.")

        backend._anthropic_messages_text = bounded
        backend._openai_chat_text = no_fallback

    def summary(self) -> dict:
        with self._lock:
            return {"limit": self.limit, "attempted": self.attempted,
                    "denied": self.denied, "exhausted": bool(self.denied)}
