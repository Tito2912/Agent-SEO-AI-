"""Capture only owned fixture sources, never provider payloads or customer code."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from threading import Lock


class FixtureSourceCapture:
    def __init__(self, backend, root: Path, *, stack: str, paths: set[str]):
        prefix, suffix = {"hugo": ("content/gauntlet/", ".md"),
                          "nuxt": ("pages/gauntlet/", ".vue")}.get(stack, ("", ""))
        if (not prefix or not paths or len(paths) > 8 or any(
                not path.startswith(prefix) or not path.endswith(suffix)
                or len(PurePosixPath(path).parts) != 3 or ".." in PurePosixPath(path).parts
                for path in paths)):
            raise ValueError("Only up to eight owned Hugo/Nuxt gauntlet sources may be captured.")
        self.backend, self.root, self.paths = backend, root, set(paths)
        self.host = f"noyaru-stack-{stack}.netlify.app"
        self.rows: list[dict] = []
        self.lock = Lock()

    def _save(self, path: str, phase: str, source: str, refusal=None):
        with self.lock:
            number = len(self.rows) + 1
            name = f"{number:03d}-{phase}-{PurePosixPath(path).name}"
            self.root.mkdir(parents=True, exist_ok=True)
            (self.root / name).write_text(source, encoding="utf-8", newline="")
            self.rows.append({"path": path, "phase": phase, "file": name,
                              "source_sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
                              "refusal": refusal})
            (self.root / "sources.json").write_text(
                json.dumps({"fixture_sources_only": True, "served_html_certified": False,
                            "sources": self.rows}, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    def __enter__(self):
        self.original_generate = self.backend._openai_generate_file_patch
        self.original_refusal = self.backend._refus_de_format
        self.original_paths = {}

        def capture_path(original, phase):
            def run(**kw):
                result = original(**kw)
                if (kw.get("file_path") in self.paths and kw.get("site_name") == self.host
                        and isinstance(result, dict) and isinstance(result.get("patched_content"), str)):
                    self._save(kw["file_path"], phase, result["patched_content"])
                return result
            return run

        for name, phase in (("_patch_via_edits", "targeted"), ("_patch_via_full_file", "full_file")):
            if hasattr(self.backend, name):
                original = getattr(self.backend, name)
                self.original_paths[name] = original
                setattr(self.backend, name, capture_path(original, phase))

        def generate(**kw):
            path = kw["file_path"]
            if path not in self.paths or kw.get("site_name") != self.host:
                raise ValueError("Source capture refuses a non-fixture generation.")
            self._save(path, "original", kw["file_content"])
            result = self.original_generate(**kw)
            if isinstance(result, dict) and isinstance(result.get("patched_content"), str):
                self._save(path, "generated", result["patched_content"])
            return result

        def refuse(path, content):
            result = self.original_refusal(path, content)
            if path in self.paths:
                self._save(path, "final", content, result)
            return result

        self.backend._openai_generate_file_patch = generate
        self.backend._refus_de_format = refuse
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.backend._openai_generate_file_patch = self.original_generate
        self.backend._refus_de_format = self.original_refusal
        for name, original in self.original_paths.items():
            setattr(self.backend, name, original)
