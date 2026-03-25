from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Sequence

from data_lake_pipeline.config import Settings


@dataclass
class AnnotationRequest:
    source: str
    external_id: str
    text: str
    source_file: str


class BaseAnnotator:
    backend_name: str = "base"

    def annotate(self, requests: Sequence[AnnotationRequest], settings: Settings) -> list[str]:
        raise NotImplementedError


class MockAnnotator(BaseAnnotator):
    backend_name = "mock"

    def annotate(self, requests: Sequence[AnnotationRequest], settings: Settings) -> list[str]:
        outputs: list[str] = []
        for req in requests:
            outputs.append(
                json.dumps(
                    {
                        "topic": "placeholder",
                        "sentiment": "neutral",
                        "safety_flags": [],
                        "rationale_short": f"Mock annotation for {req.external_id}",
                    },
                    ensure_ascii=False,
                )
            )
        return outputs



def build_annotator(settings: Settings) -> BaseAnnotator:
    backend = settings.annotator_backend.lower()
    if backend == "mock":
        return MockAnnotator()
    raise ValueError(f"Unsupported annotator backend: {settings.annotator_backend}")
