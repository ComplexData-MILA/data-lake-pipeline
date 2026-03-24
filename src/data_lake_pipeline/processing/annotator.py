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


class VLLMAnnotator(BaseAnnotator):
    backend_name = "vllm"

    def annotate(self, requests: Sequence[AnnotationRequest], settings: Settings) -> list[str]:
        try:
            from vllm import LLM, SamplingParams
        except Exception as exc:  # pragma: no cover - runtime integration path
            raise RuntimeError(
                "vLLM is not installed in this environment. Install the 'processing' extras or switch to PIPELINE_ANNOTATOR_BACKEND=mock."
            ) from exc

        prompts = [
            settings.prompt_template.format(text=req.text)
            for req in requests
        ]
        llm = LLM(
            model=settings.model_name,
            tensor_parallel_size=settings.vllm_tensor_parallel_size,
        )
        sampling_params = SamplingParams(
            temperature=settings.vllm_temperature,
            max_tokens=settings.vllm_max_tokens,
        )
        outputs = llm.generate(prompts, sampling_params)
        return [item.outputs[0].text for item in outputs]


class SGLangAnnotator(BaseAnnotator):
    backend_name = "sglang"

    def annotate(self, requests: Sequence[AnnotationRequest], settings: Settings) -> list[str]:
        raise NotImplementedError(
            "SGLang integration is intentionally left as a stub. "
            "Implement your SGLang or Agent SDK client behind this interface."
        )


def build_annotator(settings: Settings) -> BaseAnnotator:
    backend = settings.annotator_backend.lower()
    if backend == "mock":
        return MockAnnotator()
    if backend == "vllm":
        return VLLMAnnotator()
    if backend == "sglang":
        return SGLangAnnotator()
    raise ValueError(f"Unsupported annotator backend: {settings.annotator_backend}")
