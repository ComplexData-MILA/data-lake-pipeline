from __future__ import annotations

import asyncio
import json
from typing import Any

from data_lake_pipeline.protocols import (
    AsyncFilter,
    AsyncProcessor,
    FilterResult,
    ProcessorResult,
    StageContext,
)


class QualityFilter(AsyncFilter):
    """Quality filter using OpenAI SDK. Requires OPENAI_API_KEY or OPENAI_BASE_URL."""

    def __init__(
        self,
        prompt_template: str | None = None,
        threshold: float = 0.7,
        model: str = "default",
        **kwargs: Any,
    ) -> None:
        self.prompt_template = prompt_template or (
            "Rate the quality of this social media post (0.0-1.0). "
            "Consider: coherence, informativeness, relevance. "
            'Return JSON: {"score": <float>, "reason": "<brief explanation>"}\n\n'
            "Post: {text}"
        )
        self.threshold = threshold
        self.model = model
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI()
        return self._client

    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[FilterResult]:
        tasks = [self._process_one(r) for r in records]
        return await asyncio.gather(*tasks)

    async def _process_one(self, record: dict[str, Any]) -> FilterResult:
        prompt = self.prompt_template.format(text=record.get("text", ""))
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            parsed = json.loads(response.choices[0].message.content or "{}")
            score = float(parsed.get("score", 0))
            return FilterResult(
                passed=score >= self.threshold,
                score=score,
                reason=parsed.get("reason"),
                output=parsed,
            )
        except Exception as e:
            return FilterResult(passed=False, reason=f"Error: {e}")


class TopicFilter(AsyncFilter):
    """Topic filter using OpenAI SDK. Requires OPENAI_API_KEY or OPENAI_BASE_URL."""

    def __init__(
        self,
        topics: list[str] | None = None,
        prompt_template: str | None = None,
        threshold: float = 0.5,
        model: str = "default",
        **kwargs: Any,
    ) -> None:
        self.topics = topics or ["finance", "technology", "politics"]
        self.prompt_template = prompt_template or (
            "Classify if this post is about any of these topics: {topics}. "
            'Return JSON: {"relevant": <bool>, "topic": "<matched topic or none>", "confidence": <float>}\n\n'
            "Post: {text}"
        )
        self.threshold = threshold
        self.model = model
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI()
        return self._client

    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[FilterResult]:
        tasks = [self._process_one(r) for r in records]
        return await asyncio.gather(*tasks)

    async def _process_one(self, record: dict[str, Any]) -> FilterResult:
        topics_str = ", ".join(self.topics)
        prompt = self.prompt_template.format(
            text=record.get("text", ""), topics=topics_str
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            parsed = json.loads(response.choices[0].message.content or "{}")
            confidence = float(parsed.get("confidence", 0))
            relevant = bool(parsed.get("relevant", False))
            return FilterResult(
                passed=relevant and confidence >= self.threshold,
                score=confidence,
                reason=f"Topic: {parsed.get('topic')}",
                output=parsed,
            )
        except Exception as e:
            return FilterResult(passed=False, reason=f"Error: {e}")


class EnrichmentProcessor(AsyncProcessor):
    """Enrichment processor using OpenAI SDK. Requires OPENAI_API_KEY or OPENAI_BASE_URL."""

    def __init__(
        self,
        prompt_template: str | None = None,
        model: str = "default",
        **kwargs: Any,
    ) -> None:
        self.prompt_template = prompt_template or (
            "Analyze this social media post and provide:\n"
            "1. A brief summary\n"
            "2. Key entities mentioned\n"
            "3. Sentiment (positive/negative/neutral)\n\n"
            'Return JSON: {"summary": "<text>", "entities": ["<entity>", ...], "sentiment": "<sentiment>"}\n\n'
            "Post: {text}"
        )
        self.model = model
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI()
        return self._client

    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[ProcessorResult]:
        tasks = [self._process_one(r) for r in records]
        return await asyncio.gather(*tasks)

    async def _process_one(self, record: dict[str, Any]) -> ProcessorResult:
        prompt = self.prompt_template.format(text=record.get("text", ""))
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            parsed = json.loads(response.choices[0].message.content or "{}")
            return ProcessorResult(output=parsed)
        except Exception as e:
            return ProcessorResult(output={"error": str(e)})
