from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class IngestStats:
    counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def increment(self, key: str, amount: int = 1) -> None:
        self.counts[key] += amount

    def to_dict(self) -> Dict[str, int]:
        return dict(self.counts)

    def __repr__(self) -> str:
        return repr(self.to_dict())
