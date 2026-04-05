"""Filter DSL for dataset and annotation filtering."""
from typing import Literal, Union

from pydantic import BaseModel


class BooleanFilter(BaseModel):
    """Filter on boolean field in dataset or annotation."""
    type: Literal["boolean"] = "boolean"
    field: str
    value: bool

    def compile(self, available_columns: set[str]) -> str:
        """Compile to DuckDB WHERE clause."""
        if self.field not in available_columns:
            return "FALSE"
        return f"{self.field} = {self.value}"


class RawDuckFilter(BaseModel):
    """Raw DuckDB SQL WHERE clause."""
    type: Literal["raw_duck"] = "raw_duck"
    sql: str

    def compile(self, available_columns: set[str]) -> str:
        """Return raw SQL."""
        return self.sql


class AllFilter(BaseModel):
    """AND combination of filters."""
    type: Literal["all"] = "all"
    filters: list["FilterNode"]

    def compile(self, available_columns: set[str]) -> str:
        """Compile AND logic."""
        if not self.filters:
            return "TRUE"
        parts = [f.compile(available_columns) for f in self.filters]
        return "(" + " AND ".join(parts) + ")"


class AnyFilter(BaseModel):
    """OR combination of filters."""
    type: Literal["any"] = "any"
    filters: list["FilterNode"]

    def compile(self, available_columns: set[str]) -> str:
        """Compile OR logic."""
        if not self.filters:
            return "FALSE"
        parts = [f.compile(available_columns) for f in self.filters]
        return "(" + " OR ".join(parts) + ")"


FilterNode = Union[BooleanFilter, RawDuckFilter, AllFilter, AnyFilter]

AllFilter.model_rebuild()
AnyFilter.model_rebuild()
