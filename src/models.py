from pydantic import BaseModel


class StreamingConfigs(BaseModel):
    chunk_size: int = 100
