from dataclasses import dataclass, asdict
import json
from typing import Any, Dict

@dataclass
class MyMessage:
    message_id: int
    payload: str
    ts: str  # timestamp string, можно менять формат

    def to_json(self) -> bytes:
        # сериализация в JSON bytes
        return json.dumps(asdict(self)).encode("utf-8")

    @staticmethod
    def from_json(b: bytes) -> "MyMessage":
        try:
            data = json.loads(b.decode("utf-8"))
            return MyMessage(
                message_id=int(data["message_id"]),
                payload=str(data["payload"]),
                ts=str(data["ts"])
            )
        except Exception as e:
            raise ValueError(f"Deserialize error: {e}")
