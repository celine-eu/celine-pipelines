from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
import base64
import json
import logging
import re

import requests

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = """\
You are analyzing an aerial orthophoto crop of a single building rooftop. Your task: determine if solar photovoltaic (PV) panels are installed.

Think step by step:
1. What is the roof material and color?
2. Are there any distinct rectangular objects on the roof that differ from the roof surface?
3. If yes: how many? Are they dark blue/black? Are they arranged in a regular grid, line, or pyramid pattern?
4. Could they be something else? (skylights, solar thermal with 1-2 panels and pipes, vents, roof windows, dark patches)

Classification rules:
- PV = 6 or more dark blue/black rectangular panels in a grid, line along roof edge, or pyramid layout
- NOT PV = 1-2 panels (likely solar thermal), skylights, dark roof sections, shadows, tarps, vents

Respond with your step-by-step reasoning followed by a JSON conclusion on the last line:
{"has_pv": true/false, "confidence": 0.0-1.0, "panel_count": estimated_number_or_0}"""


@dataclass
class DetectionResult:
    building_id: str
    has_pv: bool
    confidence: float
    model_name: str
    reasoning: str
    raw_response: str = ""
    description: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


class Detector(ABC):
    @abstractmethod
    def detect(self, image_bytes: bytes, building_id: str) -> DetectionResult:
        ...

    def detect_batch(
        self, items: list[tuple[str, bytes]]
    ) -> list[DetectionResult]:
        results = []
        for bid, img in items:
            try:
                results.append(self.detect(img, bid))
            except Exception as e:
                logger.error("Detection failed for %s: %s", bid, e)
                results.append(DetectionResult(
                    building_id=bid,
                    has_pv=False,
                    confidence=0.0,
                    model_name=getattr(self, "model", "unknown"),
                    reasoning=f"error: {e}",
                ))
        return results


class OllamaDetector(Detector):
    def __init__(
        self,
        host: str = "http://localhost:11434",
        model: str = "qwen2.5vl:7b",
        timeout: int = 120,
        confidence_threshold: float = 0.6,
        temperature: float = 0.2,
        prompt: str = DEFAULT_PROMPT,
    ):
        self.host = host.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.confidence_threshold = confidence_threshold
        self.temperature = temperature
        self.prompt = prompt

    def _ensure_min_size(self, image_bytes: bytes) -> bytes:
        from PIL import Image as PILImage
        from io import BytesIO as _BytesIO

        img = PILImage.open(_BytesIO(image_bytes))
        min_dim = 64
        if img.width < min_dim or img.height < min_dim:
            scale = max(min_dim / img.width, min_dim / img.height)
            img = img.resize(
                (max(min_dim, int(img.width * scale)), max(min_dim, int(img.height * scale))),
                PILImage.LANCZOS,
            )
            buf = _BytesIO()
            img.save(buf, format="JPEG", quality=90)
            return buf.getvalue()
        return image_bytes

    def detect(self, image_bytes: bytes, building_id: str) -> DetectionResult:
        image_bytes = self._ensure_min_size(image_bytes)
        b64 = base64.b64encode(image_bytes).decode("utf-8")

        payload = {
            "model": self.model,
            "prompt": self.prompt,
            "images": [b64],
            "stream": False,
            "options": {"temperature": self.temperature},
        }

        try:
            resp = requests.post(
                f"{self.host}/api/generate",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "")
        except requests.RequestException as e:
            logger.error("Ollama request failed for %s: %s", building_id, e)
            return DetectionResult(
                building_id=building_id,
                has_pv=False,
                confidence=0.0,
                model_name=self.model,
                reasoning=f"request_error: {e}",
            )

        parsed = self._parse_response(raw)

        # Extract reasoning (everything before the JSON line)
        reasoning_text = raw
        json_match = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
        if json_match:
            reasoning_text = raw[:json_match.start()].strip()

        return DetectionResult(
            building_id=building_id,
            has_pv=parsed["confidence"] >= self.confidence_threshold
            if parsed["has_pv"] is None
            else parsed["has_pv"],
            confidence=parsed["confidence"],
            model_name=self.model,
            reasoning=parsed.get("short_reasoning") or reasoning_text[:500],
            raw_response=raw,
            description=reasoning_text[:500],
        )

    def _parse_response(self, raw: str) -> dict:
        result = {"has_pv": None, "confidence": 0.0, "short_reasoning": ""}

        json_match = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group())
                if "has_pv" in data:
                    result["has_pv"] = bool(data["has_pv"])
                if "confidence" in data:
                    c = float(data["confidence"])
                    result["confidence"] = max(0.0, min(1.0, c))
                if "reasoning" in data:
                    result["short_reasoning"] = str(data["reasoning"])[:500]
                return result
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

        raw_lower = raw.lower()
        if "true" in raw_lower or "yes" in raw_lower:
            result["has_pv"] = True
        elif "false" in raw_lower or "no" in raw_lower:
            result["has_pv"] = False

        conf_match = re.search(r"(?:confidence|conf)[:\s]*([0-9]*\.?[0-9]+)", raw_lower)
        if conf_match:
            result["confidence"] = max(0.0, min(1.0, float(conf_match.group(1))))

        return result


def create_detector(config: dict) -> Detector:
    detector_type = config.get("type", "ollama")
    if detector_type == "ollama":
        cfg = config.get("ollama", {})
        return OllamaDetector(
            host=cfg.get("host", "http://localhost:11434"),
            model=cfg.get("model", "qwen2.5vl:7b"),
            timeout=cfg.get("timeout", 120),
            confidence_threshold=cfg.get("confidence_threshold", 0.6),
            temperature=cfg.get("temperature", 0.2),
            prompt=cfg.get("prompt", DEFAULT_PROMPT),
        )
    else:
        raise ValueError(f"Unknown detector type: {detector_type}")
