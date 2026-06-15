"""Kafka ObjectDetection payload field toggles (config + CLI)."""

import base64
from typing import Any, Dict, Optional

import cv2 as cv

# Registry: add new optional fields here and wire them in build_frame_entry / build_detection_entry.
PAYLOAD_FIELD_DEFAULTS = {
    "image": True,           # imageData (base64 JPEG) + imageURL placeholder
    "geo_location": True,    # frame-level GeoLocation
    "obj_geolocation": True, # per-detection obj_geolocation
}

# CLI --no-payload aliases → registry key
_NO_PAYLOAD_ALIASES = {
    "image": "image",
    "no-image": "image",
    "geo": "geo_location",
    "geo_location": "geo_location",
    "obj_geo": "obj_geolocation",
    "obj_geolocation": "obj_geolocation",
}


def resolve_payload_options(config: dict, args=None) -> Dict[str, bool]:
    """Merge config.json kafka_payload with CLI overrides."""
    opts = dict(PAYLOAD_FIELD_DEFAULTS)
    cfg = config.get("kafka_payload")
    if isinstance(cfg, dict):
        for key in PAYLOAD_FIELD_DEFAULTS:
            if key in cfg:
                opts[key] = bool(cfg[key])

    if args is None:
        return opts

    if getattr(args, "no_image", False):
        opts["image"] = False

    for token in getattr(args, "no_payload", None) or []:
        key = _NO_PAYLOAD_ALIASES.get(token.strip().lower())
        if key:
            opts[key] = False

    return opts


def payload_summary(opts: Dict[str, bool]) -> str:
    enabled = [k for k, on in opts.items() if on]
    disabled = [k for k, on in opts.items() if not on]
    parts = []
    if enabled:
        parts.append("on: " + ", ".join(enabled))
    if disabled:
        parts.append("off: " + ", ".join(disabled))
    return "; ".join(parts) if parts else "defaults"


def encode_frame_image(frame_image, jpeg_quality: int = 80) -> str:
    if frame_image is None:
        return ""
    _, buf = cv.imencode(".jpg", frame_image, [cv.IMWRITE_JPEG_QUALITY, jpeg_quality])
    return base64.b64encode(buf).decode("utf-8")


def build_frame_entry(
    frame_id: int,
    *,
    payload_opts: Dict[str, bool],
    frame_image=None,
    jpeg_quality: int = 80,
    latitude=None,
    longitude=None,
    altitude=None,
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "frameID": frame_id,
        "imageURL": "",
        "detections": [],
    }

    if payload_opts.get("image", True):
        entry["imageData"] = encode_frame_image(frame_image, jpeg_quality)
    else:
        entry["imageData"] = ""

    if payload_opts.get("geo_location", True):
        entry["GeoLocation"] = {
            "latitude": latitude,
            "longitude": longitude,
            "altitude": altitude,
        }

    return entry


def build_detection_entry(
    *,
    payload_opts: Dict[str, bool],
    object_id,
    object_class,
    confidence,
    bbox,
    obj_gps: Optional[dict] = None,
) -> Dict[str, Any]:
    det: Dict[str, Any] = {
        "objectID": object_id,
        "class": object_class,
        "confidence": confidence,
        "bbox": bbox,
    }
    if payload_opts.get("obj_geolocation", True) and obj_gps is not None:
        det["obj_geolocation"] = obj_gps
    return det
