"""Response envelope — matches the OpenG2P id-generator convention."""

from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def make_response(response_data: dict) -> dict:
    return {
        "id": "openg2p.auditmanager",
        "version": "1.0",
        "responsetime": _now_iso(),
        "response": response_data,
        "errors": [],
    }


def make_error_response(error_code: str, message: str) -> dict:
    return {
        "id": "openg2p.auditmanager",
        "version": "1.0",
        "responsetime": _now_iso(),
        "response": None,
        "errors": [{"errorCode": error_code, "message": message}],
    }
