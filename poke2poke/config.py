from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

log = logging.getLogger(__name__)


class ConfigError(ValueError):
    """Raised when service configuration is incomplete or invalid."""


@dataclass(frozen=True)
class Settings:
    port: int
    db_path: str
    poke_inbound_url: str
    cdn_upload_url: str
    cdn_hosts: frozenset[str]
    tokens: dict[str, str]
    api_keys: dict[str, str]
    max_message_chars: int = 4000
    max_attachments: int = 10
    max_upload_bytes: int = 100 * 1024 * 1024
    poke_push_timeout_s: float = 20.0
    poke_push_attempts: int = 4
    allow_private_upload_sources: bool = False


def _env_handles(prefix: str) -> set[str]:
    return {key[len(prefix) :].lower() for key in os.environ if key.startswith(prefix)}


def _read_config(config_path: Path | None) -> Mapping[str, object]:
    path = config_path or Path(
        os.environ.get("POKE2POKE_CONFIG", str(Path(__file__).resolve().parents[1] / "config.json"))
    )
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as config_file:
        data = json.load(config_file)
    return data if isinstance(data, dict) else {}


def _mapping(data: object) -> Mapping[str, object]:
    return data if isinstance(data, dict) else {}


def _url(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _is_https(value: str) -> bool:
    return urlparse(value).scheme.lower() == "https"


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings(config_path: Path | None = None) -> Settings:
    config = _read_config(config_path)
    token_config = {
        str(key).lower(): value for key, value in _mapping(config.get("tokens")).items()
    }
    api_config = {
        str(key).lower(): value for key, value in _mapping(config.get("api_keys")).items()
    }
    token_handles = set(token_config)
    api_handles = set(api_config)
    token_handles |= _env_handles("POKE2POKE_KEY_")
    api_handles |= _env_handles("POKE_API_KEY_")

    tokens = {
        handle: os.environ.get(
            f"POKE2POKE_KEY_{handle.upper()}", str(token_config.get(handle, ""))
        ).strip()
        for handle in token_handles
    }
    tokens = {handle: value for handle, value in tokens.items() if value}
    api_keys = {
        handle: os.environ.get(
            f"POKE_API_KEY_{handle.upper()}", str(api_config.get(handle, ""))
        ).strip()
        for handle in api_handles | set(tokens)
    }
    api_keys = {handle: value for handle, value in api_keys.items() if value}

    hosts_value = os.environ.get("POKE2POKE_CDN_HOSTS", config.get("cdn_hosts", []))
    if isinstance(hosts_value, str):
        hosts = frozenset(part.strip().lower() for part in hosts_value.split(",") if part.strip())
    else:
        raw_hosts = hosts_value if isinstance(hosts_value, (list, tuple, set, frozenset)) else []
        hosts = frozenset(str(host).strip().lower() for host in raw_hosts if str(host).strip())
    inbound = _url(os.environ.get("POKE_INBOUND_URL", config.get("poke_inbound_url", "")))
    upload = _url(os.environ.get("CDN1_UPLOAD_URL", config.get("cdn_upload_url", "")))
    problems: list[str] = []
    if not inbound:
        problems.append(
            "poke_inbound_url is required (set POKE_INBOUND_URL or config.json 'poke_inbound_url')"
        )
    elif not _is_https(inbound):
        problems.append("poke_inbound_url must use https://")
    if not upload:
        problems.append(
            "cdn_upload_url is required (set CDN1_UPLOAD_URL or config.json 'cdn_upload_url')"
        )
    elif not _is_https(upload):
        problems.append("cdn_upload_url must use https://")
    if not hosts:
        problems.append(
            "cdn_hosts is required (set POKE2POKE_CDN_HOSTS or config.json 'cdn_hosts')"
        )
    if not tokens:
        problems.append(
            "at least one token is required (set POKE2POKE_KEY_<HANDLE> or config.json 'tokens')"
        )
    if problems:
        raise ConfigError("Invalid configuration: " + "; ".join(problems))

    for handle in tokens:
        if handle not in api_keys:
            log.warning("push notifications disabled for handle %s", handle)
    try:
        port = int(str(os.environ.get("POKE2POKE_PORT", config.get("port", 8765))))
    except (TypeError, ValueError) as exc:
        raise ConfigError("Invalid configuration: port must be an integer") from exc
    return Settings(
        port=port,
        db_path=os.environ.get("POKE2POKE_DATA", str(config.get("db_path", "data.sqlite"))),
        poke_inbound_url=inbound,
        cdn_upload_url=upload,
        cdn_hosts=hosts,
        tokens=tokens,
        api_keys=api_keys,
        max_message_chars=int(str(config.get("max_message_chars", 4000))),
        max_attachments=int(str(config.get("max_attachments", 10))),
        max_upload_bytes=int(str(config.get("max_upload_bytes", 100 * 1024 * 1024))),
        poke_push_timeout_s=float(str(config.get("poke_push_timeout_s", 20))),
        poke_push_attempts=int(str(config.get("poke_push_attempts", 4))),
        allow_private_upload_sources=_bool_env(
            "POKE2POKE_ALLOW_PRIVATE_UPLOAD_SOURCES",
            bool(config.get("allow_private_upload_sources", False)),
        ),
    )
