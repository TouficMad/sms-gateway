"""Loads carriers and routes from YAML, with ${ENV_VAR} substitution for secrets."""
import os
import re

import yaml

_ENV = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")


def _expand(value):
    if isinstance(value, str):
        return _ENV.sub(lambda m: os.getenv(m.group(1), m.group(2) or ""), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load(path=None) -> dict:
    path = path or os.getenv("ROUTES_CONFIG", "config/routes.yaml")
    with open(path) as f:
        config = _expand(yaml.safe_load(f))
    validate(config)
    return config


def validate(config: dict):
    carriers = config.get("carriers") or {}
    if not carriers:
        raise ValueError("config must define at least one carrier")
    from .routing import route_prefixes

    for route in config.get("routes", []):
        for prefix in route_prefixes(route):
            if prefix and not prefix.isdigit():
                raise ValueError(f"route prefix must be digits: {route['name']}: {prefix!r}")
        for option in route["carriers"]:
            if option["carrier"] not in carriers:
                raise ValueError(f"route {route['name']} uses unknown carrier {option['carrier']}")
