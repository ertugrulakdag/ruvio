#!/usr/bin/env python3
"""Render the observer's Prometheus targets for the peer-reachable SUT address."""

import ipaddress
from pathlib import Path


LAB_ROOT = Path(__file__).resolve().parents[1]


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip():
            raise ValueError(f"Invalid .env entry at line {line_number}")
        values[key.strip()] = value.strip()
    return values


def main() -> None:
    env_path = LAB_ROOT / ".env"
    values = read_env_file(env_path)
    sut_ip = values.get("SUT_ADDRESS", "")
    if not sut_ip:
        raise ValueError("SUT_ADDRESS is required in .env")

    if sut_ip == "host.docker.internal":
        target_host = sut_ip
    else:
        private_ip = ipaddress.ip_address(sut_ip)
        if not isinstance(private_ip, ipaddress.IPv4Address):
            raise ValueError("SUT_ADDRESS must be an IPv4 address")
        target_host = str(private_ip)
    api_port = int(values.get("API_PORT", "8080"))
    if not 1 <= api_port <= 65535:
        raise ValueError("API_PORT must be in [1, 65535]")

    template = (LAB_ROOT / "observer" / "prometheus.yml.template").read_text(encoding="utf-8")
    rendered = template.replace("__SUT_ADDRESS__", target_host)
    rendered = rendered.replace("__API_PORT__", str(api_port))
    output_path = LAB_ROOT / "observer" / "prometheus.generated.yml"
    output_path.write_text(rendered, encoding="utf-8")
    output_path.chmod(0o644)
    print("Rendered Prometheus scrape targets for the peer-reachable SUT address.")


if __name__ == "__main__":
    main()
