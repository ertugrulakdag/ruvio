#!/usr/bin/env bash
set -euo pipefail

if [[ "$EUID" -ne 0 ]]; then
  echo "Run this installer as root (for example, with sudo)." >&2
  exit 1
fi

role="${1:-}"
peer_ip="${2:-}"
extra_ip="${3:-}"
if [[ "$role" != sut && "$role" != observer ]]; then
  echo "Usage: $0 <sut|observer> <peer-ipv4> [ui-client-ipv4|grafana-client-ipv4]" >&2
  exit 2
fi
if ! python3 -c 'import ipaddress,sys; address=ipaddress.ip_address(sys.argv[1]); sys.exit(0 if isinstance(address, ipaddress.IPv4Address) else 1)' "$peer_ip"; then
  echo "Peer address must be a valid IPv4 address." >&2
  exit 2
fi
if [[ -n "$extra_ip" ]] &&
  ! python3 -c 'import ipaddress,sys; address=ipaddress.ip_address(sys.argv[1]); sys.exit(0 if isinstance(address, ipaddress.IPv4Address) else 1)' "$extra_ip"; then
  echo "Optional client address must be a valid IPv4 address." >&2
  exit 2
fi
if [[ -n "$extra_ip" && "$role" == sut ]]; then
  extra_config="LAB_RUVIO_UI_IP=$extra_ip"
elif [[ -n "$extra_ip" && "$role" == observer ]]; then
  extra_config="LAB_GRAFANA_IP=$extra_ip"
else
  extra_config=""
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
install -m 0755 "$script_dir/apply-docker-ingress-filter.sh" \
  /usr/local/sbin/ruvio-scenario-lab-firewall
install -d -m 0755 /etc/default
printf 'LAB_ROLE=%s\nLAB_PEER_IP=%s\n%s\n' "$role" "$peer_ip" "$extra_config" \
  > /etc/default/ruvio-scenario-lab-firewall
chmod 0644 /etc/default/ruvio-scenario-lab-firewall

cat > /etc/systemd/system/ruvio-scenario-lab-firewall.service <<'UNIT'
[Unit]
Description=Restrict Ruvio Scenario Lab Docker-published ports to its peer
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/ruvio-scenario-lab-firewall
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now ruvio-scenario-lab-firewall.service
systemctl restart ruvio-scenario-lab-firewall.service
