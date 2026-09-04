#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "HTTP: $0 http <endpoint> <cert> <key> <target> <namespace> <topology> <user> <group> <server-ca>"
  echo "MQTT: $0 mqtt <broker> <port> <cert|-> <key|-> <target> <namespace> <topology> <user> <group> <binary> <broker-ca|-> [base-url] [use-tls] [server-ca]"
  exit 2
}

[[ $# -ge 1 ]] || usage
protocol="$1"
install_dir="/etc/symphony-remote-agent"
sudo install -d -m 0700 "$install_dir"

if [[ "$protocol" == "http" ]]; then
  [[ $# -eq 10 ]] || usage
  endpoint="$2"; bootstrap_cert="$3"; bootstrap_key="$4"; target="$5"; namespace="$6"
  topology="$7"; service_user="$8"; service_group="$9"; server_ca="${10}"
  curl_args=(--fail --silent --show-error --cert "$bootstrap_cert" --key "$bootstrap_key" --cacert "$server_ca")
  credentials="$(curl "${curl_args[@]}" -X POST "$endpoint/targets/getcert/$target?namespace=$namespace")"
  public="$(printf '%s' "$credentials" | jq -r '.public')"
  private="$(printf '%s' "$credentials" | jq -r '.private')"
  [[ -n "$public" && "$public" != "null" && -n "$private" && "$private" != "null" ]] || { echo "invalid credential response"; exit 1; }
  printf '%s\n' "$public" | sudo tee "$install_dir/client.crt" >/dev/null
  printf '%s\n' "$private" | sudo tee "$install_dir/client.key" >/dev/null
  sudo chmod 0600 "$install_dir/client.crt" "$install_dir/client.key"
  curl "${curl_args[@]}" "$endpoint/files/remote-agent" -o /tmp/symphony-remote-agent
  sudo install -m 0755 /tmp/symphony-remote-agent "$install_dir/remote-agent"
  rm -f /tmp/symphony-remote-agent
  sudo install -m 0644 "$server_ca" "$install_dir/server-ca.crt"
  cat <<JSON | sudo tee "$install_dir/config.json" >/dev/null
{"requestEndpoint":"$endpoint/solutionversion/tasks","responseEndpoint":"$endpoint/solutionversion/task/getResult","baseUrl":"$endpoint"}
JSON
  ca_arguments="-server-ca-cert=$install_dir/server-ca.crt"
else
  [[ "$protocol" == "mqtt" && $# -ge 12 ]] || usage
  broker="$2"; port="$3"; certificate="$4"; key="$5"; target="$6"; namespace="$7"
  topology="$8"; service_user="$9"; service_group="${10}"; binary="${11}"; broker_ca="${12}"; base_url="${13:-}"
  use_tls="${14:-true}"; server_ca="${15:-}"
  sudo install -m 0755 "$binary" "$install_dir/remote-agent"
  if [[ "$certificate" != "-" && "$key" != "-" ]]; then
    sudo install -m 0600 "$certificate" "$install_dir/client.crt"
    sudo install -m 0600 "$key" "$install_dir/client.key"
  fi
  ca_arguments=""
  if [[ "$use_tls" == "true" ]]; then
    [[ "$broker_ca" != "-" ]] || { echo "broker CA is required for TLS MQTT"; exit 1; }
    sudo install -m 0644 "$broker_ca" "$install_dir/broker-ca.crt"
    ca_arguments="-mqtt-ca-cert=$install_dir/broker-ca.crt"
  fi
  if [[ -n "$server_ca" ]]; then
    sudo install -m 0644 "$server_ca" "$install_dir/server-ca.crt"
    ca_arguments="$ca_arguments -server-ca-cert=$install_dir/server-ca.crt"
  fi
  cat <<JSON | sudo tee "$install_dir/config.json" >/dev/null
{"baseUrl":"$base_url","mqttBroker":"$broker","mqttPort":$port,"mqttUseTLS":$use_tls,"targetName":"$target","namespace":"$namespace"}
JSON
fi

sudo install -m 0644 "$topology" "$install_dir/topology.json"
sudo chown -R "$service_user:$service_group" "$install_dir"
sudo tee /etc/systemd/system/symphony-remote-agent.service >/dev/null <<UNIT
[Unit]
Description=Symphony Remote Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$service_user
Group=$service_group
WorkingDirectory=$install_dir
ExecStart=$install_dir/remote-agent -protocol=$protocol -config=$install_dir/config.json -client-cert=$install_dir/client.crt -client-key=$install_dir/client.key $ca_arguments -target-name=$target -namespace=$namespace -topology=$install_dir/topology.json
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=$install_dir /tmp

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now symphony-remote-agent.service