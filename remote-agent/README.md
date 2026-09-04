# Symphony Remote Agent

The remote agent executes Symphony target-provider operations outside the control-plane host. It supports outbound HTTPS polling and request/response MQTT while keeping durable operation state in Symphony.

## Build

```bash
go build -tags remote -o remote-agent .
```

The `remote` tag enables bounded local log capture and forwarding. Production API images package Linux and Windows binaries at `/v1alpha2/files/remote-agent` and `/v1alpha2/files/remote-agent.exe`.

## Configuration

HTTP:

```json
{
  "requestEndpoint": "https://symphony.example/v1alpha2/solutionversion/tasks",
  "responseEndpoint": "https://symphony.example/v1alpha2/solutionversion/task/getResult",
  "baseUrl": "https://symphony.example/v1alpha2"
}
```

MQTT:

```json
{
  "baseUrl": "https://symphony.example/v1alpha2",
  "mqttBroker": "broker.example",
  "mqttPort": 8883,
  "mqttUseTLS": true,
  "targetName": "edge-01",
  "namespace": "default"
}
```

Use absolute paths when running as a service:

```bash
./remote-agent \
  -protocol=http \
  -config=/etc/symphony-remote-agent/config.json \
  -client-cert=/etc/symphony-remote-agent/client.crt \
  -client-key=/etc/symphony-remote-agent/client.key \
  -server-ca-cert=/etc/symphony-remote-agent/server-ca.crt \
  -target-name=edge-01 \
  -namespace=default \
  -topology=/etc/symphony-remote-agent/topology.json
```

See `bootstrap/README.md` for enrollment and service installation.
