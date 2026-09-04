# Remote Agent Bootstrap

Enable the chart feature before enrollment:

```bash
helm upgrade --install symphony packages/helm/symphony \
  --set remoteAgent.enabled=true \
  --set remoteAgent.subjects=bootstrap-client
```

The default opt-in profile creates `symphony-remote-agent-bootstrap`. Extract its PEM files before running bootstrap:

```bash
kubectl get secret symphony-remote-agent-bootstrap -o jsonpath='{.data.tls\.crt}' | base64 -d > bootstrap.crt
kubectl get secret symphony-remote-agent-bootstrap -o jsonpath='{.data.tls\.key}' | base64 -d > bootstrap.key
chmod 600 bootstrap.key
```

For a separately issued bootstrap certificate, set `remoteAgent.bootstrapCA.secretName` to the Kubernetes secret containing its CA certificate. The bootstrap certificate subject must match one of the semicolon-separated `remoteAgent.subjects` entries.

HTTP bootstrap uses the supplied bootstrap client certificate to request a target-specific working certificate and download the packaged binary. MQTT bootstrap uses the supplied broker client certificate directly.

Linux:

```bash
sudo ./bootstrap.sh http https://symphony.example/v1alpha2 bootstrap.crt bootstrap.key edge-01 default topology.json remote-agent remote-agent server-ca.crt
```

Windows:

```powershell
.\bootstrap.ps1 -Protocol http -Endpoint https://symphony.example/v1alpha2 `
  -CertificatePath bootstrap.crt -KeyPath bootstrap.key -ServerCAPath server-ca.crt `
  -TargetName edge-01 -Namespace default -TopologyPath topology.json -RunMode service
```

Service installation requires administrator/root privileges. Keep private-key files restricted to the service identity.
