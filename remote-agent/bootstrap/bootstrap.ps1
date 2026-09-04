[CmdletBinding()]
param(
    [ValidateSet('http', 'mqtt')][string]$Protocol = 'http',
    [string]$Endpoint,
    [string]$Broker,
    [int]$BrokerPort = 8883,
    [bool]$MQTTUseTLS = $true,
    [Parameter(Mandatory)][string]$CertificatePath,
    [Parameter(Mandatory)][string]$KeyPath,
    [string]$ServerCAPath,
    [string]$BrokerCAPath,
    [Parameter(Mandatory)][string]$TargetName,
    [string]$Namespace = 'default',
    [Parameter(Mandatory)][string]$TopologyPath,
    [string]$AgentPath,
    [ValidateSet('service', 'schedule')][string]$RunMode = 'service'
)

$ErrorActionPreference = 'Stop'
$installDir = Join-Path $env:ProgramData 'Symphony\RemoteAgent'
New-Item -ItemType Directory -Force -Path $installDir | Out-Null
$currentUser = [Security.Principal.WindowsIdentity]::GetCurrent().Name
& icacls.exe $installDir /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "${currentUser}:(OI)(CI)F" | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Failed to restrict the remote-agent installation directory ACL.' }

if ($Protocol -eq 'http') {
    if (-not $Endpoint -or -not $ServerCAPath) { throw 'Endpoint and ServerCAPath are required for HTTP mode.' }
    $curl = Get-Command curl.exe -ErrorAction Stop
    $credentialsJson = & $curl.Source --fail --silent --show-error --cert $CertificatePath --key $KeyPath --cacert $ServerCAPath -X POST "$Endpoint/targets/getcert/$TargetName`?namespace=$Namespace"
    if ($LASTEXITCODE -ne 0) { throw 'Remote-agent certificate enrollment failed.' }
    $credentials = $credentialsJson | ConvertFrom-Json
    Set-Content -Path (Join-Path $installDir 'client.crt') -Value $credentials.public -Encoding ascii
    Set-Content -Path (Join-Path $installDir 'client.key') -Value $credentials.private -Encoding ascii
    & $curl.Source --fail --silent --show-error --cert $CertificatePath --key $KeyPath --cacert $ServerCAPath "$Endpoint/files/remote-agent.exe" --output (Join-Path $installDir 'remote-agent.exe')
    if ($LASTEXITCODE -ne 0) { throw 'Remote-agent binary download failed.' }
    Copy-Item $ServerCAPath (Join-Path $installDir 'server-ca.crt') -Force
    @{ requestEndpoint="$Endpoint/solutionversion/tasks"; responseEndpoint="$Endpoint/solutionversion/task/getResult"; baseUrl=$Endpoint } |
        ConvertTo-Json | Set-Content (Join-Path $installDir 'config.json') -Encoding utf8NoBOM
    $caArguments = "-server-ca-cert=`"$(Join-Path $installDir 'server-ca.crt')`""
} else {
    if (-not $Broker -or -not $AgentPath) { throw 'Broker and AgentPath are required for MQTT mode.' }
    if ($MQTTUseTLS -and -not $BrokerCAPath) { throw 'BrokerCAPath is required for TLS MQTT mode.' }
    Copy-Item $AgentPath (Join-Path $installDir 'remote-agent.exe') -Force
    Copy-Item $CertificatePath (Join-Path $installDir 'client.crt') -Force
    Copy-Item $KeyPath (Join-Path $installDir 'client.key') -Force
    $caArguments = ''
    if ($MQTTUseTLS) {
        Copy-Item $BrokerCAPath (Join-Path $installDir 'broker-ca.crt') -Force
        $caArguments = "-mqtt-ca-cert=`"$(Join-Path $installDir 'broker-ca.crt')`""
    }
    if ($ServerCAPath) {
        Copy-Item $ServerCAPath (Join-Path $installDir 'server-ca.crt') -Force
        $caArguments += " -server-ca-cert=`"$(Join-Path $installDir 'server-ca.crt')`""
    }
    @{ mqttBroker=$Broker; mqttPort=$BrokerPort; mqttUseTLS=$MQTTUseTLS; targetName=$TargetName; namespace=$Namespace; baseUrl=$Endpoint } |
        ConvertTo-Json | Set-Content (Join-Path $installDir 'config.json') -Encoding utf8NoBOM
}

Copy-Item $TopologyPath (Join-Path $installDir 'topology.json') -Force
$agent = Join-Path $installDir 'remote-agent.exe'
$agentArguments = @(
    "-protocol=$Protocol",
    "-config=$(Join-Path $installDir 'config.json')",
    "-client-cert=$(Join-Path $installDir 'client.crt')",
    "-client-key=$(Join-Path $installDir 'client.key')",
    "-target-name=$TargetName",
    "-namespace=$Namespace",
    "-topology=$(Join-Path $installDir 'topology.json')"
)
if ($Protocol -eq 'http') {
    $agentArguments += "-server-ca-cert=$(Join-Path $installDir 'server-ca.crt')"
} else {
    if ($MQTTUseTLS) { $agentArguments += "-mqtt-ca-cert=$(Join-Path $installDir 'broker-ca.crt')" }
    if ($ServerCAPath) { $agentArguments += "-server-ca-cert=$(Join-Path $installDir 'server-ca.crt')" }
}
$serviceName = "Symphony-RemoteAgent-$TargetName"

if ($RunMode -eq 'service') {
    & $agent install @agentArguments
    & $agent start -target-name=$TargetName
} else {
    $arguments = ($agentArguments | ForEach-Object { '"' + $_.Replace('"', '\"') + '"' }) -join ' '
    $action = New-ScheduledTaskAction -Execute $agent -Argument $arguments -WorkingDirectory $installDir
    $trigger = New-ScheduledTaskTrigger -AtLogOn
    $principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Highest
    Register-ScheduledTask -TaskName $serviceName -Action $action -Trigger $trigger -Principal $principal -Force | Out-Null
    Start-ScheduledTask -TaskName $serviceName
}