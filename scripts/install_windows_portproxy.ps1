param(
    [int]$Port = 8000,
    [string]$ListenAddress = "0.0.0.0",
    [string]$WslAddress = "",
    [string]$RuleName = "Slurm Scheduler 8000"
)

$ErrorActionPreference = "Stop"

if ($WslAddress -eq "") {
    $WslAddress = (wsl.exe hostname -I).Trim().Split(" ")[0]
}

if ($WslAddress -eq "") {
    throw "Could not detect WSL IP address. Pass -WslAddress explicitly."
}

netsh interface portproxy delete v4tov4 listenaddress=$ListenAddress listenport=$Port 2>$null | Out-Null
netsh interface portproxy add v4tov4 listenaddress=$ListenAddress listenport=$Port connectaddress=$WslAddress connectport=$Port | Out-Null

if (-not (Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -DisplayName $RuleName -Direction Inbound -Action Allow -Protocol TCP -LocalPort $Port | Out-Null
}

Write-Host "Forwarding $ListenAddress`:$Port -> $WslAddress`:$Port"
netsh interface portproxy show v4tov4
