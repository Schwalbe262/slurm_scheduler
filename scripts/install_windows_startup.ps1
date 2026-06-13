param(
    [string]$TaskName = "SlurmSchedulerWeb",
    [string]$Distro = "",
    [string]$LinuxProjectDir = "/home/peets/NEC/slurm_scheduler"
)

$ErrorActionPreference = "Stop"

$wslArgs = if ($Distro -eq "") {
    "-d Ubuntu -- bash -lc 'cd $LinuxProjectDir && bash scripts/start_web.sh >> logs/web.log 2>&1'"
} else {
    "-d $Distro -- bash -lc 'cd $LinuxProjectDir && bash scripts/start_web.sh >> logs/web.log 2>&1'"
}

$action = New-ScheduledTaskAction -Execute "wsl.exe" -Argument $wslArgs
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Start Slurm Scheduler Web UI in WSL" -Force | Out-Null

Write-Host "Installed Windows startup task: $TaskName"
Write-Host "Start manually: Start-ScheduledTask -TaskName $TaskName"
Write-Host "Remove: Unregister-ScheduledTask -TaskName $TaskName -Confirm:`$false"
