# Remote Access

The scheduler runs on the Windows/WSL host and is exposed to trusted clients through Tailscale.

## Scheduler URL

```text
http://<scheduler-host>:8000/
```

Expected checks from a client inside the tailnet:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
curl "$SCHEDULER_URL/api/health"
curl "$SCHEDULER_URL/api/jobs"
```

## Host Components

The host needs three pieces running:

- Tailscale Windows service.
- Tailscale Serve TCP forwarding for port 8000.
- WSL scheduler service.

Check from Windows PowerShell on the host:

```powershell
Get-Service Tailscale
tailscale serve status
Get-ScheduledTask -TaskName SlurmSchedulerWeb
```

The Tailscale serve status should include TCP forwarding for port 8000 to `127.0.0.1:8000`.

## Tailscale Serve

Use raw TCP forwarding for direct IP access:

```powershell
tailscale serve --bg --tcp 8000 127.0.0.1:8000
```

HTTP serve mode may work with MagicDNS but can return `404 page not found` when accessed through the raw Tailscale IP. TCP forwarding is the expected mode for:

```text
http://<scheduler-host>:8000/
```

## WSL Startup

The scheduler service is installed inside WSL as a user systemd service:

```bash
systemctl --user status slurm-scheduler.service
systemctl --user start slurm-scheduler.service
```

On Windows login, the `SlurmSchedulerWeb` scheduled task starts WSL and starts the service:

```powershell
Start-ScheduledTask -TaskName SlurmSchedulerWeb
```

If the task is missing, register it from Administrator PowerShell:

```powershell
$Action = New-ScheduledTaskAction `
  -Execute "wsl.exe" `
  -Argument '-d Ubuntu-24.04 -- bash -lc "cd ~/slurm_scheduler && systemctl --user start slurm-scheduler.service || bash scripts/start_web.sh >> logs/web.log 2>&1"'

$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask `
  -TaskName "SlurmSchedulerWeb" `
  -Action $Action `
  -Trigger $Trigger `
  -Settings $Settings `
  -Description "Start WSL Slurm scheduler web service at Windows login"
```

## Troubleshooting

`404 page not found`:

- Usually Tailscale HTTP serve is active instead of TCP serve.
- Run `tailscale serve --bg --tcp 8000 127.0.0.1:8000`.

Timeout:

- Check Tailscale is running.
- Check WSL scheduler is listening on port 8000.
- Check the Windows scheduled task has started WSL.

WSL service inactive:

```bash
systemctl --user restart slurm-scheduler.service
journalctl --user -u slurm-scheduler.service -n 80 --no-pager
```

Wrong WSL distro:

```powershell
wsl.exe -l -v
```

Update the scheduled task argument to use the correct distro name.
