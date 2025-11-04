# Запускає app.main на вказану кількість секунд (за замовчуванням 120).
# Логи зберігаються в run_session.log, trade_log.jsonl, summary_log.jsonl.
# Якщо ці файли існують, вони перейменовуються з додаванням мітки часу.
# Використання: .\run_app_for_120s.ps1 -Seconds 180
# Використання командного рядка:

# cmd /d /c powershell -NoProfile -ExecutionPolicy Bypass -Command "& { . 'C:/Users/vikto/Desktop/AiOne_t_v2-main/venv/Scripts/Activate.ps1'; & 'C:/Users/vikto/Desktop/AiOne_t_v2-main/tools/run_app_for_120s.ps1' -Seconds 120 }"

param(
    [int]$Seconds = 120,
    [string]$RedisHost = $null
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

# Уніфікуємо UTF-8 кодування для консолі та файлових операцій (без BOM):
try {
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [Console]::OutputEncoding = $utf8NoBom
    [Console]::InputEncoding = $utf8NoBom
    $OutputEncoding = $utf8NoBom
}
catch {
    Write-Verbose "Не вдалося встановити UTF-8 кодування консолі: $_"
}

# Додатково перемикаємо кодову сторінку на UTF-8 (вирішує «кракозябри» у VS Code Terminal)
try { chcp 65001 | Out-Null } catch { }

$pythonExe = Join-Path $repoRoot 'venv\Scripts\python.exe'

$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'
$env:REDIS_PORT = if ($env:REDIS_PORT) { $env:REDIS_PORT } else { '6379' }

# Вибір REDIS_HOST: за замовчуванням — localhost (Docker). За потреби можна ввімкнути fallback на WSL IP через USE_WSL_REDIS=true
if (-not $env:REDIS_HOST) {
    $env:REDIS_HOST = '127.0.0.1'
    if ($env:USE_WSL_REDIS -eq 'true') {
        try {
            $wslIp = & wsl -d Ubuntu -- bash -lc "hostname -I | awk '{print $1}'" 2>$null
            if ($LASTEXITCODE -eq 0) {
                $wslIp = "$wslIp".Trim()
                if ($wslIp -and $wslIp -ne '127.0.0.1') {
                    $env:REDIS_HOST = $wslIp
                    Write-Host "[prep] USE_WSL_REDIS=true → використовую WSL IP: $wslIp" -ForegroundColor Cyan
                }
            }
        }
        catch { }
    }
}
# If caller passed RedisHost, set it in the environment so child process inherits it
if ($RedisHost) {
    $env:REDIS_HOST = $RedisHost
    Write-Host "[run] Using REDIS_HOST=$env:REDIS_HOST"
}
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFileOut = Join-Path $repoRoot "tools\run_session_stdout_$timestamp.log"
$logFileErr = Join-Path $repoRoot "tools\run_session_stderr_$timestamp.log"
$combinedLog = Join-Path $repoRoot "tools\run_session_$timestamp.log"
$tradeLog = Join-Path $repoRoot 'trade_log.jsonl'
$summaryLog = Join-Path $repoRoot 'summary_log.jsonl'

# Do not delete existing logs; write to timestamped files to avoid file locks

if (Test-Path $tradeLog) {
    $tradeBak = Join-Path $repoRoot "trade_log_$timestamp.jsonl.bak"
    Move-Item -Path $tradeLog -Destination $tradeBak -Force
    Write-Host "[prep] trade_log.jsonl → $(Split-Path $tradeBak -Leaf)"
}

if (Test-Path $summaryLog) {
    $summaryBak = Join-Path $repoRoot "summary_log_$timestamp.jsonl.bak"
    Move-Item -Path $summaryLog -Destination $summaryBak -Force
    Write-Host "[prep] summary_log.jsonl → $(Split-Path $summaryBak -Leaf)"
}

Write-Host "[run] Запускаю python -m app.main на $Seconds секунд…"

$proc = Start-Process -FilePath $pythonExe -ArgumentList '-m', 'app.main' -WorkingDirectory $repoRoot -PassThru -RedirectStandardOutput $logFileOut -RedirectStandardError $logFileErr

for ($elapsed = 30; $elapsed -le $Seconds; $elapsed += 30) {
    Start-Sleep -Seconds 30
    Write-Host "[run] Минуло $elapsed с"
    if ($proc.HasExited) {
        break
    }
}

if (-not $proc.HasExited) {
    Write-Host "[run] Зупиняю процес після $Seconds с" -ForegroundColor Yellow
    Stop-Process -Id $proc.Id -Force
    Start-Sleep -Seconds 2
}
else {
    Write-Host "[run] Процес завершився самостійно з кодом $($proc.ExitCode)"
}

$logContent = @()
if (Test-Path $logFileOut) {
    $logContent += Get-Content -Path $logFileOut
}
if (Test-Path $logFileErr) {
    $logContent += Get-Content -Path $logFileErr
}
if ($logContent.Count -gt 0) {
    Set-Content -Path $combinedLog -Value $logContent -Encoding utf8
}

Write-Host "[run] Завершено. Останні рядки run_session.log:" -ForegroundColor Cyan
if (Test-Path $combinedLog) {
    Get-Content -Path $combinedLog -Tail 120 -Encoding utf8
}
else {
    Write-Host "[warn] run_session.log відсутній" -ForegroundColor Yellow
}
