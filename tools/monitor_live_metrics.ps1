param(
  [int]$Port = 9126,
  [string]$OutDir = "reports\prof_canary6",
  [int]$IntervalSec = 60
)

$csv1 = Join-Path $OutDir "live_metrics.csv"
$csv5 = Join-Path $OutDir "live_metrics_5m.csv"

if (!(Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }
if (!(Test-Path $csv1)) { "ts,p95_ms,switch_delta_per_min,explain_lines_delta" | Out-File -Encoding utf8 $csv1 }
if (!(Test-Path $csv5)) { "ts5m,p95_ms,p95_samples,switch_per_min,explain_per_min" | Out-File -Encoding utf8 $csv5 }

$prevSwitch = $null
$prevExplain = $null
$prevTime = Get-Date

# буфер останніх записів для 5-хв вікна
$win = New-Object System.Collections.Generic.List[object]

while ($true) {
  try {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $txt = (Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$Port/metrics").Content

    # 1) p95 із гістр. квантілей, якщо присутні
    $p95 = ""
    $m = [regex]::Match($txt, 'ai_one_stage1_latency_ms\{[^}]*quantile="0.95"[^}]*\}\s+([0-9.]+)')
    if ($m.Success) { $p95 = $m.Groups[1].Value }

    # 2) лічильники
    $curSwitch = 0.0
    $ms = [regex]::Match($txt, 'ai_one_profile_switch_total\s+([0-9.]+)')
    if ($ms.Success) { $curSwitch = [double]$ms.Groups[1].Value }

    $curExplain = 0.0
    # сума по всіх символах: візьмемо перший сумарний рядок або просумуємо
    $exLines = 0.0
    $exMatches = [regex]::Matches($txt, 'ai_one_explain_lines_total(?:\{[^}]*\})?\s+([0-9.]+)')
    foreach ($em in $exMatches) { $exLines += [double]$em.Groups[1].Value }
    $curExplain = $exLines

    $now = Get-Date
    $minutes = [Math]::Max(0.0001, ($now - $prevTime).TotalMinutes)

    $switchRate = ""
    $explainRate = ""
    if ($prevSwitch -ne $null) { $switchRate  = [Math]::Round(($curSwitch  - $prevSwitch)  / $minutes, 3) }
    if ($prevExplain -ne $null){ $explainRate = [Math]::Round(($curExplain - $prevExplain) / $minutes, 3) }

    "$ts,$p95,$switchRate,$explainRate" | Out-File -Append -Encoding utf8 $csv1

    # 5-хв агрегати
    $row = [PSCustomObject]@{
      ts=$ts; p95=$p95; switchPerMin=$switchRate; explainPerMin=$explainRate; at=$now
    }
    $win.Add($row)
    # прибрати записи старші 5хв
    $cut = $now.AddMinutes(-5)
    $win = $win | Where-Object { $_.at -ge $cut } | ForEach-Object { $_ }

    # якщо є бодай 2 записи — рахуємо
    if ($win.Count -ge 2) {
      $p95vals = @()
      foreach ($r in $win) { if ($r.p95 -ne "" -and $r.p95 -match '^[0-9.]+$') { $p95vals += [double]$r.p95 } }
      $p95avg = if ($p95vals.Count) { [Math]::Round(($p95vals | Measure-Object -Average).Average, 1) } else { "" }
      $sw = @(); foreach ($r in $win) { if ($r.switchPerMin -ne "") { $sw += [double]$r.switchPerMin } }
      $ex = @(); foreach ($r in $win) { if ($r.explainPerMin -ne "") { $ex += [double]$r.explainPerMin } }
      $swAvg = if ($sw.Count) { [Math]::Round(($sw | Measure-Object -Average).Average, 3) } else { "" }
      $exAvg = if ($ex.Count) { [Math]::Round(($ex | Measure-Object -Average).Average, 3) } else { "" }
      $ts5 = Get-Date -Format "yyyy-MM-dd HH:mm:00"
      "$ts5,$p95avg,${($p95vals.Count)},$swAvg,$exAvg" | Out-File -Append -Encoding utf8 $csv5
    }

    $prevSwitch  = $curSwitch
    $prevExplain = $curExplain
    $prevTime    = $now
  } catch {
    # no-op: пишемо порожній рядок із ts, аби бачити розриви
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts,,," | Out-File -Append -Encoding utf8 $csv1
  }
  Start-Sleep -Seconds $IntervalSec
}
