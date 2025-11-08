Start-Sleep -Seconds 1800
$csv1 = 'reports\prof_canary6\live_metrics_5m.csv'
$csv2 = 'reports\replay\prof_canary6\live_metrics_5m.csv'
$csv = if (Test-Path $csv1) { $csv1 } elseif (Test-Path $csv2) { $csv2 } else { $null }
$out = 'reports\summary\prof_canary6\live_metrics_5m_summary_first.md'
$dir = Split-Path $out -Parent
if (!(Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
if ($csv -and (Test-Path $csv)) {
  $line = (Get-Content $csv | Select-Object -Last 1)
  if ($line -and ($line -notlike 'ts5m,*')) {
    $parts = $line -split ','
    if ($parts.Length -ge 5) {
      $ts   = $parts[0]
      $p95  = $parts[1]
      $p95n = $parts[2]
      $sw   = $parts[3]
      $ex   = $parts[4]
      @(
        '# Live metrics 5m (first 30 min)'
        ''
        "ts5m=$ts"
        "p95_ms_avg=$p95; n_count=$p95n"
        "switch_per_min_avg=$sw"
        "explain_per_min_avg=$ex"
      ) | Set-Content -Encoding utf8 $out
    }
  }
}
