param(
    [int]$Duration = 30,
    [int]$Warmup = 10,
    [int[]]$ConnectionSteps = @(1000, 5000, 10000, 25000, 50000),
    [int[]]$RateSteps = @(1, 5, 10, 25, 50),
    [int]$ThroughputConnections = 1000
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$resultDir = Join-Path $repoRoot "benchmark-results"
New-Item -ItemType Directory -Force -Path $resultDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$rawOutput = Join-Path $resultDir "$stamp-suite.txt"
$markdownOutput = Join-Path $resultDir "$stamp-suite.md"

function Invoke-Benchmark {
    param([string]$Mode, [int]$Connections, [int]$Rate)

    $lines = docker compose run --rm bench `
        --url ws://proxy:8080/ws `
        --mode $Mode `
        --connections $Connections `
        --duration $Duration `
        --warmup $Warmup `
        --messages-per-second $Rate
    $lines | Add-Content -Path $rawOutput
    $values = @{}
    foreach ($line in $lines) {
        if ($line -match '^([^=]+)=(.*)$') {
            $values[$matches[1]] = $matches[2]
        }
    }
    return $values
}

docker compose up -d --build redis chat1 chat2 chat3 chat4 proxy
docker compose build bench

$rows = @()
foreach ($connections in $ConnectionSteps) {
    $result = Invoke-Benchmark -Mode connections -Connections $connections -Rate 0
    $rows += [pscustomobject]@{
        Test = "connections"
        Connections = $result.connected
        Published = $result.published_messages_per_second
        Delivered = $result.delivered_messages_per_second
        P50 = "-"
        P95 = "-"
        P99 = "-"
        Max = "-"
        Errors = $result.errors
    }
    if ([int]$result.connected -lt [Math]::Floor($connections * 0.98) -or [int]$result.errors -gt 0) {
        break
    }
}

foreach ($rate in $RateSteps) {
    $result = Invoke-Benchmark -Mode throughput -Connections $ThroughputConnections -Rate $rate
    $rows += [pscustomobject]@{
        Test = "throughput ($rate msg/s/client)"
        Connections = $result.connected
        Published = $result.published_messages_per_second
        Delivered = $result.delivered_messages_per_second
        P50 = $result.latency_p50_ms
        P95 = $result.latency_p95_ms
        P99 = $result.latency_p99_ms
        Max = $result.latency_max_ms
        Errors = $result.errors
    }
    $deliveryRatio = if ([double]$result.sent -gt 0) {
        [double]$result.received / [double]$result.sent
    } else { 1.0 }
    if ([int]$result.errors -gt 0 -or $deliveryRatio -lt 0.99) {
        break
    }
}

$markdown = @(
    "# BlazeChat benchmark ($stamp)"
    ""
    "- Server budget: 4 CPU cores (chat 3.0, Redis 0.5, HAProxy 0.5)"
    "- Warmup: ${Warmup}s; measurement: ${Duration}s"
    "- Sustainable threshold: >=99% own-message echo and zero connection errors"
    ""
    "| Test | Connections | Published chat/s | Delivered msg/s | p50 ms | p95 ms | p99 ms | max ms | Errors |"
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|"
)
foreach ($row in $rows) {
    $markdown += "| $($row.Test) | $($row.Connections) | $($row.Published) | $($row.Delivered) | $($row.P50) | $($row.P95) | $($row.P99) | $($row.Max) | $($row.Errors) |"
}
$markdown | Set-Content -Encoding utf8 -Path $markdownOutput
docker compose stats --no-stream
Write-Host "Raw results: $rawOutput"
Write-Host "Markdown table: $markdownOutput"
