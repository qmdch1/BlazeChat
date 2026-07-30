param(
    [int]$Connections = 1000,
    [int]$Duration = 30,
    [int]$Warmup = 10,
    [int]$MessagesPerSecond = 10
)

$ErrorActionPreference = "Stop"
$resultDir = Join-Path $PSScriptRoot "..\benchmark-results"
New-Item -ItemType Directory -Force -Path $resultDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$output = Join-Path $resultDir "$stamp.txt"

docker compose up -d --build redis chat1 chat2 chat3 chat4 proxy
try {
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:8080/health" -TimeoutSec 1
            if ($health.status -eq "ok") { break }
        } catch {}
        Start-Sleep -Seconds 1
    }

    docker compose build bench
    docker compose run --rm bench `
        --url ws://proxy:8080/ws `
        --connections $Connections `
        --duration $Duration `
        --warmup $Warmup `
        --messages-per-second $MessagesPerSecond |
        Tee-Object -FilePath $output
} finally {
    docker compose stats --no-stream
}

Write-Host "Result saved to $output"
