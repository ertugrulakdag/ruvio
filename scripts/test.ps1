param(
    [switch]$Clippy,
    [switch]$NativeResilience,
    [switch]$DockerResilience
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot

function Invoke-Native {
    param(
        [string]$Label,
        [string]$Executable,
        [string[]]$Arguments
    )

    Write-Host "`n==> $Label"
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Get-PythonCommand {
    foreach ($candidate in @("python3", "python", "py")) {
        if (Get-Command $candidate -ErrorAction SilentlyContinue) {
            if ($candidate -eq "py") {
                return @("py", "-3")
            }
            return @($candidate)
        }
    }
    throw "Python 3 is required. Install Python and add it to PATH."
}

try {
    Invoke-Native "Rust formatting" "cargo" @("fmt", "--check")
    if ($Clippy) {
        Invoke-Native "Rust Clippy" "cargo" @("clippy", "--all-targets", "--", "-D", "warnings")
    }
    Invoke-Native "Rust tests" "cargo" @("test", "--all-targets")
    Invoke-Native "Store smoke example" "cargo" @("run", "--example", "04_store_smoke")

    $Python = Get-PythonCommand
    if ($NativeResilience) {
        Invoke-Native "Native crash/recovery tests" $Python[0] (@($Python | Select-Object -Skip 1) + @(
            "scripts/native_resilience.py", "--build"
        ))
    }

    if ($DockerResilience) {
        Invoke-Native "Build resilience test image" "docker" @(
            "build", "--tag", "ruvio:resilience-test", "."
        )
        Invoke-Native "Docker resilience tests" $Python[0] (@($Python | Select-Object -Skip 1) + @(
            "scripts/docker_resilience.py"
        ))
    }
}
finally {
    Pop-Location
}
