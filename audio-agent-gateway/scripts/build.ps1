param([string]$Python = "python")
$ErrorActionPreference = "Stop"
& $Python "$PSScriptRoot\build_windows.py"
if ($LASTEXITCODE -ne 0) { throw "Gateway build failed." }
