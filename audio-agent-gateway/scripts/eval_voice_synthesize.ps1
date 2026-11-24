param([Parameter(Mandatory)][string]$OutputDirectory)
$ErrorActionPreference = 'Stop'

# Windows PowerShell 5.1 can read this script as ASCII. The caller supplies the
# Chinese phrases in a UTF-8 manifest and owns the temporary output directory.
$manifest = Get-Content -Raw -Encoding UTF8 -LiteralPath (Join-Path $OutputDirectory 'inputs.json') | ConvertFrom-Json
$tts = New-Object -ComObject SAPI.SpVoice
$voice = $tts.GetVoices() | Where-Object { $_.GetDescription() -like 'Microsoft Huihui Desktop*' } | Select-Object -First 1
if (-not $voice) { throw 'Microsoft Huihui Desktop is unavailable.' }
$tts.Voice = $voice
$generated = @{}
foreach ($item in $manifest) {
    if (-not $item.file -or $generated.ContainsKey($item.file)) { continue }
    $stream = New-Object -ComObject SAPI.SpFileStream
    $stream.Format.Type = 18 # SAFT16kHz16BitMono
    $stream.Open((Join-Path $OutputDirectory $item.file), 3, $false)
    try {
        $tts.AudioOutputStream = $stream
        $tts.Rate = $item.rate
        $null = $tts.Speak($item.text)
    } finally {
        $stream.Close()
    }
    $generated[$item.file] = $true
    if (($generated.Count % 200) -eq 0) { Write-Output ("Synthesized {0} distinct clips" -f $generated.Count) }
}
