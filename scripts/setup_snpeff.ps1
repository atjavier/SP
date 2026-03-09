Param(
  [string]$InstallDir = "",
  [string]$DataDir = "./data",
  [string]$Genome = "GRCh38.86",
  [string]$DownloadUrl = "https://snpeff.blob.core.windows.net/versions/snpEff_latest_core.zip",
  [string]$ZipPath = "",
  [switch]$SkipGenomeDownload
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
  $here = $PSScriptRoot
  return (Resolve-Path (Join-Path $here "..")).Path
}

function Upsert-DotEnvLine {
  Param(
    [string]$DotEnvPath,
    [string]$Key,
    [string]$Value
  )

  if (-not (Test-Path $DotEnvPath)) {
    New-Item -ItemType File -Path $DotEnvPath -Force | Out-Null
  }

  $lines = Get-Content -Path $DotEnvPath -ErrorAction SilentlyContinue
  if ($null -eq $lines) { $lines = @() }

  $escapedKey = [Regex]::Escape($Key)
  $idx = -1
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "^\s*$escapedKey\s*=") {
      $idx = $i
      break
    }
  }

  $newline = "$Key=$Value"
  if ($idx -ge 0) {
    $lines[$idx] = $newline
  } else {
    if ($lines.Count -gt 0 -and $lines[$lines.Count - 1].Trim().Length -ne 0) {
      $lines += ""
    }
    $lines += $newline
  }

  Set-Content -Path $DotEnvPath -Value $lines -Encoding UTF8
}

$repoRoot = Get-RepoRoot
$dotEnvPath = Join-Path $repoRoot ".env"

if ([string]::IsNullOrWhiteSpace($InstallDir)) {
  $InstallDir = Join-Path $repoRoot "instance\\tools\\snpeff"
}

New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

$zipPathResolved = ""
if (-not [string]::IsNullOrWhiteSpace($ZipPath)) {
  $resolved = $null

  # Support passing a directory, a wildcard path, or a direct file path.
  if (Test-Path $ZipPath -PathType Container) {
    $candidate = Get-ChildItem -Path $ZipPath -File -Filter "*.zip" -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -match "(?i)snpeff" } |
      Select-Object -First 1
    if ($candidate) {
      $resolved = $candidate.FullName
    }
  } elseif ($ZipPath -match "[\\*\\?]") {
    $candidate = Get-ChildItem -Path $ZipPath -File -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($candidate) {
      $resolved = $candidate.FullName
    }
  } elseif (Test-Path $ZipPath -PathType Leaf) {
    $resolved = (Resolve-Path $ZipPath).Path
  }

  if (-not $resolved) {
    $pwd = (Get-Location).Path
    throw "ZipPath does not exist or did not match a zip file: $ZipPath`nHint: if the path has spaces, wrap it in quotes. Current directory: $pwd"
  }

  $zipPathResolved = $resolved
  Write-Host "Using local SnpEff zip: $zipPathResolved"
} else {
  if ([string]::IsNullOrWhiteSpace($DownloadUrl)) {
    throw "Either -ZipPath or -DownloadUrl must be provided."
  }

  $zipPathResolved = Join-Path $InstallDir "snpEff_latest_core.zip"
  Write-Host "Downloading SnpEff to: $zipPathResolved"
  Invoke-WebRequest -Uri $DownloadUrl -OutFile $zipPathResolved
}

Write-Host "Extracting SnpEff into: $InstallDir"
Expand-Archive -Path $zipPathResolved -DestinationPath $InstallDir -Force

$jar = Get-ChildItem -Path $InstallDir -Recurse -Filter "snpEff.jar" -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $jar) {
  throw "snpEff.jar not found after extraction. InstallDir=$InstallDir"
}

$snpeffHome = $jar.Directory.FullName
Write-Host "Found snpEff.jar at: $($jar.FullName)"
Write-Host "Setting SP_SNPEFF_HOME to: $snpeffHome"

Upsert-DotEnvLine -DotEnvPath $dotEnvPath -Key "SP_SNPEFF_ENABLED" -Value "1"
Upsert-DotEnvLine -DotEnvPath $dotEnvPath -Key "SP_SNPEFF_HOME" -Value $snpeffHome
Upsert-DotEnvLine -DotEnvPath $dotEnvPath -Key "SP_SNPEFF_JAR_PATH" -Value $jar.FullName
Upsert-DotEnvLine -DotEnvPath $dotEnvPath -Key "SP_SNPEFF_DATA_DIR" -Value $DataDir
Upsert-DotEnvLine -DotEnvPath $dotEnvPath -Key "SP_SNPEFF_GENOME" -Value $Genome

if (-not $SkipGenomeDownload) {
  $dataDirFs = $DataDir
  if (-not [System.IO.Path]::IsPathRooted($DataDir)) {
    $dataDirFs = Join-Path $snpeffHome $DataDir
  }
  New-Item -ItemType Directory -Path $dataDirFs -Force | Out-Null

  Write-Host "Downloading genome database: $Genome"
  Push-Location $snpeffHome
  try {
    & java "-Xmx2g" "-jar" $jar.FullName "download" "-v" "-dataDir" $DataDir $Genome
  } finally {
    Pop-Location
  }
}

Write-Host ""
Write-Host "Done."
Write-Host "Updated .env at: $dotEnvPath"
Write-Host "You can now run: python src\\serve.py"
