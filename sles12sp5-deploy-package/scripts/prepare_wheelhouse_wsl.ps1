param(
  [Parameter(Mandatory = $true)]
  [string]$Requirements,

  [Parameter(Mandatory = $true)]
  [string]$Output,

  [string]$Version = "",
  [string]$WslDistro = "",
  [string]$SmscRequirements = ""
)

$ErrorActionPreference = 'Stop'
$wsl = Get-Command wsl.exe -ErrorAction Stop | Select-Object -First 1

function Get-WslPath {
  param([string]$Path)
  $prefix = @()
  if ($WslDistro) { $prefix = @('-d', $WslDistro) }
  $result = & $wsl.Source @prefix wslpath -a $Path 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "wslpath 转换失败，路径 '$Path'：`n$result"
  }
  return (($result | Select-Object -Last 1).Trim())
}

function Get-WslDistros {
  $output = & $wsl.Source --list --quiet 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "无法读取 WSL 发行版列表：`n$output"
  }
  return @(
    $output |
      ForEach-Object { "$_" -replace "`0", "" } |
      ForEach-Object { $_.Trim() } |
      Where-Object { $_ -match '\S' }
  )
}

function Test-WslPython311 {
  param([string]$Distro)
  $check = "command -v python3.11 >/dev/null 2>&1 && python3.11 -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)'"
  & $wsl.Source -d $Distro -e bash -lc $check > $null 2>&1
  return $LASTEXITCODE -eq 0
}

if (-not $WslDistro) {
  foreach ($candidate in (Get-WslDistros)) {
    if (Test-WslPython311 $candidate) {
      $WslDistro = $candidate
      break
    }
  }
  if (-not $WslDistro) {
    throw "未找到带 Python 3.11 的 WSL 发行版。请先运行 scripts/ensure_wsl_environment.ps1 -AllowInstall，或用 -WslDistro 指定发行版。"
  }
  Write-Host "自动选择 WSL 发行版: $WslDistro"
} elseif (-not (Test-WslPython311 $WslDistro)) {
  throw "指定的 WSL 发行版 $WslDistro 缺少 Python 3.11。请先运行 scripts/ensure_wsl_environment.ps1 -AllowInstall -WslDistro $WslDistro。"
}

$reqWsl = Get-WslPath (Resolve-Path -LiteralPath $Requirements).Path
$outPath = New-Item -ItemType Directory -Force -Path $Output | Select-Object -ExpandProperty FullName
$outWsl = Get-WslPath $outPath
$scriptWsl = Get-WslPath (Resolve-Path -LiteralPath $PSCommandPath).Path

$cmd = "bash '$scriptWsl' --requirements '$reqWsl' --output '$outWsl'"
if ($Version) { $cmd += " --version '$Version'" }
if ($SmscRequirements) {
  $smscWsl = Get-WslPath (Resolve-Path -LiteralPath $SmscRequirements).Path
  $cmd += " --smsc-requirements '$smscWsl'"
}

$prefix = @()
if ($WslDistro) { $prefix = @('-d', $WslDistro) }
& $wsl.Source @prefix -e bash -lc $cmd
if ($LASTEXITCODE -ne 0) {
  throw "WSL 依赖包构建失败，退出码 $LASTEXITCODE"
}
