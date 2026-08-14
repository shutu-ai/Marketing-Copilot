param(
  [string]$WslDistro = "",
  [string]$PreferredDistro = "AlmaLinux-8",
  [switch]$AllowInstall,
  [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

function Write-Step {
  param([string]$Message)
  if (-not $Quiet) { Write-Host $Message }
}

function Get-WslPath {
  param(
    [string]$Path,
    [string]$Distro
  )
  $prefix = @()
  if ($Distro) { $prefix = @('-d', $Distro) }
  $result = & $wsl.Source @prefix wslpath -a $Path 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "wslpath 转换失败，路径 '$Path'：`n$result"
  }
  return (($result | Select-Object -Last 1).Trim())
}

function Get-InstalledDistros {
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

function Set-WslPython {
  param([string]$Distro)
  $setupScript = Join-Path $PSScriptRoot 'ensure_wsl_environment.sh'
  $scriptWsl = Get-WslPath (Resolve-Path -LiteralPath $setupScript).Path $Distro
  Write-Step "在 WSL 发行版 $Distro 中安装 Python 3.11 和 pip"
  & $wsl.Source -d $Distro -e bash -lc "bash '$scriptWsl' --install"
  if ($LASTEXITCODE -ne 0) {
    throw "在 WSL 发行版 $Distro 中安装 Python 3.11 失败，退出码 $LASTEXITCODE"
  }
}

$wsl = Get-Command wsl.exe -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $wsl) {
  if (-not $AllowInstall) {
    throw "未检测到 wsl.exe。请先启用 WSL，或运行 scripts/ensure_wsl_environment.ps1 -AllowInstall。"
  }
  Write-Step "未检测到 wsl.exe，尝试执行 wsl --install --no-distribution"
  & wsl.exe --install --no-distribution
  if ($LASTEXITCODE -ne 0) {
    throw "WSL 安装命令失败，退出码 $LASTEXITCODE"
  }
  throw "WSL 组件已安装或启用，可能需要管理员权限或重启；完成后再运行 scripts/ensure_wsl_environment.ps1 -AllowInstall。"
}

$distros = @(Get-InstalledDistros)
$selected = $null

if ($WslDistro) {
  if (Test-WslPython311 $WslDistro) {
    $selected = $WslDistro
  } elseif ($AllowInstall) {
    Set-WslPython $WslDistro
    $selected = $WslDistro
  } else {
    throw "指定的 WSL 发行版 $WslDistro 没有 Python 3.11。可运行 scripts/ensure_wsl_environment.ps1 -AllowInstall -WslDistro $WslDistro。"
  }
} else {
  foreach ($candidate in $distros) {
    if (Test-WslPython311 $candidate) {
      $selected = $candidate
      break
    }
  }

  if (-not $selected -and $AllowInstall) {
    if ($PreferredDistro -notin $distros) {
      Write-Step "安装 WSL 发行版 $PreferredDistro"
      & $wsl.Source --install -d $PreferredDistro
      if ($LASTEXITCODE -ne 0) {
        throw "安装 WSL 发行版 $PreferredDistro 失败，退出码 $LASTEXITCODE"
      }
      $distros = @(Get-InstalledDistros)
    }
    $selected = $PreferredDistro
    if ($selected -notin $distros) {
      throw "WSL 发行版 $PreferredDistro 安装后仍未出现在系统中，请重启或检查安装结果。"
    }
    Set-WslPython $selected
  }
}

if (-not $selected) {
  $available = if ($distros.Count -gt 0) { $distros -join ', ' } else { '（无）' }
  Write-Step "未找到带 Python 3.11 的 WSL 发行版。当前可用：$available"
  Write-Step "可运行 scripts/ensure_wsl_environment.ps1 -AllowInstall 自动安装 $PreferredDistro 和 Python 3.11。"
  exit 3
}

Write-Step "使用 WSL 发行版: $selected"
Write-Output $selected
