# WSL 构建环境参考

## 为什么不用官方 SLES 12 SP5 WSL 镜像

当前 `wsl.exe --list --online` 不提供 SLES 12 SP5，因此不能通过官方 WSL 在线镜像自动安装 SLES 12 SP5。本技能不会尝试安装 SLES 12 SP5 WSL 镜像，而是使用本机已有或可自动创建的兼容 Linux 发行版生成 manylinux2014 兼容轮子。

推荐的本机参考环境：

```text
发行版: AlmaLinux-8
Python: 3.11.13
用途:   生成 manylinux2014_x86_64 cp311 离线依赖包
```

`AlmaLinux-8` 的 glibc 为 2.28，高于 manylinux2014 要求的 2.17 基线；SLES 12 SP5 的 glibc 为 2.22。只要依赖包只使用 `manylinux2014_x86_64` 的 CPython 3.11 轮子，并避免使用目标机 Python 3.11.5 运行时之外的本地扩展，就可以用于 SLES 12 SP5 手动部署包。

## 自动检查 WSL

检查 WSL 是否可用：

```powershell
wsl.exe --status
wsl.exe --list --quiet
```

检查 WSL 中是否有 Python 3.11：

```powershell
wsl.exe -d AlmaLinux-8 -e python3.11 --version
```

## 自动准备兼容环境

在技能目录或项目根目录运行：

```powershell
.\scripts\ensure_wsl_environment.ps1 -AllowInstall
```

脚本行为：

1. 如果 `wsl.exe` 不存在，会尝试执行 `wsl.exe --install --no-distribution`。
2. 列出本机已安装的 WSL 发行版，优先选择已有且带 Python 3.11 的发行版。
3. 如果没有满足条件的发行版，默认安装 `AlmaLinux-8`，然后安装 Python 3.11 和 pip。
4. 安装完成或校验通过后输出选中的发行版名称。

首次启用 WSL 或安装新发行版可能需要管理员权限、网络下载和重启。重启后重新运行同一个命令即可继续。

## 手动安装 Python 3.11

如果 WSL 中已有兼容发行版，但没有 Python 3.11，可以只准备 Python：

```powershell
.\scripts\ensure_wsl_environment.ps1 -AllowInstall -WslDistro AlmaLinux-8
```

也可以在 WSL 内直接运行：

```bash
bash /path/to/skill/scripts/ensure_wsl_environment.sh --install
```

该脚本支持 `dnf`、`yum` 和 `apt-get` 系发行版，并会检查 Python 版本和 pip。

## 使用其他已有 WSL 发行版

只要发行版有 Python 3.11、pip 和网络，`prepare_wheelhouse_wsl.ps1` 会自动发现并使用它。也可以显式指定：

```powershell
.\scripts\prepare_wheelhouse_wsl.ps1 `
  -Requirements .\backend\requirements.txt `
  -Output .\release `
  -WslDistro AlmaLinux-8
```

## 需要真实 SLES 12 SP5 环境时的说明

如果后续需求不仅是生成兼容 wheelhouse，而是必须在真实 SLES 12 SP5 上验证部署或运行测试，WSL 自动准备只能提供兼容构建环境，不能替代 SLES 12 SP5 目标机。此时需要组织内已有的 SLES 12 SP5 虚拟机、物理机、rootfs 或容器镜像，并在这类环境上完成最终验证。

## 常见问题

- `wsl.exe` 不存在：先启用 WSL，或用 `ensure_wsl_environment.ps1 -AllowInstall` 尝试安装；如提示重启，重启后重新运行。
- 已安装发行版没有 Python 3.11：使用 `ensure_wsl_environment.ps1 -AllowInstall` 自动安装，或在发行版内运行 `ensure_wsl_environment.sh --install`。
- 误以为需要安装 SLES 12 SP5：当前官方 WSL 在线源没有该镜像，应使用 AlmaLinux-8 等兼容环境生成 manylinux2014 依赖包。
- 构建包在目标机报 glibc 版本错误：确认依赖包全部来自 `--only-binary=:all:` 的 `manylinux2014_x86_64`，不要混入本地编译的 Ubuntu/AlmaLinux wheel。
