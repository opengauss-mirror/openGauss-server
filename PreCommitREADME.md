# pre-commit 提交前检查（openGauss-GaussMaster）

本仓库在 `git commit` 前自动对**本次改动**做基础检查与格式化，减少低级错误进入版本库。

## 检查项

| 类别 | 工具 | 说明 |
|------|------|------|
| 基础 | pre-commit-hooks | 去尾随空格、文件末尾换行、YAML/JSON/TOML 语法、大文件、私钥检测 |
| 拼写 | codespell | 常见英文拼写错误 |


## 一键安装（推荐）

在仓库根目录执行：

```bash
bash setup-pre-commit.sh
```

脚本会：

1. 在 `.pre-commit-venv/` 创建**隔离**虚拟环境（不污染系统 Python）
2. 安装 `pre-commit`
3. 执行 `pre-commit install` 与 `pre-commit install-hooks`

可选：指定 pip 镜像

```bash
PIP_MIRROR=https://pypi.tuna.tsinghua.edu.cn/simple bash setup-pre-commit.sh
```

## 手动安装

```bash
pip install pre-commit
pre-commit install
pre-commit install-hooks
```

## 日常使用

- **提交时**：`git commit` 会自动跑钩子；失败则 commit 中止，按提示修复后重试。
- **手动试跑**（不提交）：

```bash
.pre-commit-venv/bin/pre-commit run --files GaussMaster/startup.py
# 或检查全部已暂存文件
.pre-commit-venv/bin/pre-commit run
```

- **跳过单次检查**（不推荐，仅紧急场景）：

```bash
git commit --no-verify -m "message"
```

## 卸载

```bash
.pre-commit-venv/bin/pre-commit uninstall
rm -rf .pre-commit-venv
```

## 常见问题

### 1. python版本问题

使用 `setup-pre-commit.sh` 安装时，需要保证 `python >= 3.10`


### 2. 首次运行较慢

`install-hooks` 会从 gitcode 镜像拉取钩子仓库并构建 darker 等环境，属正常现象。


### 3. Windows

建议在 Git Bash 或 WSL 中运行 `setup-pre-commit.sh`。也可在 PowerShell 中手动创建 venv 并 `pre-commit install`。

## 配置文件一览

| 文件 | 作用 |
|------|------|
| `.pre-commit-config.yaml` | 钩子列表与版本 |
| `setup-pre-commit.sh` | 隔离环境一键安装脚本 |

## 维护说明

- `.pre-commit-venv/` 已加入 `.gitignore`，勿提交到远程。
