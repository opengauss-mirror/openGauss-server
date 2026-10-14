#!/usr/bin/env bash
# =============================================================================
# setup-pre-commit.sh  —— pre-commit 一键安装脚本（openGauss Plugin）
#
# 作用：在当前仓库为开发者一键配置好 pre-commit 本地检查环境。
#
# 设计原则（延续优秀实践）：
#   * 不污染系统/全局环境：所有 Python 工具装进项目隔离虚拟环境
#     .pre-commit-venv/（已建议加入 .gitignore），
#     不动系统 python、不执行 `pip config set`、不写 ~/.config/pip/pip.conf。
#   * pip 镜像源仅本脚本临时使用（-i / 环境变量），不影响其它项目。
#   * 幂等：可重复运行。
#   * C++ 工具链（clang-format, clang-tidy, cppcheck）需自行安装，本脚本仅检查并提示。
#
# 用法：
#   bash setup-pre-commit.sh
#   PIP_MIRROR=https://pypi.tuna.tsinghua.edu.cn/simple bash setup-pre-commit.sh
#
# 前提：
#   - 能访问 gitcode.com；
#   - 本机有 python>=3.10（用于运行 pre-commit 工具本身）；
#   - 本机已安装 clang-format, clang-tidy（LLVM 工具链），推荐版本 18 或 17。
# =============================================================================
set -euo pipefail

PIP_MIRROR="${PIP_MIRROR:-https://mirrors.aliyun.com/pypi/simple/}"
PIP_HOST="$(printf '%s' "$PIP_MIRROR" | sed -E 's#^https?://([^/]+)/.*#\1#')"
VENV_DIR=".pre-commit-venv"

log()  { printf '\033[1;32m[setup]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn ]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# =============================================================================
# 1. 基础环境检查
# =============================================================================
command -v git >/dev/null 2>&1 || die "未找到 git"
git rev-parse --show-toplevel >/dev/null 2>&1 || die "当前不在 git 仓库内，请在仓库根目录运行"
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
[ -f .pre-commit-config.yaml ] || die "当前仓库没有 .pre-commit-config.yaml，无法安装"

log "===== 检查 Python 3 环境 ====="
PY=""
for c in python3.13 python3.12 python3.11 python3.10 python3; do
  if command -v "$c" >/dev/null 2>&1; then
    ver="$("$c" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo 0.0)"
    major="${ver%%.*}"; minor="${ver##*.}"
    if [ "${major:-0}" -eq 3 ] && [ "${minor:-0}" -ge 10 ]; then PY="$c"; break; fi
  fi
done
[ -n "$PY" ] || die "需要 python>=3.10（pre-commit 要求）。请先安装。"
log "使用 python: $PY ($("$PY" --version 2>&1))"
log "使用 pip 镜像(临时): $PIP_MIRROR"

# =============================================================================
# 2. 检查 C++ 工具链（只检查，不自动安装，因为需要系统权限）
# =============================================================================
log "===== 检查 C++ 工具链 ====="
MISSING_DEPS=()
for tool in clang-format clang-tidy; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    MISSING_DEPS+=("$tool")
  fi
done

if [ ${#MISSING_DEPS[@]} -gt 0 ]; then
  warn "以下 C++ 工具未安装，pre-commit 将无法正常工作："
  for tool in "${MISSING_DEPS[@]}"; do
    warn "  - $tool"
  done
  warn ""
  warn "请根据你的系统安装 LLVM 工具链（推荐版本 17 或 18）："
  warn "  - openEuler/CentOS: sudo yum install llvm-toolset-18-clang-tools-extra"
  warn "  - Ubuntu/Debian:   sudo apt install clang-format clang-tidy"
  warn "  - 通用方式:        从 https://github.com/llvm/llvm-project/releases 下载预编译包"
  warn ""
  warn "检测到缺失工具，安装将继续，但建议先安装后再使用 pre-commit。"
  echo ""
else
  log "C++ 工具链检查通过："
  log "  - clang-format: $(clang-format --version | head -1)"
  log "  - clang-tidy:   $(clang-tidy --version | head -1)"
fi

# =============================================================================
# 3. 创建隔离的 Python 虚拟环境
# =============================================================================
log "===== 创建隔离的 Python 环境 ====="
if [ ! -d "$VENV_DIR" ]; then
  log "创建隔离环境: $VENV_DIR/"
  "$PY" -m venv "$VENV_DIR" || die "创建 venv 失败（可能缺 python venv 模块）"
else
  log "复用已存在的隔离环境: $VENV_DIR/"
fi
VENV_PY="$VENV_DIR/bin/python"

# =============================================================================
# 4. 安装 Python 工具到隔离环境
# =============================================================================
log "===== 安装 Python 工具到隔离环境 ====="
# 基础工具：pre-commit 是必须的
PKGS=(pre-commit)

# 智能检测：如果你的配置引用了 flake8 或 mypy，自动安装
if grep -q "id: flake8" .pre-commit-config.yaml 2>/dev/null; then
  log "检测到 flake8 配置，加入安装列表"
  PKGS+=(flake8)
fi
if grep -q "id: mypy" .pre-commit-config.yaml 2>/dev/null; then
  log "检测到 mypy 配置，加入安装列表"
  PKGS+=(mypy)
fi

log "将安装: ${PKGS[*]}"
"$VENV_PY" -m pip install --disable-pip-version-check -q \
  -i "$PIP_MIRROR" --trusted-host "$PIP_HOST" --upgrade pip
"$VENV_PY" -m pip install --disable-pip-version-check \
  -i "$PIP_MIRROR" --trusted-host "$PIP_HOST" \
  "${PKGS[@]}"

PRE_COMMIT="$VENV_DIR/bin/pre-commit"

# =============================================================================
# 5. 安装 Git 钩子并预热
# =============================================================================
log "===== 安装 Git 钩子 ====="
"$PRE_COMMIT" install

log "===== 预热钩子环境（首次会联网拉取，请稍候…） ====="
PIP_INDEX_URL="$PIP_MIRROR" PIP_TRUSTED_HOST="$PIP_HOST" \
  "$PRE_COMMIT" install-hooks

# =============================================================================
# 6. 收尾与提示
# =============================================================================
if ! grep -qxF "$VENV_DIR/" .gitignore 2>/dev/null && ! grep -qxF "$VENV_DIR" .gitignore 2>/dev/null; then
  warn "建议将 $VENV_DIR/ 加入 .gitignore（避免误提交隔离环境）"
fi

log ""
log "==========================================="
log "✅ 安装完成！"
log "==========================================="
log ""
log "📌 使用说明："
log "  1. 之后每次 git commit 会自动检查本次改动"
log "  2. 手动检查所有文件: $PRE_COMMIT run --all-files"
log "  3. 手动检查指定文件: $PRE_COMMIT run --files <文件路径>"
log ""
log "📦 卸载方法："
log "  1. $PRE_COMMIT uninstall"
log "  2. rm -rf $VENV_DIR/"
log "  完成即可完全卸载，系统环境干干净净。"
log ""
log "⚠️  注意：如果 C++ 工具链未完整安装，请先安装后再使用 pre-commit。"
