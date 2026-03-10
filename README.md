# TPCC-Tester

本项目为 2023-2025 全国大学生计算机系统能力大赛-数据库管理系统设计赛决赛模拟评测脚本，目前由 [db-camp](https://github.com/db-camp) 社区开发和维护。

欢迎未来的数据库大赛参赛选手使用和PR。

## 环境配置

本项目使用 `uv` 进行 Python 依赖管理。请确保您的系统已安装 `uv`。

1. **安装 `uv`** (如果尚未安装):
   ```bash
   curl -sSL https://astral.sh/uv/install.sh | sh
   ```

2. **创建虚拟环境并同步依赖**:
   ```bash
   uv sync
   ```

3. **运行项目**:
   ```bash
   python3 -m tpcc.main --scale 1 --init --benchmark --threads 16 --transactions 300
   ```

## 支持项目

如果你觉得这个项目对你有帮助，请考虑给它一个⭐️ Star支持！

[![Star History Chart](https://api.star-history.com/svg?repos=db-camp/TPCC-Tester&type=Date)](https://star-history.com/#db-camp/TPCC-Tester&Date)

[![Visitors](https://api.visitorbadge.io/api/visitors?path=https://github.com/db-camp/TPCC-Tester&label=visitors&countColor=%23263759)](https://visitorbadge.io/status?path=https://github.com/db-camp/TPCC-Tester)
