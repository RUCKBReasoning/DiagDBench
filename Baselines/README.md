# 数据库智能体测评方法

本目录包含五种方法，统一输出 `diagnosis_report.md`、`solution_script.sh`、
`optimized_verified_workload.sql`。诊断输入是用户现象、初始化与验证负载、账号及运行时观测。

| 方法 | 入口 | 诊断方式 |
|---|---|---|
| ReAct | `ReAct/react.py` | 显式动作/观察循环 |
| Codex | `Codex/codex.py` | 原生 Codex CLI 循环，受控 MCP 工具 |
| Claude Code | `Claude_Code/claude_code.py` | 原生 Claude Agent SDK，原生 WebSearch/WebFetch 与受控 MCP 工具 |
| DB-GPT | `DB-GPT/db_gpt.py` | 原生多智能体诊断，再编译恢复产物；目前仅 PostgreSQL |
| DBAIOps | `DBAIOps/dbaiops.py` | 知识检索问答诊断，再编译恢复产物 |

## Prompt 约束

`input_contract.py` 统一维护输入格式与五种方法共享的测评约束。ReAct 在模板渲染时加入，
Codex/Claude 在原生任务 Prompt 中加入，DB-GPT 在各角色的角色说明和模板中加入，
DBAIOps 在诊断系统 Prompt 中加入。两种报告转代码适配器也加入相同约束。

- 仅允许优化 `verified_workload.sql`；初始化负载保持原样，不混入优化产物。
- SQL 数量、顺序、逐条结果、重复值、NULL、事务边界、副作用和持久状态必须等价。
  无法证明等价或没有具体性能收益时返回空优化字段，保存器保留原验证负载。
- 恢复脚本必须可从异常状态独立执行，不能依赖本次诊断已做的操作。
- 成功退出意味着异常及相关恢复期残留已清理，并可立即验证负载。
  需按事件实际情况，对回滚/undo、锁释放、I/O、维护完成、资源及服务就绪做有界轮询和后置检查。
  无法确认恢复应非零退出，不能用固定 sleep、后台任务或忽略错误代替完成证明。
- 报告区分已观测、建议和未验证事项。保存器只做格式与 Bash 语法检查；
  SQL 语义与真实恢复效果仍需后续评测验证，不能把文件保存成功等同于恢复成功。

## 原生框架的能力与隔离

| 能力 | Codex | Claude Code |
|---|---|---|
| 自主循环 | 原生 CLI | 原生 Agent SDK |
| 数据库 Bash/SQL | `db_execute`，本样本封闭容器 | 同左 |
| 本地文件 | `workspace_*` | 同左 |
| Python/Bash 辅助分析 | `analysis_execute`，独立离线容器 | 同左 |
| 计划、观察日志 | `plan_*`、`read_observation` | 同左 |
| 文档与网络 | 固定文档、官方 PG 在线检索、PG/MySQL 官方文档读取 | 同左，另有原生 `WebSearch`/`WebFetch` |
| 辅助专家 | 同模型、只读、共享预算的 MCP 专家 | 未开启 |

Codex 当前使用 Chat Completions 网关适配 Responses 协议。该网关不能执行
Responses 原生 `web_search`，因此在线检索由 MCP 提供；不将其描述为完整的原生网页工具评测。
Claude 的原生网页功能仍取决于实际模型供应商支持，离线测试不验证供应商的在线搜索服务。

每个样本只开放 `<sample_output>/agent_workspace/inputs/` 和 `work/`。
`inputs/` 只含批准的现象、负载、账号；`work/` 用于生成的笔记和脚本。
上级目录、绝对路径、隐藏路径、符号链接、硬链接均拒绝访问。
初始化日志、注入代码、原始样本、控制上下文和兄弟样本输出不开放。

两个原生 Agent 在初始化和注入完成后，都在**同一个**数据库容器中封闭宿主机挂载，
检查存活进程的文件引用，并断开外部网络。SQL 文件读取受到相同边界约束。
封闭失败直接失败退出，不放宽隔离，也不靠杀死异常进程来通过检查。
正常数据库进程和诊断证据仍保留。依赖原始文件或外部网络持续运行的异常，
可能不兼容此隔离方式，需要单独记录。

宿主机原生 shell/文件工具保持关闭，以受控工具提供同类功能；
`cwd` 和只读沙箱本身并不能防止读取宿主机其他目录。
Codex 禁用用户配置、规则与项目说明自动载入，并在网关过滤工具。
Claude 使用独立配置目录、空配置来源、严格 MCP 配置，关闭 CLAUDE.md/记忆自动载入，
并用 PreToolUse 拒绝未知能力。控制器和 SDK 运行文件不属于模型可访问的工作区。

默认数据库动作预算为 20（含提交），辅助工具预算为 100。
Claude SDK 总轮数为两者之和，原生网页调用计入 SDK 轮数；
MCP 辅助动作另外受辅助预算约束。各方法总算力并不自动相等。

## 目录与公共接口

顶层公共模块的职责如下：

| 文件 | 职责 |
|---|---|
| `__init__.py` | 轻量包初始化，不再提供历史 ReAct 模块别名，也不提前加载诊断运行依赖 |
| `input_contract.py` | 合并初始化/验证负载的输入呈现、保留阶段边界、渲染 Prompt，并统一追加诊断与恢复约束 |
| `preparation.py` | 准备阶段异常分类、数据准备与 I/O 调度、LLM 重试策略、异常注入命令，以及顺序批次 CLI |
| `container_boundary.py` | 样本暂存与四文件白名单、诊断阶段隔离、可信注入命名空间及受限容器执行 |
| `script.sh` | 单方法配置检查、运行锁及入口分派 |

各方法、评估入口和测试直接导入现存模块；ReAct 使用
`from Baselines.ReAct import react`。Codex 和 Claude Code 保留各自的样本驱动。
导入 `preparation.py` 不会启动批次；批次调度仅在脚本或模块入口执行时运行。
样本暂存与容器边界位于同一模块，保留原有函数签名、公开文件白名单及
`_sample_staging_dir` 配置字段，不改变异常机制或评估结果协议。

新路径使用大小写准确的 `ReAct`、`Codex`、`Claude_Code`。
Codex 驱动、协议适配和产物 schema 都在自身目录；Claude Code 复用其中的
MCP、工具和隔离组件，整理目录时需同步保留这些调用关系。

规范配置名为 `workflow_type: "codex"`，选项块为 `"codex"`。
统一入口为 `Baselines/Codex/codex.py`，默认密钥环境变量为 `CODEX_MODEL_KEY`。
具体配置、调用指令和参数见 `Baselines/Codex/README.md`。

`Configs/` 的每个配置叶目录仅保留一个 `example.json`，历史运行目录与重复配置已清理。
独立诊断配置位于 `Configs/baselines/<method>/example.json`，保留五种方法。
所有 LLM/Judge 的地址、密钥、API 版本和模型名均为空，运行前需复制到 Configs 之外的本地配置并填写；
基线密钥可通过示例中的 `api_key_env` 指定；Judge 密钥可通过 `api_key_env` 环境变量提供，无需写入配置。DBAIOps 知识服务地址需单独填写，使用独立密钥环境变量。

| 配置 | 入口与用途 |
|---|---|
| `Configs/evaluation/codex/example.json` | `Evaluation/test_baseline_eval.py`：Codex 诊断与评估 |
| `Configs/evaluation/<react\|db_gpt\|dbaiops>/example.json` | `Evaluation/test_baseline_eval.py`：指定方法诊断与评估 |
| `Configs/evaluation/scoring/example.json` | `Evaluation/test_eval.py`：评估既有诊断产物 |

Codex 与独立评估入口默认加载上表对应示例；其他方法组合评估需通过 `--config` 显式指定。
组合配置分为 `baseline`、`eval` 两块，独立评估配置使用平铺字段。
新示例使用 3 次测量、3 次 Judge、60 秒恢复等待与 8 倍耗时归零阈值，默认单个并行任务。
示例不包含实际评估样本；运行前须填写输入路径、输出目录、模型信息、容器前缀与端口。

`script.sh CONFIG_JSON` 用于启动单个方法：校验配置、定位 Python 环境、
检查容器前缀与端口、执行 dry-run、锁定输出目录、保存配置快照，再启动对应入口。
它读取 `Configs/baselines` 类型的方法配置，不读取上述评估配置。
`preparation.py` 的批次入口接收 `--config-dir`、`--output-dir` 和可选 `--framework`；
前者存放方法 JSON，后者存放 `agentic_smoke_status.json` 与 `suite_status.json`。
默认配置目录为 `Configs/baselines`，输出目录为 `Output/baseline_suite`；支持 `<method>/example.json`，也兼容用户提供的旧式 `<method>.json`。
批次仍要求每个方法并行度为 2、预检通过且配置哈希匹配；负责进程组监督、
端口选择、失败状态记录与本批次容器/tmux 清理。

已清除批次调度、Codex 协议适配和 Claude SDK 配置中的临时调试上报代码。

## 运行与验证

在项目根目录、包含依赖的 `db_diag` Python 环境中：

```bash
python Baselines/Codex/codex.py --config YOUR_CONFIG.json --dry-run
python Baselines/Codex/codex.py --config YOUR_CONFIG.json --preflight
python Baselines/Claude_Code/claude_code.py --config YOUR_CONFIG.json --dry-run
bash Baselines/script.sh YOUR_CONFIG.json
python Evaluation/test_baseline_eval.py --config YOUR_EVAL_CONFIG.json --dry-run
python Evaluation/test_eval.py --config YOUR_SCORING_CONFIG.json
python Baselines/preparation.py --help

python -m unittest discover -s Baselines/Codex/Test -p 'test_*.py' -v
python -m Baselines.Codex.Test.host_probe
python -m Baselines.Codex.Test.integration --dbms postgresql
python -m Baselines.Codex.Test.integration --dbms mysql
```

原生 CLI/SDK 回归测试使用本地模拟模型服务，不消耗外部模型额度。
Docker 集成测试使用独立小型测试容器，不能替代全量样本诊断效果评测。
