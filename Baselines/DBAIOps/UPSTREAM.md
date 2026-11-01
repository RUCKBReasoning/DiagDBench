# DBAIOps：上游来源与无浏览器适配边界

## 核对基准

- 上游：`https://github.com/OpenDataBox/DBAIOps.git`
- 固定提交：`86fa8bfaa32eb55f227370285ec09ae4b7f32ee9`
- 本地 Git 对象中的 162 个上游文件在本目录均存在且逐字节一致；本轮未修改这些原版文件。
- 此次 GitHub 连接不可用，因此固定提交不代表已核验远端最新版本。
- 原仓库是浏览器扩展，不是 Python 数据库诊断包。`native_runner.py` 是本项目移植的无浏览器适配器，覆盖中文、文本知识问答分支；它没有调用整个浏览器应用，也没有复现 AWR 分析、巡检、页面采集、交互历史、图像展示和多语言翻译等完整功能。

## 主要对照位置

- `DBAIOps_v1.1.1/js/popup/services/request-service.js`：`streamChat` 与 `callOllamaAPI`。
- `js/popup/managers/parameter-rules-manager.js`：`default-fast-search` 中文规则。
- `js/popup/managers/knowledge-base-manager.js`：PG/MySQL 知识库 ID。
- `js/popup/services/settings-service.js`：语言指令。
- `i18n/locales/zhcn.json`：原生系统提示和上下文标签。

## 本轮修正

1. 移除远程服务失败后自动改用 `Baselines/Codex/docs` 的本地 BM25 知识回退。正式路径只使用 DBAIOps 知识服务；HTTP 失败、服务拒绝或空结果不会切换知识来源或继续调用诊断模型。
2. 按上游 `isSupportImg="true"` 请求解析 `data.dataList`，保留原响应用于追踪；不将旧格式或错误响应误当作本地检索触发器。图像列表只随原始响应保存，本适配器不提供图像渲染。
3. 原版参数规则不只配置知识查询，也覆盖诊断 LLM 的 temperature。诊断恢复为规则值 `0.7`；配置中的温度仍可用于后续评测产物转换，二者在 trace 中分别记录。
4. 从原版中文 locale 读取提示词；恢复带“相似度：”/“版本：”的知识结果分支，以及其他文本按页面内容截取 2000 字符的原版分支。无浏览器环境使用 `about:blank` 代替页面 URL；保留并明确标记评测追加约束。
5. 每次启动使用唯一目录，避免同秒重复运行覆盖；知识数据集 ID 可由 `knowledge_service.dataset_id` 传入。

## 共用入口之外的非原生代码

```text
Evaluation/method_adapters.py::run_agent
  -> dbaiops.py::run_dbaiops_agent(prompt, config, container_name, output_dir, files, logger)
  -> script.sh
  -> collect_db_snapshot.py（可选只读采集；评测入口启用）
  -> native_runner.py::run
       -> retrieve_knowledge（远程知识服务）
       -> build_native_messages / chat_completion（移植的知识问答）
       -> generate_artifact_action（额外 LLM 调用及格式修复）
```

- `dbaiops.py`：数据库类型/端口/配置适配、独立知识服务密钥、超时和结果读取。共用 `prompt` 参数不直接使用，实际从白名单症状/两阶段 workload 文件和快照重建问题。
- `collect_db_snapshot.py`：PG/MySQL 运行状态采集，是评测证据适配，不属于浏览器扩展功能。
- `native_runner.py`：Python HTTP 客户端、OpenAI/Azure 路由、非流式调用、重试/上下文预算与追踪；保留规则、检索和文本提示的对应行为，但不是直接运行原 JavaScript。
- 原浏览器模型请求还携带 `similarity`、`top_n`、`knowledge_base_id` 扩展字段；本适配器已在独立知识请求中应用检索参数，不向标准 chat-completions 服务重复传这些非标准字段。这是显式的传输适配，并非逐字节请求复刻。
- 共用 `DIAGNOSIS_REQUIREMENTS` 和事实/推断约束追加到系统提示；这属于实验条件，会影响提示词，未删除安全/证据边界。
- `generate_artifact_action`：原生问答完成后额外调用模型，产生统一恢复脚本和可选 SQL 改写，并做格式修复重试。模型不得改写冻结的诊断报告；失败时仍保留 `native_diagnosis_report.md`。恢复能力、token 和耗时不能全部归因于原版 DBAIOps。
- `test_native_runner.py`、本说明文档也是本项目新增，而非上游文件。

## 配置与验证边界

- 知识服务密钥与 LLM 密钥独立，不能相互替代。
- 旧 `knowledge_service.local_knowledge_dir` / `DBAIOPS_LOCAL_KNOWLEDGE_DIR` 在共用入口只触发废弃警告，不再启用回退。`--local-knowledge-dir` CLI 参数以及 `retrieve_knowledge(..., fallback_dir=..., dbms=...)` 旧关键字形式仍兼容，但只警告并忽略回退配置，不会恢复自动替代算法。
- 默认 PG 数据集为 `2104`，MySQL 为 `2102`；服务是否可用及数据集权限必须在实际环境验证。
- 本轮未启动浏览器、连接数据库、请求模型/知识服务或执行生成脚本。`test_native_runner.py` 通过模拟服务核验上游规则、请求/提示分支、失败行为、报告冻结及产物隔离，不代表真实服务端到端测试通过。

## 跨文件接口兼容

- 共用评测器和独立 CLI 使用 `Baselines/native_runtime.py` 的同一凭据解析规则，支持配置文件、`api_key_env` 和独立知识密钥；不替换为 LLM 密钥。
- 配置中的相对 `conda_home` 固定以项目根目录解析。直接执行 `script.sh` 时，样本、输出和环境相对路径以调用者工作目录解析，进入方法目录不会改变其含义。
- `run_dbaiops_agent` 的参数顺序和 finish 返回字段不变。它通过 `DIAGDBENCH_NATIVE_RUN_DIR` 与脚本约定本次唯一目录，读取精确产物文件；独立脚本不设置该变量时仍自动创建唯一目录。
- `Baselines/test_native_interfaces.py` 覆盖真实模块分发、独立入口、脚本参数、旧参数、产物回传及共用 `save_finish`。数据库、Docker 和模型边界采用模拟，不代表真实服务已可用。


## DBA 授权恢复产物转换（2026-11-02）

- 生产转换器参考原项目 `Test/DB_Baselines/converter.py` 的
  `2026-11-01-dba-recovery-v1` 策略，统一使用 `Baselines/recovery_artifacts.py`，
  当前版本为 `2026-11-02-dba-recovery-v2`。不依赖或同步原项目的 `Test` 目录。
- 这仍是**原生诊断之后的额外模型转换**，不是上游方法新增的原生能力。
  原生报告在转换期间保持不变；旧报告中的“仅观察/需人工授权”不再自动限制
  已获授权的目标数据库修复。脚本须重新观察数据库、确认目标与因果关系，
  才能实施有依据的设置调整、对象维护和会话取消/终止；不得盲用历史 PID。
- 转换输入仅包含冻结报告、原始现象、完整初始化/验证 SQL，以及配置中的
  `dbms`、`database_name`、`database_schema_name`。不附加完整配置、原始快照、
  注入脚本、答案、旧恢复脚本或评分；凭据由评测器执行时通过环境变量注入。
  历史快照仍可用于原生诊断，但不能代替恢复脚本的实时目标检查。
- 模型生成两项字符串：`solution_script.sh` 和可选 `verified_workload.sql`。
  对外仍返回原四字段 `action=finish`，入口签名、文件名及共用调用链不变。
  未提供有效改写时，转换器逐字节复制原始验证 SQL 到
  `optimized_verified_workload.sql`，不拼接初始化 SQL，避免评测因空产物跳过恢复。
  非空改写仍须保持验证语句数量、顺序及每条语句的完整可观察语义。
- 保留 `--context-chars` 等既有参数以兼容调用；恢复转换不再静默截断公开证据，
  超过模型输入预算明确失败。缺失原始 `verified_workload.sql` 也明确失败，
  不再用混有初始化阶段的 `merged_workload.sql` 替代。
- 脚本要求 1–1800 秒有限超时、数据库客户端、错误传播、针对性后置检查。
  校验不再将 SQL 写操作当作只读违规；MySQL 的显式 `KILL QUERY/CONNECTION`
  与 OS `kill` 区分处理。证据不足仍应明确失败，不能为“必须修复”而伪造目标。
  静态命令拦截和 `bash -n` **不是安全沙箱，也不能证明修复正确**；实际执行仍
  由既有评测器及容器隔离负责，禁止 OS/容器操作及读取私有注入材料。
- 新增 `conversion_prompt.txt`、`conversion_meta.json`，记录策略版本、输入身份、
  产物摘要及是否使用原始 SQL 回退；`execution_status=not_executed` 仅描述
  转换阶段未执行脚本，真实执行结果以评测日志为准。
- `Baselines/test_recovery_artifacts.py` 覆盖修复 SQL、输入边界、上下文预算、
  报告冻结、回退、格式重试、元数据及公共评测保存/管理员凭据接口。
  全部离线模拟，不请求模型或数据库，不执行生成脚本。未更改评分、重跑历史
  实验或覆盖旧产物；旧缓存不会自动升级，需要另行显式生成新的运行产物。
