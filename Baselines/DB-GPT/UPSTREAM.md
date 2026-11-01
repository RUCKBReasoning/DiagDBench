# DB-GPT：上游来源与 DiagDBench 适配边界

## 核对基准

- 上游：`https://github.com/TsinghuaDatabaseGroup/DB-GPT.git`
- 固定提交：`73cb3297fae0823c4bf8c3451513f7dbd3961f4b`
- 核对的是本地原仓库的 Git 对象，不是已修改的工作区，也不是远端最新版本。此次无法连通 GitHub，未验证远端 HEAD。
- 本目录保留诊断 CLI 所用的多智能体实现，并非完整上游应用镜像。网页界面、文档抽取/知识生成流水线 `doc2knowledge`、完整 Prometheus 部署、异常生成器、历史实验产物等未全部收录。不能据此声称提供了整个上游项目的全部功能。

## 本轮修正

1. 移除默认的词项检索替代路径，恢复 `KBServiceFactory`、已注册专家知识库、原生向量检索、相似度阈值及按 `cause_name` 去重逻辑。保留依赖/查询失败时明确报错的兼容性修正。
2. 缺少 `sentence-transformers` 时不再生成哈希词频向量冒充语义嵌入；使用原版 SentenceTransformer 路径及同一远程模型后备路径。
3. `ReporterAgent` 恢复上游提示词。恢复专家到指标类别的上游条件分支，包括其原有子串匹配行为；不再把 workload/index/query 专家改派到额外的 query 类别。
4. 补回九份 `knowledge_base/*/content/*.json` 原始专家知识，以及 `prometheus_metrics.yaml`、`yaml_utils.py`，均与固定提交逐字节相同。
5. 将有限的 Docker/PostgreSQL 观测目录移到 `benchmark_metrics.py`，由评测启动器显式选择 `container`；原生 CLI 默认选择上游 `prometheus` 指标目录。它是证据来源适配，不是完整 Prometheus 等价实现，也不会启动或连接原仓库硬编码的服务器。
6. 每次启动使用唯一运行目录；运行状态、workload 缓存及最终报告写入该目录。启动器读取本次指定的 `native_result.json`，不再从共享 `alert_results` 按修改时间猜测结果。此修正不表示上游全部调试日志都已改造成并发隔离。
7. 修复短指标序列采样步长为零、top-five 替换后评分表未更新的问题；保留已有空值/常数序列处理、导入路径与模型传输兼容性修正。

## 不属于上游方法的接口及行为

```text
Evaluation/method_adapters.py::run_agent
  -> db_gpt.py::run_db_gpt_agent(prompt, config, container_name, output_dir, files, logger)
  -> script.sh
  -> collect_runtime_metrics.py
  -> run_diagnose.py / multiagents（原生诊断）
  -> artifact_adapter.py（额外 LLM 调用）
  -> action=finish / diagnosis_report.md / solution_script.sh / verified_workload.sql
```

- `db_gpt.py`：评测参数转换、Docker 端口解析、PostgreSQL 限制、运行超时及结果读取；入口接受共用 `prompt`，但实际按白名单样本文件重建输入，不直接使用该字符串。
- `collect_runtime_metrics.py` / `benchmark_metrics.py`：Docker/SQL 实时观测，不等于上游 node/postgres exporter 全量时间序列。`script.sh` 将症状、两阶段工作负载和观测组织成原生告警输入；这是本项目生成的告警描述，不是原始 Prometheus 告警。
- `benchmark_policy.py` 与 `BaseAgent`：向角色描述及任务模板追加共用诊断/安全/SQL 等价性约束。该约束被有意保留，确实会影响原生提示词，不能称为完全未经改动的原生实验。
- `artifact_adapter.py`：冻结原生报告后，再调用模型生成恢复脚本及可选 SQL 改写，带格式验证和重试。它不是上游 DB-GPT 的诊断能力；恢复成绩和这部分 token/耗时应与原生诊断分开说明。
- `native_preflight.py`：本地资源检查，不调用模型、不生成嵌入、不创建向量索引。通过仅表示本地先决条件检查通过，不保证远程服务或模型可用。
- `configs/*.py` 是从上游示例落地的本地配置，包含环境变量式凭据/模型地址、数据库连接和运行目录；OpenAI/Azure 传输、上下文预算、依赖懒加载等也是发布/运行兼容性适配。

## 原生知识库准备（本轮未执行）

当前检查时，`knowledge_base/info.db` 没有表，`chroma_db` 不存在，默认本地 SentenceTransformer 权重目录也不存在。因此不能宣称原生诊断已端到端运行成功。

1. 使用 DB-GPT 专用环境及本仓库依赖；配置 `configs/model_config.py` 的语义嵌入服务/模型。生成索引和检索时的模型与维度必须一致；知识向量嵌入与 `multiagents/llms/sentence_embedding.py` 的语义相似度嵌入是两条使用路径，都不能被哈希向量替代。
2. 原版 `init_database.py --create-tables` 可初始化元数据；`--increament` 可导入尚未登记的知识文件。**这些操作会写入知识库，嵌入服务可能联网并计费，运行前应检查配置及备份现有库。** 本轮仅恢复原始知识源，没有执行这些命令。
3. 模型/库准备完成后再执行 `script.sh --preflight`；删除旧的 `DB_GPT_KB_MODE=bundled` 设置。不提供静默降级模式。
4. 独立原生 CLI 可通过 `DB_GPT_METRICS_SOURCE=prometheus` 使用原版指标命名。评测启动器固定使用其实际采集的 `container` 观测；不能将两类证据混用。

## 离线回归

`test_upstream_contract.py` 覆盖语义嵌入失败边界、原生检索调用/去重、专家路由、指标边界及运行产物隔离。测试不执行异常注入、恢复脚本、数据库或模型请求；Bash 仅做 `bash -n` 语法检查。

## 跨文件接口兼容

- `run_db_gpt_agent` 参数顺序及 finish 返回字段不变；共用入口仍通过原模块路径动态加载。
- 通过 `Baselines/native_runtime.py` 统一项目相对环境路径解析，并使用 `DIAGDBENCH_NATIVE_RUN_DIR` 将 Python 入口、启动脚本与精确产物文件绑定。旧的独立脚本调用不设置该变量时仍自动创建目录。
- 原生交互状态模板使用相对源码文件的绝对位置，运行目录改变不会读错模板。`multiagents` 仅为已声明的公开导出初始化模型注册，普通属性探测不再意外要求模型密钥。
- `Baselines/test_native_interfaces.py` 覆盖真实分发器、启动脚本、报告文件及共用 `save_finish` 的串联；外部数据库/模型被模拟。原有知识库初始化与模型依赖要求不因此改变。


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
