# E004：一次完成固定验证包

这份说明用于保留黄区已有构建和实验记录，接入 E004，得到提前预取、进度协调和加载／重算三个配对结果。先确认原工作区并完成保全与接入，再启动提示词处理端（P）、生成端（D）和代理，然后执行固定请求包，最后用独立通信演示补充路径判断。

## 1. 准备版本和配置

准备分为原工作区确认、保留构建的代码接入和运行配置三步；确认得到的实际路径与版本决定接入命令。

### 1.1 确认原工作区与构建位置

在原先运行 E001–E003 的同一容器、同一 Python 环境中，进入现有统一 KV 缓存管理项目（UCM）的仓库根目录，执行以下六条只读命令。目的是确定代码起点、本地修改和构建位置；执行到本节末尾即停下并反馈。

```bash
git rev-parse --show-toplevel
git log -1 --format='%h %cs %D'
git remote -v
git --no-optional-locks status --short --branch --untracked-files=normal --ignored=matching
python -S -c "import sys; print(sys.executable); print(sys.version.split()[0])"
find . -maxdepth 6 \( -name CMakeCache.txt -o -name '*.so' \) -print
```

例如，提交行可能是 `<提交号> 2026-09-0x HEAD -> feature_26h1, origin/feature_26h1`；状态行可能包含 ` M test/...`、`?? experiments/...`、`!! build/`。它们分别表示已有文件被修改、新文件尚未纳入 Git、文件被 Git 忽略规则排除。编译产物和实验记录常在后两类；这些输出是在说明保全对象。

回传五项即可：仓库根目录与提交行；`origin` 的仓库名；所有已跟踪文件的改动路径，以及未跟踪／忽略项中的构建和实验目录；Python 路径与版本；构建缓存和共享库各一至两个路径示例，完整输出保留在黄区。远端地址只需仓库名，凭据无需回传。详细输出含义见附录 A。某条命令报错时，保留该条命令与错误首行，结束检查；文件查找没有输出时反馈“未找到”，由蓝区根据安装位置继续定位。

**先完成这六条检查并反馈，再进入 1.2。**

### 1.2 保全原目录并接入个人分支

接入以保留现有构建投入为目标，依次确定来源、保存本地内容、核对原生库（C/C++ 编译生成的共享库），再选择切换方式。以下是反馈后的执行顺序；涉及实际目录和库文件的命令在 1.1 核对后给出。

个人分支来自 `https://github.com/jxrjxrjxr/unified-cache-management.git`。官方 `origin` 保持原地址，个人仓库使用独立远端名 `personal`；已有同名远端时先核对其地址。获取个人分支只增加代码对象和远端引用，随后可比较黄区实际提交与 `personal/kvready/e004` 的差异。

本地内容按已修改源码、未跟踪实验文件、被忽略的编译产物分别保存；备份范围与恢复路径要覆盖这三类。保留原目录绝对路径最有利于沿用已有构建缓存。若原目录中的实验状态适合独立保留，可另建 Git worktree（同一仓库的独立源码工作目录）承载 E004，原工作区继续保存旧实验与编译产物。

选择原地切换时，先完成并检查可恢复备份，再核对目标分支与本地文件的路径冲突。选择独立 worktree 时，先核对 Python 安装指向和原生库加载路径，再确定需要复用的具体库文件。两种路径都以确认源码、二进制接口和构建参数匹配为复用条件；构建缓存与安装好的库分别处理。

E004 相对其个人 E003 起点新增的是 Python 实现与文档。黄区 2026 年 9 月初的官方提交与该起点之间的差异，需由实际提交号核对。原生代码和相关接口一致时优先直接复用已编译库；确有必要原生差异时，保留原构建目录，先评估所需目标和重编译范围。附录 B 保存安装脚本会触发构建的具体依据。

接入完成的检查结果应同时包含：E004 提交号；已保存的旧工作区位置或恢复方式；实际 Python 源码与原生库加载位置；既有构建是否直接复用，以及必要时的增量构建范围。完成这些核对后进入 1.3。

### 1.3 准备固定运行配置

在已经完成接入的 E004 仓库根目录准备模型、存储和结果路径，四种模式使用同一份配置。

把 `docs/kvready_e004/run-config.example.json` 复制到 `/tmp/e004.json`，填写已有模型目录、已跑通的 ASU 配置路径、专用 ASU 可用字节数及其来源。ASU 是实验使用的外部存储后端；`storage_available_bytes` 填实际已分配容量，`storage_capacity_source` 记录管理接口、配置或管理员确认来源，`storage_dedicated_to_test` 确认为 `true`。

```bash
cp docs/kvready_e004/run-config.example.json /tmp/e004.json
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
```

首次填写这些配置即可。测试会使用现场分词器及模型自带的对话模板，生成两类正常维护文档请求，冻结精确 token（模型输入单元）序列；模板首尾包含在 8192 输入长度内。每类 20 个不同会话族，所有模式重复相同输入。附录说明容量估算与输出文件。

## 2. 启动三个服务

三个窗口分别运行代理、P 和 D；P 固定使用物理卡 4、5，D 固定使用 6、7。原有 ASU 服务沿用已经确认的启动方式。

```bash
python docs/kvready_e004/launch.py proxy --config /tmp/e004.json
```

```bash
python docs/kvready_e004/launch.py p --config /tmp/e004.json
```

```bash
python docs/kvready_e004/launch.py d --config /tmp/e004.json
```

启动器从既有 ASU 配置生成角色副本。两端共同使用 TP=2（两卡张量并行）、BF16（16 位浮点）数值格式、即时执行模式、9216 上下文上限和 0.75 设备内存利用率上限。连接器负责让 D 先分配 KV（注意力键值缓存）空间，P 随后生产，数据实际完成后 D 输出结果。

需要提前查看完整命令时，在对应启动命令末尾加 `--show`。模型启动完成后，测试窗口执行下一节。

## 3. 运行固定样例并回传四行

一个命令依次完成无缓存参考、生产端标定、正确性题目和正式性能对照，详细结果自动保留。

```bash
python test/kvready_e004/test_pd.py --config /tmp/e004.json --output /tmp/e004-results-01
```

测试先用四条长上下文问题验证参考答案，再执行 20 条 P 端标定请求。计算服务率取参考请求的首 token 等待时间，存储服务率取已完成任务的实际字节和时长；参数随后锁定。四种模式各运行四道正确性题，共 16 条；正式阶段为两类输入、四模式、每类 20 条、两批，共 320 条。前缀准备请求另行统计。

四模式为：`late` 等 P 完成后读取已有前缀；`eager` 立即预取；`progress` 根据 P 进度安排预取；`joint` 在同一调度基础上选择 P 加载或重算。客户端按固定到达时间表排队，实际在途请求最多四条，客户端限流等待计入用户时延。正式输出逐条核对 128 token；正确性问题自然结束。

回传终端最后四行即可：

```text
RUN commit=... model=... completed=320/320 correctness=PASS status=valid first_failure=NONE
PREFETCH eager_vs_late_ttft=... progress_vs_eager_ttft=... decode_gap_change=...
RESTORE joint_vs_progress_ttft=... joint_P_load=... joint_P_recompute=...
PATH status=not_run reason=run_transfer_demo_separately
```

TTFT 是从计划到达到 D 首个输出 token 的等待时间；百分比依次对应两个固定批次，正值表示等待减少。PATH 行由独立通信演示补充。可以用 `python test/kvready_e004/report.py /tmp/e004-results-01` 重建摘要，无需重跑请求。

出现错误后测试停止该批次，并保留第一个失败阶段。新的验证由蓝区集中修复后再交付；不在黄区调整矩阵、延长请求数或填补异常值。超时后先确认后台搬运已经结束，再按后端正常关闭协议处理自己的服务；未知完成状态下保留缓冲区和服务，不直接复用数据空间。

## 4. 运行独立通信演示

通信演示在相同 KV 字节量下比较直接传输和存储路径，并测量它们与专家通信同时执行的耗时；专家通信使用 HCCL（华为集合通信库）表达 MoE（专家混合模型）的集合通信负载。

固定请求包结束后，按正常关闭流程停止上述窗口启动的 E004 代理、P 和 D 服务，确认它们的搬运已经完成，继续保留测试所用 ASU 服务。将下列两个路径填为 `/tmp/e004.json` 中的模型目录和 ASU 配置文件。

```bash
export MODEL=/home/models/Qwen2.5-14B-Instruct
export ASU_YAML=/path/to/ucm/examples/ucm_config_asu.yaml
ASCEND_RT_VISIBLE_DEVICES=4,5,6,7 torchrun --standalone --nproc-per-node=4 test/kvready_e004/test_transfer.py --model-config "$MODEL/config.json" --asu-config "$ASU_YAML" --output /tmp/e004-results-01 --services-stopped
python test/kvready_e004/report.py /tmp/e004-results-01
```

该入口只操作物理卡 4–7，并将实际耗时写入同目录的 `transfer.json`。随后只重建摘要并回传更新的四行，已完成的 P/D 请求包保留原结果。若终端显示 `QUARANTINED`（搬运完成状态待确认），先关闭此次演示对应的后端传输，确认设备访问结束，再终止演示进程；等待期间保留其缓冲区。

## 附录 A. 环境检查的输出含义

本附录按六条检查命令对应解释输出，供回传和后续接入时查阅。

| 检查 | 应看到的内容 | 对接入的作用 |
|---|---|---|
| 仓库根目录 | 现有 UCM 的绝对路径 | 固定原构建目录位置；报“不是 Git 仓库”时重新确认工作位置 |
| Git 提交 | 短提交号、提交日期、分支或引用 | 定位九月初实际源码；日期与分支按真实输出记录 |
| 远端 | `origin` 指向官方仓库；如已有个人远端也会列出 | 决定 `personal` 的接入方式，保留官方来源 |
| 文件状态 | ` M` 为未暂存修改；`A/M/D` 所在列表示暂存或工作区变化；`??` 为未跟踪；`!!` 为忽略项 | 分别保护源码、实验残留和编译产物；用户报告没有 add/commit，仍以实际输出为准 |
| Python | 解释器绝对路径和版本，例如 `/usr/local/bin/python`、`3.11.x` | 固定已有运行环境；`-S` 跳过 Python 的自动 site 初始化，包括 `.pth` 启动钩子 |
| 构建文件 | `CMakeCache.txt` 为构建配置缓存；`.so` 为已编译共享库 | 确定缓存和库的位置。检查仅搜索原仓库六层目录；外部安装目录与更深层文件留待按输出定位 |

首次输出只用于确定迁移路径。回传后再检查 Python 包的安装位置、构建缓存中的源码路径，以及黄区提交到 E004 的原生代码差异；不以“文件存在”代替二进制兼容检查。

## 附录 B. 构建保留的依据与操作边界

本附录集中保存用户报告、已查证的构建行为，以及接入前需要保留的状态。

- **现场来源。** 用户于 2026-09-23 说明：黄区仓库直接从官方 `feature_26h1` 在 2026 年 9 月初拉取；已有编译生成物和 E001–E003 实验残留，全部为本地行为，没有执行 Git add 或 commit。准确提交、安装方式与产物路径等待 1.1 的实际输出。本说明据此替代旧版直接从 `origin` 取 E004 并切分支的步骤。
- **安装行为。** E004 所继承的 `setup.py` 会在构建扩展时执行 CMake 配置、构建和安装；可编辑安装将产物安装回源码目录。普通安装可能把库放到 Python 的安装目录。已有库能否被 E004 使用，取决于对应加载位置与版本组合。
- **缓存行为。** `CMakeLists.txt` 将 Git `HEAD` 写入 `UCM_COMMIT_ID` 编译定义。重新配置后，提交号变化可能使大量对象重新编译；所以重新执行安装命令也需要先评估构建影响。复制旧 `build/` 到新目录还涉及 CMake 缓存中的绝对路径。
- **原生库配套。** ASU 路径涉及 `ucmpipelinestore*.so`、`libasustore.so`、`libkv_client.so`、`libkv_transport.so` 以及 UCM 共享扩展，按实际构建成套核对。独立 worktree 会取得受 Git 管理的源码，未跟踪产物和原来的可编辑安装指向需另行处理。[Git worktree 文档](https://git-scm.com/docs/git-worktree)
- **Python 检查。** 仓库提供 `ucm_patch.pth`，正常启动 Python 可能执行 UCM 的启动钩子。1.1 使用 `python -S` 读取解释器信息，后续安装位置检查同样避免导入 UCM 或启动设备。
- **文件保护。** Git 分支切换默认允许覆盖与目标分支同路径的忽略项；`stash -u` 会收走未跟踪文件，`stash -a` 还会收走忽略项。环境检查阶段不执行切换、stash、清理、重置或重新安装。后续以具体备份与冲突核对替代通用强制命令；新建保护分支只保存提交位置，未提交内容仍需单独保全。[Git checkout 文档](https://git-scm.com/docs/git-checkout#Documentation/git-checkout.txt---overwrite-ignore)、[Git stash 文档](https://git-scm.com/docs/git-stash)

## 附录 C. 资源、结果和验证范围

本附录集中说明运行条件、成本口径和交付时的验证状态。

- **模型选择。** 示例配置使用 Qwen2.5-14B-Instruct，可在同一固定包中选择已部署的 Qwen2.5-7B-Instruct 并重新核算资源。0.5B 模型用于接线验证；正式机制收益以具有实际搬运与计算负担的 7B 或 14B 模型衡量。
- **存储容量。** E004 使用的 ASU 接口没有已确认的命名空间删除能力，因此测试保留对象，按整个固定包提前核算容量。按 14B 的模型配置，360 条完整上下文等价预算再加 25% 余量约为 675 GiB；7B 约为 196.9 GiB。程序打印所需字节；实际布局和现场存储内部开销仍需纳入已分配容量。容量未填写、来源为空或空间不足时，程序在发出请求前结束。
- **缓存身份。** 每个模式、批次和标定样本使用独立命名空间。前缀准备后确认实际保存完成，再调用 P/D 本地缓存清理。正式结果还核对 D 的前缀块数、本地命中为零、完整恢复范围和所有分片完成。
- **原始记录。** `run.json` 保存代码提交和已安装的 vLLM、vLLM Ascend、torch、torch_npu、UCM 版本，缺失版本保持 `unavailable`；`workload.json` 保存相同输入，`arrival_schedule.json` 保存到达时间表，`calibration.json` 保存固定标定，`requests.jsonl` 保存实际响应和阶段事件，`comparison.json` 保存配对统计，`summary.txt` 保存四行摘要。
- **指标解释。** 首 token 等待同时包含调度与模型执行，标定中的计算服务率是保守服务估计。原始输出文本保留以供定位；正确性由明确答案、上下文依赖、块范围和完成契约共同判断。缺失、非有限值和不完整配对保持 `invalid`；无适用条件的可选统计量保持 `not_applicable`。
- **时间范围。** 单请求 120 秒包含客户端等待，单模式批次 10 分钟，正式包 40 分钟。预置和标定时长分别保存，模型启动发生在固定包之前；不将准备成本算入正式 TTFT。
- **蓝区检查。** 使用已有 Python 环境运行 `python -m unittest discover -s test/kvready_e004 -p 'test_*.py' -v`，入口绕开历史测试根的设备管理钩子。CPU 检查覆盖实际 Python 策略、HTTP 协议、统计和受控接口替身；蓝区检查不包含真实 NPU（神经网络处理器）、ASU 和 HCCL 实测；相关性能由黄区运行获得。实际版本以运行时 Git `HEAD` 和远端交付记录为准。
- **通信范围。** 演示先预热 5 次，再记录 20 次；真实 KV 路径与代表专家通信的同机实验用于定位共享资源争用。它与真实模型端到端结果分别保留。缺少 HCCL、AIV（实验使用的 ASU 设备传输后端）或相关依赖时，PATH 保持 `not_run` 并记录原因。
- **依赖。** 测试入口采用 Python 3.10 及以上的标准库接口；蓝区回归在已有 Python 3.12 环境执行，另核对 3.10 语法兼容。沿用已有 UCM、vLLM Ascend、torch_npu、httpx、transformers 和 PyYAML。启动器选择新增连接器，现有 UCM 默认路径保持原配置。缺少依赖或现场接口不兼容时，保留具体错误交蓝区处理。
