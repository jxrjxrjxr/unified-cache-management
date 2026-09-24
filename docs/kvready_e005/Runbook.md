# E005：运行原生 UCM 的 PD 分离基线并返回 E004

E005 要回答：在 E004 使用的机器和镜像上，已有 UCM（统一 KV 缓存管理）的 PD 分离流程能否启动、完成 KV 交接并取得一组小规模性能数据？P（Prefill）负责处理提示词，D（Decode）负责逐 token 生成；KV 是注意力计算中可复用的键和值。下面按保留现场、取得代码、指定资源、运行采集和返回 E004 的顺序执行。

## 1. 保留 E004 现场并释放测试设备

使用原有容器与 Python，让 E005 和 E004 共享相同的软件运行条件。E005 使用独立源码目录、文件缓存与结果目录；E004 的源码、现场补丁、编译产物、存储服务和 nids_aiv_ub 目录继续保留。

在 E004 的 P、D 和代理服务窗口正常按 Ctrl+C，等窗口返回命令提示符。E004 的 ASU（外部 KV 存储服务）可以保留，因为 E005 通过共享文件目录交接 KV。确认拟使用的设备已释放；出现其他使用者的任务时，在第 3 节改选自己可用的卡。

在宿主机进入已有容器：

~~~bash
docker exec -it jxr_kvready bash
~~~

在这个容器终端设置三个目录。E004_DIR 指向已经编译好的原目录，E005_DIR 指向同级新目录，E005_SESSION 保存这次采集：

~~~bash
export E004_DIR=/home/j00977581/unified-cache-management
export E005_DIR=/home/j00977581/unified-cache-management-e005
export E005_SESSION=/home/j00977581/kvready-e005-01
export E005_PY="$(command -v python)"
cd "$E004_DIR"
git status --short
git rev-parse HEAD
npu-smi info
~~~

目录变量是后续命令共用的入口。保持这一终端，按第 2 节继续；已有构建应由这里的 Python 导入。

## 2. 取得 E005 分支并保留返回依据

沿用个人 fork（GitHub 上已有的个人仓库副本），从原目录获取 kvready/e005，再创建独立工作树。Git 工作树共用 Git 对象，但具有各自的源码目录与检出版本。

在刚才的容器终端执行：

~~~bash
bash <<'BASH'
set -euo pipefail
cd "$E004_DIR"
test ! -e "$E005_DIR"
test ! -e "$E005_SESSION"
git fetch https://github.com/jxrjxrjxr/unified-cache-management.git \
  refs/heads/kvready/e005:refs/remotes/e005-origin/kvready/e005
git worktree add --detach "$E005_DIR" refs/remotes/e005-origin/kvready/e005
git -C "$E005_DIR" log -1 --oneline
printf 'E005_CHECKOUT_READY\n'
BASH
~~~

看到 E005_CHECKOUT_READY 后进入第 3 节。原目录继续检出 E004，现场未提交修改仍位于原目录。后续有明确的新交付时，在停止 E005 后执行下面这一组更新；首次执行跳过此组：

~~~bash
bash <<'BASH'
set -euo pipefail
git -C "$E004_DIR" fetch https://github.com/jxrjxrjxr/unified-cache-management.git \
  refs/heads/kvready/e005:refs/remotes/e005-origin/kvready/e005
git -C "$E005_DIR" diff --exit-code
git -C "$E005_DIR" diff --cached --exit-code
git -C "$E005_DIR" merge --ff-only refs/remotes/e005-origin/kvready/e005
git -C "$E005_DIR" log -1 --oneline
BASH
~~~

每次新采集使用新的 E005_SESSION 编号，例如 kvready-e005-02；已有结果目录保留原样。

## 3. 指定选卡、端口并复用构建

先在一个位置填好资源，再生成配置。P 与 D 各使用 TP=2（两张卡共同承担张量并行），保留 HCCL（昇腾集合通信库）初始化和通信路径。默认 P 选物理卡 3、5，D 选 6、7。

~~~bash
export P_DEVICES=3,5
export D_DEVICES=6,7
export P_PORT=8100
export D_PORT=8200
export PROXY_PORT=8300
export P_DIST_PORT=29500
export D_DIST_PORT=29600

export P_HCCL_IF_BASE_PORT=
export D_HCCL_IF_BASE_PORT=
export P_HCCL_HOST_SOCKET_PORT_RANGE=
export D_HCCL_HOST_SOCKET_PORT_RANGE=
export P_HCCL_NPU_SOCKET_PORT_RANGE=
export D_HCCL_NPU_SOCKET_PORT_RANGE=

cd "$E005_DIR"
"$E005_PY" docs/kvready_e005/prepare.py \
  --e004 "$E004_DIR" --session "$E005_SESSION" \
  --model /home/models/Qwen3-4B \
  --p-devices "$P_DEVICES" --d-devices "$D_DEVICES" \
  --p-port "$P_PORT" --d-port "$D_PORT" --proxy-port "$PROXY_PORT" \
  --p-dist-port "$P_DIST_PORT" --d-dist-port "$D_DIST_PORT" \
  --p-hccl-if-base-port "$P_HCCL_IF_BASE_PORT" \
  --d-hccl-if-base-port "$D_HCCL_IF_BASE_PORT" \
  --p-hccl-host-socket-port-range "$P_HCCL_HOST_SOCKET_PORT_RANGE" \
  --d-hccl-host-socket-port-range "$D_HCCL_HOST_SOCKET_PORT_RANGE" \
  --p-hccl-npu-socket-port-range "$P_HCCL_NPU_SOCKET_PORT_RANGE" \
  --d-hccl-npu-socket-port-range "$D_HCCL_NPU_SOCKET_PORT_RANGE"
~~~

P_PORT、D_PORT、PROXY_PORT 是三个 HTTP 服务端口。P_DIST_PORT 与 D_DIST_PORT 是各自 vLLM 内部通信的起始端口，规划时各留出 100 个连续端口的间隔。HCCL 三类参数分别对应基础端口、主机侧端口范围、设备侧端口范围；空值继承容器已有设置或运行库默认值，填入值后按 P/D 分别透传。首次使用默认空值保持现有通信条件。字段依据与版本适用范围见附录 B。

准备脚本比较 E004 原生源码及构建入口与 E005 基线，并把已有共享库链接到 E005 对应位置。随后生成两端 UCM 配置、指标配置和 config.json。看到 CONFIG_OK 后，执行：

~~~bash
source "$E005_SESSION/env.sh"
"$E005_PY" "$E005_ROOT/docs/kvready_e005/launch.py" p --config "$E005_CONFIG" --show
"$E005_PY" "$E005_ROOT/docs/kvready_e005/launch.py" d --config "$E005_CONFIG" --show
"$E005_PY" "$E005_ROOT/docs/kvready_e005/launch.py" proxy --config "$E005_CONFIG" --show
~~~

三条命令展示最终选卡、端口与启动命令，供现场核对。**config.json 是运行时选卡和端口的唯一入口**：启动前需要临时调整时，直接编辑其中的 p_devices、d_devices 和相应端口，再执行上面三条展示命令。P_DEVICES 等 Shell 变量用于生成配置；生成后以 config.json 为准。

准备过程中如出现明确错误，到此结束该批次并保留首个错误。原 E004 入口可立即继续使用，反馈方式见第 5 节。

## 4. 一次启动并完成固定性能采集

执行一个命令，自动完成导入检查、启动服务、等待就绪、请求测试和进程回收。各阶段日志直接保存到实验目录。

~~~bash
"$E005_PY" "$E005_ROOT/docs/kvready_e005/run.py" --config "$E005_CONFIG"
~~~

SERVICES_READY 表示 P、D 和代理均已就绪。接下来运行 18 条请求：2 条串行预热，8 条串行测量，8 条并发度为 2 的测量。每条输入固定 1024 token，输出固定 32 token；所有输入都有独立的首块，关闭本地前缀缓存。Qwen3 使用关闭思考模式的模板。每条请求先经过原生代理提交给 P，P 的 KV 写入文件存储后，D 从同一目录读取并生成输出。

每阶段同时收集 P 保存完成与 D 加载完成的指标，两张卡都达到该阶段对应的块数后继续。预热阶段的 KV 交接证据用于决定是否进入正式采集。HTTP 错误、响应不完整、输出长度异常或 KV 证据不足会结束这一批次，原始记录留存。

正常结束示例：

~~~text
RUN status=valid stage=complete completed=18/18 cleanup=complete commit=...
PERF c1_ttft_ms=... c1_tpot_ms=... c1_out_tok_s=... c2_ttft_ms=... c2_tpot_ms=... c2_out_tok_s=...
PD p_save_blocks=... d_load_blocks=... p=3,5 d=6,7
~~~

TTFT 是客户端发出请求至收到第一个非空输出片段的时间；TPOT 是首末输出片段间时间除以 31，作为客户端平均后续 token 间隔；out_tok_s 是该批输出 token 总数除以批次耗时。串行结果记为 c1，并发度 2 的结果记为 c2。完整批次有效时发布性能摘要，全部 18 条原始响应均保留；预热数据单独记录。

启动最多等待 600 秒，整个请求批次最多运行 600 秒，单请求网络等待上限 120 秒。到达首个失败或期限即结束；脚本先请求自己启动的进程组正常退出，30 秒后回收这些组的残留进程。按 Ctrl+C 也进入同一回收流程。

## 5. 保存摘要并返回 E004

先读取摘要，再用原来的 E004 入口恢复工作。成功运行反馈三行摘要；失败运行反馈同一摘要及 ERROR 行。

~~~bash
cat "$E005_SESSION/summary.txt"
cd "$E004_DIR"
git status --short
git rev-parse HEAD
npu-smi info
~~~

cleanup=complete 表示脚本的进程回收步骤已完成；结合 npu-smi 确认 E005 任务已释放拟用设备，再回到原 E004 服务窗口执行原启动命令。E005 的源码选择只发生在子进程环境中，Shell 的 Python 安装、UCM 可编辑安装和原 E004 目录继续保持原入口。E004 的 ASU 若一直保留，可继续使用；E004 的运行配置和选卡现场补丁也仍在原目录中。

E004 服务需要重新启动时，沿用已有实验目录：

~~~bash
source /home/j00977581/kvready-e004-01/env.sh
~~~

随后在 E004 原有代理、P、D 窗口分别重用原来已确认的启动命令与现场选卡配置。E005 的任务到“恢复 E004 入口并回传摘要”为止。E005 的 cache、日志和结果继续保存在 E005_SESSION，方便后续集中分析。

准备阶段还未生成摘要时，回传下面一行，把省略号替换成首个错误：

~~~text
RUN status=not_run stage=prepare error=...
~~~

## 附录 A. 结果判读与文件入口

本附录把一次采集的判断与相应证据对应起来，详细记录留在黄区。

| 结果 | 支持的判断 | 下一动作 |
|---|---|---|
| 完整批次 valid，双卡保存/加载计数成立 | 指定选卡、镜像、模型及文件存储路径完成了原生 PD 性能基线 | 返回 E004，带回三行摘要 |
| startup 失败且出现 EI0014 | E004 自定义连接器和代理移除后，原生双卡服务仍在 HCCL 初始化失败 | 返回 E004，带回失败阶段与错误码 |
| 服务就绪，PD 证据失败 | 模型服务可启动，原生 UCM 的存储交接尚需根据本批记录分析 | 保存首个错误并返回 E004 |
| cleanup=incomplete | 自动回收发生异常 | 查阅 outcome.json，并只处理记录中的 E005 进程；确认释放后恢复 E004 |

| 文件 | 内容 |
|---|---|
| config.json | 模型、解释器、选卡、端口、源码提交、库复用清单 |
| runtime.json | 软件包版本、各服务最终命令、进程号与通信环境变量 |
| import.log、p.log、d.log、proxy.log、benchmark.log | 导入、启动与请求阶段的完整日志 |
| results/workload.json、results/requests.jsonl | 18 条实际输入、每条成功或失败响应与客户端时延 |
| results/*-before.prom、results/*-warmup.prom、results/*-c1.prom、results/*-c2.prom | 每端原生指标的阶段快照 |
| results/result.json、outcome.json、summary.txt | 批次结果、含清理状态的执行结果和手抄摘要 |

## 附录 B. 基线、验证与适用范围

本附录集中保存版本、环境假设和结论边界。软件验证记录见 [Validation.md](Validation.md)，原始要求见 [Requirements.md](Requirements.md)。

- 基线为 feature_26h1 的 3f748a5959614af998ec31c18e0e44926f5ae368；E005 仅新增 docs/kvready_e005 与 test/kvready_e005。原生连接器、代理、存储实现及 E003 的通用测量文件保持基线内容。
- 入口依据是同提交的 [centralized_pd.md](../source/user-guide/pd-disaggregation/centralized_pd.md)、ucm/pd/toy_proxy_server.py、ucm/integration/vllm/ucm_connector.py 及 examples/metrics/metrics_configs.yaml。文件后端采用 Cache|Posix，逐层模式关闭，每个缓存实例预算 1 GiB；设备内存利用率配置为 0.60，模型上下文上限 4096。
- 默认拓扑沿用 E004 已记录的同一台八卡 950DT、同一个容器。P 与 D 是同机两个服务实例。跨物理主机部署需要共享文件系统与网络地址方案，属于另一次实验。
- E005 成功支持这套具体条件的可运行性；ASU、AIV/UB 路径和 E004 预取逻辑仍由 E004 验证。E005 失败可判断失败阶段，单次原生基线仍不能在机器与镜像之间直接指定根因。E004 原诊断已记录独立双卡程序失败；这一记录与 E005 结果共同判读。
- 共享库复用以同容器、同解释器、已有构建确实对应保留源码为前提；脚本核验 Git 原生源码/构建输入和实际导入，不重建依赖。nids_aiv_ub、E004 test 的构建产物和所有现场补丁留在原位置。缺少 Cache/Posix 共享库时结束准备，保留缺口信息。
- vLLM 内部端口会从 VLLM_PORT 开始寻找可用端口；100 端口间隔是部署规划，实际分配由镜像内版本决定。HCCL 参数按运行库支持情况生效，E005 不修改网卡、驱动、系统端口保留表或通信算法。官方字段说明：[基础端口](https://www.hiascend.com/doc_center/source/zh/canncommercial/63RC2/modeldev/tfmigr2/tfmigr2_000131.html)、[主机端口范围](https://www.hiascend.com/document/detail/zh/CANNCommunityEdition/81RC1beta1/maintenref/envvar/envref_07_0143.html)、[设备端口范围](https://www.hiascend.com/document/detail/zh/canncommercial/850/commlib/hcclug/hcclug_000092.html)。这些跨版本参考定义字段，950DT 现场支持范围由实际 CANN（昇腾计算运行时）决定；首次默认留空。
- UCM 指标来自完成等待后的原生统计，按两张卡的 worker_id（执行进程标识）检查累计块数。它是这一固定批次的双分片证据，逐请求传输因果追踪不在 E005 范围内。客户端 TPOT 受流式分块和网络缓冲影响；18 条请求用于基础性能采集，不作为容量上限或稳定性长测。
- 截至 2026-09-24，蓝区负责源码、软件检查和推送，黄区 NPU、HCCL 和真实性能结果由用户执行取得。全量日志留在黄区，后续只依据实际摘要安排必要的主线处理。
