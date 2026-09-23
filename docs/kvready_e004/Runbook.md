# E004：从新机器部署到固定验证

这份说明把新机器上的个人目录、源码、容器和运行配置串成一条执行流程，再完成 E004 的预取、加载／重算和通信路径对照。先直接克隆个人分支并编译，再自动生成配置和启动服务，最后运行固定请求包与通信演示。

UCM 是统一 KV 缓存管理项目，KV 是注意力计算中的键和值；提示词处理端记为 P，逐 token（模型处理的基本单元）生成端记为 D。模型统一使用 `/home/models/Qwen3-4B`，P 使用物理卡 4、5，D 使用 6、7。正常完成后统一回传四行摘要，完整记录留在黄区。

## 1. 建立目录、取得代码并完成编译

准备按源码、容器和编译三个阶段推进。原《KVReady：个人运行手册》（KnowledgeVault：`1_Projects/1.1_Professional/KVReady/Experiments/Runbook.md`，下文简称原手册）的代理、容器脚本和构建方法继续使用；E004 所需命令集中列在下面。

### 1.1 新建个人目录并直接克隆分支

登录新机器后，在宿主机 Bash 中执行。需要代理时先执行原手册第 2 节，沿用已有内部凭据。

```bash
mkdir -p /home/j00977581
cd /home/j00977581
git clone -b kvready/e004 https://github.com/jxrjxrjxr/unified-cache-management.git unified-cache-management
git clone -b ucm-adapt-0910 https://gitcode.com/dtad5/nids_aiv_ub.git nids_aiv_ub
```

两条克隆命令完成后进入 1.2。UCM 的 `origin` 直接指向个人仓库，检出分支为 `kvready/e004`。ASU 是实验采用的外部 KV 存储后端，AIV 是这条存储链路使用的设备传输后端；AIV 按原手册的 GitCode 地址与 `ucm-adapt-0910` 分支直接进入编译。

网络中断后可重试未完成的克隆。目的目录已存在时保留它；若为中断产生的不完整目录，先改名保存，再用原目录名重新克隆。

### 1.2 用原启动脚本创建个人容器

继续在宿主机执行。原手册第 4、5 节说明启动脚本和多终端用法；镜像使用完整标签，创建名为 `jxr_kvready` 的个人容器。

```bash
cp -n /home/f00855140/demo_start.sh /home/j00977581/demo_start.sh
E004_IMAGE=quay.io/ascend/vllm-ascend:v0.23.0-a5-openeuler
docker image inspect "$E004_IMAGE" >/dev/null 2>&1 || docker pull "$E004_IMAGE"
bash /home/j00977581/demo_start.sh "$E004_IMAGE" jxr_kvready
docker exec -it jxr_kvready bash
```

进入容器后继续 1.3。启动脚本沿用原手册的目录、驱动与设备挂载：容器内使用 `/home/j00977581` 和 `/home/models`，运行进程通过环境变量选择物理卡 4–7。后续每个新终端都用同一条 `docker exec` 进入这个容器。

容器已经创建时，后续使用 `docker start jxr_kvready` 和 `docker exec -it jxr_kvready bash`。启动脚本路径缺失或创建失败时，保留首个错误，按附录 C 记录 `container` 阶段。

### 1.3 在容器中编译 AIV 和 UCM

在刚进入的容器终端执行。需要软件源代理时，按原手册第 2 节为该终端设置。下面合并原手册第 6 节与第 7.1 节：先生成 `libumc.a`，再由 UCM 的可编辑安装完成原生编译。可编辑安装使 Python 直接使用这份源码。

```bash
bash <<'BASH'
set -euo pipefail
cd /home/j00977581
mkdir -p e004-build-logs
cd nids_aiv_ub
bash scripts/build_all.sh 2>&1 | tee /home/j00977581/e004-build-logs/aiv.log
mkdir -p /home/j00977581/unified-cache-management/kv_semantics/src/trans_provider/aiv/lib
cp build/libumc.a /home/j00977581/unified-cache-management/kv_semantics/src/trans_provider/aiv/lib/libumc.a
cd /home/j00977581/unified-cache-management
export PLATFORM=ascend
export BUILD_UCM_ASU=1
export BUILD_KV_CLIENT_PROVIDER_AIV=1
export KV_CLIENT_AIV_PROVIDER_ROOT=/home/j00977581/unified-cache-management/kv_semantics/src/trans_provider/aiv
python -m pip install -v -e . --no-build-isolation 2>&1 | tee /home/j00977581/e004-build-logs/ucm.log
printf 'BUILD_OK\n'
BASH
```

看到 `BUILD_OK` 后进入第 2 节。该流程使用容器配套 Python 完成一次构建；E004 使用专用测试入口，依赖随 UCM 与推理镜像提供。后续仅拉取 E004 的 Python、配置或文档改动时，可继续使用这份构建结果。

## 2. 自动生成配置并启动存储

准备脚本把模型布局、已有 ASU 参数和实际内存条件写入实验目录；随后用这些配置启动一个供 E004 使用的新存储服务。

### 2.1 一次生成运行文件

在编译完成的容器终端执行，正常路径直接使用下面两行：

```bash
cd /home/j00977581/unified-cache-management
python docs/kvready_e004/prepare.py
```

`prepare.py` 只承担部署准备：读取本地 Qwen3-4B 配置和仓库 ASU 模板，生成 `/home/j00977581/kvready-e004-01` 中的 `run-config.json`、`asu.yaml`、`env.sh` 与内存预算记录。模型路径、三个服务地址、ASU 地址与端口均已填好，Python 路径取执行该命令的解释器。

成功输出形如：

```text
CONFIG_OK model=Qwen3-4B request_gib=506.250 demo_gib=0.844 inmem_budget_gib=...
Environment: /home/j00977581/kvready-e004-01/env.sh
```

看到 `CONFIG_OK` 后进入 2.2。脚本按主机可用内存与容器内存限额的较小余量，预留至少 64 GiB 且不少于四分之一，剩余部分用于判断固定包是否容纳得下。GiB 为 1024³ 字节。Qwen3-4B 的固定包与演示合计预算约 507.094 GiB；这项检查在启动 ASU 和提交请求之前完成。

目录已存在时继续使用其中的 `env.sh`；需要独立实验目录时，用 `python docs/kvready_e004/prepare.py --session /home/j00977581/kvready-e004-02`，并把后续 `source` 路径统一改为新编号。容量不足或模型配置读取失败时，脚本输出 `RUN status=not_run first_failure=configuration ...`，按附录 C 保留该结束状态。

### 2.2 在 ASU 窗口启动专用存储服务

保留一个容器终端作为 ASU 窗口，执行以下命令并保持运行。参数沿用原手册第 8 节，日志归入 E004 实验目录。

```bash
source /home/j00977581/kvready-e004-01/env.sh
set -o pipefail
/home/j00977581/nids_aiv_ub/build_server/bin/kv_hash_staged_server \
  --bind 127.0.0.1 --port 19003 \
  --urma-device udmac0d1e4 --jetties 1 --seg-size 5448576 \
  --backing inmem --conn-mode rm --profile ubc \
  --max-accepts 0 --recv-slots 128 --stop-after-phase 3 --max-block 1572864 \
  2>&1 | tee "$E004_SESSION/asu.log"
```

服务开始监听后进入第 3 节。该进程从空存储开始，供 E004 的 P、D 和最后的通信演示使用，并持续保留到演示结束。若 19003 被其他服务占用，把命令中的端口和实验目录 `asu.yaml` 的 `asu_ports` 同步改为一个空闲端口，再启动自己的 ASU。

## 3. 启动三个服务并等待就绪

代理、P 和 D 各占一个容器终端，每个新终端先从宿主机执行 `docker exec -it jxr_kvready bash`，再加载同一份 `env.sh`。P 使用物理卡 4、5，D 使用 6、7；第 2 节的 ASU 窗口继续运行。

代理窗口：

```bash
source /home/j00977581/kvready-e004-01/env.sh
set -o pipefail
"$E004_PY" docs/kvready_e004/launch.py proxy --config "$E004_CONFIG" 2>&1 | tee "$E004_SESSION/proxy.log"
```

P 窗口：

```bash
source /home/j00977581/kvready-e004-01/env.sh
set -o pipefail
ASCEND_RT_VISIBLE_DEVICES=4,5 "$E004_PY" docs/kvready_e004/launch.py p --config "$E004_CONFIG" 2>&1 | tee "$E004_SESSION/p.log"
```

D 窗口：

```bash
source /home/j00977581/kvready-e004-01/env.sh
set -o pipefail
ASCEND_RT_VISIBLE_DEVICES=6,7 "$E004_PY" docs/kvready_e004/launch.py d --config "$E004_CONFIG" 2>&1 | tee "$E004_SESSION/d.log"
```

启动器使用指定解释器派生服务，从自动生成的 ASU 配置生成 P/D 副本。两端使用 TP=2（两卡张量并行）、BF16（16 位浮点）、即时执行模式、9216 上下文上限和 0.75 设备内存利用率上限。连接器让 D 先分配 KV 空间，P 随后生产，所需数据完成后 D 输出。

三个服务窗口保持运行且无启动错误后，再打开一个容器测试窗口执行下面整块。它最多等待 10 分钟，同时检查代理及两个模型服务的健康接口；`SERVICES_READY` 表示可以进入第 4 节。

```bash
source /home/j00977581/kvready-e004-01/env.sh
"$E004_PY" -S - <<'PY'
import json, os, time, urllib.request
from pathlib import Path
c = json.loads(Path(os.environ['E004_CONFIG']).read_text())
urls = [c['proxy_url'].rstrip('/') + '/healthcheck',
        c['prefill_url'].rstrip('/') + '/health', c['decode_url'].rstrip('/') + '/health']
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
deadline = time.monotonic() + 600
while time.monotonic() < deadline:
    pending = []
    for url in urls:
        try:
            with opener.open(url, timeout=2) as r:
                if r.status != 200: pending.append(url)
        except Exception:
            pending.append(url)
    if not pending:
        print('SERVICES_READY')
        break
    time.sleep(2)
else:
    raise SystemExit('STARTUP_STOPPED pending=' + ','.join(pending))
PY
```

如果启动日志显示端口占用，把 `run-config.json` 中相应 URL 改为现场空闲端口，正常停止自己已启动的 E004 服务，再用同样命令启动三个服务。三个进程共同读取新配置。其他导入、原生库或框架接口错误按附录 C 作为启动阶段结束，保留日志；编译日志和服务日志留在实验目录。

## 4. 执行固定请求包

一个命令依次完成参考答案、生产端标定、正确性题目和正式对照。两类输入都是 8192 个 token，正式阶段为四模式、两批，共 320 条请求；20 条标定、16 条正确性和前缀准备分别统计。

```bash
source /home/j00977581/kvready-e004-01/env.sh
set -o pipefail
"$E004_PY" test/kvready_e004/test_pd.py --config "$E004_CONFIG" --output "$E004_RESULTS" 2>&1 | tee "$E004_SESSION/test.log"
```

四模式为 `late`（等待 P 完成后读取）、`eager`（立即预取）、`progress`（按 P 进度预取）、`joint`（联合选择 P 加载或重算）。各模式复用同一组冻结输入；客户端在途最多四条，等待计入用户时延。

成功输出示例：

```text
RUN commit=... model=... completed=320/320 correctness=PASS status=valid first_failure=NONE
PREFETCH eager_vs_late_ttft=... progress_vs_eager_ttft=... decode_gap_change=...
RESTORE joint_vs_progress_ttft=... joint_P_load=... joint_P_recompute=...
PATH status=not_run reason=run_transfer_demo_separately
```

TTFT 是计划到达到 D 首个输出 token 的等待时间；正百分比表示等待减少。`RUN ... status=valid` 时进入第 5 节；`invalid` 时保留这一批次的首个失败阶段并按附录 C 结束，完整记录供蓝区集中处理。结果目录已存在时先读取其中的 `summary.txt`；已有请求保持其原始结果，继续执行相应的后续阶段。

## 5. 执行通信演示并统一回传

通信演示比较相同 KV 字节量的直接传输和存储路径，再叠加专家通信。专家通信以 HCCL（华为集合通信库）表达 MoE（专家混合模型）的集合通信负载，使用物理卡 4–7。

请求包有效完成后，在三个服务窗口按 Ctrl+C 正常结束自己的 E004 服务，等待各窗口进程退出并确认搬运结束，继续保留 ASU 服务。在测试窗口执行：

```bash
source /home/j00977581/kvready-e004-01/env.sh
E004_MODEL=$("$E004_PY" -S -c 'import json,os; print(json.load(open(os.environ["E004_CONFIG"]))["model_path"])')
set -o pipefail
ASCEND_RT_VISIBLE_DEVICES=4,5,6,7 "$E004_PY" -m torch.distributed.run \
  --standalone --nproc-per-node=4 test/kvready_e004/test_transfer.py \
  --model-config "$E004_MODEL/config.json" --asu-config "$E004_ASU" \
  --output "$E004_RESULTS" --services-stopped 2>&1 | tee "$E004_SESSION/transfer.log"
"$E004_PY" test/kvready_e004/report.py "$E004_RESULTS"
```

演示记录写入同目录 `transfer.json`，最后一个命令重建摘要。统一回传最后四行即可；中间环境检查、路径清单和完整日志继续留在黄区。`PATH not_run` 或 `invalid` 保留其原因，同时保留已完成的请求包结果。

若出现 `QUARANTINED`，表示设备搬运完成状态待确认；保持相关进程与缓冲区，按后端正常协议关闭此次演示的传输，确认访问结束后再终止演示进程。该执行包以对应失败阶段结束。

## 附录 A. 配置选择与来源

本附录集中列出新机器的用户报告、采用的配置及其依据，供执行时查阅。

| 项目 | E004 采用值 | 来源 |
|---|---|---|
| 个人目录与 UCM | `/home/j00977581/unified-cache-management`；个人仓库为 `origin`，分支 `kvready/e004` | 用户于 2026-09-23 要求从新机器直接克隆 |
| 设备 | 八卡 950DT；P 为物理 4、5，D 为 6、7 | 用户报告新机器仍为八卡 950DT，沿用主管认可的分配 |
| 模型 | `/home/models/Qwen3-4B`；关闭思考模式 | 用户提供的已有模型；标准逐层 GQA（多个查询头共享一组 KV 头）适配既有 E004 路径 |
| 容器 | `jxr_kvready`；镜像 `quay.io/ascend/vllm-ascend:v0.23.0-a5-openeuler` | 原手册第 4、5 节；使用标签建立新容器 |
| AIV 与构建 | GitCode `dtad5/nids_aiv_ub` 的 `ucm-adapt-0910`；`build_all.sh`；UCM 可编辑安装 | 原手册第 3、6、7.1 节 |
| ASU | `127.0.0.1:19003`，AIV 后端，`inmem`，设备 `udmac0d1e4` | 原手册第 8–10 节的启动参数和环境变量 |
| P／D／代理 | `127.0.0.1` 的 8100／8200／8300 端口 | E004 已有启动配置 |
| 配置与结果 | `/home/j00977581/kvready-e004-01`；Python 为容器实际解释器 | `prepare.py` 自动生成 |

Qwen3-4B 官方配置为 36 层、8 个 KV 头、每头 128 维。按 BF16 计算，每 token 的全模型 KV 为 144 KiB，6144-token 前缀为 0.84375 GiB。固定包保留全部存储对象，以 360 条完整 8192-token 上下文加 25% 余量计算为 506.25 GiB，演示另加 0.84375 GiB。脚本读取本地 `config.json` 重算字节数。[官方配置](https://huggingface.co/Qwen/Qwen3-4B/blob/main/config.json)

测试在生成输入模板时指定 `enable_thinking=False`，让固定短输出直接回答问题。[官方模型说明](https://huggingface.co/Qwen/Qwen3-4B) 用户列出的其他模型为 DeepSeek-V4-Flash、DeepSeek-V4-Flash-DSpark、DeepSeek-V4-Flash-MXFP8、DeepSeek-V4.1-Flash、Qwen3.5-4B；E004 固定使用 Qwen3-4B，保持统一的逐层 KV 布局与最小修改范围。

## 附录 B. 资源口径、历史与验证状态

本附录保存资源估算的适用范围、历史流程去向与交付证据，正文按上述固定配置执行。

- **部署来源。** 用户于 2026-09-23 告知旧服务器已回收，新机器目录结构几乎一致，尚未建立个人目录和容器。旧机器的六文件备份、原地切换和构建保留流程作为历史保存在 [c6e1791 版 Runbook](https://github.com/jxrjxrjxr/unified-cache-management/blob/c6e1791dca8d1c8531a88eb961f9d97b92e0ba6e/docs/kvready_e004/Runbook.md)。原手册继续承载首次部署经验与 E001 历史实验。
- **软件来源。** Docker 脚本正文、镜像与 GitCode 构建在黄区执行；蓝区依据原手册沿用调用方式。GitCode 按用户要求直接克隆、编译，交付流程不设置其版本核验或回传步骤。新机器真实编译、NPU（神经网络处理器）、ASU 和 HCCL 执行结果由固定批次留证。
- **内存估算。** `--backing inmem` 按新 ASU 服务使用主机内存的部署方式规划；`memory-budget.json` 记录主机可用内存、cgroup（容器资源控制组）限额余量和预留量。它是启动时的准入估算，不是 ASU 的已分配配额或内存锁定；`--seg-size` 与 `--max-block` 保留原脚本值，不用于推算总容量。独立 ASU 进程只供 E004 使用，主机共享内存的后续变化仍会影响实际余量。
- **缓存身份。** 模式、批次与标定样本使用独立命名空间。前缀准备后确认保存完成，再清理 P/D 本地缓存；正式结果检查本地命中为零、前缀块数、完整恢复范围和所有分片完成。ASU 接口没有已确认的命名空间删除能力，固定包按累计对象保留核算空间。
- **指标与记录。** `run.json` 保存 Git 提交和运行时软件版本；`workload.json`、`arrival_schedule.json`、`calibration.json`、`requests.jsonl`、`comparison.json` 分别保存输入、到达时间、标定、原始响应与配对统计；`summary.txt` 保存四行反馈。缺失和非有限值保持 `invalid`，无适用条件的统计量保持 `not_applicable`。
- **时间与通信。** 单请求 120 秒含客户端等待，单模式批次 10 分钟，正式包 40 分钟；准备和标定分别保存。通信演示每格预热 5 次、记录 20 次，与模型服务分阶段使用物理卡 4–7。代表专家通信与真实 MoE 服务收益分别判断；PATH 保留实际执行状态和原因。
- **蓝区检查。** 2026-09-23 使用已有 Python 3.12 运行 `python -m unittest discover -s test/kvready_e004 -p 'test_*.py' -v`，41 项通过，覆盖策略、协议、样例、统计和部署配置，设备调用采用受控替身。官方 Qwen3-4B 分词器实检通过：40 个性能会话和 4 个正确性样例均为 8192-token 输入，所需事实与关闭思考模式的模板完整。11 个 Bash 块、1 个内嵌 Python 块及 4 个变更 Python 文件的 3.10 语法检查通过。蓝区 CPU 检查与黄区硬件性能分别记录。
- **依赖与改动。** 沿用原镜像和 UCM 构建方法，E004 使用已有 vLLM Ascend、torch_npu、httpx、transformers、PyYAML。新机器适配集中在部署脚本、默认配置及样例关闭思考模式，预取、加载和重算算法沿用已确认实现。框架接口或依赖缺口以首个具体错误交蓝区处理。

## 附录 C. 执行包的结束状态

本附录统一正常完成与主流程受阻的反馈方式，详细证据留在实验目录和 `e004-build-logs` 中。

正常完成时，回传第 5 节生成的四行摘要。已经产生 `summary.txt` 的失败批次同样回传原摘要，保留首个失败阶段，蓝区据此集中处理主流程问题。

准备或启动受阻时先按正文的现场分支处理；仍无法继续则保留现场，统一反馈下面一行。花括号替换为实际阶段和第一处关键错误，后续常规环境信息继续留在黄区。

```text
RUN status=not_run first_failure={clone|container|build|configuration|startup} detail={第一处关键错误}
```

实际请求或设备搬运的完成状态未知时，先结束新请求提交，相关进程与缓冲区按既定完成协议保留，确认搬运结束后再正常退出。已完成的请求与记录继续保存，故障在蓝区收敛后再安排后续执行包。
