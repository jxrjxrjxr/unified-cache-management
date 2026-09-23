# E004：一次完成固定验证包

这份说明用于把 E004 分支放到黄区运行，得到提前预取、进度协调和加载／重算三个配对结果。先准备运行配置，再启动提示词处理端（P）、生成端（D）和代理，然后执行固定请求包，最后用独立通信演示补充路径判断。

## 1. 准备版本和配置

准备步骤固定代码来源、模型和专用外部存储空间，后面的模式使用同一套配置。

在现有容器的 UCM 仓库中取得指定提交。先查看工作区，保留黄区已有修改；获取分支后，将最后一条命令输出的提交号与交付消息核对。

```bash
git status --short
git fetch origin kvready/e004
git switch kvready/e004
git log -1 --oneline
```

如果该仓库尚无本地分支，第一次改用 `git switch --track origin/kvready/e004`。本地已有分支时，核对工作区和来源后使用 `git merge --ff-only origin/kvready/e004`。交付提交号与运行 `HEAD` 一致后继续。

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

## 附录：资源、结果和验证范围

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
