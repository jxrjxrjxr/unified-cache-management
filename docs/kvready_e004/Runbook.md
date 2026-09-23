# E004：保留已有构建，完成固定验证包

这份说明把已有 UCM（统一 KV 缓存管理项目）工作区接入 E004，保留编译产物和旧实验记录，并完成预取、加载／重算与通信路径的固定验证。先保存旧文件并切换代码，再配置和启动提示词处理端（P）、生成端（D）及代理，最后运行请求包和独立通信演示。

各阶段给出成功标志和现场处理方式。按顺序在黄区执行，正常流程完成后统一回传四行摘要；完整日志留在黄区。已有环境反馈集中在附录 A。

## 1. 保存旧实验并切换代码

接入分为保存六个已修改文件、获取个人分支和恢复旧实验三个操作；第三个仅在需要回退时使用。编译目录、共享库和历史日志保留原位。

### 1.1 保存六个文件与公共环境

在原容器、原运行环境打开 Bash 终端，粘贴下面整块。它只在仓库旁边新建一个实验目录，复制六个文件、保存差异，并创建供后续窗口使用的 `env.sh`。目录名末尾的 `01` 是实验编号；该目录已经存在时，继续使用其 `env.sh` 和后续步骤，或另选空闲编号开始一次新的准备。若更换实验编号，后续所有 `source` 路径同步替换为新目录。

```bash
bash <<'BASH'
set -euo pipefail
E004_REPO=/home/j00977581/unified-cache-management
E004_SESSION=/home/j00977581/kvready-e004-01
E004_PY=/usr/local/python3.12.13/bin/python
E004_BASE=3f748a5959614af998ec31c18e0e44926f5ae368
cd "$E004_REPO"
test "$(git rev-parse HEAD)" = "$E004_BASE"
git diff --cached --quiet
test -x "$E004_PY"
test ! -e "$E004_SESSION"
E004_FILES=(
  examples/ucm_config_asu.yaml
  test/common/llmperf/utils/openai_chat_completions_client.py
  test/common/llmperf/utils/token_benchmark.py
  test/config.yaml
  test/suites/E2E/test_uc_performance.py
  ucm/integration/vllm/ucm_connector.py
)
mkdir -p "$E004_SESSION/backup/tracked"
git rev-parse HEAD > "$E004_SESSION/backup/commit.txt"
git symbolic-ref --short HEAD > "$E004_SESSION/backup/branch.txt"
git diff --binary HEAD > "$E004_SESSION/backup/local.patch"
git diff --name-only HEAD > "$E004_SESSION/backup/changed.txt"
diff -u <(printf '%s\n' "${E004_FILES[@]}" | sort) \
  <(sort "$E004_SESSION/backup/changed.txt")
cp -p --parents -- "${E004_FILES[@]}" "$E004_SESSION/backup/tracked/"
for f in "${E004_FILES[@]}"; do
  cmp -- "$f" "$E004_SESSION/backup/tracked/$f"
done
cp -p "$E004_SESSION/backup/tracked/examples/ucm_config_asu.yaml" "$E004_SESSION/asu.yaml"
{
  printf 'export E004_REPO=%q\nexport E004_SESSION=%q\nexport E004_PY=%q\nexport E004_BASE=%q\n' \
    "$E004_REPO" "$E004_SESSION" "$E004_PY" "$E004_BASE"
  declare -p E004_FILES
  cat <<'ENV'
export E004_CONFIG="$E004_SESSION/run-config.json"
export E004_RESULTS="$E004_SESSION/results"
export E004_ASU="$E004_SESSION/asu.yaml"
export PYTHONPATH="$E004_REPO${PYTHONPATH:+:$PYTHONPATH}"
export ASCEND_RT_VISIBLE_DEVICES=4,5,6,7
export NO_PROXY=127.0.0.1,localhost
export no_proxy="$NO_PROXY"
export PYTHONDONTWRITEBYTECODE=1
cd "$E004_REPO"
ENV
} > "$E004_SESSION/env.sh"
printf 'BACKUP_OK session=%s\n' "$E004_SESSION"
BASH
```

看到 `BACKUP_OK` 就进入 1.2。复制结果已经逐文件比较；`backup/tracked/` 保存原文件，`local.patch` 保存 Git 差异。编译产物继续由原目录承载。

若在 `BACKUP_OK` 之前结束，仓库中的六个原文件仍保持原样。磁盘空间或目录权限问题在现场解决后，使用新的空闲实验编号重新执行；若提交或修改清单与已确认状态不同，按附录 D 记录准备阶段结束，保留现场内容。

### 1.2 获取个人分支并原地切换

下面整块先获取代码，核对分支差异，再恢复六个受 Git 管理的文件并切换。旧 ASU 配置已保存为实验目录中的 `asu.yaml`，后续服务使用这份副本。官方 `origin` 继续指向官方仓库。

```bash
bash <<'BASH'
set -euo pipefail
source /home/j00977581/kvready-e004-01/env.sh
E004_REMOTE=personal
E004_URL=https://github.com/jxrjxrjxr/unified-cache-management.git
if git remote get-url "$E004_REMOTE" >/dev/null 2>&1; then
  test "$(git remote get-url "$E004_REMOTE")" = "$E004_URL"
else
  git remote add "$E004_REMOTE" "$E004_URL"
fi
git fetch "$E004_REMOTE" "refs/heads/kvready/e004:refs/remotes/$E004_REMOTE/kvready/e004"
E004_TARGET=$(git rev-parse "$E004_REMOTE/kvready/e004")
printf '%s\n' "$E004_TARGET" > "$E004_SESSION/target.commit"
git merge-base --is-ancestor "$E004_BASE" "$E004_TARGET"
git diff --quiet "$E004_BASE" "$E004_TARGET" -- . \
  ':!docs/kvready_e004' ':!test/kvready_e004' ':!ucm/kvready' \
  ':!ucm/pd/kvready_proxy.py' ':!ucm/integration/vllm/kvready_connector.py' \
  ':!test/common/llmperf/utils/openai_chat_completions_client.py' \
  ':!test/common/llmperf/utils/token_benchmark.py'
if git show-ref --verify --quiet refs/heads/kvready/e004; then
  test "$(git rev-parse kvready/e004)" = "$E004_TARGET"
  E004_CHECKOUT=(git checkout --no-overwrite-ignore kvready/e004)
else
  E004_CHECKOUT=(git checkout --no-overwrite-ignore -b kvready/e004 --track "$E004_REMOTE/kvready/e004")
fi
test "$(git rev-parse HEAD)" = "$E004_BASE"
git diff --cached --quiet
diff -u <(printf '%s\n' "${E004_FILES[@]}" | sort) <(git diff --name-only HEAD | sort)
for f in "${E004_FILES[@]}"; do
  cmp -- "$f" "$E004_SESSION/backup/tracked/$f"
done
if git restore --source="$E004_BASE" --staged --worktree -- "${E004_FILES[@]}" && "${E004_CHECKOUT[@]}"; then
  :
else
  E004_OLD_BRANCH=$(cat "$E004_SESSION/backup/branch.txt")
  if test "$(git rev-parse HEAD)" != "$E004_BASE" || test "$(git symbolic-ref --short HEAD)" != "$E004_OLD_BRANCH"; then
    git checkout --no-overwrite-ignore "$E004_OLD_BRANCH"
  fi
  test "$(git rev-parse HEAD)" = "$E004_BASE"
  git restore --source="$E004_BASE" --staged -- "${E004_FILES[@]}"
  for f in "${E004_FILES[@]}"; do cp -p -- "$E004_SESSION/backup/tracked/$f" "$f"; done
  printf 'SWITCH_STOPPED: original six files restored; see the Git message above.\n'
  exit 1
fi
git branch --set-upstream-to="$E004_REMOTE/kvready/e004" kvready/e004
test "$(git rev-parse HEAD)" = "$E004_TARGET"
git diff --quiet HEAD
printf 'SWITCH_OK commit=%s\n' "$E004_TARGET"
BASH
```

看到 `SWITCH_OK` 就进入第 2 节。切换后的 Python 源码来自 E004，六个旧文件在备份目录中，原有 `.so`、构建缓存和实验日志留在原位置。该过程沿用已有编译产物。

常见分支在现场按下表处理，处理后重试 1.2，环境结果留在黄区。

| 现象 | 现场动作 |
|---|---|
| 获取代码时网络失败 | 恢复网络后重试 1.2；六个原文件尚未恢复到基线 |
| `personal` 已存在但地址不同 | 把块内 `E004_REMOTE=personal` 改成空闲别名，例如 `personal-e004`，再执行 |
| `kvready/e004` 已存在且与目标提交不同 | 先运行 `git worktree list`。该分支未被工作目录使用时，运行 `git branch -m kvready/e004 "kvready/e004-before-$(date +%Y%m%d-%H%M%S)"` 保存旧分支，再执行 1.2；被使用时保留原状态，按附录 D 结束准备阶段 |
| Git 点名文件会被覆盖，随后显示 `SWITCH_STOPPED` | 六个原文件已还原。按下面示例保存 Git 点名的每个冲突路径，再重试 1.2 |

碰撞处理只移动 Git 点名的相对路径。将示例 `P` 替换为实际路径；多个路径逐个执行，目录整体被点名时保留其层级。

```bash
source /home/j00977581/kvready-e004-01/env.sh
P='docs/kvready_e004/Runbook.md'
mkdir -p "$E004_SESSION/collisions/$(dirname "$P")"
test ! -e "$E004_SESSION/collisions/$P" && \
  mv -- "$P" "$E004_SESSION/collisions/$P"
```

### 1.3 需要时恢复旧实验

这一步是回退入口；正常执行直接进入第 2 节。回退在 E004 服务及其搬运已正常结束后进行，并以六个旧文件的备份为准。

```bash
bash <<'BASH'
set -euo pipefail
source /home/j00977581/kvready-e004-01/env.sh
git diff --quiet HEAD
git diff --cached --quiet
git checkout --no-overwrite-ignore "$(cat "$E004_SESSION/backup/branch.txt")"
test "$(git rev-parse HEAD)" = "$(cat "$E004_SESSION/backup/commit.txt")"
for f in "${E004_FILES[@]}"; do cp -p -- "$E004_SESSION/backup/tracked/$f" "$f"; done
printf 'RESTORED: original branch and six local files; build artifacts remain in place.\n'
BASH
```

如果曾保存碰撞项，切回旧分支后再把 `collisions/` 中相应路径移回；移动前确认目的位置空闲。E004 运行后的新改动先另存，回退检查通过后再执行该块。

## 2. 填写一次运行配置

配置把模型、ASU（实验使用的外部存储后端）和结果目录固定下来，四种模式共同使用同一份文件。沿用 1.1 创建的公共环境，并将下面三个值替换为现场实际值。

```bash
source /home/j00977581/kvready-e004-01/env.sh
export E004_MODEL='/实际模型目录/Qwen2.5-14B-Instruct'
export E004_STORAGE_GIB='实际专用可用GiB数'
export E004_STORAGE_SOURCE='实际容量依据，例如服务配置或管理员已确认的分配记录'
```

模型选现场已有的 Qwen2.5-14B-Instruct 或 Qwen2.5-7B-Instruct。需要回看旧模型路径时，在黄区查看 `"$E004_SESSION/backup/tracked/test/config.yaml"` 中的 `tokenizer_path`；已有路径对应 0.5B 时，另填现场已有的 7B／14B。GiB 为 1024³ 字节，容量值取 ASU 实际分配给该实验的可用空间及已知内部开销口径。

14B 的固定包预算约 675 GiB，7B 约 196.9 GiB；程序按模型配置计算准确字节数。容量适合 7B 时，在发出任何测试请求前改选已有 7B。专用容量需要覆盖固定请求包和独立演示，演示对 14B 另需约 1.125 GiB。容量来源以 ASU 配置或已有管理信息为准，主机文件系统的 `df` 与远端 ASU 配额分别判断。

确认这份容量确实专用于 E004 后，在同一终端继续执行：

```bash
export E004_STORAGE_DEDICATED=true
"$E004_PY" -S - <<'PY'
import json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(os.environ['E004_REPO']) / 'test/kvready_e004'))
from workload import model_layout, required_storage_bytes
model = Path(os.environ['E004_MODEL']).resolve()
assert model.is_dir(), 'Set E004_MODEL to an existing local model directory'
layout = model_layout(model, 128)
need = required_storage_bytes(layout)
demo = 6144 * layout['kv_bytes_per_token']
available = int(float(os.environ['E004_STORAGE_GIB']) * 1024**3)
assert os.environ.get('E004_STORAGE_DEDICATED') == 'true'
assert available >= need + demo, f'Need {need + demo} bytes including the communication demo; choose an existing 7B or sufficient dedicated capacity'
config = json.loads(Path('docs/kvready_e004/run-config.example.json').read_text())
config.update(model_path=str(model), asu_config_path=os.environ['E004_ASU'],
              runtime_dir=str(Path(os.environ['E004_SESSION']) / 'runtime'),
              storage_available_bytes=available,
              storage_capacity_source=os.environ['E004_STORAGE_SOURCE'],
              storage_dedicated_to_test=True)
assert config['storage_capacity_source'].strip()
Path(os.environ['E004_CONFIG']).write_text(json.dumps(config, indent=2, ensure_ascii=False))
print(f'CONFIG_OK request_bytes={need} demo_bytes={demo} available_bytes={available}')
PY
```

看到 `CONFIG_OK` 后进入第 3 节。模型路径或容量不合适时，修正上面的变量并重新生成配置即可；该步骤使用标准库读取模型信息，尚未发送请求。现场容量或模型资源无法满足时，以 `configuration` 作为该执行包的结束阶段，保留结果至统一反馈。

## 3. 启动三个服务并等待就绪

三个窗口沿用原容器和既有 CANN（昇腾计算软件栈）环境，各自加载同一份 `env.sh`，随后分别运行代理、P 和 D；P 使用物理卡 4、5，D 使用 6、7。原有 ASU 服务沿用已跑通的启动方式，已有 E001–E003 模型服务先按原运行方式正常停止并释放这些卡。

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

启动器使用指定解释器派生服务，从保存的 ASU 配置生成 P/D 副本。两端使用 TP=2（两卡张量并行）、BF16（16 位浮点）、即时执行模式、9216 上下文上限和 0.75 设备内存利用率上限。连接器让 D 先分配 KV（注意力键值缓存）空间，P 随后生产，所需数据完成后 D 输出。

三个服务窗口保持运行且无启动错误后，在第四个测试窗口执行下面整块。它最多等待 10 分钟，同时检查代理及两个模型服务的健康接口；`SERVICES_READY` 表示可以进入第 4 节。

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

如果启动日志显示端口占用，把 `run-config.json` 中相应 URL 改为现场空闲端口，正常停止自己已启动的 E004 服务，再用同样命令启动三个服务。三个进程共同读取新配置。其他导入、原生库或框架接口错误按附录 D 作为启动阶段结束，保留日志；已有编译产物和 Python 依赖保持原位。

## 4. 执行固定请求包

一个命令依次完成参考答案、生产端标定、正确性题目和正式对照。两类输入都是 8192 个 token（模型处理的输入单元），正式阶段为四模式、两批，共 320 条请求；20 条标定、16 条正确性和前缀准备分别统计。

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

TTFT 是计划到达到 D 首个输出 token 的等待时间；正百分比表示等待减少。`RUN ... status=valid` 时进入第 5 节；`invalid` 时保留这一批次的首个失败阶段并按附录 D 结束，完整记录供蓝区集中处理。结果目录已存在时先读取其中的 `summary.txt`；已有请求保持其原始结果，继续执行相应的后续阶段。

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

## 附录 A. 黄区环境记录

本附录保存用户于 2026-09-23 完成的环境反馈，作为完整执行流程的固定起点。

| 项目 | 用户回传结果 | 对接入的作用 |
|---|---|---|
| 仓库根目录 | `/home/j00977581/unified-cache-management` | 保持原目录和构建缓存路径 |
| Git 提交 | `3f748a5`，2026-09-09，`feature_26h1` 与 `origin/feature_26h1` | 与 E004 的共同起点一致 |
| 远端 | `origin` 的 fetch/push 均为官方 `ModelEngine-Group/unified-cache-management` | 保留官方来源，个人仓库使用 `personal` |
| 本地内容 | 六个已跟踪 Python／YAML 修改；另有 `bin`、`build-ucm-asu`、E002 日志、共享库和 `libumc.a` | 六文件复制保全，未跟踪／忽略产物原位保留 |
| Python | `/usr/local/python3.12.13/bin/python`，版本 `3.12.13` | 通过公共环境文件绑定全部服务与测试 |
| 构建文件 | 用户报告 5 个 txt 文件、18 个 `.so`；示例 `./build-ucm-asu/CMakeCache.txt`、`./ucm/store/asu/libkv_client.so` | 确认原目录内存在构建缓存与原生库 |

六个修改路径为 `examples/ucm_config_asu.yaml`、`test/common/llmperf/utils/openai_chat_completions_client.py`、`test/common/llmperf/utils/token_benchmark.py`、`test/config.yaml`、`test/suites/E2E/test_uc_performance.py`、`ucm/integration/vllm/ucm_connector.py`，均为 Python 或运行配置。

用户要求环境反馈至此结束，主流程完成前的剩余差异通过现场变量、预期输出与条件分支处理。该约定落实为正文的连续执行流程；正常完成后只回传四行结果，准备或运行受阻时按附录 D 一次反馈该执行包的结束状态。

## 附录 B. 构建保留的依据与操作边界

本附录集中保存用户报告、已查证的构建行为，以及接入前需要保留的状态。

- **现场来源。** 用户于 2026-09-23 完成附录 A 的反馈：黄区仓库从官方 `feature_26h1` 拉取，检出提交为 `3f748a5`；已有编译生成物和 E001–E003 实验残留，全部为本地行为，没有执行 Git add 或 commit。六个修改路径与解释器绝对路径已经确认。
- **分支差异。** 蓝区核对 `3f748a5959614af998ec31c18e0e44926f5ae368..48070c17da34ba1c5ba5b2152ed6dd00b7373464`：19 个新增 Python、文档及配置文件；两个已有文件修改为 `test/common/llmperf/utils/openai_chat_completions_client.py`、`test/common/llmperf/utils/token_benchmark.py`，来自 E003 的请求统计修复。原生源码、头文件、构建脚本和既有 UCM 运行源码均保持一致。该分支更新本身无需触发原生重编译；黄区本地修改、安装指向与真实加载结果另行核对。
- **安装行为。** E004 所继承的 `setup.py` 会在构建扩展时执行 CMake 配置、构建和安装；可编辑安装将产物安装回源码目录。普通安装可能把库放到 Python 的安装目录。已有库能否被 E004 使用，取决于对应加载位置与版本组合。
- **缓存行为。** `CMakeLists.txt` 将 Git `HEAD` 写入 `UCM_COMMIT_ID` 编译定义。重新配置后，提交号变化可能使大量对象重新编译；所以重新执行安装命令也需要先评估构建影响。复制旧 `build/` 到新目录还涉及 CMake 缓存中的绝对路径。
- **原生库配套。** ASU 路径涉及 `ucmpipelinestore*.so`、`libasustore.so`、`libkv_client.so`、`libkv_transport.so` 以及 UCM 共享扩展，按实际构建成套核对。Git worktree（同一仓库的独立源码工作目录）会取得受 Git 管理的源码，未跟踪产物和原来的可编辑安装指向需另行处理。[Git worktree 文档](https://git-scm.com/docs/git-worktree)
- **Python 初始化。** 仓库提供 `ucm_patch.pth`，正常启动 Python 可能执行 UCM 启动钩子。配置生成和健康检查使用 `python -S`，实际服务保留正常初始化。四个终端沿用原容器已有的 CANN 环境初始化；公共文件只固定实验所需变量。
- **文件保护。** `git checkout --no-overwrite-ignore` 在忽略项发生路径碰撞时中止切换；受 Git 管理的六个文件在逐文件复制验证后才通过 `git restore` 恢复基线。获取远端对象先于工作区恢复，切换失败时回填六个原文件。备份覆盖将被替换的内容，原位保留覆盖构建产物和历史日志；这是代码迁移的保全方式，完整磁盘故障备份另属基础设施。[Git checkout 文档](https://git-scm.com/docs/git-checkout#Documentation/git-checkout.txt---overwrite-ignore)、[Git restore 文档](https://git-scm.com/docs/git-restore)、[Git fetch 文档](https://git-scm.com/docs/git-fetch)

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

## 附录 D. 执行包的结束状态

本附录统一正常完成与主流程受阻的反馈方式，完整证据保留在实验目录中。

正常完成时，回传第 5 节生成的四行摘要。已产生 `summary.txt` 的失败批次同样回传原摘要，保留首个失败阶段；完成的请求与原始结果继续保留，蓝区集中处理主流程问题。

准备或启动阶段受阻、尚未生成摘要时，现场按正文已有分支处理。分支仍无法继续时，将执行包记为 `not_run`，统一反馈下面一行；花括号替换为实际值，错误只取第一处关键报错，完整日志留在黄区。它是该执行包的结束记录，后续由蓝区处理，不再展开常规环境问答。

```text
RUN status=not_run first_failure={backup|switch|configuration|startup} detail={第一处关键错误}
```

实际请求或设备搬运的完成状态未知时，结束的是新请求提交，相关服务、进程和缓冲区按既定完成协议保留。确认搬运结束后再正常退出；服务重启、重新跑包或额外扩量均留待故障收敛后安排。
