# 门店结算测试调度台

共用一个 SQLite 测试库的门店结算用例调度服务：网页提交预置用例，后台按规则派发，
每个用例启动**独立本地进程**实际查询或写入测试库，网页实时展示排队状态与结果。

## 调度规则

| 规则 | 实现位置 |
| --- | --- |
| 核账用例只读，最多同时运行 2 项（`SETTLE_MAX_AUDIT` 可调） | `app/dispatcher.py` `_dispatch_loop` |
| 退款用例写库，独占运行：启动前等待所有核账结束，运行期间不派发任何用例 | 同上 |
| 严格 FIFO：只有队首可以出队，后提交的核账不能越过已排队的退款 | 同上 |
| 退款进程退出后（无论成功、业务失败、崩溃还是超时）都从起始快照恢复数据库 | `Dispatcher._run_refund` + `SeedManager.restore` |
| 恢复完成（或确认失败）后才派发下一项 | 恢复在派发线程内同步执行 |
| 恢复失败：记录错误、暂停派发，但仍然接收新用例并入队 | `_paused` / `submit()` |
| 重试恢复成功后从原队列原顺序续跑 | `retry_restore` |
| 网页可手动装填"下一次恢复失败"，用于演练 | `POST /api/restore/simulate-failure` |
| 退款用例按目标订单核验写入，误写其他订单即判失败 | `app/verification.py` + `Dispatcher._run_refund` |

## 退款写入核验（误写检测）

退款用例在启动子进程前，由调度器在同一数据库内布设
`AFTER INSERT/UPDATE/DELETE` 触发器，把 `orders`、`ledger_entries`、
`refund_requests` 的每次变更连同新旧行、新旧订单号写入内部日志表，并打上本次
用例的 `task_id`。子进程退出后、**恢复数据库之前**读取日志并核验：

- 用例只允许写自己的**目标订单**（`CaseDefinition.target_orders`）。
- **新增 / 删除**按该行的 `order_id` 判断；**更新**同时检查新旧 `order_id`，
  把行从一个订单挪到另一个订单也会被发现。
- 提交了对非目标订单的写入即判失败；**同一事务里改回原值也算**（`UPDATE` 触发器
  在值未变化时同样触发）。
- **回滚的写入不计**：日志与业务写入在同一事务内，回滚时一并回滚。
- **进程提交后崩溃也会取证**：取证发生在子进程退出之后、恢复之前，日志已随
  `COMMIT` 落盘；读取方打开库时 SQLite 会自动回滚崩溃进程未提交的事务。
- **取证失败（无法布设或读取、日志不属于本用例）记为"未核验"**：用例不能通过
  （即使子进程报告成功），但**仍会照常执行复位**。
- 核账用例只读，不参与该核验，页面显示"不适用"。

状态：`passed`（核验通过）/ `failed`（发现误写，附带误写表名与非目标订单号）/
`unverified`（未核验，附带原因）。核验结论与退款结果、复位结果一同显示在调度页，
`GET /api/state` 中每个任务的 `verification` 字段可见。

其他实现要点：

- **每个用例一个进程**：`python -m app.worker --case AUD-001 --db /data/settlement.db`，
  stdout 输出一行 JSON 报告，退出码 0=通过 / 2=失败 / 其他=崩溃。
- **只读契约由数据库强制**：核账进程连接后执行 `PRAGMA query_only = ON`，写操作会直接报错。
- **恢复是原子的**：先复制到 `*.restore-stage` 并做 `PRAGMA integrity_check`，再 `rename` 覆盖，
  读取方不会看到写了一半的库。
- **退款写在一个 `BEGIN IMMEDIATE` 事务里**，核账不会读到中间状态。
- 子进程超时（默认 30s）会被 `SIGKILL` 整组结束后走恢复流程。
- 服务启动时无条件把测试库恢复为起始快照（队列在内存中，重启即清空队列）。

## 预置用例

| ID | 类型 | 说明 | 预期 |
| --- | --- | --- | --- |
| AUD-001 | 核账 | 订单金额与支付流水核对 | 通过 |
| AUD-002 | 核账 | 退款流水不超过支付流水 | 通过 |
| AUD-003 | 核账 | 订单状态与净额一致性 | 通过 |
| AUD-004 | 核账 | 退款申请与退款流水对账 | 通过 |
| REF-001 | 退款 | 对 ORD-1001 部分退款 25.00 元 | 通过，写库后恢复 |
| REF-002 | 退款 | 对 ORD-1002 全额退款 42.00 元 | 通过，写库后恢复 |
| REF-003 | 退款 | 重复退款（ORD-1003 已全额退款） | 业务拒绝（用例判失败，库不变） |
| REF-004 | 退款 | 超额退款（ORD-1005 退 25.00 > 19.99） | 业务拒绝 |
| REF-005 | 退款 | 退款提交后故意异常退出 | 失败，但**已提交的写入仍会被恢复** |
| REF-006 | 退款 | 演示：正常退 ORD-1002，但在同一事务内误改 ORD-1004 后又改回并提交 | **核验失败**（误写 `refund_requests` / `ORD-1004`），退款本身成功，库恢复 |

种子数据：5 笔订单、7 条流水、2 条退款申请，位于 `data/settlement.seed.db`（首次启动自动生成）。

## 本地运行

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m app                    # http://127.0.0.1:8000
```

## Docker Compose

```bash
docker compose up --build         # http://127.0.0.1:8000
```

- 镜像基于 `python:3.12-slim`，以非 root 用户运行，内置 `HEALTHCHECK`。
- 测试库与快照存放在命名卷 `settlement-data`（容器内 `/data`）。
- 端口可用 `SETTLE_PORT` 覆盖；核账并发上限用 `SETTLE_MAX_AUDIT`。
- 容器内跑一次写入核验自检（不进入网页队列、不写命名卷）：

  ```bash
  docker compose run --rm verification-selfcheck
  ```

  该一次性服务使用 `selfcheck` profile，`docker compose up` 不会启动它。

网页提交 `REF-006` 即可看到"误写另一订单、改回后提交"的演示结果：
核验失败，列出 `refund_requests` 与 `ORD-1004`，退款与复位结果照常显示。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 调度台页面 |
| GET | `/api/presets` | 预置用例列表 |
| GET | `/api/state` | 队列、状态、结果、**核验结论（`verification`）**、数据库摘要（网页每 0.9s 轮询） |
| POST | `/api/cases` | `{"preset_id": "REF-001", "copies": 1}`，返回 202；未知用例 404；队列满 429 |
| POST | `/api/restore/simulate-failure` | `{"count": 1}` 装填模拟恢复失败 |
| POST | `/api/restore/retry` | 重试恢复；无待恢复项时 409 |
| GET | `/api/db/fingerprint` | 只读查看测试库全部数据，便于人工核对 |
| GET | `/healthz` | 健康检查 |

## 测试

```bash
PYTHONPATH=. python3 -m pytest tests -q
```

覆盖：四项核账通过且不改库、退款成功/失败后均恢复、崩溃提交后仍恢复、
核账不越过排队退款、退款等待运行中核账、核账并发恰为 2、恢复失败暂停/拒派发/重试续跑、
重试再失败仍暂停、超时杀进程后恢复、重启恢复起始状态、核账写库被数据库层拒绝、
HTTP 层 404/409/暂停暂停期间提交、页面渲染、HTTP 提交 REF-006 后核验失败结果可见。

写入核验另有一组测试（`tests/test_verification.py`），覆盖：目标订单写入通过、
非目标订单的新增/更新/删除均判失败、更新检查新旧订单号、改回原值仍判失败、
回滚写入不计、提交后硬崩溃仍能取证并归到本次用例、崩溃于事务中视为回滚、
取证失败为"未核验"且不能通过、布设失败仍执行用例并复位、复位前已读取证据、
连续多个退款用例的取证互不串台、以及演示用例 REF-006。

### 写入核验自检（不依赖网页预置）

`app.worker` 内置三个崩溃注入钩子，仅供测试与人工自检使用（不是网页预置）：

```bash
# 提交对非目标订单的写入后硬崩溃（应判 failed，证据归到本次用例）
python -m app.worker --case SELFTEST-STRAY-COMMIT-CRASH --db data/settlement.db
# 事务中写入非目标订单后硬崩溃（应视为回滚，不计）
python -m app.worker --case SELFTEST-STRAY-ROLLBACK-CRASH --db data/settlement.db
# 误写并改回、提交目标退款后硬崩溃（应判 failed）
python -m app.worker --case SELFTEST-STRAY-RESTORED-COMMIT-CRASH --db data/settlement.db
```

注意：这些钩子自身不布设取证，直接运行 worker 时看不到核验结论；它们通过
`Dispatcher.submit_selftest(...)` 走完整退款流水（布设 → 崩溃 → 取证 → 复位），
自动化测试即这样调用。

## 已知限制

- 队列与结果保存在内存，服务重启即清空并恢复起始库；未做多副本共享调度。
- SQLite 恢复采用"整文件替换"，要求同一时刻只有本服务在写这个库。
- 未内置登录鉴权，默认只在受信网络内使用。
