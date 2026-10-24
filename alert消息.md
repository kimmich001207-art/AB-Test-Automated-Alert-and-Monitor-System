这个文件实际叫 [alerts.py](alerts.py)，项目里没有 `alert.py`。它是 Airflow DAG 的最后一个任务 `send_xpc_alerts`，DAG 每小时跑一次，顺序是 `resolve_xpc_metadata → collect_and_evaluate_xpc → send_xpc_alerts`。前两个任务负责解析 XPC 元数据、用 Spark 算出各实验组的指标，这个文件负责把结果整理成消息发到 Slack 的 `#aq-xpc-alerts` 频道。

## 整体结构

文件从上到下分成五块：

| 区块 | 作用 |
|---|---|
| 标题辅助函数（20–65 行） | 生成负责人 @提及、AB 测试 / 容器 / 版位的显示名 |
| 格式化函数（72–103 行） | 把数字转成百分比、千分位、"相对对照组变化 x%" 等文本 |
| Slack 发送（110–116 行） | 发 webhook 并检查返回值 |
| 发送频率逻辑（123–156 行） | 判断这一轮该不该发报告 |
| 消息拼装和主入口（163 行之后） | 拼装三种消息，`send_alerts` 统一调度 |

## 会发的三种消息

### 1. 新版本上线通知（`_send_new_version_alerts`，395 行）
- 从 metadata 表取出当前所有 XPC 版本，再取出已经通知过的 key，跳过已通知的。
- 按 `(container_id, container_name, abtest_id, version, abtest_instance_id)` 分组。同一个版本如果在多个版位上线，会由 `_merge_version_rows` 合并成一条，版位 ID 用逗号拼起来；只要其中有一行的版位是 `None`，就显示 "all placements"。
- 发送成功后，每一行都用 `record_alert` 写一条去重记录，下次就不会重复发。

### 2. 定时业务指标报告（`_report_cadence` + `_business_metric_message`）
发送频率规则（123 行）：
- XPC 已结束，或者还没开始：不发
- 开始后前 `HOURLY_REPORT_HOURS = 6` 小时内：**每小时**发一次
- 之后只在 KST `DAILY_REPORT_HOUR = 23` 点那一轮发，也就是**每天**一次

报告内容：先列对照组（`CONTROL_OPTION = 0`），再按 option 编号列实验组。每组显示流量占比、用户数，以及 rev/user、clicks/user、views/user、impressions/user 四个指标。实验组的数值后面会附上相对对照组的变化，比如 `0.0312 (-30.97% vs ctrl)`，由 `_fmt_ratio` 生成。

同一个容器下所有该发的 AB 测试会合并成**一条**消息（`_container_metric_message`），并用 `report:container:{id}:{run_ts}` 这样的 key 去重（586 行）。

### 3. 收入下降告警（`_revenue_alert_policy` + `_revenue_loss_message`）
每个实验组都会按流量分档判断要不要告警（377 行）：

| 实验组流量 | 是否告警 | `rev_drop` 阈值 |
|---|---|---|
| 没有数据 | 不告警 | — |
| < 0.1% | 不告警（样本太少、噪声大） | — |
| 0.1% – 1% | 告警 | > 25% |
| ≥ 1% | 告警 | > 5% |

`rev_drop` 是上游 Spark 任务算好的 `(对照组 rev/user − 实验组 rev/user) / 对照组 rev/user`。超过阈值就单独发一条 `REVENUE LOSS ALERT` 并 @负责人。

## 主入口 `send_alerts` 的流程（469 行）
1. 把 `run_ts_kst`（格式 `YYYYmmddHHMMSS`）解析成 KST 时间。
2. 发新版本通知，同时拿到 `current_versions`。
3. 用 `current_versions` 建一个以 `(container_id, abtest_id)` 为 key 的元数据表。
4. 读出当天所有指标结果，按 `(container_id, abtest_id)` 分组。
5. 逐个 AB 测试处理：该发报告的放进 `report_groups`；收入下降的当场发告警。
6. 按容器发合并后的报告，并按 key 去重。
7. 统计 `sent` 和 `failed`。**只要有一条发送失败就抛 `RuntimeError`**，让 Airflow 把任务标成失败。

## 读代码时注意到的几点
- **收入告警没有去重。** 新版本通知和报告都写了去重记录，但收入告警（565 行）没有。只要下降一直持续，每小时都会重发一次。另外第 7 步抛异常后，如果 Airflow 重试这个任务，收入告警也会重复发出。如果这是有意的"持续提醒"，那没问题，否则可以考虑加去重。
- **`_version_message`（163 行）没有被任何地方调用**，像是旧实现留下来的。
- **新版本通知并不是每个容器一条。** `_container_version_message` 的标题写的是容器名，但分组 key 里包含 `abtest_id` 和 `version`，所以实际是每个 AB 测试版本发一条。
- **`config.MIN_GLOBAL_USERS` 在这个文件里没有用到。** 这个过滤应该是在上游的 [collect_and_evaluate_xpc.py](collect_and_evaluate_xpc.py) 里做的。

如果想看其中某个函数的细节，或者想改上面提到的问题，可以告诉我。