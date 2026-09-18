# stock_mm — Binance 定价、Gate 做市（SNDK 永续）

Binance `SNDKUSDT` 只提供参考价，不逐笔对冲；所有开仓、减仓、平仓都在 Gate `SNDK_USDT` 完成。
正常双边 POST_ONLY 挂单；库存偏向哪边就降低那边的加仓积极性；行情快速变化先撤危险订单；
库存超限或迟迟无法消化，就在 Gate 用带价格保护的 reduce-only IOC 主动减仓。

自包含实现（不依赖 `nb-arbitrage`）：Binance/Gate 连接器、本地订单簿、订单状态机、策略与指标全部在本仓库内。

## 运行

```bash
# 1. 复制配置
cp config.example.toml config.toml        # 按需修改 H、δ 等参数
# 2. 密钥（live 模式需要；paper 模式不需要）
cat > .env <<EOF
GATE_API_KEY=...
GATE_API_SECRET=...
EOF
# 3. 纸面模式：真实行情 + 本地模拟撮合，不发真实订单
cargo run --release -- --config config.toml --paper
# 4. 实盘
cargo run --release -- --config config.toml --live
# 其他
cargo run -- --print-default-config       # 输出全部默认参数
RUST_LOG=info,stock_mm=debug cargo run -- ...   # 打开 reconcile / 危险单 的调试日志
cargo test                                 # 65 个单元/集成测试
```

`Ctrl-C` / `SIGTERM`：撤销全部挂单后退出；`exit.liquidate_on_shutdown = true` 时同时清仓。
退出前会再做一次交易所侧 cancel-all 并打印交易所持仓，便于对账。

## 启动检查

启动时读取并打印两所合约元数据，不假定一致：

- Gate：`order_price_round`（tick）、`quanto_multiplier`（乘数）、数量上下限、`orders_limit`、资金费时间、合约默认费率；
  live 模式再读取**账户实际** maker/taker 费率、账户模式（单向/双向）、可用保证金，并撤掉合约上已有挂单、读取现有持仓。
- Binance：`exchangeInfo` 的 `contractType`、`status`、tick / step / minNotional（只用于核对，报价始终用 Gate tick）。

## 与方案的对应关系

| 方案章节 | 实现位置 | 说明 |
|---|---|---|
| 一、三类输入 | `exchange/binance.rs`、`exchange/gate_public.rs`、`exchange/gate_private.rs` | Binance bookTicker+aggTrade；Gate book_ticker / order_book_update(100ms, 20 档) / trades；Gate orders / usertrades / positions / balances |
| 二.1 自然价差 β₀、F | `market/basis.rs` | β_t = 1e4·(M_G/M_B − 1)，M_G 用**扣除自身订单的外部盘口**；1 小时同类时段中位数；5 分钟窗口只做漂移观察；两边不同步、断流、外部盘口过宽（被自己主导）时不更新 |
| 二.2 D_stop | `market/basis.rs` | D_stop = max(5δ, 正常样本 D 的 99% 分位)；超过 → 冻结 β₀、停止新增、撤开仓单、有仓位进入减仓；恢复需 D ≤ 0.5·D_stop 持续 10 s |
| 二.3 交易时段 | `market/session.rs` | 常规/盘前/盘后/夜盘/周末/事件窗口（美东时区）；β₀、样本数按时段独立；周末与事件窗口不开新仓 |
| 三.1 半点差 δ | `strategy/quote.rs::half_spread_bps` | δ = max(δ_cfg, f_m + e, V90, 1e4·tick/F)，f_m 为账户实际 maker 费率（返佣为负） |
| 三.2 报价中心 | `strategy/quote.rs::quote_centre` | u = clip(Q/H, −1, 1)，r = F(1 − δu/1e4) |
| 三.3 内层 | `strategy/quote.rs::build_quotes` | B₀ = floor[min(B_limit, G_bid+tick, G_ask−tick)]，A₀ 对称；只比外部盘口好一个 tick，绝不突破自身边界 |
| 三.4 外两层 | 同上 | 内层外 0.5δ、1.25δ，取整重合合并；三层同基准金额 |
| 三.5 数量 | 同上 + `strategy/inventory.rs` | v = 0.05H；买 v(1−u)、卖 v(1+u)；开仓单受净/总库存最坏情况额度裁剪，减仓单不超过可减量，不足最小量跳过 |
| 双向持仓映射 | `strategy/quote.rs::side_purposes` | 持多 → 卖单一律 reduce_only 平多（不开空）；持空 → 买单一律平空；多空同时存在 → 两侧都挂减仓单先消总仓 |
| 四.1 危险单 | `order/reconcile.rs` + `SideBounds` | 每个周期（每次 Binance 更新都会触发）先检查**所有层**：买单 > B_limit 或卖单 < A_limit，即使 1 tick 也立即改到目标价，命令排在最前 |
| 四.2 改单阈值 | `order/reconcile.rs` | 内层 2 tick、外层 4 tick；只减量保留价格（保队列）；加量超过 30% 才改 |
| 四.3 方向性保护 | `strategy/protection.rs`、`market/impact.rs` | v₁₀₀ ≥ 1δ 轻度 / ≥ 2δ 强；Gate 100 ms 单边占比 > 80% 且成交额 > 95% 分位；轻度：半量+1.5 倍距离；强：撤该侧开仓单、另一侧半量紧跟；恢复 500 ms 半量 → 2 s 全量；减仓单不受保护缩量影响，强信号逆向持仓且库存已在带外 → 立即主动减回目标带 |
| 四.4 无条件撤开仓单 | `strategy/risk.rs`、`engine/mod.rs` | 行情断流/订单簿失序/私有回报失联/持仓对不上/保证金不足/基差异常/主动减仓中/亏损限额/停止 |
| 五.1 成交后顺序 | `engine/mod.rs::on_fill` → `cycle` | 先更新库存与 u，再重算报价；平仓单跟随当前 F 与库存，不绑开仓价 |
| 五.2 减仓侧更积极 | `strategy/quote.rs` | \|u\| ≥ 0.5：减仓侧 = ceil[max(A_limit, G_bid+tick)]（去掉"只改善一个 tick"约束） |
| 五.3 库存退出 | `strategy/inventory.rs`、`engine/mod.rs` | 带外 > `band_timeout_ms` → 停止加仓并主动减回 ±`band_ratio`·H；计时只在回带且无在途加仓单时结束；带外遇强逆向信号不等超时；达 H 或亏损限额 → 清仓并停止 |
| 五.4 IOC 执行 | `strategy/reduce.rs` | 先撤冲突挂单（等待确认或超时）→ 按 Gate 外部深度在滑点上限内计算可成交量与最差价 → reduce_only IOC → 只按实际成交减库存 → 部分成交后重算再发 → 超时升级 `market_then_halt` / `halt_only`，默认不自动回到正常做市 |
| 六、订单状态 | `order/manager.rs` | 一单一在途请求，新意图覆盖旧意图；改单/撤单失败或超时 → 查询而非假定；新单超时不重发；WS `ack` 不算最终结果；amend size = 已成交 + 目标剩余（总量语义）；成交按 trade id 幂等，撤单回报不会抹掉迟到成交 |
| 六、最坏成交额度 | `strategy/inventory.rs::worst_case` | 分别计算"全部买单成交"/"全部卖单成交"，含在途、改单/撤单未确认；双向模式另查总持仓 |
| 撤单成功 ≠ 资金可用 | `engine/mod.rs` | 可用保证金只来自交易所账户快照（2 s 轮询），本地不凭预计释放加回 |
| 七、执行流程 | `engine/mod.rs::cycle` | 9 步顺序实现，每个行情/订单/成交/定时事件触发一次 |
| 验收指标 | `metrics.rs` | 分买卖侧 50 ms / 200 ms / 1 s markout、库存消化时间、主动退出成本、含库存估值与全部费用的真实权益，每 10 s 追加到 `metrics.jsonl` |

## 关键实现选择（与方案原文有关的假设）

- **不使用 `nb-arbitrage`**：该私有库的永续交易/订单/资产模块只支持 Binance/Bybit/Aster，没有 Gate 永续，因此本项目自带 Gate 永续的 REST、公共/私有 WS 与 WS 交易 API（`futures.login` / `order_place` / `order_amend` / `order_cancel`，REST 兜底）。
- **交易 WS 多 IP 轮询**：live 下单会反复解析 `gate_ws` 域名，钉到最多 `gate_trade_ws_pool`（默认 6）个不同 IP，place/amend/cancel 在这些已登录连接上轮询，把限频摊到不同 Gate 后端；解析不到多个 IP 时退回单连接。
- **双向持仓模式**：按方案实现映射（卖单优先平多、买单优先平空），同时兼容单向模式（reduce_only 语义一致）。启动时读取账户 `in_dual_mode`，运行中变化会告警。
- **"被自己主导"的判定**：β 样本用扣除自身订单后的外部盘口；若外部盘口宽于 `basis_max_ext_spread_bps`（默认 15 bps）视为无信息，不计入样本。
- **Gate 冲击信号**默认按"轻度"处理（`impact_is_strong = false`），与 v₁₀₀ 叠加时取更强者。
- **`|u| ≥ 0.5` 停止新增风险**（`stop_add_u`）：方案表格中 0.5H 一行文字有缺失，这里取保守解释——0.5H 起只挂减仓单并允许减仓侧更积极。
- **基差异常有仓位时的减仓目标**是目标带（±0.1H），不是清零；硬上限与亏损限额才清零。
- **"未决的加仓订单"** 解释为在途（未确认）的加仓订单；否则双边常驻挂单会让 3 秒计时永不结束。
- **纸面模式撮合**偏乐观：Gate 成交价触及/穿过我方价位即成交、外部盘口穿过我方价位即成交、IOC 按本地深度扫单；用于验证逻辑而非估计收益。
- 参考价 F 不可得（Binance 断流）时撤销所有挂单；此时若库存在带外超时且 Gate 行情正常，用 Gate 中间价作为参考执行减仓。
- **标记价/指数价只做异常检查**：订阅 Gate `futures.tickers`，|F/mark − 1| 超过 `risk.mark_deviation_bps` 时不新增风险；从不作为可成交价。
- **最坏成交额度中的在途改单**：改单在途期间按 max(旧量, 新量) 计入；正处在报价槽位且用途一致的在途订单视为"可重新指定"（由 reconcile 改到目标量），否则新下的单会把自己的槽位额度挤掉。
- **强逆向信号触发的主动减仓**只在库存已经在目标带外时触发，目标是减回目标带（不是清零）。带内的小仓位只靠保护逻辑撤开仓单：实盘数据显示被扫后 200 ms 内以 taker 追价全平，是净亏损来源之一。
- `orders.use_amend = false` 时走撤单重挂：只发撤单，确认终态后下一周期按**当时**的 F 与库存重新下单。

## 目录

```
src/
  main.rs            启动：元数据、账户、连接器、事件循环、信号
  config.rs          TOML 配置（全部参数有默认值）
  types.rs           Side/Purpose、tick 网格、合约元数据
  exchange/          binance（REST+WS）、gate_rest、gate_public、gate_private、gate_trade（WS API）
  market/            book（本地 L2，扣自身、扫单均价）、reference（M_B、v100、V90）、basis（β₀/D_stop/漂移）、session、impact
  strategy/          inventory（Q/u/带计时/最坏敞口）、quote（F→r→三层报价→开平映射→裁剪）、protection、reduce（IOC 状态机）、risk
  order/             model、manager（订单状态机）、reconcile（危险单优先的差量改单）
  engine/            9 步事件循环、paper 模拟撮合、事件定义
  metrics.rs         markout / 库存消化 / 退出成本 / 真实权益
```

## 日志

- `status`（每 5 s）：时段、基差状态与样本数、β₀/β_t/漂移、F、δ、两所盘口、持仓、挂单数、保护状态、减仓阶段、权益、可用保证金、不可信原因。
- `fill`：每笔成交的方向/用途/层/数量/价格/maker/费用/成交后持仓。
- `reduce IOC` / `EMERGENCY market reduce` / `STRATEGY HALTED`：主动减仓与停止。
- `RUST_LOG=stock_mm=debug` 额外输出每次 reconcile 的 危险/改价/改量/新增/撤销/保留 计数与内层价格。
