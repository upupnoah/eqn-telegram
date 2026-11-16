# EquationNews Telegram

用 Rust 将方程式新闻的 12 个频道推送到受邀用户的 Telegram 私聊。主频道通过 WebSocket 通知触发同步，中文正文、频道、原文链接和去重编号来自 MCP。

## 在 Telegram 中使用

受邀用户首次私聊机器人时，服务会将邀请绑定到其 Telegram 用户 ID。每个人单独管理自己的订阅。

| 操作 | 用途 |
| --- | --- |
| `/start` 或 `/help` | 打开菜单 |
| `/latest` | 在一条消息里浏览最近三条快讯，点按钮阅读全文 |
| `/channels` | 按频道浏览 |
| `/search BTC` | 按关键词查询近 30 天的快讯 |
| `/status` | 查看连接、个人队列、投递耗时，重试被拒收的消息 |
| `/pause`、`/resume` | 暂停接收或恢复补发 |

消息按 UTC+7 显示。自动推送使用 Telegram 原生富文本展示标题、正文、币种、来源和发布时间。上游提供原始出处时，卡片会增加来源按钮。

## 运行

使用支持 Rust 2024 edition 的当前稳定工具链。BotFather 创建的 token 和订阅者配置放在仓库外的私有文件中。以下值均为示例。

```json
{
  "token": "<Telegram bot token>",
  "owner_id": 123456789,
  "allowed_usernames": ["invited_username"]
}
```

```sh
export EQN_CONFIG_FILE="$HOME/.config/eqn-telegram/config.json"
export EQN_STATE_DIR="$HOME/.local/state/eqn-telegram"
chmod 600 "$EQN_CONFIG_FILE"
cargo build --release --locked
target/release/eqn-telegram check
target/release/eqn-telegram configure
target/release/eqn-telegram run
```

`check` 检查 Telegram 身份和各频道的可访问性，`configure` 设置命令及机器人简介。使用 `access` 查看邀请的绑定状态。使用 `smoke` 给所有者发送一条标明「版式预览」的真实快讯。运维命令应使用与服务相同的配置及状态目录。

## 实时链路与故障恢复

- WebSocket 事件立即触发中文索引查询，随后短间隔复查一次。全频道空闲时每次成功查询结束约 2 秒后再次检查。同步器共享 MCP 限速器，遇限流或上游错误后退避。重连会主动补查。详见[数据源接口文档](https://eqn.news/api-docs)。
- 分页读完后在一个 SQLite 事务内保存快讯、每人待发记录和游标。重复编号自动合并，并回看最近一分钟内晚到的索引记录。每个状态目录仅运行一个 worker。
- 提交后立即唤醒投递器，并发服务不同私聊。同一私聊最多一个在途推送，互动回复与推送共享该私聊的发送间隔。全局节流和 HTTP 429 冷却共同限制发送速率。详见[Telegram 限速说明](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)。
- 收到 Telegram 确认后记录消息 ID。网络错误按退避时间重试，HTTP 400 的投递单独保留并支持手动重试。HTTP 403 会暂停该用户的投递，重新允许私聊后可用 `/resume` 恢复。暂停期间的待发消息保留，已确认记录保留约 30 天。
- 投递采用至少一次语义。若 Telegram 接收成功后连接中断，或进程在本地保存确认前退出，重试可能产生重复。状态页的「入队至确认」包含个人排队和限流，「发布至确认」还包含上游索引时间。两项都以 Bot API 确认为终点。

## 部署与检查

仓库附有用于持续运行的 [systemd 单元](deployment/eqn-telegram.service)。在其他机器上部署时，按实际用户和绝对路径调整单元，授予运行用户对状态目录的写权限。升级前保留上一版二进制，并使用 SQLite 在线备份或停服备份数据库。重启时会恢复待发队列和 Telegram 更新游标。

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
systemctl status eqn-telegram.service
journalctl -u eqn-telegram.service -n 50 --no-pager
```

成功投递日志包含快讯编号、此次发送耗时及发布至确认的毫秒数。核查部署时可先检查 WebSocket 心跳，再在 Telegram 中用 `/status` 确认最近同步，使用 `/latest` 查看真实消息。
