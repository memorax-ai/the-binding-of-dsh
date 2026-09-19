# The Binding of DSH

[![Powered by Harmony](https://memorax-ai.github.io/dsh-harmony/harmony-powered.svg)](https://memorax-ai.github.io/dsh-harmony/)

[English](README.md) | [简体中文](README.zh-CN.md)

一个通过 DSH 原生 Connection 和 Typert Gateway，在 Host 与 Client 之间提供双向 RPC 的 DSH 插件。

## 功能

- 向已连接的浏览器或 Node peer 发起定向 Host-to-Client 调用。
- 支持双向 Typert Gateway 调用。
- 处理请求关联、取消和连接生命周期。
- 旧版 Host 上，RPC 与事件共用原生 `events.host` WebSocket。
- 新版 Host 上，双向 RPC 使用 Binding 通道，事件使用原生 Gateway 流。

Connection 负责 peer 寻址和连接生命周期，Typert Gateway 负责服务描述、编解码、调用和错误处理。

## 安装

```sh
npm install the-binding-of-dsh
```

包已在 `package.json` 中声明 DSH 客户端入口和 Harmony patch，因此可以作为常规 DSH 插件启用。Harmony 0.8.6
及以上版本会自动推导 Connection patch 引入的浏览器模块依赖，并安排 Binding 模块的加载顺序。

## 开发

需要 Node.js 22.22.3 或更高版本。

```sh
npm install
npm run check
```

## 许可证

MIT

## 新旧版事件适配

`BrowserPeerClient` 在连接时协商事件传输。旧版继续使用 `onEvent()`，保留
`{ channel, envelope: { rpcId, payload } }` 格式；旧版解码器已随包提供，新版安装无需
`dsh-host-apiproxy`。业务类型可通过 `onEvent<T>()` 或 `BrowserPeerEvent<T>` 指定。

DSH 0.1.5/0.1.6 使用 `gateway-v1`：

- `subscribe(event, listener)`：订阅原生事件，保留事件名和参数。
- `handleEvent(event, handler)`：处理交互事件，收到 `eventId`、`agentId`、`request` 和 `signal`。
  返回符合上游约定的结果会自动应答；返回 `undefined` 继续交给后续处理器/Host，抛错表示拒绝。
- `stream(endpoint, payload, signal)`：打开会话等业务流，参数使用原生 `{ args: ... }`，返回异步迭代器。

连接须收到 Gateway ready 才报告成功。取消和断线会中止交互信号，并阻止过期应答。
可再次调用 `connect()`，或用 `reconnectDelayMs` 启用自动重试；事件处理器保留。
重连后的 `onState(true)` 需重新获取业务状态、打开业务流；Binding 不重放事件，也不重建 Agent 上下文。
`close()` 停止重试、关闭通道、取消请求并撤销 Remote 注册。

新版不通过 `onEvent()` 模拟旧事件。Studio 仍需接入这些新接口，才能完成其 UI 兼容。
仅支持旧事件的旧版客户端连接新版 Host 时继续收到 HTTP 409，避免误报连接正常。

2026-09-19 验证：31 项测试通过；隔离启动 DSH `0.1.5-rc.2` 和 `0.1.6-alpha.2`，
双向 RPC、鉴权、事件通知、交互应答/委托/异常/取消及重连均通过。完整示例及复测命令见英文 README。
