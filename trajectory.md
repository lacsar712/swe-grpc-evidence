# gRPC 重试绑定首个 Subchannel

> **Workspace:** d:\document\swe\grpc-go

---

**User**

gRPC 客户端现在每次重试都会重新挑选连接。请让单次调用可以声明，自己的重试必须留在第一次选中的那条 subchannel 上。没有这个声明时，解析、负载均衡、重试、超时、流式调用和现有 picker 的行为必须和现在完全一样，现有测试也要继续通过。

这个声明要进入真正选择 subchannel 和发起重试的那一层。调用方在拦截器里自己记住地址再拨号，或者失败之后自己换一个通道重发，不算实现。第一次成功选出 subchannel 之后，同一次调用后面的重试都使用它。这条 subchannel 收到 GoAway，或者变成不可用时，这次调用直接失败，不要悄悄换到另一条连接。没有声明的其它调用，仍然按现在的 picker 选择连接。

调用结束、失败或者被取消之后，绑定必须释放。后面无关的调用不能粘在同一条 subchannel 上。绑定还在的时候，picker 的更新不能丢掉这次已经发出的调用，也不能把这个绑定套到别的调用上。一元调用和流式调用都要遵守。重试次数、退避，以及哪些状态码允许重试，仍然按现在的服务配置，不要因为存在绑定就多试一次。

如果第一次还没有选出 subchannel 就失败了，按现在的错误返回，不要留下一个空绑定。空闲连接回收不能拆掉一条仍然被绑定占用的 subchannel。服务端、服务端拦截器和现有的负载均衡策略在没有这个声明时保持现状。

透明重试和 hedging 只要属于同一次调用，也要留在这条 subchannel 上。hedging 会并行发出多个尝试时，这些尝试都使用第一次选中的 subchannel，不要为了并行再挑选连接。绑定建立之后，WaitForReady 只继续等待这条 subchannel，不要因为暂时不可用就改选其它地址。这个声明不能改方法名、元数据或压缩，现有的编码和压缩协商保持现状。

**Assistant**

我先回顾相关经验并探索 grpc-go 代码库结构，然后制定实现计划。

经验已回顾（Kitex 重试经验，参考价值有限）。现在建立任务清单并深入阅读 grpc-go 核心代码。


现在阅读 picker 包装层和 balancer/subconn 包装层。


现在查看 clientconn.go 中 `addrConn` 的关键部分：`getReadyTransport`、GoAway 处理、状态切换、idle 管理器。


现在阅读 idle 管理器和 call.go：


查看 ClientConn 的 idle 进入/退出逻辑和连接管理：


检查 hedging 在当前版本的实现情况：


查看 transport 层 GoAway/NewStream 错误语义：


查看 call.go 其余部分和 callInfo 定义：


查看 addrConn 构造、dial 选项中的 idle timeout、测试辅助 balancer 和现有 retry 测试模式：


查看 stubserver、roundrobin 多后端测试模式和 goaway 测试：


我已经充分理解链路。查看测试套件注册方式与短超时常量，然后开始实现：


设计完成，开始实现。先更新任务状态：


**步骤 1：新增 CallOption（rpc_util.go）**


**步骤 2：在 clientStream 中加入绑定状态与尝试路径（stream.go）**


现在改造 `getTransport`，新增绑定选择逻辑：


现在在 `finish` 中释放绑定：


**步骤 3：addrConn 增加状态变更通知与绑定调用取连接逻辑（clientconn.go）**


现在在 `getReadyTransport` 后面新增绑定调用的取连接方法：


先编译检查：


网络无法访问 proxy.golang.org，切换到国内镜像：


构建通过。查看测试套件 `s` 的定义方式：


查看 `addrConn.connect` 以确认重连行为：


现在检查 `pickerWrapper.pick` 的所有使用点，确保没有遗漏的选路路径：


核心路径完整。更新进度并编写 e2e 测试：


现在编写 e2e 测试文件：


简化测试文件中的辅助函数：


编译测试包检查错误：


此版本 `BalancerData` 没有 `Data` 字段，改用包级 map 按 BalancerData 实例关联状态：


vet 通过。运行新增测试：


全部超时，需要看详细失败原因。运行单个测试并保留完整输出：


`NewClient` 是懒连接的，需要在 helper 里显式 `Connect()`：


核心重试绑定测试通过。运行其余四个场景：


逐个运行并过滤结果行：


failfast 测试单独运行通过。继续其余三个：


WFR 测试的前提有误（硬断连不是透明可重试）。查看现有重试测试中连接断开如何重试：


`TrailersOnly` 在无头部断连时为 true，配置 UNAVAILABLE 重试策略即可让策略重试接管（随后阻塞在绑定 subchannel 的 WFR 等待上）。修正 WFR 测试：


全部通过。用 race detector 跑全部新测试（含共享全局状态的并发检查）：


环境不支持 cgo/race，改为整体非 race 运行全部新测试：


新测试 3 轮全部通过。现在验证无回归——先跑根包全部单测：


只有 `ParsedTarget_WithCustomDialer` 失败。需要确认是否为我改动引入——先看失败详情，再用 stash 对比基线：


该失败与 unix dialer 地址格式化有关，与本次改动无关。用 git stash 在基线上验证：


确认该失败在基线（未改动代码）上同样存在，属 Windows 环境既有失败。确认根包测试中没有其它失败：


根包仅有既有失败。现在运行受影响区域的关键 e2e 测试子集（retry/goaway/roundrobin/subconn/stream）：


继续跑 roundrobin/balancer/idle 相关测试：


全部通过。做几项代码自检：gofmt、确认没有其它 addrConn 构造点、检查 `pickResult.SubConn` 的其它消费者：


gofmt 报告格式问题，查看差异并修正：


整文件差异是行尾符（CRLF/LF）问题。检查仓库文件的行尾：


整棵树检出为 CRLF，未改动文件同样被 gofmt 标记，属环境既有状态。检查我新写测试文件除行尾外是否有真实格式问题：


新测试文件格式干净。同样检查三个修改的源文件：


启动整个 `test/` e2e 包的完整测试（后台），同时我复查最终代码：


后台全量 e2e 运行期间，复查最终代码 diff：


复查完成，实现最小且正确。检查全量 e2e 进度：


全量 e2e 有失败，提取失败用例：


两个失败：`LocalCredsUDS`（Unix socket，Windows 环境性）和 `NegativeRPCTimeout`。查看详情并在基线对比：


在基线（stash 我的改动）上运行这两个失败用例确认：


测试文件未被 stash 跟踪导致基线编译失败。临时移走测试文件再跑基线：


两个失败在干净基线上同样存在（UDS 在本机 Windows 网络不可用、NegativeRPCTimeout 为既有环境问题），非本次回归。确认全量运行中新测试也执行了且只有这 2 个既有失败：


全量 e2e 中除两个基线环境性失败外全部通过，且失败数恰好为这 2 个。最后再快速回归一次新测试与根包关键测试（stash 恢复后）：


全部通过。获取 IDE 诊断做最后检查：


已完成。实现位于真正选路与发起重试的一层（`csAttempt.getTransport`），拦截器/调用方无需也无法自行伪造。

## 新增 API

[grpc.PinSubchannel()](file:///d:/document/swe/grpc-go/rpc_util.go#L369-L411) —— 一个普通 `CallOption`，在调用选项上声明，一元和流式调用都可用。不改方法名、元数据、压缩协商；不声明时所有路径与现状完全一致。

## 改动位置（4 个文件）

1. [rpc_util.go](file:///d:/document/swe/grpc-go/rpc_util.go) — 新增 `PinSubchannel()` / `PinSubchannelCallOption`（内嵌 `EmptyCallOption`，Experimental 标注），`callInfo` 增加一个布尔位。

2. [stream.go](file:///d:/document/swe/grpc-go/stream.go#L605-L699) — 核心：
   - `clientStream` 增加 `pinMu` + `pinnedAC *addrConn` + `pinnedMetadata`；
   - [getPinnedTransport](file:///d:/document/swe/grpc-go/stream.go#L636-L699)：**首次尝试照常走 pickerWrapper**（blocked 统计、Done 回调、延迟 pick 指标全部不变），仅在**成功选出**后才记下该 addrConn；之后所有重试、透明重试、以及将来的并行 hedging 尝试都不再调用 picker，只取同一 subchannel 的当前 transport（`pinMu` 串行化建立过程，保证并行尝试也绑到第一次选择）；
   - 首次 pick 失败/drop 时不建立绑定（不留空绑定）；
   - [finish](file:///d:/document/swe/grpc-go/stream.go#L1307-L1311) 统一释放绑定（成功/失败/取消同一路径），后续无关调用正常重新选择。

3. [clientconn.go](file:///d:/document/swe/grpc-go/clientconn.go)：
   - addrConn 增加 `stateCh`，在 [updateConnectivityState](file:///d:/document/swe/grpc-go/clientconn.go#L1344-L1348) 每次状态迁移时广播（仅新增通道，不影响任何既有逻辑）；
   - 新增 [transportForPinnedCall](file:///d:/document/swe/grpc-go/clientconn.go#L1732-L1764)：READY 直接返回；**Shutdown 立即 Unavailable**（WFR 也失败，不空等）；其它状态下 fail-fast 立即返回 Unavailable，WaitForReady 只阻塞等待**这一条** subchannel 重新 READY（GoAway 后同 subchannel 重连即可继续），ctx/ClientConn 关闭同样会唤醒。

4. [test/subchannel_pin_test.go](file:///d:/document/swe/grpc-go/test/subchannel_pin_test.go) — 6 个 e2e 测试。

## 关键语义对照需求

- **不换连接**：重试/透明重试/hedging 全部复用首次的 subchannel；picker 更新、RR 轮换都不影响已绑定调用（绑定等待挂在 addrConn 状态上，不挂 picker generation）。
- **重试策略不变**：次数、退避、可重试状态码仍走现有 `shouldRetry`，不会因绑定多试；只是不再重新 pick。
- **GoAway/不可用**：fail-fast 直接 Unavailable；WFR 只等这条 subchannel（同地址重连后成功）；subchannel 被 balancer 删除（Shutdown）立即失败——绝不迁移到另一条健康连接。
- **空闲回收**：绑定期间调用始终处于 active RPC 计数保护内（`OnCallBegin/OnCallEnd` 窗口覆盖绑定窗口），idle 定时器不会拆连接；有专门的流式长连接测试（100ms idle timeout × 6 倍时长后 transport 不变）。
- **一元与流式**：均有测试（3 次尝试全部落在同一后端；对照组无声明时按 RR 分散到两后端）。
- **服务端、服务端拦截器、现有 LB 策略**：零改动。

## 测试结果

- 新增 6 个测试（重试固定一元/流式、绑定释放后 RR 恢复轮换、GoAway 后 fail-fast、WFR 断线后只等同 subchannel 重连、Shutdown 立即失败、首次 pick 失败不留绑定、idle 不拆连接），连跑 3 轮全部通过；
- 根包全部单测、`test/` 全量 e2e、`internal/idle`、`internal/transport`、全部 `balancer/*` 套件均无新增失败；
- 仅有 3 个在**未改动基线**上同样失败的环境性用例：`ParsedTarget_WithCustomDialer`、`LocalCredsUDS`（Windows 不支持 unix socket）、`NegativeRPCTimeout`（本机计时环境问题），与本次改动无关。

---

