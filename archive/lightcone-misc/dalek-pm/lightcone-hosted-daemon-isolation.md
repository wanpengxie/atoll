# lightcone 托管 Runtime 资源隔离设计

> 所属设计文档索引：[lightcone-redesign.md](./lightcone-redesign.md)
> 状态：方案方向已定，工程实现暂缓
> 最后更新：2026-04-20

---

## 决策结论

托管版 runtime 的长期方向采用 **K8S + sandboxed runtime**。

核心结论：
- lightcone core 不依赖 K8S，保持 runtime-backend agnostic。
- K8S 只作为 hosted execution plane，不进入核心 Context Governance 架构。
- Hosted Sandbox Manager 通过 K8S API 管理 runtime。
- K8S 负责 orchestration，gVisor / Kata / Firecracker-based RuntimeClass 负责隔离。
- nsjail 降级为 local/dev 或 lightweight backend，不作为 hosted production 主方案。
- 该方案后续再实现，当前阶段暂缓工程落地。

一句话：

> 用 K8S 管平台，用 gVisor / Kata / Firecracker 管隔离，用 lightcone 管 context 和 agent 协作。

---

## 场景判断

托管版需要支持用户通过各种 CLI 工具调用模型、操作目录、使用浏览器、发送请求：
- Claude / Codex / Kimi 等 CLI；
- git / npm / pip / curl 等开发工具；
- workspace 文件读写；
- browser automation；
- 外部网络请求；
- provider API key / browser cookies / agent token 等敏感凭证。

这不是单进程 jailed process，而是 **托管多租户 AI coding workspace**。

因此不适合用 nsjail 自研一套 image / volume / lifecycle / dependency / browser / logs / quota 管理体系。

---

## 威胁模型

假设每一个 agent 的 prompt 都可能被污染，agent 会主动尝试：
- 读取其他租户文件；
- 枚举宿主机进程；
- 读取环境变量和 token；
- 访问 cloud metadata service；
- 扫描内网服务；
- 访问 DB / internal API；
- 读取浏览器 cookies；
- 大量写盘或 fork bomb；
- 利用 CLI / browser exfiltrate 数据。

平台必须保证：
- User A 的 hosted runtime 不能访问 User B 的数据；
- runtime pod 不能访问 lightcone DB；
- runtime pod 不能访问 K8S API；
- runtime pod 不能访问 internal admin API；
- runtime pod 不能访问其他 tenant namespace / workspace；
- runtime pod 只能访问 agent-scoped API endpoint 和授权外网。

---

## 架构边界

```
Control Plane
  ├── lightcone main server
  ├── Context Assembly
  ├── Orchestrator Scheduler
  ├── Hosted Sandbox Manager
  └── DB / Secret Manager / Object Storage

Runtime Plane
  ├── user hosted daemon
  ├── CLI tools
  ├── browser runtime
  ├── workspace volume
  └── egress proxy
```

原则：
- lightcone main server 是可信 control plane。
- hosted daemon / CLI / browser 是不可信 runtime plane。
- 两者必须隔离。
- Context Boundary 是 Team。
- Runtime Isolation Boundary 是 tenant / user / hosted workspace。

不要混淆：

```
Team
  = context 隔离和共享边界

tenant / user / hosted workspace
  = runtime 资源隔离边界
```

---

## 推荐部署形态

### 阶段一：同集群隔离

```
K8S Cluster
  ├── namespace: lightcone-system
  │     ├── lightcone-server
  │     └── hosted-sandbox-manager
  │
  ├── namespace: runtime-user-a
  │     └── hosted daemon / agent runtime pod
  │
  └── namespace: runtime-user-b
        └── hosted daemon / agent runtime pod
```

要求：
- control namespace 和 runtime namespace 强隔离；
- runtime namespace 默认 deny 网络；
- runtime pod 使用 sandboxed RuntimeClass；
- runtime workload 最好跑独立 node pool；
- Sandbox Manager 使用最小 RBAC；
- runtime pod 只能访问 agent-scoped endpoint。

### 阶段二：控制面与运行时集群分离

```
Control Plane Cluster
  ├── lightcone-server
  └── hosted-sandbox-manager

Runtime Cluster(s)
  ├── user runtime namespaces
  ├── sandboxed RuntimeClass
  ├── workspace PVCs
  └── egress proxy
```

长期更推荐该形态，安全边界更清楚，runtime cluster 可独立扩容和替换。

---

## K8S Runtime 模型

```
HostedRuntimeSandbox
  = Runtime Image
  + Runtime Profile
  + Workspace Volume
  + Secret Broker
  + Egress Policy
```

映射：
- Runtime Image → OCI container image。
- Runtime Profile → RuntimeClass + PodSpec + ResourceQuota + NetworkPolicy。
- User Workspace → PVC / ephemeral volume。
- Secret Broker → K8S Secret / External Secret / sidecar broker。
- Egress Policy → NetworkPolicy + egress proxy。
- Browser Capability → same pod container or sidecar。

RuntimeClass 选择：
- gVisor：启动较快，隔离强于裸 Docker。
- Kata：microVM 隔离，边界更强，开销更大。
- Firecracker-based runtime：长期高安全方向。

---

## 五层隔离

### Layer 1：进程隔离

使用 K8S Pod + sandboxed RuntimeClass 隔离运行时进程。

裸 Docker 不是强安全边界；必须通过 gVisor / Kata / Firecracker-based runtime 增强隔离。

### Layer 2：文件系统隔离

每个 tenant / user / hosted workspace 拥有独立 workspace volume。

原则：
- runtime pod 只挂载当前 workspace；
- 不挂载宿主机敏感路径；
- 不挂载 Docker socket；
- 不挂载 control plane service account；
- persistent workspace 需要 quota；
- ephemeral workspace 可在任务完成后清理。

### Layer 3：网络隔离

默认 deny。

禁止：
- cloud metadata service；
- 内网段；
- DB；
- internal admin API；
- K8S API；
- 其他 tenant namespace / workspace；
- 未授权端口扫描。

允许：
- agent-scoped lightcone endpoint；
- 模型供应商 API；
- 包管理源；
- GitHub / Git provider；
- 用户授权公网目标。

网络出口应经过 egress proxy，便于审计和策略控制。

### Layer 4：资源隔离

使用：
- ResourceQuota；
- LimitRange；
- Pod resource requests / limits；
- PVC quota；
- idle shutdown；
- warm pool；
- runtime usage audit。

必须防止单个租户耗尽 CPU、内存、磁盘、网络和并发 runtime。

### Layer 5：凭证隔离

不把全量长期 secret 放进环境变量。

使用 Secret Broker：
- 按 user / tool / task scope 发放 short-lived secret；
- agent 只持有 per-agent / per-tool token；
- secret 访问必须审计；
- CLI provider key 不互相共享；
- browser cookies 与 workspace scope 绑定。

---

## 产品路径影响分级

### 不可用影响，必须先设计清楚

这些问题没解决，托管版不能上线：
- tenant 隔离失败；
- secret 泄露；
- egress control 缺失；
- workspace 数据丢失或串租户；
- CLI / browser runtime 兼容性不足；
- runtime 冷启动过慢；
- cost / quota / idle shutdown 缺失；
- audit / observability 缺失。

### 重大影响，但可在工程阶段解决

- K8S RBAC；
- RuntimeClass 选择；
- node pool 策略；
- ingress / WebSocket；
- image version；
- PVC 性能；
- autoscaling；
- logging pipeline；
- upgrade strategy。

### 轻微影响

- YAML / Helm 管理；
- 本地 dev 与线上差异；
- CI 镜像构建；
- K8S 学习成本；
- runtime profile 配置复杂度。

---

## 暂缓原因

当前阶段的核心工作仍是：
- Context Governance；
- Goal State；
- Context Assembly；
- Orchestrator / Worker 协作模型；
- Human context ownership；
- Team context boundary。

K8S hosted runtime 是重要的托管版基础设施，但它不是 lightcone core 的第一性能力。过早实现会把产品路径拖入基础设施建设。

因此：
- 方案记录并冻结方向；
- 不在当前阶段实现；
- 后续进入托管版 runtime 开发时，再基于本文档拆解工程设计。

---

## 后续进入实现前必须补齐

- 选择 RuntimeClass：gVisor / Kata / Firecracker-based runtime；
- 定义 Runtime Image 版本策略；
- 定义 Runtime Profile；
- 定义 Secret Broker；
- 定义 egress proxy；
- 定义 workspace PVC / ephemeral volume 策略；
- 定义 cold start / warm pool / idle shutdown；
- 定义 runtime audit event；
- 定义 Sandbox Manager RBAC；
- 定义 control plane / runtime plane 网络边界。

---

## 参考方向

- gVisor / runsc；
- Kata Containers；
- Firecracker-based runtime；
- K8S RuntimeClass；
- K8S NetworkPolicy；
- K8S ResourceQuota / LimitRange；
- External Secrets / Secret Store CSI。

