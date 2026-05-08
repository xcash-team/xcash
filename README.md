# Xcash

企业级开源加密货币支付网关 —— 专注链上价值流通

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/)
[![Django](https://img.shields.io/badge/django-5.2-green.svg)](https://www.djangoproject.com/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-18-blue.svg)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/redis-8-red.svg)](https://redis.io/)
[![React](https://img.shields.io/badge/react-19-blue.svg)](https://react.dev/)

Xcash 是一个面向商家的加密货币金融基础设施。支持 EVM 兼容链和 Bitcoin、Tron 等，
提供 支付、充值、提币、自动归集等完整的加密货币金融网关能力。
完全自托管，资产安全永远是第一驱动力。

## 核心功能
- 支付收款；买家付款直达您的收款地址，平台不托管资金。
- 充提币；允许您的平台用户像交易所用户一样充值和提现代币。

## 适用场景

- 电商、游戏、SaaS 等平台接入加密货币支付
- 交易所或钱包服务商需要充提币基础设施
- 跨境业务使用稳定币（USDT/USDC）进行结算
- 企业内部数字资产管理与链上资金调度

## 链支持

| 功能 | ETH | BTC | BNB Chain | Arbitrum | Base | Tron | Polygon | Avalanche | Optimism | 其他 EVM |
|:----:|:---:|:---:|:---------:|:--------:|:----:|:----:|:-------:|:---------:|:--------:|:--------:|
| 支付 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 充值 | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| 提币 | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |

> 所有 EVM 兼容链均可通过后台配置接入，无需额外开发。

## 代币支持

EVM 链支持任意 ERC-20 代币，只需在后台添加代币合约地址即可启用。
Tron 链当前仅支持支付功能，且仅支持 USDT。

## 截图

![管理后台 Dashboard](xcash/static/xcash-dashboard.jpeg)

## 特性

- 🔗 **多链支持** — 支持所有 EVM 兼容链（Ethereum、BSC、Polygon 等）和 Bitcoin、Tron，更多链即将到来
- 💎 **资金直达** — 支付场景下，买家付款直接转入商户自己的钱包地址，资金全程不经过第三方，零信任风险
- 🔐 **完全自托管** — 基于 BIP44 HD 钱包派生地址，账户由你自己掌控，不依赖任何第三方托管
- 🚀 **一键部署** — 提供环境初始化脚本和 Docker Compose，几条命令即可启动完整服务
- 💰 **完整支付网关** — 支付收款、充值、提币、自动归集、Webhook 通知，覆盖加密货币收付款全链路
- 📊 **强大的管理后台** — 内置经营看板、多维度数据统计、开箱即用的运营管理能力，让你对业务全局一目了然

## 云服务

如果你不想自己部署和维护，可以直接使用官方托管版本：

👉 **[xca.sh](https://xca.sh)** — 开箱即用，免部署，持续更新

## 架构

```mermaid
graph LR
    Buyer["买家<br/>支付页面"]
    Merchant["商家系统"]

    subgraph Xcash
        API["Xcash API"]
        Worker["Xcash Worker<br/>交易监听 · 归集 · 状态流转"]
        Signer["Xcash Signer<br/>独立签名服务"]
        Webhook["Xcash Webhook<br/>异步通知"]
    end

    Blockchain["区块链网络<br/>EVM · Bitcoin"]

    Buyer -->|发起支付| API
    Merchant <-->|创建账单 / 查询| API
    API <--> Worker
    API <--> Signer
    Worker <-->|监听 · 广播| Blockchain
    Webhook -->|推送事件| Merchant
```

## 部署前的准备

在开始部署之前，请确保具备以下条件：

### 1. 服务器

- 最低配置：1 核 CPU、2 GB 内存（`PERFORMANCE=low`）
- 推荐配置：4 核 CPU、8 GB 内存（`PERFORMANCE=middle`）
- 高性能配置：8 核 CPU、16 GB 内存（`PERFORMANCE=high`）
- 操作系统：Linux（推荐 Ubuntu 22.04+ / Debian 12+）
- 安装 Docker 和 Docker Compose

`PERFORMANCE` 不设置时默认使用 `low`。部署规模较小时建议先从 `low` 启动，只开启实际需要的链与功能；业务量、链扫描压力或并发请求增加后再切换到 `middle` 或 `high`。

### 2. 域名

准备一个已解析到服务器 IP 的域名，用于访问管理后台和 API 接口。后续通过反向代理（Nginx/Caddy）处理 HTTPS 证书。

### 3. 链 RPC 地址

准备好所需公链的 RPC 访问凭证，网关需要与区块链节点通信才能正常工作：

- **EVM 链**（Ethereum、BSC、Arbitrum、Base、Polygon 等）：各链的 RPC 节点地址。推荐使用 [QuickNode](https://www.quicknode.com/)、[Alchemy](https://www.alchemy.com/) 或 [Infura](https://www.infura.io/) 等节点服务商获取。
- **Tron 链**：需要在 [TronGrid](https://www.trongrid.io/) 注册并获取 API Key。

启动服务后，登录管理后台进入 **链管理** 页面填写以上 RPC 配置。

> **重要：充提币功能需要手动开启原生币扫描**
> 
> 系统默认关闭了 EVM 原生币扫描。**充值、提币功能依赖原生币扫描才能正常工作**，因为 Gas 分发、归集等链上交易必须通过原生币扫描来感知和确认。
> 
> 开启方法：
> 1. 登录管理后台，进入 **系统->平台参数** 页面
> 2. 开启 **「开启 EVM 原生币扫描」** 开关
> 
> **⚠️ 充提币功能需要 EVM 链的 RPC 节点必须支持高频调用。** 原生币扫描需要持续轮询链上区块，服务商提供的免费套餐通常有严格的请求次数限制（如每天几千次），完全无法满足系统的扫描需求。请务必使用支持高频调用的付费 RPC 套餐（如 QuickNode、Alchemy 的 Growth/Scale 套餐，或自建全节点）。

## 快速开始

### 环境要求

- Docker 和 Docker Compose

### 1. 克隆项目

```bash
git clone https://github.com/xca-sh/xcash.git
cd xcash
```

### 2. 初始化环境变量

```bash
./scripts/init_env.sh
```

自动生成 `.env` 文件并填充所有必需的密钥（Django Secret、数据库密码、Signer 密钥等）。   
请妥善保存并保密此 `.env` 文件，如若丢失将失去系统内资产。

### 3. 设置访问域名

编辑 `.env` 设置 `SITE_DOMAIN` 为你的域名：

```env
SITE_DOMAIN=xcash.example.com
```

请确保该域名的 DNS 已解析到你的服务器 IP，并配置好反向代理（如 Nginx/Caddy）将流量转发至 `http://localhost:6688`，由反向代理处理 HTTPS 证书。启动后通过 `https://你的域名` 访问。内网部署同样建议配置内部域名。

### 4. 启动服务

```bash
docker compose up -d
```

首次启动时，如果数据库内还没有任何管理员账号，系统会自动创建默认后台账号：

```text
username: admin
password: Admin@123456
```

首次登录后台后，系统会继续引导你绑定 OTP；完成登录后请立即修改默认密码。

### 5. 配置链 RPC

系统已预置主流链的基础信息，但 **RPC 节点地址需要你自行填写**，网关才能与区块链通信。

登录管理后台，进入 **链管理** 页面，为你需要使用的链填写 RPC 地址。推荐使用 [QuickNode](https://www.quicknode.com/)、[Alchemy](https://www.alchemy.com/) 或 [Infura](https://www.infura.io/) 等节点服务商。

### 6. 更新项目

拉取最新代码后重新构建镜像并重启容器。

```bash
git pull
docker compose down --remove-orphans
docker compose up -d --build
```

## API 对接

部署完成后，参考 [API 对接文档](API.md) 接入支付、充币、提币和 Webhook 回调。

## 技术栈

- **后端**：Django 5.2 + Django REST Framework
- **任务队列**：Celery + Redis
- **数据库**：PostgreSQL
- **区块链交互**：web3.py（EVM）、bit（Bitcoin）
- **钱包派生**：BIP44 HD 钱包（bip-utils）
- **前端支付页**：React 19 + Vite + Tailwind CSS
- **部署**：Docker Compose

## 路线图

- [ ] Solana 链支持
- [x] TRON 链支持
- [ ] 完善文档站

## 商业支持

如果你在部署或使用过程中需要专业协助，欢迎联系我们获取技术支持服务。

📮 联系邮箱：tech@xca.sh

## 贡献

欢迎提交 Issue 和 Pull Request。

## License

[MIT](LICENSE)
