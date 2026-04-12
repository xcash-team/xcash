# Xcash API 对接文档

Base URL: `https://pay.xca.sh/v1/`

---

## 认证机制

除特别标注的公开接口外，所有 API 请求都需要 HMAC-SHA256 签名认证。

### 获取凭证

在 Xcash 管理后台创建项目后，系统自动生成：

| 字段 | 格式 | 说明 |
|------|------|------|
| `appid` | `XC-` + 8位字符 | 项目唯一标识，如 `XC-A3BK7NMG` |
| `hmac_key` | 32位字符串 | HMAC 签名密钥 |

凭证仅在管理后台可见，无 API 接口获取。

### 项目就绪条件

项目必须同时满足以下条件才能调用 API：

1. 已配置 IP 白名单（支持 CIDR，`*` 表示允许所有 IP）
2. 已配置 Webhook URL
3. 已启用 Webhook 通知
4. 项目状态为启用
5. 至少配置了一个收款地址（Invoice 场景）

### 请求头

所有需签名的请求必须携带以下 Header：

```
XC-Appid:     {appid}
XC-Timestamp: {unix_timestamp}
XC-Nonce:     {uuid}
XC-Signature: {hmac_signature}
Content-Type: application/json
```

| Header | 说明 |
|--------|------|
| `XC-Appid` | 项目 AppID |
| `XC-Timestamp` | 当前 Unix 时间戳（秒），与服务器时间差不超过 ±300 秒 |
| `XC-Nonce` | 唯一随机字符串（建议 UUID），同一 AppID 下 300 秒内不可重复 |
| `XC-Signature` | HMAC-SHA256 签名（见下方计算方式） |

### 签名计算

```
message   = {nonce} + {timestamp} + {request_body}
signature = HMAC-SHA256(message, hmac_key).hexdigest()
```

- `nonce`：`XC-Nonce` Header 的值
- `timestamp`：`XC-Timestamp` Header 的值（字符串形式）
- `request_body`：HTTP 请求体原始内容（GET 请求为空字符串 `""`）
- 使用 `hmac_key` 作为密钥，SHA-256 作为哈希算法，输出小写十六进制字符串

### 签名示例（Python）

```python
import hmac
import hashlib
import json
import time
import uuid

appid = "XC-A3BK7NMG"
hmac_key = "your_32_char_hmac_key_here"

timestamp = str(int(time.time()))
nonce = str(uuid.uuid4())
body = json.dumps({"out_no": "order-001", "title": "Premium Plan", "currency": "USD", "amount": "29.99"})

message = nonce + timestamp + body
signature = hmac.new(
    hmac_key.encode(),
    message.encode(),
    hashlib.sha256
).hexdigest()

headers = {
    "XC-Appid": appid,
    "XC-Timestamp": timestamp,
    "XC-Nonce": nonce,
    "XC-Signature": signature,
    "Content-Type": "application/json",
}
```

### 签名示例（Node.js）

```javascript
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');

const appid = 'XC-A3BK7NMG';
const hmacKey = 'your_32_char_hmac_key_here';

const timestamp = Math.floor(Date.now() / 1000).toString();
const nonce = uuidv4();
const body = JSON.stringify({ out_no: 'order-001', title: 'Premium Plan', currency: 'USD', amount: '29.99' });

const message = nonce + timestamp + body;
const signature = crypto.createHmac('sha256', hmacKey).update(message).digest('hex');

const headers = {
    'XC-Appid': appid,
    'XC-Timestamp': timestamp,
    'XC-Nonce': nonce,
    'XC-Signature': signature,
    'Content-Type': 'application/json',
};
```

---

## 统一响应格式

### 成功响应

直接返回业务数据 JSON，HTTP 状态码 `200`。

### 错误响应

```json
{
  "code": "1001",
  "message": "AppID无效",
  "detail": ""
}
```

---

## 接口列表

| 方法 | 路径 | 说明 | 签名 |
|------|------|------|------|
| POST | `/v1/invoice` | 创建账单 | 需要 |
| GET | `/v1/invoice/{sys_no}` | 查询账单 | 不需要 |
| POST | `/v1/invoice/{sys_no}/select-method` | 选择支付方式 | 不需要 |
| GET | `/v1/deposit/address` | 获取充币地址 | 需要 |
| POST | `/v1/withdrawal` | 发起提币 | 需要 |

---

## 创建账单

**POST** `/v1/invoice`

**需要签名**

创建一个加密货币支付账单。买家可通过返回的 `pay_url` 页面完成支付，也可通过 API 选择支付方式后直接转账。

### 请求参数

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `out_no` | string | 是 | 商户订单号，最长 32 位，同一项目下唯一 |
| `title` | string | 是 | 账单标题，最长 32 位 |
| `currency` | string | 是 | 计价币种，支持法币（如 `USD`）或加密货币（如 `USDT`） |
| `amount` | string | 是 | 金额，范围 0.00000001 ~ 1000000 |
| `duration` | integer | 否 | 支付有效期（分钟），范围 5~60，默认 10 |
| `methods` | object | 否 | 限定支付方式，格式 `{"币种": ["链码"]}` |
| `email` | string | 否 | 买家邮箱 |
| `redirect_url` | string | 否 | 支付完成后跳转地址 |

**methods 说明：**

- 不传：使用项目已配置的全部支付方式
- 指定：仅允许指定的币种+链组合，如 `{"USDT": ["ethereum-mainnet", "tron-mainnet"], "ETH": ["ethereum-mainnet"]}`
- 当 `currency` 为加密货币时，`methods` 会被自动限定为该币种

### 请求示例

```json
{
  "out_no": "order-20240101-001",
  "title": "Premium Plan",
  "currency": "USD",
  "amount": "29.99",
  "duration": 15,
  "methods": {
    "USDT": ["ethereum-mainnet", "tron-mainnet"],
    "ETH": ["ethereum-mainnet"]
  },
  "redirect_url": "https://example.com/payment/success"
}
```

### 响应示例

```json
{
  "appid": "XC-A3BK7NMG",
  "sys_no": "INV-xxxxxxxx",
  "out_no": "order-20240101-001",
  "title": "Premium Plan",
  "currency": "USD",
  "amount": "29.99",
  "methods": {
    "USDT": ["ethereum-mainnet", "tron-mainnet"],
    "ETH": ["ethereum-mainnet"]
  },
  "chain": null,
  "crypto": null,
  "crypto_address": null,
  "pay_address": null,
  "pay_amount": null,
  "pay_url": "https://pay.xca.sh/payment/INV-xxxxxxxx",
  "started_at": null,
  "created_at": "2024-01-01T00:00:00Z",
  "expires_at": "2024-01-01T00:15:00Z",
  "redirect_url": "https://example.com/payment/success",
  "payment": null,
  "status": "waiting"
}
```

创建后 `chain`、`crypto`、`pay_address`、`pay_amount` 为空，需要买家选择支付方式后才会分配。

### 限流

256 次/分钟（默认全局限流）

---

## 查询账单

**GET** `/v1/invoice/{sys_no}`

**无需签名** — 此接口为公开接口，买家可直接访问。

### 路径参数

| 字段 | 说明 |
|------|------|
| `sys_no` | 账单系统编号，如 `INV-xxxxxxxx` |

### 响应示例

```json
{
  "sys_no": "INV-xxxxxxxx",
  "title": "Premium Plan",
  "currency": "USD",
  "amount": "29.99",
  "methods": {
    "USDT": ["ethereum-mainnet", "tron-mainnet"]
  },
  "chain": "ethereum-mainnet",
  "crypto": "USDT",
  "crypto_address": "0x1234...abcd",
  "pay_address": "0x1234...abcd",
  "pay_amount": "29.87",
  "pay_url": "https://pay.xca.sh/payment/INV-xxxxxxxx",
  "started_at": "2024-01-01T00:00:05Z",
  "created_at": "2024-01-01T00:00:00Z",
  "expires_at": "2024-01-01T00:15:00Z",
  "redirect_url": "https://example.com/payment/success",
  "payment": null,
  "status": "waiting"
}
```

> 注意：公开接口不返回 `appid` 和 `out_no` 字段。

### 限流

60 次/分钟（按 sys_no + IP 维度）

---

## 选择支付方式

**POST** `/v1/invoice/{sys_no}/select-method`

**无需签名** — 此接口由买家侧调用。

买家选择使用哪种加密货币和链进行支付，选择后系统分配收款地址和应付金额。

### 路径参数

| 字段 | 说明 |
|------|------|
| `sys_no` | 账单系统编号 |

### 请求参数

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `crypto` | string | 是 | 加密货币符号，如 `USDT` |
| `chain` | string | 是 | 链码，如 `ethereum-mainnet`、`tron-mainnet` |

### 请求示例

```json
{
  "crypto": "USDT",
  "chain": "tron-mainnet"
}
```

### 响应示例

选择成功后返回完整账单信息，此时 `pay_address` 和 `pay_amount` 已分配：

```json
{
  "appid": "XC-A3BK7NMG",
  "sys_no": "INV-xxxxxxxx",
  "out_no": "order-20240101-001",
  "title": "Premium Plan",
  "currency": "USD",
  "amount": "29.99",
  "methods": {
    "USDT": ["ethereum-mainnet", "tron-mainnet"]
  },
  "chain": "tron-mainnet",
  "crypto": "USDT",
  "crypto_address": "TXyz...1234",
  "pay_address": "TXyz...1234",
  "pay_amount": "29.87",
  "pay_url": "https://pay.xca.sh/payment/INV-xxxxxxxx",
  "started_at": "2024-01-01T00:00:05Z",
  "created_at": "2024-01-01T00:00:00Z",
  "expires_at": "2024-01-01T00:15:00Z",
  "redirect_url": "https://example.com/payment/success",
  "payment": null,
  "status": "waiting"
}
```

### 限流

10 次/分钟（按 sys_no + IP 维度）

---

## 获取充币地址

**GET** `/v1/deposit/address`

**需要签名**

为指定用户获取某条链上某种加密货币的充币地址。同一 `(uid, chain, crypto)` 组合始终返回相同地址。

### 查询参数

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `uid` | string | 是 | 用户标识，1~128 位字母数字及 `_-` |
| `chain` | string | 是 | 链码，如 `ethereum-mainnet`、`tron-mainnet` |
| `crypto` | string | 是 | 加密货币符号，如 `USDT` |

### 请求示例

```
GET /v1/deposit/address?uid=user123&chain=ethereum-mainnet&crypto=USDT
```

> GET 请求签名时，`request_body` 为空字符串 `""`。

### 响应示例

```json
{
  "deposit_address": "0xAbCd...1234"
}
```

### 限流

60 次/分钟（按 appid + IP 维度）

---

## 发起提币

**POST** `/v1/withdrawal`

**需要签名**

从项目 Vault 地址向指定地址发起提币。系统会校验余额、地址合法性、单笔/日限额。

### 请求参数

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `out_no` | string | 是 | 商户提币单号，最长 128 位，同一项目下唯一 |
| `to` | string | 是 | 收款地址（不可为平台内部地址，不可为合约地址） |
| `uid` | string | 否 | 用户标识，最长 32 位 |
| `crypto` | string | 是 | 加密货币符号，如 `USDT` |
| `chain` | string | 是 | 链码，如 `ethereum-mainnet`、`tron-mainnet` |
| `amount` | string | 是 | 提币金额，范围 0.00000001 ~ 1000000 |

### 请求示例

```json
{
  "out_no": "withdraw-20240101-001",
  "to": "0x9876...fedc",
  "uid": "user123",
  "crypto": "USDT",
  "chain": "ethereum-mainnet",
  "amount": "100"
}
```

### 响应示例

```json
{
  "sys_no": "WDR-xxxxxxxx",
  "hash": "",
  "status": "reviewing"
}
```

### 提币状态说明

| 状态 | 说明 |
|------|------|
| `reviewing` | 审核中（需管理员审批） |
| `pending` | 已审批，等待广播 |
| `confirming` | 已广播，链上确认中 |
| `completed` | 完成 |
| `rejected` | 已拒绝 |
| `failed` | 广播失败 |

### 限流

30 次/分钟（按 appid + IP 维度）

---

## Webhook 回调

当业务状态发生变化时（账单支付成功、充币到账、提币状态更新），Xcash 会向项目配置的 Webhook URL 发送 POST 请求。

### 回调请求头

```
XC-Appid:     {appid}
XC-Nonce:     {event_nonce}
XC-Timestamp: {unix_timestamp}
XC-Signature: {hmac_signature}
Content-Type: application/json
```

签名算法与 API 请求签名完全一致：

```
message   = {nonce} + {timestamp} + {request_body}
signature = HMAC-SHA256(message, hmac_key).hexdigest()
```

**商户应验证签名以确保回调来源可信。**

### 响应要求

- 返回 HTTP `200`，响应体为 `ok`（字符串）
- 非 200 或响应体不为 `ok` 视为投递失败

### 重试机制

- 5xx 错误或网络异常：自动重试，退避间隔 `2^(n+1)` 秒
- 4xx 错误：不重试
- 连续失败超限后自动禁用 Webhook

### 统一格式

所有 Webhook 回调均使用 `type` + `data` 的统一结构：

```json
{
  "type": "invoice | deposit | withdrawal",
  "data": { ... }
}
```

### 账单回调（Invoice）

当账单进入 `confirming`（开启预通知时）或 `completed` 状态时推送：

```json
{
  "type": "invoice",
  "data": {
    "sys_no": "INV-xxxxxxxx",
    "out_no": "order-20240101-001",
    "crypto": "USDT",
    "chain": "ethereum-mainnet",
    "pay_address": "0x1234...abcd",
    "pay_amount": "29.87",
    "hash": "0xabcd...1234",
    "block": 12345678
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `sys_no` | string | 系统账单号 |
| `out_no` | string | 商户订单号 |
| `crypto` | string \| null | 币种符号，未选支付方式时为 null |
| `chain` | string \| null | 链码，未选支付方式时为 null |
| `pay_address` | string \| null | 收款地址 |
| `pay_amount` | string \| null | 应付金额 |
| `hash` | string \| null | 链上交易哈希，未匹配链上交易时为 null |
| `block` | integer \| null | 区块高度，未匹配链上交易时为 null |

> **预通知条件**：`confirming` 状态的回调仅在项目开启了预通知、且账单通过 API 创建、且使用完整区块确认模式时触发。

### 充币回调（Deposit）

用户充币到账后推送：

```json
{
  "type": "deposit",
  "data": {
    "uid": "user123",
    "chain": "ethereum-mainnet",
    "block": 12345678,
    "hash": "0xabcd...1234",
    "crypto": "USDT",
    "amount": "500",
    "status": "completed"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `uid` | string \| null | 用户标识，无关联用户时为 null |
| `chain` | string | 链码 |
| `block` | integer | 区块高度 |
| `hash` | string | 链上交易哈希 |
| `crypto` | string | 币种符号 |
| `amount` | string | 充币金额 |
| `status` | string | `confirming`（开启预通知时）或 `completed` |

### 提币回调（Withdrawal）

提币状态变更时推送：

```json
{
  "type": "withdrawal",
  "data": {
    "sys_no": "WDR-xxxxxxxx",
    "out_no": "withdraw-20240101-001",
    "chain": "ethereum-mainnet",
    "hash": "0xabcd...1234",
    "amount": "100",
    "crypto": "USDT",
    "status": "completed",
    "uid": "user123"
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `sys_no` | string | 系统提币单号 |
| `out_no` | string | 商户提币单号 |
| `chain` | string | 链码 |
| `hash` | string | 链上交易哈希 |
| `amount` | string | 提币金额 |
| `crypto` | string | 币种符号 |
| `status` | string | 提币状态（见"提币状态说明"） |
| `uid` | string | 用户标识，仅在创建提币时传入了 `uid` 的情况下才出现 |

---

## 账单状态说明

| 状态 | 说明 |
|------|------|
| `waiting` | 待支付 |
| `confirming` | 链上确认中 |
| `completed` | 已完成 |
| `expired` | 已超时 |

---

## 支持的链码

| 链码 | 链名 |
|------|------|
| `ethereum-mainnet` | Ethereum |
| `bsc-mainnet` | BNB Smart Chain |
| `polygon-mainnet` | Polygon |
| `base-mainnet` | Base |
| `tron-mainnet` | TRON |
| `bitcoin-mainnet` | Bitcoin |

> 实际可用链取决于项目配置。

---

## 错误码

### 通用错误（1xxx）

| 错误码 | 说明 | HTTP 状态码 |
|--------|------|-------------|
| 1000 | 参数错误 | 400 |
| 1001 | AppID 无效 | 400 |
| 1002 | IP 禁止 | 403 |
| 1003 | 签名错误 | 403 |
| 1004 | 项目未配置 | 400 |
| 1005 | 无访问权限 | 403 |
| 1006 | 手续费不足 | 403 |
| 1007 | out_no 重复 | 400 |
| 1008 | Timestamp 未设置或过期 | 400 |
| 1009 | 请求重复（Nonce 重放） | 400 |

### 链与加密货币错误（2xxx）

| 错误码 | 说明 | HTTP 状态码 |
|--------|------|-------------|
| 2000 | 无效链 | 400 |
| 2001 | 无效加密货币 | 400 |
| 2002 | 链不支持此加密货币 | 400 |
| 2003 | 地址格式错误 | 400 |
| 2004 | 不能为合约地址 | 400 |
| 2005 | 链与加密货币设置错误 | 400 |

### 提币错误（3xxx）

| 错误码 | 说明 | HTTP 状态码 |
|--------|------|-------------|
| 3000 | 提币地址不合法 | 400 |
| 3001 | 余额不足 | 400 |
| 3002 | 链上资源不足 | 400 |
| 3004 | 超出单笔提币限额 | 400 |
| 3005 | 超出当日提币限额 | 400 |

### 充币错误（4xxx）

| 错误码 | 说明 | HTTP 状态码 |
|--------|------|-------------|
| 4000 | 无效 UID | 400 |

### 账单错误（5xxx）

| 错误码 | 说明 | HTTP 状态码 |
|--------|------|-------------|
| 5000 | 账单币种错误 | 400 |
| 5002 | 差额账单数值错误 | 400 |
| 5003 | 支付时间错误 | 400 |
| 5004 | 差额不足 | 400 |
| 5005 | 无效 sys_no | 400 |
| 5006 | 账单状态错误 | 400 |
| 5007 | 不允许的链与加密货币 | 400 |
| 5008 | 无可用支付方式 | 400 |
| 5009 | 待支付账单过多 | 400 |
| 5010 | 无效的支付方式 | 400 |
| 5011 | 账单不存在 | 400 |
| 5012 | 账单已过期 | 400 |

---

## 完整对接流程

### 收款（Invoice）

```
商户服务器                        Xcash                          买家
    |                              |                              |
    |-- POST /v1/invoice --------->|                              |
    |<-- 返回 sys_no, pay_url -----|                              |
    |                              |                              |
    |-- 将 pay_url 给买家 -------->|                              |
    |                              |<-- 买家访问 pay_url ----------|
    |                              |<-- 选择支付方式 --------------|
    |                              |-- 返回 pay_address, amount -->|
    |                              |                              |
    |                              |<-- 买家链上转账 --------------|
    |                              |                              |
    |<-- Webhook: invoice ---------|                              |
    |-- 响应 "ok" ---------------->|                              |
```

### 充币（Deposit）

```
商户服务器                        Xcash                          用户
    |                              |                              |
    |-- GET /v1/deposit/address -->|                              |
    |<-- 返回 deposit_address -----|                              |
    |                              |                              |
    |-- 展示地址给用户 ----------->|                              |
    |                              |<-- 用户链上转账 --------------|
    |                              |                              |
    |<-- Webhook: deposit ---------|                              |
    |-- 响应 "ok" ---------------->|                              |
```

### 提币（Withdrawal）

```
商户服务器                        Xcash
    |                              |
    |-- POST /v1/withdrawal ------>|
    |<-- 返回 sys_no, status ------|
    |                              |
    |                              |-- 管理员审批（如需要）
    |                              |-- 链上广播
    |                              |-- 链上确认
    |                              |
    |<-- Webhook: withdrawal -------|
    |-- 响应 "ok" ---------------->|
```
