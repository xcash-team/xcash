import { useState } from "react"
import { CheckCircle, Copy, Check } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { formatChainName } from "@/lib/format"

function PaymentProgress({ invoice }) {
  const [copiedHash, setCopiedHash] = useState(false)

  const payment = invoice?.payment
  if (!payment) {
    return null
  }

  const isConfirmed = payment.status === "已确认"

  const progress = payment.confirm_progress
  const confirmedCount = progress?.has_confirmed_count ?? 0
  const needConfirmCount = progress?.need_confirmed_count ?? 0

  const progressPercentage = (() => {
    if (!progress) return 0
    if (typeof progress.progress === "number") {
      return Math.min(progress.progress, 100)
    }
    if (progress.need_confirmed_count) {
      return Math.min(((progress.has_confirmed_count ?? 0) / progress.need_confirmed_count) * 100, 100)
    }
    return 0
  })()

  const truncateHash = (hash) => {
    if (!hash) return ""
    return `${hash.slice(0, 6)}...${hash.slice(-6)}`
  }

  const handleCopyHash = () => {
    if (!payment.hash) return

    navigator.clipboard
      .writeText(payment.hash)
      .then(() => {
        setCopiedHash(true)
        setTimeout(() => setCopiedHash(false), 2000)
      })
      .catch((err) => {
        console.error("复制失败:", err)
      })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg flex items-center gap-2">
          {isConfirmed ? <CheckCircle className="h-5 w-5" /> : <div className="h-5 w-5 rounded-full border-2 border-slate-300 border-t-slate-600 animate-spin" />}
          <span>{isConfirmed ? "支付完成" : "支付确认中"}</span>
        </CardTitle>
        <CardDescription className="flex items-center justify-between gap-3 text-sm">
          <div className="flex items-center gap-2">
            <span>交易:</span>
            <code className="text-xs font-mono">{truncateHash(payment.hash)}</code>
          </div>
          <Button size="sm" variant="ghost" onClick={handleCopyHash} className="h-7 px-2">
            {copiedHash ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          </Button>
        </CardDescription>
      </CardHeader>

      <CardContent className="space-y-4">
        {/* 交易信息 */}
        <div className="grid grid-cols-2 gap-3 text-sm">
          <div>
            <div className="text-xs text-slate-500">支付金额</div>
            <div className="font-medium mt-0.5">{payment.amount} {payment.crypto}</div>
          </div>
          <div>
            <div className="text-xs text-slate-500">网络</div>
            <div className="font-medium mt-0.5">{formatChainName(payment.chain)}</div>
          </div>
          <div>
            <div className="text-xs text-slate-500">区块高度</div>
            <div className="font-medium mt-0.5">#{payment.block?.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-xs text-slate-500">支付时间</div>
            <div className="font-medium mt-0.5 text-xs">{new Date(payment.datetime).toLocaleString()}</div>
          </div>
        </div>

        {/* 确认进度 */}
        {progress && !isConfirmed && (
          <div className="bg-slate-50 rounded-lg p-4 space-y-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-slate-600">区块确认</span>
              <span className="font-medium">{confirmedCount} / {needConfirmCount}</span>
            </div>
            <Progress
              value={progressPercentage}
              className="h-2"
            />
            <div className="text-xs text-slate-500 text-right">
              {progressPercentage.toFixed(0)}% 完成
            </div>
          </div>
        )}

        {/* 支付完成 */}
        {isConfirmed && (
          <div className="bg-slate-50 rounded-lg p-6 text-center space-y-4">
            <div className="mx-auto w-12 h-12 bg-slate-900 rounded-full flex items-center justify-center">
              <CheckCircle className="h-6 w-6 text-white" />
            </div>
            <div>
              <h3 className="font-semibold text-slate-900 mb-1">支付确认完成</h3>
              <p className="text-sm text-slate-600">交易已成功完成并获得网络确认</p>
            </div>

            {invoice.return_url && (
              <Button
                onClick={() => window.open(invoice.return_url, "_blank")}
                className="w-full"
              >
                返回商户
              </Button>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export default PaymentProgress
