// src/components/StepCompleted.jsx
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { CheckCircle2 } from "lucide-react"
import { useI18n } from "@/hooks/useI18n"

function StepCompleted({ invoice }) {
  const { t } = useI18n()

  const confirmingProgress = invoice?.payment?.confirm_progress || {}
  const progress = confirmingProgress.progress || 0
  const hasConfirmedCount = confirmingProgress.has_confirmed_count || 0
  const needConfirmedCount = confirmingProgress.need_confirmed_count || 0

  return (
    <div className="space-y-4 animate-in fade-in-0 slide-in-from-bottom-2 duration-300">
      <Card>
        <CardContent className="pt-6 pb-6">
          {/* Celebration header */}
          <div className="text-center mb-6">
            {/* Confetti dots */}
            <div className="flex justify-center gap-2 mb-4">
              {[
                { color: "bg-orange-400", delay: "delay-75" },
                { color: "bg-amber-300", delay: "delay-100" },
                { color: "bg-emerald-400", delay: "delay-150" },
                { color: "bg-blue-400", delay: "delay-75" },
                { color: "bg-orange-300", delay: "delay-100" },
                { color: "bg-emerald-300", delay: "delay-200" },
              ].map((dot, i) => (
                <div
                  key={i}
                  className={`w-2 h-2 rounded-full ${dot.color} animate-in fade-in-0 zoom-in-50 duration-500 ${dot.delay}`}
                />
              ))}
            </div>

            {/* Checkmark */}
            <div className="w-16 h-16 mx-auto mb-4 bg-emerald-500/10 border-2 border-emerald-500/30 rounded-full flex items-center justify-center shadow-[0_0_32px_rgba(16,185,129,0.25)] animate-in zoom-in-50 duration-500">
              <CheckCircle2 className="w-8 h-8 text-emerald-400" />
            </div>

            <h2 className="text-xl font-bold text-card-foreground mb-1">
              {t("payment.paymentCompleted") || "支付成功！"}
            </h2>
            <p className="text-sm text-slate-500">
              {t("confirmation.transactionConfirmed") || "区块链交易已确认"}
            </p>
          </div>

          {/* Block confirmation progress */}
          <div className="bg-emerald-500/[0.06] border border-emerald-500/15 rounded-xl p-4 mb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-semibold text-emerald-400">
                {t("confirmation.blockConfirmation") || "区块确认"}
              </span>
              <span className="text-sm font-bold text-emerald-300 font-mono tabular-nums">
                {progress}%
              </span>
            </div>
            <div className="relative h-2 w-full overflow-hidden rounded-full bg-emerald-900/40">
              <div
                className="h-full bg-gradient-to-r from-emerald-500 to-emerald-400 transition-all duration-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]"
                style={{ width: `${progress}%` }}
              />
            </div>
            <div className="flex justify-between mt-2 text-xs text-emerald-700">
              <span>{t("confirmation.confirmed") || "已确认"} {hasConfirmedCount} {t("confirmation.blocks") || "区块"}</span>
              <span>{t("confirmation.needs") || "需要"} {needConfirmedCount} {t("confirmation.blocks") || "区块"}</span>
            </div>
          </div>

          {/* Amount summary */}
          <div className="bg-background/40 dark:bg-white/[0.03] border border-border rounded-xl p-4 mb-4">
            <div className="flex justify-between items-center">
              <div>
                <div className="text-xs text-slate-500 mb-1">{t("invoice.amountDue") || "实付金额"}</div>
                <div className="font-mono font-bold text-card-foreground tabular-nums text-lg">
                  {invoice?.pay_amount} {invoice?.crypto}
                </div>
              </div>
              <div className="text-right">
                <div className="text-xs text-slate-500 mb-1">{invoice?.currency}</div>
                <div className="font-bold text-emerald-400 text-lg">{invoice?.amount}</div>
              </div>
            </div>
          </div>

          {/* Transaction hash */}
          {invoice?.payment?.hash && (
            <div className="space-y-2 mb-4">
              <span className="text-xs font-semibold text-slate-600 uppercase tracking-wide">
                {t("payment.transactionHash") || "交易哈希"}
              </span>
              <code className="block break-all bg-background/40 dark:bg-white/[0.03] border border-border rounded-xl p-3 text-xs font-mono text-slate-500 leading-relaxed">
                {invoice.payment.hash}
              </code>
            </div>
          )}

          {/* Return to merchant */}
          {invoice?.return_url && (
            <Button
              onClick={() => window.open(invoice.return_url, "_blank")}
              className="w-full bg-emerald-500 hover:bg-emerald-400 text-white font-semibold shadow-[0_0_24px_rgba(16,185,129,0.25)] hover:shadow-[0_0_32px_rgba(16,185,129,0.35)] transition-all duration-200 cursor-pointer"
            >
              {t("payment.returnToMerchant") || "返回商户"}
            </Button>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export default StepCompleted
