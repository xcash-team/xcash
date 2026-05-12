import { useEffect, useState } from "react"
import QRCode from "qrcode"
import { Copy, Check, Clock, CheckCircle2, ArrowLeft } from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { getCryptoIconUrl, getChainIconUrl, getChainDisplayName } from "@/lib/cryptoIcons"
import { useI18n } from "@/hooks/useI18n"

function PaymentAddress({ invoice, onReset }) {
  const { t } = useI18n()
  const [qrCodeUrl, setQrCodeUrl] = useState("")
  const [copiedField, setCopiedField] = useState("")

  const hasPayment = Boolean(invoice?.payment)
  const isConfirming = invoice?.status === "confirming"
  const isCompleted = invoice?.status === "completed"
  const confirmingProgress = invoice?.payment?.confirm_progress || {}
  const progress = confirmingProgress.progress || 0
  const hasConfirmedCount = confirmingProgress.has_confirmed_count || 0
  const needConfirmedCount = confirmingProgress.need_confirmed_count || 0

  useEffect(() => {
    if (!invoice?.pay_address) {
      return
    }

    QRCode.toDataURL(invoice.pay_address, {
      width: 256,
      margin: 2,
      color: { dark: "#0A0F1E", light: "#ffffff" },
    })
      .then(setQrCodeUrl)
      .catch((err) => {
        console.error("QR code generation failed:", err)
      })
  }, [invoice?.pay_address])

  if (!invoice?.pay_address) {
    return null
  }

  const handleCopy = (value, field) => {
    navigator.clipboard
      .writeText(value)
      .then(() => {
        setCopiedField(field)
        setTimeout(() => setCopiedField(""), 2000)
      })
      .catch((err) => {
        console.error("Copy failed:", err)
      })
  }

  const CopyButton = ({ value, field }) => (
    <button
      onClick={() => handleCopy(value, field)}
      className={`inline-flex items-center justify-center h-7 w-7 rounded-lg transition-all duration-200 cursor-pointer ${
        copiedField === field
          ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/25"
          : "bg-white/[0.05] text-slate-500 border border-white/[0.08] hover:bg-white/[0.1] hover:text-slate-300 hover:border-white/[0.15]"
      }`}
    >
      {copiedField === field ? (
        <Check className="h-3.5 w-3.5" />
      ) : (
        <Copy className="h-3.5 w-3.5" />
      )}
    </button>
  )

  return (
    <Card>
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <div className="flex-1">
            <CardTitle className="text-white text-lg">
              {isCompleted
                ? t("payment.paymentCompleted")
                : isConfirming
                  ? t("payment.paymentConfirming")
                  : t("payment.paymentInfo")}
            </CardTitle>
            <CardDescription className="text-sm mt-1">
              {isCompleted ? (
                <span className="text-emerald-400 font-medium flex items-center gap-1.5">
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  {t("confirmation.transactionConfirmed")}
                </span>
              ) : isConfirming ? (
                <span className="text-amber-400 font-medium flex items-center gap-1.5">
                  <Clock className="h-3.5 w-3.5" />
                  {t("confirmation.waitingConfirmation")}
                </span>
              ) : (
                <span className="text-slate-500">
                  {t("payment.transferTo")}{" "}
                  <span className="font-mono font-medium text-orange-400">
                    {invoice.pay_amount} {invoice.crypto}
                  </span>{" "}
                  {t("payment.toAddress")}
                </span>
              )}
            </CardDescription>
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-5">
        {/* Confirmation progress */}
        {hasPayment && (isConfirming || isCompleted) && (
          <div className="space-y-3">
            <div className="bg-emerald-500/[0.06] border border-emerald-500/15 rounded-xl p-5">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-semibold text-emerald-400">
                  {t("confirmation.blockConfirmation")}
                </span>
                <span className="text-lg font-bold text-emerald-300 font-mono tabular-nums">
                  {progress}%
                </span>
              </div>
              <div className="relative h-2 w-full overflow-hidden rounded-full bg-emerald-900/40">
                <div
                  className="h-full bg-gradient-to-r from-emerald-500 to-emerald-400 transition-all duration-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]"
                  style={{ width: `${progress}%` }}
                />
              </div>
              <div className="flex items-center justify-between mt-3 text-xs text-emerald-700">
                <span>
                  {t("confirmation.confirmed")} {hasConfirmedCount} {t("confirmation.blocks")}
                </span>
                <span>
                  {t("confirmation.needs")} {needConfirmedCount} {t("confirmation.blocks")}
                </span>
              </div>
            </div>

            {/* Transaction hash */}
            {invoice.payment.hash && (
              <div className="space-y-2">
                <span className="text-xs font-semibold text-slate-600 uppercase tracking-wide">
                  {t("payment.transactionHash")}
                </span>
                <code className="block break-all bg-white/[0.03] border border-white/[0.06] rounded-xl p-3 text-xs font-mono text-slate-500 leading-relaxed">
                  {invoice.payment.hash}
                </code>
              </div>
            )}
          </div>
        )}

        {/* QR Code — only when not yet paid */}
        {!hasPayment && (
          <div className="flex justify-center">
            {qrCodeUrl ? (
              <div className="inline-flex flex-col items-center gap-3">
                <div className="bg-white rounded-2xl p-4 shadow-[0_0_50px_rgba(249,115,22,0.08)]">
                  <img
                    src={qrCodeUrl}
                    alt={t("payment.scanQRCode")}
                    className="h-40 w-40"
                  />
                </div>
                <p className="text-xs font-medium text-slate-600">{t("payment.scanQRCode")}</p>
              </div>
            ) : (
              <div className="flex h-48 w-48 items-center justify-center bg-white/[0.03] rounded-2xl border border-white/[0.06]">
                <div className="h-6 w-6 animate-spin rounded-full border-2 border-orange-500/30 border-t-orange-500" />
              </div>
            )}
          </div>
        )}

        {/* Amount and network */}
        <div className="grid grid-cols-2 gap-3">
          <div className="bg-white/[0.03] rounded-xl p-3 border border-white/[0.06]">
            <div className="flex items-center justify-between mb-2 h-6">
              <span className="text-xs font-medium text-slate-500">
                {t("payment.paymentAmount")}
              </span>
              <CopyButton value={invoice.pay_amount} field="amount" />
            </div>
            <div className="flex items-center gap-2">
              <img
                src={getCryptoIconUrl(invoice.crypto)}
                alt={invoice.crypto}
                className="w-5 h-5 rounded-full flex-shrink-0"
                onError={(e) => {
                  e.target.style.display = "none"
                }}
              />
              <span className="font-mono font-semibold text-white text-sm tabular-nums">
                {invoice.pay_amount} {invoice.crypto}
              </span>
            </div>
          </div>

          <div className="bg-white/[0.03] rounded-xl p-3 border border-white/[0.06]">
            <div className="text-xs font-medium text-slate-500 mb-2 h-6 flex items-center">
              {t("payment.network")}
            </div>
            <div className="flex items-center gap-2">
              <img
                src={getChainIconUrl(invoice.chain)}
                alt={invoice.chain}
                className="w-5 h-5 rounded-full flex-shrink-0"
                onError={(e) => {
                  e.target.style.display = "none"
                }}
              />
              <span className="font-medium text-white text-sm">
                {getChainDisplayName(invoice.chain)}
              </span>
            </div>
          </div>
        </div>

        {/* Payment address */}
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-600 uppercase tracking-wide">
              {t("payment.paymentAddress")}
            </span>
            <CopyButton value={invoice.pay_address} field="address" />
          </div>
          <code className="block break-all bg-white/[0.03] border border-white/[0.06] rounded-xl p-3 text-xs font-mono text-slate-400 leading-relaxed">
            {invoice.pay_address}
          </code>
        </div>

        {/* Contract address */}
        {invoice.crypto_address && (
          <div className="space-y-2">
            <span className="text-xs font-semibold text-slate-600 uppercase tracking-wide">
              {invoice.crypto} {t("payment.contractAddress")}
            </span>
            <code className="block break-all bg-white/[0.02] border border-white/[0.04] rounded-xl p-3 text-xs font-mono text-slate-700 leading-relaxed select-none">
              {invoice.crypto_address.slice(0, 6)}...{invoice.crypto_address.slice(-8)}
            </code>
          </div>
        )}

        {/* Reselect payment method */}
        {!hasPayment && onReset && (
          <div className="pt-1">
            <Button
              variant="ghost"
              onClick={onReset}
              className="w-full text-slate-500 hover:text-slate-300 hover:bg-white/[0.06] border border-white/[0.06] hover:border-white/[0.12] cursor-pointer"
              size="sm"
            >
              <ArrowLeft className="w-3.5 h-3.5 mr-1" />
              {t("payment.reselectMethod")}
            </Button>
          </div>
        )}

        {/* Return to merchant */}
        {isCompleted && invoice.return_url && (
          <div className="pt-1">
            <Button
              onClick={() => window.open(invoice.return_url, "_blank")}
              className="w-full bg-emerald-500 hover:bg-emerald-400 text-white font-semibold shadow-[0_0_24px_rgba(16,185,129,0.25)] hover:shadow-[0_0_32px_rgba(16,185,129,0.35)] transition-all duration-200 cursor-pointer"
            >
              {t("payment.returnToMerchant")}
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export default PaymentAddress
