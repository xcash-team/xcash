// src/components/SummaryBar.jsx
import LogoMark from "@/components/LogoMark"
import { useI18n } from "@/hooks/useI18n"

function SummaryBar({ invoice, isDark, toggleTheme }) {
  const { t, locale, setLocale } = useI18n()
  const toggleLocale = () => setLocale(locale === "zh" ? "en" : "zh")

  const getStatusStyle = (status) => {
    switch (status) {
      case "waiting":
        return { badge: "bg-blue-500/10 text-blue-300 border border-blue-500/20", dot: "bg-blue-400 animate-pulse" }
      case "confirming":
        return { badge: "bg-amber-500/10 text-amber-300 border border-amber-500/20", dot: "bg-amber-400 animate-pulse" }
      case "finalizing":
        return { badge: "bg-emerald-500/10 text-emerald-300 border border-emerald-500/20", dot: "bg-emerald-400 animate-pulse" }
      case "completed":
        return { badge: "bg-emerald-500/10 text-emerald-300 border border-emerald-500/20", dot: "bg-emerald-400" }
      case "expired":
        return { badge: "bg-slate-500/10 text-slate-500 border border-slate-500/20", dot: "bg-slate-600" }
      default:
        return { badge: "bg-slate-500/10 text-slate-500 border border-slate-500/20", dot: "bg-slate-600" }
    }
  }

  const hasPayMethod = Boolean(invoice?.crypto && invoice?.pay_amount)
  // 后端 invoice.status 真正切到 completed 还要等 worker 的 RPC 二次校验，
  // 区块层达标到 invoice 完成之间是空窗期；前端把这段显示为 finalizing
  // 而不是继续闪烁的 confirming，避免用户看到 100% 进度 + amber 徽章产生
  // 「卡住了」的错觉。
  const progress = invoice?.payment?.confirm_progress?.progress ?? 0
  const displayStatus =
    invoice?.status === "confirming" && progress >= 100
      ? "finalizing"
      : invoice?.status
  const statusStyle = getStatusStyle(displayStatus)

  return (
    <div className="bg-orange-500/[0.06] border-b border-orange-500/[0.12] px-5 py-3">
      <div className="max-w-lg mx-auto flex items-center justify-between gap-3">
        {/* Brand */}
        <div className="flex items-center gap-2 flex-shrink-0">
          <LogoMark size={20} className="opacity-80" />
          <span
            className="font-bold text-sm bg-gradient-to-r from-orange-400 to-amber-300 bg-clip-text text-transparent tracking-wide"
            style={{ fontFamily: "'Orbitron', sans-serif" }}
          >
            Xcash
          </span>
        </div>

        {/* Amount */}
        <div className="text-center flex-1 min-w-0">
          <div className="flex items-baseline justify-center gap-2 flex-wrap">
            <span className="text-base font-bold text-gray-900 dark:text-white tabular-nums">
              {invoice?.amount} {invoice?.currency}
            </span>
            {hasPayMethod && (
              <span className="text-xs font-mono text-orange-400 tabular-nums">
                ≈ {invoice.pay_amount} {invoice.crypto}
              </span>
            )}
          </div>
          {invoice?.title && (
            <div className="text-xs text-slate-500 dark:text-slate-600 truncate mt-0.5">{invoice.title}</div>
          )}
        </div>

        {/* Status */}
        <div className="flex-shrink-0">
          <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${statusStyle.badge}`}>
            <span className={`w-1.5 h-1.5 rounded-full ${statusStyle.dot}`} />
            {t(`invoice.status.${displayStatus}`) || displayStatus}
          </span>
        </div>

        {/* Locale toggle */}
        <button
          onClick={toggleLocale}
          className="flex-shrink-0 h-8 px-2.5 rounded-full flex items-center justify-center text-xs font-semibold tabular-nums bg-white/[0.06] dark:bg-white/[0.06] hover:bg-white/[0.12] dark:hover:bg-white/[0.12] border border-black/[0.08] dark:border-white/[0.1] text-slate-600 dark:text-slate-300 transition-colors"
          aria-label="Switch language"
          title={locale === "zh" ? "Switch to English" : "切换到中文"}
        >
          {locale === "zh" ? "EN" : "中"}
        </button>

        {/* Theme toggle */}
        <button
          onClick={toggleTheme}
          className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center bg-white/[0.06] dark:bg-white/[0.06] hover:bg-white/[0.12] dark:hover:bg-white/[0.12] border border-black/[0.08] dark:border-white/[0.1] transition-colors"
          aria-label="Toggle theme"
        >
          {isDark ? (
            <svg className="w-4 h-4 text-slate-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364-6.364l-.707.707M6.343 17.657l-.707.707M17.657 17.657l-.707-.707M6.343 6.343l-.707-.707M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          ) : (
            <svg className="w-4 h-4 text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
            </svg>
          )}
        </button>
      </div>
    </div>
  )
}

export default SummaryBar
