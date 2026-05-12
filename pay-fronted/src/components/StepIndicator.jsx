// src/components/StepIndicator.jsx
import { useI18n } from "@/hooks/useI18n"

const STEP_KEYS = {
  4: ["invoice.stepLabel", "payment.stepLabel", "payment.sendLabel", "invoice.completedLabel"],
  3: ["invoice.stepLabel", "payment.sendLabel", "invoice.completedLabel"],
}

function StepIndicator({ activeStep, naturalStep, onStepClick, stepCount = 4 }) {
  const { t } = useI18n()
  const keys = STEP_KEYS[stepCount] ?? STEP_KEYS[4]
  const nodes = Array.from({ length: stepCount }, (_, i) => i + 1)

  const getNodeStyle = (n) => {
    if (n < activeStep || (n < naturalStep && n !== activeStep))
      return "bg-emerald-500 text-white cursor-pointer hover:brightness-110 transition-all"
    if (n === activeStep) {
      if (n === stepCount) return "bg-emerald-500 text-white shadow-[0_0_16px_rgba(16,185,129,0.5)]"
      return "bg-orange-500 text-white shadow-[0_0_16px_rgba(249,115,22,0.5)]"
    }
    return "bg-black/[0.06] dark:bg-white/[0.06] text-slate-500 dark:text-slate-600 border border-black/[0.1] dark:border-white/[0.08]"
  }

  const getLineStyle = (n) => {
    return n < naturalStep ? "bg-orange-500/35" : "bg-black/[0.06] dark:bg-white/[0.06]"
  }

  const getLabelStyle = (n) => {
    if (n < naturalStep && n !== activeStep) return "text-emerald-600"
    if (n === activeStep) {
      if (n === stepCount) return "text-emerald-500 font-semibold"
      return "text-orange-400 font-semibold"
    }
    return "text-slate-400 dark:text-slate-700"
  }

  const isClickable = (n) => n < naturalStep && n !== activeStep

  return (
    <div className="px-6 pt-4 pb-2 max-w-lg mx-auto">
      {/* Nodes + lines */}
      <div className="flex items-center">
        {nodes.map((n) => (
          <div key={n} className="flex items-center flex-1 last:flex-none">
            <button
              onClick={() => isClickable(n) && onStepClick(n)}
              className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0 focus:outline-none ${getNodeStyle(n)}`}
              aria-label={`Step ${n}`}
            >
              {n < naturalStep && n !== activeStep ? (
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                </svg>
              ) : (
                n
              )}
            </button>
            {n < stepCount && (
              <div className={`flex-1 h-px mx-1.5 ${getLineStyle(n)}`} />
            )}
          </div>
        ))}
      </div>

      {/* Labels */}
      <div className="flex justify-between mt-1.5 px-0.5">
        {nodes.map((n, i) => (
          <div key={n} className={`text-[9px] text-center whitespace-nowrap ${getLabelStyle(n)}`}>
            {t(keys[i])}
          </div>
        ))}
      </div>
    </div>
  )
}

export default StepIndicator
