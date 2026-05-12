import { useState, useEffect } from "react"
import en from "@/locales/en.json"
import zh from "@/locales/zh.json"

const translations = {
  en,
  zh,
  "zh-CN": zh,
  "zh-TW": zh,
  "zh-HK": zh,
}

const SUPPORTED = ["en", "zh"]
const STORAGE_KEY = "xcash-locale"
const CHANGE_EVENT = "xcash-locale-change"

function normalize(lang) {
  if (!lang) return "en"
  const code = lang.split("-")[0]
  return code === "zh" ? "zh" : "en"
}

function detectInitialLocale() {
  if (typeof window === "undefined") return "en"
  // 用户手动选择过的优先；否则回退到浏览器语言。
  try {
    const stored = window.localStorage?.getItem(STORAGE_KEY)
    if (stored && SUPPORTED.includes(stored)) return stored
  } catch {
    // localStorage 不可用（隐私模式等），忽略即可
  }
  const browserLang = navigator.language || navigator.userLanguage || "en"
  return normalize(browserLang)
}

// 全局单一来源：避免多处 useI18n 各自持有独立 state，切换不同步。
let currentLocale = detectInitialLocale()
const listeners = new Set()

function setGlobalLocale(next) {
  const normalized = SUPPORTED.includes(next) ? next : normalize(next)
  if (normalized === currentLocale) return
  currentLocale = normalized
  try {
    window.localStorage?.setItem(STORAGE_KEY, normalized)
  } catch {
    // 写入失败不影响内存中的切换
  }
  listeners.forEach((fn) => fn(normalized))
  window.dispatchEvent(new CustomEvent(CHANGE_EVENT, { detail: normalized }))
}

export function useI18n() {
  const [locale, setLocaleState] = useState(currentLocale)

  useEffect(() => {
    const handler = (next) => setLocaleState(next)
    listeners.add(handler)
    // locale 在挂载到订阅之间可能已被改动，做一次对齐
    if (locale !== currentLocale) setLocaleState(currentLocale)

    const onBrowserChange = () => {
      // 仅当用户没手动选过语言时，才跟随浏览器语言变化
      try {
        if (window.localStorage?.getItem(STORAGE_KEY)) return
      } catch {
        // 无 localStorage 时也按跟随处理
      }
      setGlobalLocale(navigator.language || "en")
    }
    window.addEventListener("languagechange", onBrowserChange)
    return () => {
      listeners.delete(handler)
      window.removeEventListener("languagechange", onBrowserChange)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const t = (key, params = {}) => {
    const keys = key.split(".")
    let translation = translations[locale] || translations.en

    for (const k of keys) {
      translation = translation?.[k]
      if (!translation) break
    }

    if (!translation) {
      translation = translations.en
      for (const k of keys) {
        translation = translation?.[k]
        if (!translation) break
      }
    }

    if (!translation) {
      return key
    }

    if (typeof translation === "string" && Object.keys(params).length > 0) {
      return translation.replace(/\{\{(\w+)\}\}/g, (match, param) => {
        return params[param] !== undefined ? params[param] : match
      })
    }

    return translation
  }

  return {
    locale,
    setLocale: setGlobalLocale,
    t,
  }
}
