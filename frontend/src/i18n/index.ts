import { createI18n } from 'vue-i18n'
import zhCN from './zh-CN'
import en from './en'
import fr from './fr'
import ar from './ar'
import am from './am'
import { legacy } from './legacy'
import { legacyExtra } from './legacyExtra'
import { legacySms } from './legacySms'
import { legacyFinal } from './legacyFinal'
import { legacyFinal2 } from './legacyFinal2'
import { legacyTail } from './legacyTail'
import { remaining } from './remaining'
import { dynamic } from './dynamic'
import { localized } from './localized'
import { localizedExtra } from './localizedExtra'
import { localizedSafe } from './localizedSafe'
import { rebuildLocale, rebuiltAr, rebuiltAm } from './rebuild'
import { monitorStatus } from './monitorStatus'
export const supportedLocales = [
  { value: 'zh-CN', label: 'Chinese', direction: 'ltr' },
  { value: 'en', label: 'English', direction: 'ltr' },
  { value: 'fr', label: 'French', direction: 'ltr' },
  { value: 'ar', label: 'Arabic', direction: 'rtl' },
  { value: 'am', label: 'Amharic', direction: 'ltr' },
] as const
const storedLocale = localStorage.getItem('locale') || 'zh-CN'
export const initialLocale = supportedLocales.some(item => item.value === storedLocale) ? storedLocale : 'zh-CN'
export function setDocumentLocale(locale: string) { const selected = supportedLocales.find(item => item.value === locale) || supportedLocales[0]; document.documentElement.lang = selected.value; document.documentElement.dir = selected.direction }
setDocumentLocale(initialLocale)

function getAtPath(source: any, path: string[]): any {
  return path.reduce((value, key) => value?.[key], source)
}

function sanitizeLocalized(source: any, fallback: any, path: string[] = []): any {
  if (typeof source === 'string') {
    return source.includes('?') ? (getAtPath(fallback, path) ?? source.replace(/[?]/g, '')) : source
  }
  if (!source || typeof source !== 'object' || Array.isArray(source)) return source
  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(source)) {
    result[key] = sanitizeLocalized(value, fallback, [...path, key])
  }
  return result
}
function mergeLocale(...sources: any[]) {
  const result: Record<string, any> = {}
  for (const source of sources) {
    for (const [key, value] of Object.entries(source || {})) {
      result[key] = value && typeof value === 'object' && !Array.isArray(value)
        ? mergeLocale(result[key], value)
        : value
    }
  }
  return result
}

const legacyEnglish = {
  ...legacy.en,
  ...legacyExtra.en,
  ...legacySms.en,
  ...legacyFinal.en,
  ...legacyFinal2.en,
  ...legacyTail.en,
  ...remaining.en,
  ...dynamic.en,
}
const rebuiltLegacyAr = rebuildLocale(legacyEnglish, 'ar')
const rebuiltLegacyAm = rebuildLocale(legacyEnglish, 'am')
const messages: Record<string, any> = {
  'zh-CN': mergeLocale(zhCN, monitorStatus['zh-CN'], { legacy: { ...legacy['zh-CN'], ...legacyExtra['zh-CN'], ...legacySms['zh-CN'], ...legacyFinal['zh-CN'], ...legacyFinal2['zh-CN'], ...legacyTail['zh-CN'], ...remaining['zh-CN'], ...dynamic['zh-CN'] } }),
  'en': mergeLocale(en, monitorStatus.en, { legacy: { ...legacy.en, ...legacyExtra.en, ...legacySms.en, ...legacyFinal.en, ...legacyFinal2.en, ...legacyTail.en, ...remaining.en, ...dynamic.en } }),
  fr: mergeLocale(en, monitorStatus.fr, fr, sanitizeLocalized(localized.fr, en), sanitizeLocalized(localizedExtra.fr, en), localizedSafe.fr, { legacy: { ...legacy.fr, ...legacyExtra.fr, ...legacySms.fr, ...legacyFinal.fr, ...legacyFinal2.fr, ...legacyTail.fr, ...remaining.fr, ...dynamic.fr } }),
  ar: mergeLocale(en, monitorStatus.ar, rebuiltAr, localizedSafe.ar, { legacy: rebuiltLegacyAr }),
  am: mergeLocale(en, monitorStatus.am, rebuiltAm, localizedSafe.am, { legacy: rebuiltLegacyAm }),
}

const i18n: any = (createI18n as any)({
  legacy: false,
  locale: initialLocale,
  fallbackLocale: 'en',
  messages,
})

export default i18n
