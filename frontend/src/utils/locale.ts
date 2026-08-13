import zhCn from 'element-plus/es/locale/lang/zh-cn'
import en from 'element-plus/es/locale/lang/en'
import fr from 'element-plus/es/locale/lang/fr'
import ar from 'element-plus/es/locale/lang/ar'
export const elementLocales = { 'zh-CN': zhCn, en, fr, ar, am: en }
export function getElementPlusLocale(locale: string) { return elementLocales[locale as keyof typeof elementLocales] || zhCn }
