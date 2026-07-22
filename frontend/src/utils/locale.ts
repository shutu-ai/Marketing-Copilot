export function useElementPlusLocale(locale: string) {
  const elLocale = locale === 'en'
    ? import('element-plus/es/locale/lang/en')
    : import('element-plus/es/locale/lang/zh-cn')
  return elLocale
}
