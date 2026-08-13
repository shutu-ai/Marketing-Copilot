<template><el-config-provider :locale="elementLocale"><router-view /></el-config-provider></template>
<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { getElementPlusLocale } from './utils/locale'
const { locale } = useI18n()
const elementLocale = computed(() => getElementPlusLocale(locale.value))
</script>
