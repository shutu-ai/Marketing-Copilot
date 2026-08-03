<template>
  <el-container class="app-shell">
    <el-aside v-if="!isMobile" :width="collapse ? '76px' : '248px'" class="aside">
      <div class="logo">
        <span class="logo-mark" aria-hidden="true">M</span>
        <span v-if="!collapse" class="logo-name">Marketing Copilot</span>
      </div>
      <Sidebar :collapse="collapse" />
    </el-aside>
    <el-container>
      <el-header style="height: 64px; padding: 0">
        <TopBar :title="pageTitle" @toggle="toggleNavigation" />
      </el-header>
      <el-main class="app-main">
        <main class="page-container" tabindex="-1">
          <router-view />
        </main>
      </el-main>
    </el-container>
  </el-container>
  <el-drawer
    v-model="mobileOpen"
    direction="ltr"
    :with-header="false"
    size="280px"
    class="mobile-navigation"
  >
    <div class="logo">Marketing Copilot</div>
    <Sidebar :collapse="false" @navigate="mobileOpen = false" />
  </el-drawer>
</template>

<script setup lang="ts">
import { ref, computed, onBeforeUnmount } from 'vue'
import { useRoute } from 'vue-router'
import { useI18n } from 'vue-i18n'
import Sidebar from './Sidebar.vue'
import TopBar from './TopBar.vue'

const collapse = ref(false)
const mobileOpen = ref(false)
const mobileQuery = window.matchMedia('(max-width: 767px)')
const isMobile = ref(mobileQuery.matches)
const route = useRoute()
const { t } = useI18n()

const pageTitle = computed(() => {
  const key = route.meta.titleKey as string | undefined
  return key ? t(key) : ''
})

function handleViewportChange(event: MediaQueryListEvent) {
  isMobile.value = event.matches
  if (!event.matches) mobileOpen.value = false
}

function toggleNavigation() {
  if (isMobile.value) mobileOpen.value = !mobileOpen.value
  else collapse.value = !collapse.value
}

mobileQuery.addEventListener('change', handleViewportChange)
onBeforeUnmount(() => mobileQuery.removeEventListener('change', handleViewportChange))
</script>

<style scoped>
.aside {
  background-color: var(--mc-sidebar);
  border-right: 1px solid rgb(255 255 255 / 7%);
  box-shadow: 10px 0 30px rgb(35 64 60 / 8%);
  transition: width 0.26s ease;
  overflow: hidden;
  z-index: 2;
}
.app-shell {
  width: 100%;
  height: 100vh;
}
.app-main {
  overflow-x: hidden;
  overflow-y: auto;
  padding: 28px 30px 36px;
  background: var(--mc-bg);
}
.logo {
  height: 64px;
  display: flex;
  align-items: center;
  gap: 11px;
  padding: 0 18px;
  color: #fff;
  border-bottom: 1px solid rgb(255 255 255 / 9%);
  white-space: nowrap;
}
.logo-mark {
  width: 36px;
  height: 36px;
  display: inline-flex;
  flex: 0 0 36px;
  align-items: center;
  justify-content: center;
  color: #fffdf6;
  font-size: 16px;
  font-weight: 800;
  background: var(--mc-primary);
  border-radius: 12px;
  box-shadow: 0 7px 18px rgb(27 43 41 / 24%);
}
.logo-name {
  overflow: hidden;
  font-size: 15px;
  font-weight: 700;
  letter-spacing: -0.01em;
  text-overflow: ellipsis;
}
:deep(.mobile-navigation .el-drawer__body) {
  padding: 0;
  background: var(--mc-sidebar);
}
@media (max-width: 1024px) {
  .app-main { padding: 22px; }
}
@media (max-width: 767px) {
  .app-main { padding: 18px 14px 28px; }
}
</style>
