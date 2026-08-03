<template>
  <div class="db-query">
    <div class="page-header">
      <h2>数据库 Schema</h2>
      <div style="display:flex;gap:10px;align-items:center">
        <el-button type="primary" size="small" @click="showSqlPanel = true; scrollToSql()">
          <el-icon><Edit /></el-icon>
          SQL 查询
        </el-button>
        <el-input v-model="filterText" placeholder="搜索表名..." prefix-icon="Search" clearable style="width:220px" size="small" />
        <el-button :loading="loadingTables" @click="fetchTables" size="small">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>
    </div>

    <div ref="sqlCardRef">
    <el-card v-show="showSqlPanel" shadow="never" style="margin-bottom:16px">
      <template #header>
        <div style="display:flex;align-items:center;justify-content:space-between">
          <span>SQL 查询</span>
          <div style="display:flex;gap:8px">
            <span v-if="execTime !== null" style="font-size:12px;color:var(--mc-text-tertiary);margin-right:8px">耗时: {{ execTime }}ms</span>
            <el-button text @click="showSqlPanel = false"><el-icon><Close /></el-icon></el-button>
          </div>
        </div>
      </template>
      <el-input
        v-model="sql"
        type="textarea"
        :rows="5"
        placeholder="输入 SQL..."
        style="font-family: 'Consolas', 'Courier New', monospace; font-size: 13px"
      />
      <div style="display:flex;justify-content:space-between;align-items:center;margin-top:10px">
        <el-checkbox v-model="confirmWrite" style="margin-right:12px">确认写操作</el-checkbox>
        <div style="display:flex;gap:8px">
          <el-button @click="sql = ''; result = null">清空</el-button>
          <el-button type="primary" :loading="executing" @click="handleExecute" style="min-width:100px">
            <el-icon style="margin-right:4px"><CaretRight /></el-icon>
            执行
          </el-button>
        </div>
      </div>

      <div v-if="result" style="margin-top:16px">
        <el-alert v-if="result.type === 'error'" :title="result.error" type="error" show-icon :closable="false" />
        <div v-else style="font-size:13px;margin-bottom:8px;color:var(--mc-text-secondary)">
          <template v-if="result.type === 'select'">查询结果 — {{ result.row_count }} 行, {{ result.columns?.length }} 列</template>
          <template v-else>成功 — 影响 {{ result.affected_rows }} 行</template>
        </div>
        <el-table
          v-if="result.type === 'select'"
          :data="resultData"
          stripe border size="small" style="width:100%" max-height="400"
        >
          <el-table-column v-for="col in resultColumns" :key="col" :prop="col" :label="col" min-width="120" show-overflow-tooltip />
          <template #empty><el-empty description="暂无数据" /></template>
        </el-table>
      </div>
    </el-card>
    </div>

    <div v-loading="loadingTables" class="schema-list">
      <div v-for="tbl in filteredTables" :key="tbl.table_name" class="schema-table">
        <div class="schema-table-header" @click="toggleExpand(tbl.table_name)">
          <div class="schema-table-title">
            <el-icon :style="{ color: 'var(--mc-primary)', fontSize: '18px' }"><Collection /></el-icon>
            <span class="schema-table-name">{{ tbl.table_name }}</span>
            <el-tag v-if="tbl.table_comment" size="small" type="info" style="margin-left:8px">{{ tbl.table_comment }}</el-tag>
          </div>
          <div class="schema-table-meta">
            <span>{{ tbl.columns.length }} 列</span>
            <el-icon style="margin-left:12px;font-size:12px;transition:transform .2s" :style="{ transform: expandedTables[tbl.table_name] ? 'rotate(180deg)' : '' }">
              <ArrowDown />
            </el-icon>
          </div>
        </div>
        <div v-show="expandedTables[tbl.table_name]" class="schema-columns">
          <table class="schema-col-table">
            <thead>
              <tr>
                <th style="width:28px"></th>
                <th style="width:200px">字段名</th>
                <th style="width:110px">类型</th>
                <th style="width:70px">可空</th>
                <th style="width:60px">键</th>
                <th style="width:180px">默认值</th>
                <th>备注</th>
                <th style="width:90px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="col in tbl.columns" :key="col.column_name">
                <td style="text-align:center">
                  <el-icon v-if="col.is_pk" style="color:var(--mc-primary);font-size:16px" title="主键"><Key /></el-icon>
                </td>
                <td><span class="col-name">{{ col.column_name }}</span></td>
                <td><el-tag size="small" :type="typeTag(col.data_type)">{{ col.data_type }}</el-tag></td>
                <td>
                  <el-tag v-if="col.is_nullable === 'YES'" size="small" type="warning">YES</el-tag>
                  <el-tag v-else size="small" type="danger">NO</el-tag>
                </td>
                <td>
                  <el-tag v-if="col.is_pk" size="small" type="warning">PK</el-tag>
                  <el-tag v-if="col.foreign_key" size="small" type="success" :title="'FK → ' + col.foreign_key.ref_table + '.' + col.foreign_key.ref_column">FK</el-tag>
                </td>
                <td><code v-if="col.column_default" class="default-val">{{ col.column_default }}</code></td>
                <td><span class="col-comment">{{ col.column_comment || '-' }}</span></td>
                <td>
                  <el-button size="small" text type="primary" @click="insertColumn(tbl.table_name, col.column_name)">插入</el-button>
                </td>
              </tr>
            </tbody>
          </table>
          <div class="schema-col-actions">
            <el-button size="small" text type="primary" :icon="Search" @click.stop="quickQuery(tbl.table_name)">查询全部</el-button>
            <el-button size="small" text type="primary" :icon="Edit" @click.stop="openSqlEditor(tbl)">自定义 SQL</el-button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import { Search, Collection, Close, ArrowDown, Key, Edit, CaretRight } from '@element-plus/icons-vue'
import * as api from '@/api/dbQuery'

const tables = ref<api.TableInfo[]>([])
const loadingTables = ref(false)
const filterText = ref('')
const expandedTables = ref<Record<string, boolean>>({})

const sqlCardRef = ref<HTMLElement | null>(null)
const showSqlPanel = ref(true)
const sql = ref('')
const executing = ref(false)
const confirmWrite = ref(false)
const execTime = ref<number | null>(null)
const result = ref<api.QueryResult | null>(null)

const filteredTables = computed(() => {
  if (!filterText.value) return tables.value
  const q = filterText.value.toLowerCase()
  return tables.value.filter(t => t.table_name.toLowerCase().includes(q))
})

const resultColumns = computed(() => result.value?.columns || [])
const resultData = computed(() => {
  if (!result.value?.rows) return []
  return result.value.rows.map((row: any[]) => {
    const obj: Record<string, any> = {}
    result.value?.columns?.forEach((col, i) => { obj[col] = row[i] })
    return obj
  })
})

onMounted(() => fetchTables())

function typeTag(type: string) {
  if (/int|serial|bigint/i.test(type)) return ''
  if (/char|text|varchar/i.test(type)) return 'success'
  if (/bool/i.test(type)) return 'warning'
  if (/timestamp|date|time/i.test(type)) return 'info'
  return 'danger'
}

async function fetchTables() {
  loadingTables.value = true
  try {
    const res = await api.getTables()
    tables.value = res.data || []
    tables.value.forEach(t => { expandedTables.value[t.table_name] = true })
  } catch (e: any) {
    tables.value = []
    const msg = e?.response?.data?.detail || e?.message || '加载失败'
    ElMessage.error('加载数据库表失败: ' + msg)
  } finally {
    loadingTables.value = false
  }
}

function toggleExpand(name: string) {
  expandedTables.value[name] = !expandedTables.value[name]
}

function scrollToSql() {
  nextTick(() => {
    sqlCardRef.value?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  })
}

async function quickQuery(name: string) {
  sql.value = `SELECT * FROM ${name} LIMIT 100`
  showSqlPanel.value = true
  result.value = null
  await nextTick()
  scrollToSql()
  await handleExecute()
}

function openSqlEditor(table: api.TableInfo) {
  const columns = table.columns.map(column => `  ${column.column_name}`).join(',\n')
  sql.value = `SELECT\n${columns}\nFROM ${table.table_name}\nLIMIT 100`
  showSqlPanel.value = true
  result.value = null
  execTime.value = null
  scrollToSql()
}

function insertColumn(table: string, column: string) {
  sql.value += (sql.value ? ' ' : '') + `${table}.${column}`
  showSqlPanel.value = true
  scrollToSql()
}

async function handleExecute() {
  const trimmed = sql.value.trim()
  if (!trimmed) { ElMessage.warning('请输入 SQL'); return }
  const isWrite = !/^\s*(SELECT|WITH)\b/i.test(trimmed)
  if (isWrite && !confirmWrite.value) { ElMessage.warning('执行写操作请勾选确认'); return }

  executing.value = true; execTime.value = null
  const start = performance.now()
  try {
    const res = await api.executeQuery({ sql: trimmed })
    result.value = res.data
    execTime.value = Math.round(performance.now() - start)
    if (res.data.type === 'error') ElMessage.error(res.data.error)
  } catch (e: any) {
    const msg = e?.response?.data?.detail || e?.message || '执行失败'
    result.value = { type: 'error', error: msg }
    ElMessage.error(msg)
  } finally { executing.value = false }
}
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px;
}
.page-header h2 { margin: 0; font-size: 22px; font-weight: 600; }

.schema-list {
  display: flex; flex-direction: column; gap: 12px;
}
.schema-table {
  border: 1px solid var(--mc-border); border-radius: var(--mc-radius-lg); overflow: hidden; background: var(--mc-surface);
  box-shadow: var(--mc-shadow);
}
.schema-table-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 16px; cursor: pointer; user-select: none;
  background: var(--mc-surface-muted); transition: background .15s;
}
.schema-table-header:hover { background: var(--mc-primary-soft); }
.schema-table-title {
  display: flex; align-items: center; gap: 8px;
}
.schema-table-name {
  font-size: 15px; font-weight: 600;
  font-family: 'Consolas', 'Courier New', monospace;
}
.schema-table-meta {
  display: flex; align-items: center; font-size: 12px; color: var(--mc-text-tertiary);
}
.schema-columns {
  border-top: 1px solid var(--mc-border);
}
.schema-col-table {
  width: 100%; border-collapse: collapse; font-size: 13px;
}
.schema-col-table th {
  background: var(--mc-surface-muted); padding: 8px 10px; text-align: left;
  font-weight: 600; color: var(--mc-text-secondary); border-bottom: 1px solid var(--mc-border);
  font-size: 12px;
}
.schema-col-table td {
  padding: 8px 10px; border-bottom: 1px solid #edf2f1;
}
.schema-col-table tbody tr:hover { background: #f7f9f8; }
.col-name {
  font-family: 'Consolas', 'Courier New', monospace;
  color: var(--mc-primary-hover); font-weight: 600; font-size: 13px;
}
.default-val {
  font-family: 'Consolas', 'Courier New', monospace;
  font-size: 12px; color: var(--mc-success);
}
.col-comment { font-size: 12px; color: var(--mc-text-tertiary); }
.schema-col-actions {
  display: flex; gap: 8px; padding: 10px 16px; border-top: 1px solid var(--mc-border);
}
</style>
