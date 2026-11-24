import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import FunnelAnalyzerView from '@/views/FunnelAnalyzerView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/funnel_analyzer', name: 'funnel_analyzer', component: FunnelAnalyzerView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
