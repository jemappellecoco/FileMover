// main.js
const pageTitle   = document.getElementById('pageTitle');
const statusLine  = document.getElementById('statusLine');
const tabButtons  = document.querySelectorAll('.tab-btn');

const pages = {
  pending: document.getElementById('page-pending'),
  restore: document.getElementById('page-restore'),
  history: document.getElementById('page-history'),
  nodes:   document.getElementById('page-nodes'),      
};

const roots = {
  pending: document.getElementById('pending-root'),
  restore: document.getElementById('restore-root'),
  history: document.getElementById('history-root'),
  nodes:   document.getElementById('nodes-root'), 
};

const titles = {
  pending: '📂 待搬任務清單',
  restore: '📦 待回遷清單（Phase2）',
  history: '🗂️ 搬運歷史紀錄',
  nodes:   '🖥 節點管理',
};

// 記錄有沒有初始化過
const inited = {
  pending: false,
  restore: false,
  history: false,
   nodes:   false,  
};
const API_SELF = '/api/cluster/self';

// ⭐ 決定「節點管理」這個 tab 要不要顯示（只有 Master 顯示）
async function setupNodesTabVisibility() {
  const nodesTabBtn      = document.querySelector('.tab-btn[data-tab="nodes"]');
  const nodesPageSection = document.getElementById('page-nodes');

  if (!nodesTabBtn || !nodesPageSection) return;

  try {
    const res = await fetch(API_SELF);
    if (!res.ok) throw new Error('fail to load self');

    const self = await res.json(); // { nodeName, role, group, isMaster }

    console.log('cluster/self =', self);

    if (!self.isMaster) {
      // ❌ 不是 Master：把 tab + 頁面藏起來
      nodesTabBtn.style.display = 'none';
      nodesPageSection.style.display = 'none';
    } else {
      // ✅ 是 Master：保留 tab
      console.log('This node is Master, nodes tab enabled.');
    }
  } catch (err) {
    console.error('setupNodesTabVisibility error', err);
    // 如果連 /api/cluster/self 都掛了，就保守起見藏掉
    const nodesTabBtn2      = document.querySelector('.tab-btn[data-tab="nodes"]');
    const nodesPageSection2 = document.getElementById('page-nodes');
    if (nodesTabBtn2)      nodesTabBtn2.style.display = 'none';
    if (nodesPageSection2) nodesPageSection2.style.display = 'none';
  }
}

// === tab click handler ===
tabButtons.forEach(btn => {
  btn.addEventListener('click', async () => {
    const tab = btn.dataset.tab;
    if (!tab) return;

    // UI 狀態切換
    tabButtons.forEach(b => b.classList.toggle('active', b === btn));
    Object.keys(pages).forEach(k => {
      pages[k].classList.toggle('active', k === tab);
    });
    pageTitle.textContent = titles[tab];

    // ⭐ 每次切換都 reload（不管初始化過沒有）
    if (tab === 'pending') {
      if (!inited.pending) {
        const { initPending } = await import('./pending.js');
        initPending(roots.pending, statusLine);
        inited.pending = true;
      } else {
        window.dispatchEvent(new CustomEvent('pending-reload'));
      }
    }

    if (tab === 'restore') {
      if (!inited.restore) {
        const { initRestore } = await import('./restore.js');
        initRestore(roots.restore);
        inited.restore = true;
      } else {
        window.dispatchEvent(new CustomEvent('restore-reload'));
      }
    }

    if (tab === 'history') {
      if (!inited.history) {
        const { initHistory } = await import('./history.js');
        initHistory(roots.history);
        inited.history = true;
      } else {
        window.dispatchEvent(new CustomEvent('history-reload'));
      }
    }
         // ⭐ 新增：nodes tab
    if (tab === 'nodes') {
      if (!inited.nodes) {
        const { initNodes } = await import('./nodes.js');
        initNodes(roots.nodes, statusLine);
        inited.nodes = true;
      } else {
        window.dispatchEvent(new CustomEvent('nodes-reload'));
      }
    }
  });
  // === 啟動時先處理節點管理 tab，要不要顯示 ===
setupNodesTabVisibility();
});

// === 預設載入 pending：模擬點一下 pending tab ===
const firstTab = document.querySelector('.tab-btn[data-tab="pending"]');
if (firstTab) {
  firstTab.click();
}
// // 預設載入 pending
// initPendingTab();
// inited.pending = true;

async function initPendingTab() {
  const { initPending } = await import('./pending.js');
  initPending(roots.pending, statusLine);
}

async function initRestoreTab() {
  const { initRestore } = await import('./restore.js');
  initRestore(roots.restore);
}

async function initHistoryTab() {
  const { initHistory } = await import('./history.js');
  initHistory(roots.history);
}

async function initNodesTab() {
  const { initNodes } = await import('./nodes.js');
  initNodes(roots.nodes, statusLine);
}
