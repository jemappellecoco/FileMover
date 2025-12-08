// main.js
const pageTitle   = document.getElementById('pageTitle');
const statusLine  = document.getElementById('statusLine');
const tabButtons  = document.querySelectorAll('.tab-btn');

const pages = {
  pending: document.getElementById('page-pending'),
  restore: document.getElementById('page-restore'),
  history: document.getElementById('page-history'),
};

const roots = {
  pending: document.getElementById('pending-root'),
  restore: document.getElementById('restore-root'),
  history: document.getElementById('history-root'),
};

const titles = {
  pending: '📂 待搬任務清單',
  restore: '📦 待回遷清單（Phase2）',
  history: '🗂️ 搬運歷史紀錄',
};

// 記錄有沒有初始化過
const inited = {
  pending: false,
  restore: false,
  history: false,
};

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

  });
});

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
const firstTab = document.querySelector('.tab-btn[data-tab="pending"]');
if (firstTab) {
  firstTab.click();
}
