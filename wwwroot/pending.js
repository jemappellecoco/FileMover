// pending.js
const API_PENDING   = '/jobs/pending';
const API_EVENTS    = '/api/progress/events';
const API_CONCUR    = '/api/config/concurrency';

export function initPending(root, statusLine) {
  // 建 HTML 結構（照你原本的樣式，只拿 toolbar + table）
  root.innerHTML = `
    <div class="toolbar">
      <button id="btnPendingReload">重新整理</button>

      <label>
        樓層：
        <select id="selGroup">
          <option value="all">全部</option>
          <option value="4F">4F</option>
          <option value="7F">7F</option>
        </select>
      </label>

      <label>
        並行數：
        <select id="selParallel">
          ${[1,2,3,4,5,6,7,8,9,10].map(v => `<option value="${v}">${v}</option>`).join('')}
        </select>
      </label>
      <button id="btnSetParallel">套用</button>
      <span id="pendingCount" class="muted"></span>
    </div>

    <table id="pendingTable">
      <thead>
        <tr>
          <th style="width:60px;">No.</th>
          <th style="width:60px;">優先級</th>
          <th style="width:180px;">節目名稱</th>
          <th style="width:100px;">檔名(UserBit)</th>
          <th style="width:80px;">來源</th>
          <th style="width:80px;">目的地</th>
          <th style="width:220px;">狀態</th>
          <th style="width:220px;">進度</th>
          <th style="width:80px;">取消</th>
        </tr>
      </thead>

      <tbody>
        <tr><td colspan="8" style="text-align:center;color:#999;">載入中…</td></tr>
      </tbody>
    </table>
  `;

  const $btnReload    = root.querySelector('#btnPendingReload');
  const $tableBody    = root.querySelector('#pendingTable tbody');
  const $count        = root.querySelector('#pendingCount');
  const $selGroup     = root.querySelector('#selGroup');
  const $selParallel  = root.querySelector('#selParallel');
  const $btnSetPar    = root.querySelector('#btnSetParallel');

  let hasScheduledReloadAfterDone = false;

  // === 全域狀態 ===
  let allRows = [];              // 從 DB 撈到的完整 pending 清單
  const progressState = new Map(); // 儲存 key → 百分比（key = "TO-7" 這種）
  const rowMap = new Map();        // HistoryId → <tr>

  function escapeHtml(text) {
    if (!text) return '';
    return String(text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  // ====== 讀 /api/config/concurrency，套用到「並行數」下拉 ======
  async function loadConcurrency() {
    try {
      const resp = await fetch(API_CONCUR, { cache: 'no-store' });
      if (!resp.ok) return;

      const data = await resp.json();   // 期待 { current: 2 }
      if (typeof data.current === 'number') {
        const v = String(data.current);

        // 如果下拉沒有這個值，就動態補一個 option
        const hasOption = Array.from($selParallel.options).some(o => o.value === v);
        if (!hasOption) {
          const opt = document.createElement('option');
          opt.value = v;
          opt.textContent = v;
          $selParallel.appendChild(opt);
        }

        $selParallel.value = v;
      }
    } catch (err) {
      console.warn('loadConcurrency error', err);
    }
  }

  // ====== 建立/更新單筆 row（不重畫整張表） ======
  function renderOrUpdateRow(r, seq) {
    const id   = r.historyId;
    const key  = `TO-${id}`;          // ★ 重點：row 的 key 是 "TO-<HistoryId>"
    const existing = rowMap.get(id);

    const programName = r.programName || '';
    const fileName    = r.fileName || '';
    const source      = r.sourceStorage || r.sourcePath || '';
    const dest        = r.destStorage   || r.destPath   || '';

    const statusCode  = r.status;
    // 從後端帶來的 retry 資訊
    const retryCount   = typeof r.retryCount === 'number' ? r.retryCount : 0;
    const retryCode    = (typeof r.retryCode === 'number' ? r.retryCode : null);
    const retryMessage = r.retryMessage || '';

    const percent     = progressState.get(key) ?? 0;

    // 優先級（1–10），拿不到就用 1
    const priority = (typeof r.priority === 'number' && !isNaN(r.priority))
      ? r.priority
      : 1;

    const hasActiveProgress = percent > 0 && percent < 100;
    let statusText = hasActiveProgress ? '執行中' : '排隊中';
    let retryHtml  = '';
    const isActive = hasActiveProgress;

    // 回遷任務小標籤（依你 DB 的 code 調整：這裡先用 24/27）
    const isPhase2 = (statusCode === 24 || statusCode === 27);
    const tagHtml  = isPhase2 ? '<span class="tag-badge">回遷</span>' : '';

    // 沒有進度（percent=0）但有 retry 紀錄 → 顯示「重試等待中」
    if (!hasActiveProgress && retryCount > 0) {
      statusText = `重試等待中（第 ${retryCount} 次）`;

      const codePart = (retryCode != null) ? `(${retryCode})` : '';
      const msgPart  = escapeHtml(retryMessage);

      if (codePart || msgPart) {
        const full = `最後錯誤${codePart}：${msgPart}`;
        retryHtml = `<div class="retry-info" title="${full}">${full}</div>`;
      }
    }

    let tr;
    if (!existing) {
      tr = document.createElement('tr');
      rowMap.set(id, tr);
      $tableBody.appendChild(tr);
    } else {
      tr = existing;
    }

    tr.innerHTML = `
      <td>${seq}${tagHtml}</td>
      <td>
        <select class="pri-select" data-id="${id}"
                style="padding:2px 4px;font-size:12px;"
                ${isActive ? 'disabled' : ''}>
          ${[1,2,3,4,5,6,7,8,9,10].map(v =>
            `<option value="${v}" ${v === priority ? 'selected' : ''}>${v}</option>`
          ).join('')}
        </select>
      </td>
      <td>${programName}</td>
      <td>${fileName}</td>
      <td>${source}</td>
      <td>${dest}</td>
      <td>${statusText}${retryHtml}</td>
      <td>
        <div class="progress-wrap" data-progress-key="${key}">
          <div class="progress">
            <div style="width:${percent}%"></div>
          </div>
          <div class="progress-text">${percent}%</div>
          <div class="progress-file">${percent > 0 ? fileName : ''}</div>
        </div>
      </td>
      <td>
        <button class="btn-cancel" data-id="${id}"
                style="padding:4px 8px;font-size:12px;background:#b42318;">
            取消
        </button>
    </td>
    `;
  }

  // ====== 比對差異：新增/更新/刪除 ======
  function renderTableDiff() {
    const selected = $selGroup.value || 'all';

    let list = allRows;
    if (selected !== 'all') {
      list = list.filter(r =>
        (r.sourceGroup || '').toUpperCase() === selected.toUpperCase()
      );
    }

    const newIds = new Set(list.map(r => r.historyId));
    const oldIds = new Set(rowMap.keys());

    // 新增或更新（依目前排序給 1,2,3... 編號）
    list.forEach((r, idx) => {
      const seq = idx + 1;
      renderOrUpdateRow(r, seq);
    });

    // 移除已不存在的 row
    for (const id of oldIds) {
      if (!newIds.has(id)) {
        const tr = rowMap.get(id);
        if (tr) tr.remove();
        rowMap.delete(id);
        progressState.delete(`TO-${id}`);
      }
    }

    // 依 list 的順序重排 <tr>，No. 就會跟順序對齊
    const frag = document.createDocumentFragment();
    list.forEach(r => {
      const tr = rowMap.get(r.historyId);
      if (tr) frag.appendChild(tr);
    });
    $tableBody.innerHTML = '';
    $tableBody.appendChild(frag);

    $count.textContent = list.length + ' 筆';
  }

  // ====== 抓 pending (不重畫整張 table) ======
  async function loadPending() {
    $btnReload.disabled = true;
    $btnReload.textContent = '載入中…';

    try {
      const resp = await fetch(`${API_PENDING}?take=200&ts=${Date.now()}`, {
        cache: 'no-store'
      });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      allRows = await resp.json();

      // 可選：先依 priority DESC, historyId ASC 排一次
      allRows.sort((a, b) => {
        const pa = (a.priority ?? 1);
        const pb = (b.priority ?? 1);
        if (pa !== pb) return pb - pa;
        return a.historyId - b.historyId;
      });

      renderTableDiff();  // 差異更新，不跳動
    } catch (err) {
      console.error(err);
      $tableBody.innerHTML =
        `<tr><td colspan="8" style="color:#c00;">載入失敗：${err.message}</td></tr>`;
    } finally {
      $btnReload.disabled = false;
      $btnReload.textContent = '重新整理';
    }
  }

  // ====== 進度更新：這裡用 "destKey" 原樣（很重要！不要再多加 TO-） ======
  function setProgressForKey(destKey, percent) {
    const key = String(destKey);  // 例如 "TO-7"
    const p = Math.max(0, Math.min(100, Math.round(percent || 0)));

    progressState.set(key, p);

    root.querySelectorAll(`.progress-wrap[data-progress-key="${key}"]`)
      .forEach(wrap => {
        const bar = wrap.querySelector('.progress > div');
        const txt = wrap.querySelector('.progress-text');
        if (bar) bar.style.width = p + '%';
        if (txt) txt.textContent = p + '%';

        const tr  = wrap.closest('tr');
        if (!tr) return;

        const statusCell = tr.children[6];
        const sel        = tr.querySelector('.pri-select');

        const isActive = p > 0 && p < 100;

        if (statusCell) {
          const current = statusCell.textContent || '';

          if (p >= 100) {
            statusCell.textContent = '完成';
          } else if (isActive) {
            statusCell.textContent = '執行中';
          } else {
            // p == 0：如果本來是「重試等待中」，就不要改回排隊中
            if (!current.startsWith('重試等待中')) {
              statusCell.textContent = '排隊中';
            }
          }
        }

        // 只有真的在跑的時候才鎖住優先級
        if (sel) {
          sel.disabled = isActive;
        }
      });

    if (p >= 100 && !hasScheduledReloadAfterDone) {
      hasScheduledReloadAfterDone = true;
      setTimeout(() => {
        loadPending().finally(() => {
          hasScheduledReloadAfterDone = false;
        });
      }, 1500);
    }
  }

//   function setCurrentFileForKey(destKey, fileName) {
//     const key = String(destKey);
//     root.querySelectorAll(`.progress-wrap[data-progress-key="${key}"]`)
//       .forEach(wrap => {
//         const el = wrap.querySelector('.progress-file');
//         if (el) el.textContent = fileName;
//       });
//   }
function setCurrentFileForKey(destKey, fileName) {
    const key = String(destKey);
    const p = progressState.get(key) ?? 0;

    // 只有進度 > 0 才顯示檔案名稱
    if (p <= 0) return;

    root.querySelectorAll(`.progress-wrap[data-progress-key="${key}"]`)
      .forEach(wrap => {
        const el = wrap.querySelector('.progress-file');
        if (el) el.textContent = fileName || '';
      });
  }
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('.btn-cancel');
    if (!btn) return;

    const historyId = Number(btn.dataset.id);
    if (!historyId) return;

    if (!confirm(`確定要取消 #${historyId} 嗎？`)) return;

    try {
        const resp = await fetch(`/jobs/${historyId}/cancel-hard`, {
        method: "POST"
        });

        if (!resp.ok) throw new Error(await resp.text());

        alert(`已取消 #${historyId}`);

        // 重新載入 pending → 這筆會消失
        loadPending();

    } catch (err) {
        alert('取消失敗：' + err.message);
    }
    });

  // ====== 優先級下拉選單變更 ======
  document.addEventListener('change', async (e) => {
    const sel = e.target;
    if (!sel.classList.contains('pri-select')) return;

    const historyId = Number(sel.dataset.id);
    const newValue  = Number(sel.value);
    if (!historyId || isNaN(newValue)) return;

    const row = allRows.find(r => r.historyId === historyId);
    const current = row?.priority ?? 1;

    if (newValue === current) return;

    if (newValue < 1 || newValue > 10) {
      alert('優先級範圍為 1～10');
      sel.value = String(current);
      return;
    }

    const delta = newValue - current;
    await adjustPriority(historyId, delta);
  });

  async function adjustPriority(historyId, delta) {
    try {
      const resp = await fetch('/jobs/priority', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ historyId, delta })
      });

      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(txt);
      }

      const result = await resp.json();

      if (typeof result.priority === 'number') {
        const row = allRows.find(r => r.historyId === historyId);
        if (row) {
          row.priority = result.priority;
        }
      }

      allRows.sort((a, b) => {
        const pa = (a.priority ?? 1);
        const pb = (b.priority ?? 1);
        if (pa !== pb) return pb - pa;
        return a.historyId - b.historyId;
      });

      renderTableDiff();
    } catch (err) {
      console.error(err);
      alert('更新優先級失敗：' + err.message);
    }
  }

  // ====== 並行數「套用」 ======
  $btnSetPar.addEventListener('click', async () => {
    const v = parseInt($selParallel.value, 10);
    if (isNaN(v)) return;

    try {
      const resp = await fetch(API_CONCUR, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(v)
      });

      if (!resp.ok) {
        const txt = await resp.text();
        alert('更新失敗：' + txt);
        return;
      }

      const data = await resp.json();
      alert('並行數已更新為：' + data.current + '\n新任務會用新的設定。');
    } catch (err) {
      console.error(err);
      alert('呼叫 API 失敗：' + err.message);
    }
  });

  // ====== SSE listener ======
  function startProgressListener() {
    let es;

    function connect() {
      es = new EventSource(API_EVENTS);
      if (statusLine) {
        statusLine.textContent = '（已連線進度事件）';
      }

      es.addEventListener('progress', (e) => {
        try {
          const jobs = JSON.parse(e.data);
          if (!Array.isArray(jobs)) return;

          for (const job of jobs) {
            if (!Array.isArray(job.targets)) continue;

            for (const t of job.targets) {
              if (!t.destId) continue;
              // ★ 這裡直接用 t.destId 當 key，不再額外加 "TO-"
              setProgressForKey(t.destId, t.percent);
              if ('currentFile' in t) {
                setCurrentFileForKey(t.destId, t.currentFile || '');
              }
            }
          }
        } catch (err) {
          console.warn('progress parse error', err);
        }
      });

      es.onerror = () => {
        if (statusLine) {
          statusLine.textContent = '（進度事件斷線，重試中…）';
        }
        try { es.close(); } catch {}
        setTimeout(connect, 1500);
      };
    }

    connect();
  }

  // ====== 事件 ======
  $btnReload.addEventListener('click', loadPending);
  $selGroup.addEventListener('change', renderTableDiff);
// ====== 自動刷新（例如每 5 秒） ======
  const AUTO_REFRESH_MS = 5000;
  let autoRefreshTimer = null;

  function startAutoRefresh() {
    if (autoRefreshTimer) return;
    autoRefreshTimer = setInterval(() => {
      // 這裡如果要嚴格一點，可以檢查 tab 是否在 active
      loadPending();
    }, AUTO_REFRESH_MS);
  }
  // ====== 啟動 ======
  loadPending();
  loadConcurrency();
  startProgressListener();
    // ⭐ 放在最後面
  window.addEventListener('pending-reload', () => {
    loadPending();   // ← 每次 tab 切換會重新刷新
  });
}
