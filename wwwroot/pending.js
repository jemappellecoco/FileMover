// pending.js
const API_PENDING   = '/jobs/pending';
const API_EVENTS    = '/api/progress/events';
const API_CONCUR    = '/api/config/concurrency';
const API_HISTORY   = '/history';  
export function initPending(root, statusLine) {
  // 建 HTML 結構（照你原本的樣式，只拿 toolbar + table）
  root.innerHTML = `
    <div class="toolbar">
      <button id="btnPendingReload">重新整理</button>

    <!-- <label>
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
      <button id="btnSetParallel">套用</button>-->

      <button id="btnCancelSelected"
        style="
          margin-left:12px;
          padding:6px 14px;
          font-size:14px;
          background:#b42318;
          color:white;
          border:none;
          border-radius:4px;
          cursor:pointer;
        ">
        取消任務
      </button>
      <span id="pendingCount" class="muted"></span>
    </div>

    <!-- 🔺 上半：Pending，自己的 scroll 區域 -->
    <div id="pendingPanel"
         style="
           margin-top:8px;
           height:360px;
           overflow:auto;
           border:1px solid #eee;
           border-radius:4px;
         ">
      <table id="pendingTable">
        <thead>
          <tr>
            <th style="width:40px;"><input type="checkbox" id="chkPendingAll" /></th>
            <th style="width:60px;">No.</th>
            <th style="width:60px;">優先級</th>
            <th style="width:180px;">節目名稱</th>
            <th style="width:100px;">檔名(UserBit)</th>
            <th style="width:80px;">來源</th>
            <th style="width:80px;">目的地</th>
            <th style="width:90px;">節點</th>
            <th style="width:220px;">狀態</th>
            <th style="width:220px;">進度</th>
            <th style="width:80px;">取消</th>
          </tr>
        </thead>

        <tbody>
          <tr><td colspan="10" style="text-align:center;color:#999;">載入中…</td></tr>
        </tbody>
      </table>
    </div>

    <!-- 🔻 下半：歷史區塊 -->
    <div id="pendingHistoryPanel"
         style="margin-top:24px;border-top:1px solid #ddd;padding-top:12px;">
      <h3 style="margin:0 0 8px;font-size:16px;">歷史紀錄 / 錯誤</h3>

      <div class="toolbar">
        <label>狀態：
          <select id="pendHistStatus">
            <option value="all">全部</option>
            <option value="success">成功</option>
            <option value="fail">失敗</option>
          </select>
        </label>


        <label>顯示筆數：
          <input id="pendHistTake" type="number" min="10" step="10" value="200" style="width:100px;">
        </label>

        <label>搜尋檔名(UserBit)：
          <input id="pendHistSearch" type="text" placeholder="輸入關鍵字，例如 2101EC3F" style="width:220px;">
        </label>

        <button id="btnPendHistReload">重新整理</button>

        <span id="pendHistRowCount" class="muted"></span>
      </div>

      <div style="height:260px; overflow:auto; margin-top:4px; border:1px solid #eee; border-radius:4px;">
        <table id="pendHistTable">
          <thead>
            <tr>
              <th style="width:70px;">#</th>
              <th style="width:180px;">節目名稱</th>
              <th style="width:220px;">檔名(UserBit)</th>
              <th style="width:150px;">來源 Storage</th>
              <th style="width:150px;">目的 Storage</th>
              <th style="width:120px;">節點</th>       
              <th style="width:170px;">UpdateTime</th>
              <th style="width:100px;">Status</th>
            </tr>
          </thead>
          <tbody>
            <tr><td colspan="8" style="text-align:center;color:#999;">載入中…</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;


  const $btnReload    = root.querySelector('#btnPendingReload');
  const $tableBody    = root.querySelector('#pendingTable tbody');
  const $count        = root.querySelector('#pendingCount');
  // const $selGroup     = root.querySelector('#selGroup');
  // const $selParallel  = root.querySelector('#selParallel');
  const $btnSetPar    = root.querySelector('#btnSetParallel');
  const $btnCancelSelected= root.querySelector('#btnCancelSelected');
  const $chkAll           = root.querySelector('#chkPendingAll');
  let hasScheduledReloadAfterDone = false;
    // ⭐ 下方歷史區塊的元素
  const $histStatus   = root.querySelector('#pendHistStatus');
  const $histTake     = root.querySelector('#pendHistTake');
  const $histSearch   = root.querySelector('#pendHistSearch');
  const $btnHistReload= root.querySelector('#btnPendHistReload');
  const $histTbody    = root.querySelector('#pendHistTable tbody');
  const $histCount    = root.querySelector('#pendHistRowCount');
  
  // === 全域狀態 ===
  let allRows = [];                 // 從 DB 撈到的完整 pending 清單
  const progressState = new Map();  // key → 百分比（key = "TO-7" 這種）
  const rowMap = new Map();         // HistoryId → <tr>
  let isSelectBusy = false;         // ⭐ 使用者是否正在操作某個 select
  const selectedIds = new Set();


  // ⭐ 歷史區塊的狀態
  let histAllRows = [];
  let histCurrentRows = [];
  let histLastRenderSignature = '';
  let pendingLastRenderSignature = '';
  function isHistErrorStatus(code) {
    const n = Number(code);
    return [
      91, 92, 901, 902, 903,
      911, 912, 913, 914,
      921, 922, 923,
      999
    ].includes(n);
  }

  function histFmtDate(s) {
    if (!s) return '';
    const d = new Date(s);
    if (isNaN(d)) return s;
    return d.toLocaleString();
  }

  function histStatusLabel(code) {
    const n = Number(code);
    if (n === 11) return '搬移成功';
    if (n === 12) return '刪除成功';
    if (n === 14 || n === 17) return '等待回遷';
    if (String(n).startsWith('91')) return '搬移失敗';
    if (String(n).startsWith('92')) return '刪除失敗';
    if (n === 999) return '使用者取消';
    if (n === 901) return '資料庫錯誤 [From]';
    if (n === 902) return '資料庫錯誤 [To]';
    if (n === 903) return '未設定restore錯誤';
    return String(code ?? '');
  }

  function histPill(label, tooltip) {
  const safeTip = tooltip
    ? String(tooltip).replace(/"/g, '&quot;')
    : '';

  let cls = 'status-pill';

  if (label.includes('成功')) {
    cls += ' ok';
  } else if (label.includes('失敗') || label.includes('取消') || label.includes('錯誤')) {
    cls += ' fail';
  } else if (label.includes('等待') || label.includes('未設定restore錯誤')) {
    cls += ' pending';
  }

  return `<span class="${cls}" title="${safeTip}">${label}</span>`;
}

  function histRenderRows(rows) {
    if (!$histTbody) return;

    if (!rows.length) {
      $histTbody.innerHTML =
        `<tr><td colspan="8" style="text-align:center;color:#999;">（沒有符合條件的紀錄）</td></tr>`;
      if ($histCount) $histCount.textContent = '0 筆';
      histCurrentRows = [];
      return;
    }

    const frag = document.createDocumentFragment();
    rows.forEach((r, idx) => {
      const label   = histStatusLabel(r.status);      // pill 顯示的：搬移失敗 / 刪除成功 ...
      const detail  = r.statusText || label;         // 後端給的中文細項（例如：搬移失敗－檔案使用中）
      const tooltip = `${r.status} - ${detail}`;     // 例如：912 - 搬移失敗－檔案使用中

      const canRetry = isHistErrorStatus(r.status);

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${idx + 1}</td>
        <td>${r.programName || ''}</td>
        <td>${r.fileName || ''}</td>
        <td>${r.sourceStorage || ''}</td>
        <td>${r.destStorage || ''}</td>
        <td>${r.assignedNode || '-'}</td>
        <td>${histFmtDate(r.updateTime)}</td>
        <td>
          ${histPill(label, tooltip)}
          ${canRetry ? `
            <button class="pend-hist-retry"
                    data-id="${r.historyId}"
                    style="margin-left:6px;padding:2px 8px;font-size:12px;">
              重試
            </button>` : ''}
        </td>
      `;
      frag.appendChild(tr);
    });

    $histTbody.innerHTML = '';
    $histTbody.appendChild(frag);
    if ($histCount) $histCount.textContent = rows.length + ' 筆';
    histCurrentRows = rows;
  }

  // 依狀態 / 樓層 / 關鍵字 過濾
  function histFilterAndRender(force = false) {
    if (!$histStatus || !$histSearch) return;

    const st    = $histStatus.value || 'all';
    const kw    = ($histSearch.value || '').trim().toLowerCase();
    // const group = $selGroup ? ($selGroup.value || 'all') : 'all';

    let rows = histAllRows;

    if (st !== 'all') {
  if (st === 'success') {
    // 成功：搬移成功 + 刪除成功
    rows = rows.filter(r => r.status === 11 || r.status === 12);
  } else if (st === 'fail') {
    // 失敗：全部錯誤 + 取消
    rows = rows.filter(r => isHistErrorStatus(r.status));
    // isHistErrorStatus 已經包含：
    // 91,92,901,902,903,911,912,913,914,921,922,923,999
  }
}

    // if (group !== 'all') {
    //   rows = rows.filter(r =>
    //     (r.sourceGroup || '').toUpperCase() === group.toUpperCase()
    //   );
    // }

    if (kw) {
      rows = rows.filter(r =>
        (r.fileName || '').toLowerCase().includes(kw)
      );
    }

    const signature = JSON.stringify(rows.map(r => [r.historyId, r.status]));
    if (!force && signature === histLastRenderSignature) {
      return;
    }
    histLastRenderSignature = signature;
    histRenderRows(rows);
  }

  async function loadHistoryInPending(silent = false) {
    if (!$histTbody) return;

    if (!silent) {
      if ($btnHistReload) {
        $btnHistReload.disabled = true;
        $btnHistReload.textContent = '載入中…';
      }
      $histTbody.innerHTML =
        `<tr><td colspan="8" style="text-align:center;color:#999;">載入中…</td></tr>`;
      if ($histCount) $histCount.textContent = '';
    }

    const take = $histTake
      ? (parseInt($histTake.value || '200', 10) || 200)
      : 200;

    try {
      const resp = await fetch(
  `${API_HISTORY}?group=current&take=${take}&ts=${Date.now()}`,
  { cache: 'no-store' }
);

      // const resp = await fetch(`${API_HISTORY}?take=${take}&ts=${Date.now()}`, {
      //   cache: 'no-store'
      // });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);

      const rows = await resp.json();
      histAllRows = Array.isArray(rows) ? rows : [];
      histFilterAndRender(!silent);
    } catch (e) {
      console.error(e);
      if (!silent) {
        $histTbody.innerHTML =
          `<tr><td colspan="8" style="color:#c00;">載入失敗：${e.message}</td></tr>`;
      }
    } finally {
      if (!silent && $btnHistReload) {
        $btnHistReload.disabled = false;
        $btnHistReload.textContent = '重新整理';
      }
    }
  }
  
    // 全選 / 取消全選
  if ($chkAll) {
    $chkAll.addEventListener('change', () => {
      const checked = $chkAll.checked;
      const checks = Array.from(root.querySelectorAll('.chk-pending'));

      checks.forEach(chk => {
        chk.checked = checked;
        const id = Number(chk.dataset.id);
        if (!id) return;

        if (checked) {
          selectedIds.add(id);
        } else {
          selectedIds.delete(id);
        }
      });

      $chkAll.indeterminate = false;
    });
  }

  // ====== 偵測使用者開始操作任一個 select（優先級 / 樓層 / 並行數） ======
  root.addEventListener('mousedown', (e) => {
    const target = e.target;
    if (!target) return;

    if (target.classList.contains('pri-select') 
      // ||
        // target === $selGroup ||
        // target === $selParallel
      ) {
      isSelectBusy = true;
    }
  });

  // 點到非 select 的地方，也可以順便解除 busy
  root.addEventListener('click', (e) => {
    const t = e.target;
    if (!t) return;

    if (!t.classList.contains('pri-select') 
      // &&
        // t !== $selGroup &&
        // t !== $selParallel
      ) {
      isSelectBusy = false;
    }
  });


    // 歷史 reload / filter
  if ($btnHistReload) {
    $btnHistReload.addEventListener('click', () => {
      if ($histSearch) $histSearch.value = '';
      histLastRenderSignature = '';
      loadHistoryInPending(false);
    });
  }
  if ($histStatus) {
    $histStatus.addEventListener('change', () => histFilterAndRender(true));
  }
  if ($histTake) {
    $histTake.addEventListener('change', () => loadHistoryInPending(false));
  }
  if ($histSearch) {
    $histSearch.addEventListener('input', () => histFilterAndRender(true));
  }

  // ⭐ 歷史重試按鈕（綁在 root，避免衝到上面 pending 的 click）
  root.addEventListener('click', async (e) => {
    const btn = e.target.closest('.pend-hist-retry');
    if (!btn) return;

    const historyId = Number(btn.dataset.id);
    if (!historyId) return;

    if (!confirm(`確定要重試 #${historyId} 嗎？`)) return;

    try {
      const resp = await fetch(`/history/${historyId}/retry`, { method: 'POST' });
      if (!resp.ok) throw new Error(await resp.text());

      alert('重試任務已送出！');
      loadPending(false);
      loadHistoryInPending(false);
    } catch (err) {
      alert('重試失敗：' + err.message);
    }
  });


  // ====== 讀 /api/config/concurrency，套用到「並行數」下拉 ======
  // async function loadConcurrency() {
  //   try {
  //     const resp = await fetch(API_CONCUR, { cache: 'no-store' });
  //     if (!resp.ok) return;

  //     const data = await resp.json();   // 期待 { current: 2 }
  //     if (typeof data.current === 'number') {
  //       const v = String(data.current);

  //       // 如果下拉沒有這個值，就動態補一個 option
  //       const hasOption = Array.from($selParallel.options).some(o => o.value === v);
  //       if (!hasOption) {
  //         const opt = document.createElement('option');
  //         opt.value = v;
  //         opt.textContent = v;
  //         $selParallel.appendChild(opt);
  //       }

  //       $selParallel.value = v;
  //     }
  //   } catch (err) {
  //     console.warn('loadConcurrency error', err);
  //   }
  // }
  function getRealNameFromPath(path) {
      if (!path) return '';
      return String(path).split(/[/\\]/).pop() || '';
    }
  // ====== 建立/更新單筆 row（不重畫整張表） ======
  function renderOrUpdateRow(r, seq) {
    const id   = r.historyId;
    const key  = `TO-${id}`;          // row 的 key 是 "TO-<HistoryId>"
    const existing = rowMap.get(id);

    const programName = r.programName || '';
    const fileName    = r.fileName || '';
    const source      = r.sourceStorage || r.sourcePath || '';
    const dest        = r.destStorage   || r.destPath   || '';
    // 節點名稱
    const node        = r.assignedNode || '';
    const statusCode  = r.status;
    const retryCount   = typeof r.retryCount === 'number' ? r.retryCount : 0;
    const retryCode    = (typeof r.retryCode === 'number' ? r.retryCode : null);
    const retryMessage = r.retryMessage || '';

    const percent     = progressState.get(key) ?? 0;

    const priority = (typeof r.priority === 'number' && !isNaN(r.priority))
      ? r.priority
      : 1;
    const isChecked = selectedIds.has(id);
    const hasActiveProgress = percent > 0 && percent < 100;
    let statusText = hasActiveProgress ? '執行中' : '排隊中';
    let retryHtml  = '';
    const isActive = hasActiveProgress;

    const isPhase2 = (statusCode === 24 || statusCode === 27);
    const tagHtml  = isPhase2 ? '<span class="tag-badge">回遷</span>' : '';
    // const realFileName = '';
    // ⭐ 先保留舊的檔名，避免每次重畫把它洗掉
let realFileName = '';
if (existing) {
  const oldFileEl = existing.querySelector('.progress-file');
  if (oldFileEl) {
    realFileName = oldFileEl.textContent || '';
  }
}
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
    <td>
        <input type="checkbox"
               class="chk-pending"
               data-id="${id}"
               ${isChecked ? 'checked' : ''} />
      </td>
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
      <td>${node || '-'}</td>
      <td>${statusText}${retryHtml}</td>
      <td>
        <div class="progress-wrap" data-progress-key="${key}">
          <div class="progress">
            <div style="width:${percent}%"></div>
          </div>
          <div class="progress-text">${percent}%</div>
          <div class="progress-file">${escapeHtml(realFileName)}</div>
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
  function escapeHtml(text) {
  if (!text) return '';
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
  function renderTableDiff() {
    // const selected = $selGroup.value || 'all';

    let list = allRows;   // 直接用全部，不再分樓層
 const signature = JSON.stringify(
    list.map(r => [
      r.historyId,
      r.status,
      r.priority ?? 1,
      r.retryCount ?? 0,
      r.retryCode ?? null
    ])
  );
  if (signature === pendingLastRenderSignature) {
    return;
  }
  pendingLastRenderSignature = signature;
    const newIds = new Set(list.map(r => r.historyId));
    const oldIds = new Set(rowMap.keys());
     // ⭐ 計算這次列表的「簽名」，只看會影響畫面的欄位
 

  
    list.forEach((r, idx) => {
      const seq = idx + 1;
      renderOrUpdateRow(r, seq);
    });

   
    // 移除已不存在的 row
        // 把已經不存在的任務從 selectedIds 移除
    for (const id of Array.from(selectedIds)) {
      if (!newIds.has(id)) {
        selectedIds.delete(id);
      }
    }

    // 更新「全選」checkbox 的勾選 / indeterminate 狀態
    if ($chkAll) {
      if (list.length === 0) {
        $chkAll.checked = false;
        $chkAll.indeterminate = false;
      } else {
        const selectedCount = list.filter(r => selectedIds.has(r.historyId)).length;

        if (selectedCount === 0) {
          $chkAll.checked = false;
          $chkAll.indeterminate = false;
        } else if (selectedCount === list.length) {
          $chkAll.checked = true;
          $chkAll.indeterminate = false;
        } else {
          $chkAll.checked = false;
          $chkAll.indeterminate = true;   // 部份選取
        }
      }
    }

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
  async function loadPending(isAuto = false) {
    // ⭐ 自動刷新 & 使用者正在操作 select → 跳過這次
    if (isAuto && isSelectBusy) {
      return;
    }

    // $btnReload.disabled = true;
    // $btnReload.textContent = '載入中…';

    try {
      const resp = await fetch(`${API_PENDING}?take=200&ts=${Date.now()}`, {
        cache: 'no-store'
      });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      allRows = await resp.json();

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
        `<tr><td colspan="10" style="color:#c00;">載入失敗：${err.message}</td></tr>`;
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

        const statusCell = tr.children[8];
        const sel        = tr.querySelector('.pri-select');

        const isActive = p > 0 && p < 100;

        if (statusCell) {
          const current = statusCell.textContent || '';

          if (p >= 100) {
            statusCell.textContent = '完成';
          } else if (isActive) {
            statusCell.textContent = '執行中';
          } else {
            if (!current.startsWith('重試等待中')) {
              statusCell.textContent = '排隊中';
            }
          }
        }

        if (sel) {
          sel.disabled = isActive;
        }
      });

    if (p >= 100 && !hasScheduledReloadAfterDone) {
      hasScheduledReloadAfterDone = true;
      setTimeout(() => {
        loadPending(true).finally(() => {   // ⭐ 自動刷新
          hasScheduledReloadAfterDone = false;
        });
      }, 1500);
    }
  }

  function setCurrentFileForKey(destKey, fileName) {
    const key = String(destKey);
    const p = progressState.get(key) ?? 0;

    if (p <= 0) return;

    root.querySelectorAll(`.progress-wrap[data-progress-key="${key}"]`)
      .forEach(wrap => {
        const el = wrap.querySelector('.progress-file');
        if (el) el.textContent = fileName || '';
      });
  }
    // ====== 多選取消 ======
  if ($btnCancelSelected) {
    $btnCancelSelected.addEventListener('click', async () => {
      // 收集所有勾選的 historyId
       const ids = Array.from(selectedIds);;

      if (ids.length === 0) {
        alert('請先勾選要取消的任務');
        return;
      }

      if (!confirm(`確定要取消已勾選的 ${ids.length} 筆任務嗎？`)) {
        return;
      }

      let okCount = 0;
      let failCount = 0;
      let failMsgs = [];

      for (const id of ids) {
        try {
          const resp = await fetch(`/jobs/${id}/cancel-hard`, { method: 'POST' });
          if (!resp.ok) {
            const txt = await resp.text();
            failCount++;
            failMsgs.push(`#${id}：${txt}`);
          } else {
            okCount++;
          }
        } catch (err) {
          failCount++;
          failMsgs.push(`#${id}：${err.message}`);
        }
      }

      let msg = `已成功取消 ${okCount} 筆`;
      if (failCount > 0) {
        msg += `，失敗 ${failCount} 筆。\n\n${failMsgs.join('\n')}`;
      }
      alert(msg);

       // 取消完成後清空選取，重新載入列表
      selectedIds.clear();
      if ($chkAll) {
        $chkAll.checked = false;
        $chkAll.indeterminate = false;
      }
      loadPending(false);
    });
  }
  // ====== 單筆 checkbox 勾選 / 取消 ======
  root.addEventListener('change', (e) => {
    const target = e.target;
    if (!target || !target.classList || !target.classList.contains('chk-pending')) return;

    const id = Number(target.dataset.id);
    if (!id) return;

    if (target.checked) {
      selectedIds.add(id);
    } else {
      selectedIds.delete(id);
    }

    // 更新全選勾勾
    if ($chkAll) {
      const checks = Array.from(root.querySelectorAll('.chk-pending'));
      const checkedCount = checks.filter(c => c.checked).length;

      if (checkedCount === 0) {
        $chkAll.checked = false;
        $chkAll.indeterminate = false;
      } else if (checkedCount === checks.length) {
        $chkAll.checked = true;
        $chkAll.indeterminate = false;
      } else {
        $chkAll.checked = false;
        $chkAll.indeterminate = true;
      }
    }
  });
  // ====== 取消按鈕 ======
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
      loadPending(false);   // 手動刷新
    } catch (err) {
      alert('取消失敗：' + err.message);
    }
  });

  // ====== 優先級下拉選單變更 ======
  document.addEventListener('change', async (e) => {
    const sel = e.target;
    if (!sel.classList.contains('pri-select')) return;

    isSelectBusy = false;  // ⭐ 選完優先級 → 解鎖

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

  // // 並行數 change：也順便解除 busy（選完了）
  // $selParallel.addEventListener('change', () => {
  //   isSelectBusy = false;
  // });

  // ====== 並行數「套用」 ======
  // $btnSetPar.addEventListener('click', async () => {
  //   const v = parseInt($selParallel.value, 10);
  //   if (isNaN(v)) return;

  //   try {
  //     const resp = await fetch(API_CONCUR, {
  //       method: 'POST',
  //       headers: { 'Content-Type': 'application/json' },
  //       body: JSON.stringify(v)
  //     });

  //     if (!resp.ok) {
  //       const txt = await resp.text();
  //       alert('更新失敗：' + txt);
  //       return;
  //     }

  //     const data = await resp.json();
  //     alert('並行數已更新為：' + data.current + '\n新任務會用新的設定。');
  //   } catch (err) {
  //     console.error(err);
  //     alert('呼叫 API 失敗：' + err.message);
  //   }
  // });

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
              setProgressForKey(t.destId, t.percent);
              // 2) 如果有帶目前檔案路徑，就顯示副檔名
        if (t.currentFile) {                     // ← 或 t.fileName，看你後端欄位
          const name = getRealNameFromPath(t.currentFile);
          setCurrentFileForKey(t.destId, name);
            }
          }}
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



  // ====== 自動刷新（例如每 5 秒） ======
  const AUTO_REFRESH_MS = 5000;
  let autoRefreshTimer = null;

  function startAutoRefresh() {
    if (autoRefreshTimer) return;
    autoRefreshTimer = setInterval(() => {
      loadPending(true);   // ⭐ 自動刷新
    }, AUTO_REFRESH_MS);
  }

  // ====== 啟動 ======
  loadPending(false);
  // loadConcurrency();
  startProgressListener();
  startAutoRefresh();
// ⭐ 歷史區塊初次載入 + 每 5 秒靜默刷新
  loadHistoryInPending(false);
  setInterval(() => {
    loadHistoryInPending(true);
  }, 5000);
 
  
  // tab 切換回來時：手動刷新一次
  window.addEventListener('pending-reload', () => {
    // 🔼 上半部 Pending：重新載入
    loadPending(false);

    // 🔽 下半部歷史：清空搜尋 & 狀態，強制重畫
    if ($histSearch) $histSearch.value = '';
    if ($histStatus) $histStatus.value = 'all';
    if ($histTake)   $histTake.value   = '200';
    histLastRenderSignature = '';   // 讓下一次 filter 一定會重畫
    loadHistoryInPending(false);

    // （可選）清掉多選的勾勾
    selectedIds.clear();
    if ($chkAll) {
      $chkAll.checked = false;
      $chkAll.indeterminate = false;
    }
  });
}
