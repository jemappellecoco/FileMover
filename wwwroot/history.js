// history.js
const API_HISTORY = '/history';

export function initHistory(root) {
  root.innerHTML = `
    <div class="toolbar">
      <label>狀態：
        <select id="selHistStatus">
          <option value="all">全部</option>
          <optgroup label="搬移 Move">
            <option value="11">搬移成功 (11)</option>
            <option value="911">搬移失敗－找不到來源 (911)</option>
            <option value="912">搬移失敗－檔案使用中 (912)</option>
            <option value="913">搬移失敗－權限不足 (913)</option>
            <option value="914">搬移失敗－找不到目的地 (914)</option>
            <option value="91">搬移失敗－其他 (91)</option>
          </optgroup>
          <optgroup label="刪除 Delete">
            <option value="12">刪除成功 (12)</option>
            <option value="921">刪除失敗－找不到來源 (921)</option>
            <option value="922">刪除失敗－檔案使用中 (922)</option>
            <option value="923">刪除失敗－權限不足 (923)</option>
            <option value="92">刪除失敗－其他 (92)</option>
          </optgroup>
          <optgroup label="其他">
            <option value="14">等待回遷 (14)</option>
            <option value="17">等待回遷 (17)</option>
            <option value="999">使用者取消 (999)</option>
            <option value="901">資料庫錯誤[From] (901)</option>
            <option value="902">資料庫錯誤[To] (902)</option>
            <option value="903">未設定restore (903)</option>
          </optgroup>
        </select>
      </label>

      <label>顯示筆數：
        <input id="inpHistTake" type="number" min="10" step="10" value="200" style="width:100px;">
      </label>

      <label>搜尋檔名(UserBit)：
        <input id="inpHistSearch" type="text" placeholder="輸入關鍵字，例如 2101EC3F" style="width:220px;">
      </label>

      <button id="btnHistReload">重新整理</button>
    
      <span id="histRowCount" class="muted"></span>
    </div>

    <table id="histTable">
      <thead>
        <tr>
          <th style="width:70px;">#</th>
          <th style="width:180px;">節目名稱</th>
          <th style="width:220px;">檔名(UserBit)</th>
          <th style="width:150px;">來源 Storage</th>
          <th style="width:150px;">目的 Storage</th>
          <th style="width:170px;">UpdateTime</th>
          <th style="width:100px;">Status</th>
        </tr>
      </thead>
      <tbody>
        <tr><td colspan="7" style="text-align:center;color:#999;">載入中…</td></tr>
      </tbody>
    </table>
  `;

  const $selStatus = root.querySelector('#selHistStatus');
  const $inpTake   = root.querySelector('#inpHistTake');
  const $inpSearch = root.querySelector('#inpHistSearch');
  const $btnReload = root.querySelector('#btnHistReload');
  const $tbody     = root.querySelector('#histTable tbody');
  const $count     = root.querySelector('#histRowCount');

  let allRows = [];
  let currentRows = [];
  let lastRenderSignature = '';

  function isErrorStatus(code) {
    const n = Number(code);
    return [
      91, 92,902,903,901,
      911, 912, 913, 914,
      921, 922, 923,
      999  // ★手動取消也可重試
    ].includes(n);
  }

  function fmtDate(s) {
    if (!s) return '';
    const d = new Date(s);
    if (isNaN(d)) return s;
    return d.toLocaleString();
  }

  function statusLabel(code) {
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

  function pill(label) {
    if (label.includes('成功'))
      return `<span class="status-pill ok">${label}</span>`;

    if (label.includes('失敗') || label.includes('取消')|| label.includes('錯誤'))
      return `<span class="status-pill fail">${label}</span>`;

    if (label.includes('等待'))
      return `<span class="status-pill pending">${label}</span>`;
    if (label.includes('未設定restore錯誤'))
      return `<span class="status-pill pending">${label}</span>`;
    return `<span class="status-pill">${label}</span>`;
  }

  function renderRows(rows) {
    if (!rows.length) {
      $tbody.innerHTML =
        `<tr><td colspan="7" style="text-align:center;color:#999;">（沒有符合條件的紀錄）</td></tr>`;
      $count.textContent = '0 筆';
      currentRows = [];
      return;
    }

    const frag = document.createDocumentFragment();
    rows.forEach((r, idx) => {
      const label = statusLabel(r.status);
      const canRetry = isErrorStatus(r.status);
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${idx + 1}</td>
        <td>${r.programName || ''}</td>
        <td>${r.fileName || ''}</td>
        <td>${r.sourceStorage || ''}</td>
        <td>${r.destStorage || ''}</td>
        <td>${fmtDate(r.updateTime)}</td>
        <td>${pill(label)}
         ${canRetry ? `<button class="btn-retry" data-id="${r.historyId}" style="margin-left:6px;padding:2px 8px;font-size:12px;">重試</button>` : ''}
        </td>
      `;
      frag.appendChild(tr);
    });
    $tbody.innerHTML = '';
    $tbody.appendChild(frag);
    $count.textContent = rows.length + ' 筆';
    currentRows = rows;
  }

  // ⭐ 只在資料真的有變化時才重畫
  function filterAndRender(force = false) {
    const st = $selStatus.value || 'all';
    const kw = ($inpSearch.value || '').trim().toLowerCase();

    let rows = allRows;

    if (st !== 'all') {
      rows = rows.filter(r => String(r.status) === st);
    }

    if (kw) {
      rows = rows.filter(r => ((r.fileName || '').toLowerCase().includes(kw)));
    }

    // 建立這次畫面的簽名（只抓幾個關鍵欄位）
    const signature = JSON.stringify(
      rows.map(r => [r.historyId, r.status])
    );

    if (!force && signature === lastRenderSignature) {
      // 沒變化就不重畫 → 不會閃
      return;
    }

    lastRenderSignature = signature;
    renderRows(rows);
  }

  // ⭐ silent=true：背景刷新，不清空表格、不顯示「載入中」
  async function loadHistory(silent = false) {
    if (!silent) {
      $btnReload.disabled = true;
      $btnReload.textContent = '載入中…';
      $tbody.innerHTML =
        `<tr><td colspan="7" style="text-align:center;color:#999;">載入中…</td></tr>`;
      $count.textContent = '';
    }

    const take = parseInt($inpTake.value || '200', 10) || 200;

    try {
      const resp = await fetch(`${API_HISTORY}?take=${take}&ts=${Date.now()}`, { cache:'no-store' });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const rows = await resp.json();
      allRows = Array.isArray(rows) ? rows : [];
      // 手動載入 / 初次載入：force = true
      // 自動刷新：force = false（沒變就不畫）
      filterAndRender(!silent);
    } catch (e) {
      console.error(e);
      if (!silent) {
        $tbody.innerHTML = `<tr><td colspan="7" style="color:#c00;">載入失敗：${e.message}</td></tr>`;
      }
    } finally {
      if (!silent) {
        $btnReload.disabled = false;
        $btnReload.textContent = '重新整理';
      }
    }
  }

  // ⭐ retry handler（綁 root）
  root.addEventListener("click", async (e) => {
    const btn = e.target.closest(".btn-retry");
    if (!btn) return;

    const historyId = Number(btn.dataset.id);
    if (!historyId) return;

    if (!confirm(`確定要重試這筆任務（HistoryId = ${historyId}）嗎？`)) return;

    try {
      const resp = await fetch(`/history/${historyId}/retry`, { method: 'POST' });
      if (!resp.ok) throw new Error(await resp.text());

      alert("重試任務已送出！");
      // retry 後用正常模式 reload 一次
      loadHistory(false);

    } catch (err) {
      alert("重試失敗：" + err.message);
    }
  });

  // === 事件綁定 ===
  $btnReload.addEventListener('click', () => loadHistory(false));
  $selStatus.addEventListener('change', () => filterAndRender(true));
  $inpTake.addEventListener('change', () => loadHistory(false));
  $inpSearch.addEventListener('input', () => filterAndRender(true));

  // 第一次載入：正常模式（會顯示載入中）
  loadHistory(false);

  // tab 切換時：正常 reload 一次
  window.addEventListener('history-reload', () => loadHistory(false));

  // ⭐ 每 5 秒靜默刷新：不清空畫面，只有有變化才重畫
  setInterval(() => loadHistory(true), 5000);
}
