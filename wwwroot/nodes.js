// nodes.js
const API_SELF  = '/api/cluster/self';
const API_NODES = '/api/nodes';

const MAX_OPTIONS = [1,2,3,4,5,6,7,8,9,10];

// ⭐ 自動刷新設定
const AUTO_REFRESH_MS = 2000;   // 2 秒一次，你可調
let autoTimer = null;

// ⭐ 用來避免閃動：記住上次資料
let lastSignature = '';
let lastMap = new Map(); // nodeName -> last node dto

// ⭐ 避免使用者操作時被 refresh 蓋掉
let suppressUntil = 0;   // timestamp(ms)
function suppressRefresh(ms = 2000) {
  suppressUntil = Date.now() + ms;
}
function canAutoRefreshNow() {
  return Date.now() >= suppressUntil;
}

function normTime(t) {
  // 讓 signature 穩定：空值轉 ''
  return (t ?? '').toString();
}

function buildSignature(list) {
  // 只取會影響畫面的欄位，排序後做 signature
  const rows = [...list].sort((a,b) => (a.nodeName||'').localeCompare(b.nodeName||''));
  return rows.map(n => [
    n.nodeName, n.role, n.group, n.status,
    n.maxConcurrency, n.currentRunning,
    normTime(n.lastHeartbeat), n.hostName ?? '', n.ipAddress ?? ''
  ].join('|')).join(';;');
}

function ensureRow(n) {
  // 如果這個 node 還沒有 row，就插入一列（只在新增節點時發生）
  let tr = document.querySelector(`tr[data-node="${CSS.escape(n.nodeName)}"]`);
  if (tr) return tr;

  const tbody = document.querySelector('#nodesBody');
  tr = document.createElement('tr');
  tr.dataset.node = n.nodeName;
  tbody.appendChild(tr);
  tr.innerHTML = `
    <td class="c-node"></td>
    <td class="c-role"></td>
    <td class="c-group"></td>
    <td class="c-status"></td>
    <td class="c-max"></td>
    <td class="c-running"></td>
    <td class="c-hb"></td>
    <td class="c-host"></td>
    <td class="c-ip"></td>
  `;

  // maxConcurrency 欄位：select + button
  const maxCell = tr.querySelector('.c-max');
  maxCell.innerHTML = `
    <select class="sel-max" data-node="${n.nodeName}">
      ${MAX_OPTIONS.map(v => `<option value="${v}">${v}</option>`).join('')}
    </select>
    <button class="btn-save-max" data-node="${n.nodeName}">儲存</button>
  `;

  return tr;
}

function setText(el, text) {
  const v = text ?? '';
  if (el.textContent !== v) el.textContent = v;
}

function setRowClass(tr, status) {
  const online = status === 'Online';
  tr.classList.toggle('row-online', online);
  tr.classList.toggle('row-offline', !online);
}

// ⭐ 核心：diff 更新，不整個 innerHTML 重畫
function renderNodesDiff(list) {
  const incoming = new Map(list.map(n => [n.nodeName, n]));

  // 1) 更新/新增
  for (const n of list) {
    const tr = ensureRow(n);

    // row class
    setRowClass(tr, n.status);

    // cells
    setText(tr.querySelector('.c-node'), n.nodeName);
    setText(tr.querySelector('.c-role'), n.role);
    setText(tr.querySelector('.c-group'), n.group);

    // ✅ 狀態改成 pill（不要再 setText）
    const st = tr.querySelector('.c-status');
    const isOnline = (n.status === 'Online');
    const html = `<span class="pill ${isOnline ? 'pill-online' : 'pill-offline'}">${isOnline ? 'Online' : 'Offline'}</span>`;
    if (st.innerHTML !== html) st.innerHTML = html;

    setText(tr.querySelector('.c-running'), String(n.currentRunning ?? ''));
    setText(tr.querySelector('.c-hb'), normTime(n.lastHeartbeat));
    setText(tr.querySelector('.c-host'), n.hostName ?? '');
    setText(tr.querySelector('.c-ip'), n.ipAddress ?? '');

    // select：只有「使用者沒在操作這個 select」時才同步選取值
    const sel = tr.querySelector(`.sel-max[data-node="${n.nodeName}"]`);
    if (sel && document.activeElement !== sel) {
      const want = String(n.maxConcurrency ?? '');
      if (sel.value !== want) sel.value = want;
    }
  }

  // 2) 移除已不存在的 node
  for (const [oldName] of lastMap.entries()) {
    if (!incoming.has(oldName)) {
      const tr = document.querySelector(`tr[data-node="${CSS.escape(oldName)}"]`);
      if (tr) tr.remove();
    }
  }

  lastMap = incoming;
}

export function initNodes(root, statusLine) {
  root.innerHTML = `
    <div class="toolbar">
      <div id="nodesInfo" class="hint"></div>
      <button id="btnNodesReload">重新整理</button>
    </div>
    <table class="grid">
      <thead>
        <tr>
          <th>節點</th>
          <th>角色</th>
          <th>樓層</th>
          <th>狀態</th>
          <th>並行數上限</th>
          <th>目前執行</th>
          <th>最後心跳</th>
          <th>Host</th>
          <th>IP</th>
        </tr>
      </thead>
      <tbody id="nodesBody"></tbody>
    </table>
  `;

  const $info = root.querySelector('#nodesInfo');
  const $btn  = root.querySelector('#btnNodesReload');
  const $body = root.querySelector('#nodesBody');

  async function loadSelf() {
    const res = await fetch(API_SELF);
    if (!res.ok) throw new Error('讀取節點角色失敗');
    return await res.json(); // { nodeName, role, group, isMaster }
  }

  async function loadNodes({ silent = false } = {}) {
    if (!silent) statusLine.textContent = '載入節點狀態中...';

    try {
      const res = await fetch(API_NODES, { cache: 'no-store' });
      if (!res.ok) throw new Error('讀取節點清單失敗');

      const data = await res.json(); // NodeStatusDto[]

      const sig = buildSignature(data);
      if (sig === lastSignature) {
        // 沒變：不重畫，避免閃
        if (!silent) statusLine.textContent = `節點數：${data.length}`;
        return;
      }

      lastSignature = sig;
      renderNodesDiff(data);

      if (!silent) statusLine.textContent = `節點數：${data.length}`;
    } catch (err) {
      console.error(err);
      statusLine.textContent = '載入節點狀態失敗';
    }
  }

  function startAutoRefresh() {
    stopAutoRefresh();
    autoTimer = setInterval(() => {
      if (!canAutoRefreshNow()) return;
      // ⭐ silent 模式：不刷 statusLine，不造成跳動
      loadNodes({ silent: true });
    }, AUTO_REFRESH_MS);
  }

  function stopAutoRefresh() {
    if (autoTimer) clearInterval(autoTimer);
    autoTimer = null;
  }

  // ✅ 使用者操作 select / 點儲存時，短暫停自動刷新（避免被蓋掉）
  $body.addEventListener('focusin', (e) => {
    if (e.target.classList?.contains('sel-max')) suppressRefresh(4000);
  });

  $body.addEventListener('change', (e) => {
    if (e.target.classList?.contains('sel-max')) suppressRefresh(4000);
  });

  // ⭐ 點「儲存」按鈕 → 呼叫 PUT /api/nodes/{nodeName}/concurrency
  $body.addEventListener('click', async (e) => {
    const target = e.target;
    if (!target.classList.contains('btn-save-max')) return;

    suppressRefresh(5000);

    const nodeName = target.dataset.node;
    const $sel = $body.querySelector(`.sel-max[data-node="${nodeName}"]`);
    const max = parseInt($sel.value, 10);

    if (!Number.isFinite(max) || max <= 0) {
      alert('併發數必須 > 0');
      return;
    }

    try {
      statusLine.textContent = `儲存 ${nodeName} 的並行數中...`;

      const resp = await fetch(`${API_NODES}/${encodeURIComponent(nodeName)}/concurrency`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ maxConcurrency: max }),
      });

      if (!resp.ok) {
        console.error('UpdateConcurrency failed', resp.status);
        alert('更新並行數失敗');
        statusLine.textContent = '更新並行數失敗';
        return;
      }

      // 成功後強制載入一次（非 silent）
      await loadNodes({ silent: false });
      statusLine.textContent = `已更新節點 ${nodeName} 的並行數為 ${max}`;
    } catch (err) {
      console.error(err);
      alert('呼叫 UpdateConcurrency API 失敗');
      statusLine.textContent = '呼叫 UpdateConcurrency API 失敗';
    }
  });

  // ⭐ 入口：先確認自己是不是 Master
  (async () => {
    try {
      const self = await loadSelf();
      if (!self.isMaster) {
        $info.textContent = `目前節點：${self.nodeName}（角色：${self.role}），不是 Master，無法使用節點管理。`;
        statusLine.textContent = '此頁僅 Master 可使用。';
        $btn.disabled = true;
        $body.innerHTML = '';
        stopAutoRefresh();
        return;
      }

      $info.textContent = `目前節點：${self.nodeName}（角色：${self.role}，樓層：${self.group}）`;

      $btn.addEventListener('click', () => {
        suppressRefresh(2000);
        // 重新整理：強制刷一次（可把 lastSignature 清掉）
        lastSignature = '';
        loadNodes({ silent: false });
      });

      await loadNodes({ silent: false });
      startAutoRefresh();

    } catch (err) {
      console.error(err);
      $info.textContent = '無法取得節點角色資訊（/api/cluster/self）。';
      statusLine.textContent = '節點管理初始化失敗。';
      $btn.disabled = true;
      stopAutoRefresh();
    }
  })();
}
