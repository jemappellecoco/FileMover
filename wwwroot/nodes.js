// nodes.js
const API_SELF  = '/api/cluster/self';
const API_NODES = '/api/nodes';

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

  async function loadNodes() {
    statusLine.textContent = '載入節點狀態中...';

    try {
      const res = await fetch(API_NODES);
      if (!res.ok) throw new Error('讀取節點清單失敗');

      const data = await res.json(); // 就是你剛剛看到的陣列

      $body.innerHTML = data.map(n => `
        <tr class="${n.status === 'Online' ? 'row-online' : 'row-offline'}">
          <td>${n.nodeName}</td>
          <td>${n.role}</td>
          <td>${n.group}</td>
          <td>${n.status}</td>
          <td>${n.maxConcurrency}</td>
          <td>${n.currentRunning}</td>
          <td>${n.lastHeartbeat}</td>
          <td>${n.hostName ?? ''}</td>
          <td>${n.ipAddress ?? ''}</td>
        </tr>
      `).join('');

      statusLine.textContent = `節點數：${data.length}`;
    } catch (err) {
      console.error(err);
      statusLine.textContent = '載入節點狀態失敗';
    }
  }

  // ⭐ 入口：先確認自己是不是 Master
  (async () => {
    try {
      const self = await loadSelf();
      if (!self.isMaster) {
        // 不是 Master：只顯示訊息，不讓他用這頁（讀 / 寫）
        $info.textContent = `目前節點：${self.nodeName}（角色：${self.role}），不是 Master，無法使用節點管理。`;
        statusLine.textContent = '此頁僅 Master 可使用。';
        // 可以選擇把按鈕 disable 掉
        $btn.disabled = true;
        return;
      }

      // 是 Master：顯示提示，允許讀取 /api/nodes
      $info.textContent = `目前節點：${self.nodeName}（角色：${self.role}，樓層：${self.group}）`;
      $btn.addEventListener('click', loadNodes);
      loadNodes(); // 第一次載入

    } catch (err) {
      console.error(err);
      $info.textContent = '無法取得節點角色資訊（/api/cluster/self）。';
      statusLine.textContent = '節點管理初始化失敗。';
      $btn.disabled = true;
    }
  })();
}
