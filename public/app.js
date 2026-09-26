const $ = (selector) => document.querySelector(selector);
const state = { status: null, objects: [], busy: false };

const escapeHtml = (value = '') => String(value).replace(/[&<>'"]/g, (char) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;',
}[char]));

const formatBytes = (bytes = 0) => {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`;
};

const relativeTime = (iso) => {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 10) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
};

async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || `Request failed (${response.status})`);
  }
  return response.json();
}

function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result).split(',', 2)[1]);
    reader.onerror = () => reject(new Error('Unable to read the selected file'));
    reader.readAsDataURL(file);
  });
}

function toast(title, detail = '', error = false) {
  const element = document.createElement('div');
  element.className = `toast${error ? ' error' : ''}`;
  element.innerHTML = `<span>${error ? '!' : '✓'}</span><div><strong>${escapeHtml(title)}</strong><small>${escapeHtml(detail)}</small></div>`;
  $('#toast-region').append(element);
  setTimeout(() => element.remove(), 4200);
}

function normalizePolicyControls() {
  const replication = $('[name="replicationFactor"]');
  const read = $('[name="readQuorum"]');
  const write = $('[name="writeQuorum"]');
  const replicas = Number(replication.value);
  const minimum = Math.floor(replicas / 2) + 1;
  if (Number(read.value) < minimum) read.value = String(minimum);
  if (Number(write.value) < minimum) write.value = String(minimum);
  for (const option of [...read.options, ...write.options]) option.hidden = Number(option.value) > replicas;
}

async function refresh({ quiet = true } = {}) {
  try {
    const [status, objects] = await Promise.all([request('/api/status'), request('/api/objects')]);
    state.status = status;
    state.objects = objects.objects;
    render();
  } catch (error) {
    if (!quiet) toast('Unable to reach Vault', error.message, true);
  }
}

function render() {
  const status = state.status;
  if (!status) return;
  $('#cluster-health').textContent = status.health;
  $('#object-count').textContent = status.objectCount.toLocaleString();
  $('#logical-bytes').textContent = formatBytes(status.logicalBytes);
  const overhead = status.logicalBytes ? status.physicalBytes / status.logicalBytes : 0;
  $('#storage-overhead').textContent = `${overhead.toFixed(1)}× physical`;
  $('#capacity-fill').style.width = `${Math.min(100, Math.max(7, status.physicalBytes / 1000000 * 100))}%`;
  const online = status.nodes.filter((node) => node.status === 'healthy').length;
  $('#availability').textContent = `${(online / status.nodes.length * 100).toFixed(online === status.nodes.length ? 0 : 1)}%`;
  $('#recovery-time').textContent = `${status.metrics.lastRepairMs} ms`;
  $('#repair-count').textContent = status.metrics.repairs;
  $('#failure-count').textContent = status.metrics.failures;
  $('#policy-rf').textContent = status.policy.replicationFactor;
  $('#policy-rq').textContent = status.policy.readQuorum;
  $('#policy-wq').textContent = status.policy.writeQuorum;
  $('#storage-runtime').innerHTML = `<b>${status.storage?.processCount || status.nodes.length}</b> ${status.storage?.mode === 'http' ? 'HTTP node processes' : 'local node folders'}`;
  $('#gcs-status').textContent = status.googleCloud.enabled ? `GCS mirror · ${status.googleCloud.bucket}` : 'GCS mirror available';
  $('#availability-dots').innerHTML = Array.from({ length: 18 }, () => '<i></i>').join('');
  renderNodes(status.nodes);
  renderObjects($('#object-search').value);
  renderEvents(status.events);
}

function renderNodes(nodes) {
  const nodeCard = (node) => {
    const fill = Math.min(100, Math.max(5, node.usedBytes / 300000 * 100));
    const next = node.status === 'healthy' ? 'offline' : 'healthy';
    return `<button class="node-card ${escapeHtml(node.status)}" data-node-id="${escapeHtml(node.id)}" data-next-status="${next}" aria-label="${escapeHtml(node.id)} is ${escapeHtml(node.status)}. Click to ${next === 'healthy' ? 'heal' : 'take offline'}.">
      <span class="node-top"><span class="node-name"><i class="status-light"></i>${escapeHtml(node.id)}</span><small>${escapeHtml(node.status)}</small></span>
      <small>${escapeHtml(node.zone)} · ${node.latencyMs} ms latency</small>
      <span class="node-foot"><span>${node.replicas} replicas</span><span>${formatBytes(node.usedBytes)}</span></span>
      <span class="node-bar"><i style="width:${fill}%"></i></span>
    </button>`;
  };
  $('#node-grid').innerHTML = ['zone-1', 'zone-2', 'zone-3'].map((zone) => `<div class="zone-column">${nodes.filter((node) => node.zone === zone).map(nodeCard).join('')}</div>`).join('');
}

function renderObjects(query = '') {
  const normalized = query.toLowerCase().trim();
  const objects = state.objects.filter((object) => object.key.toLowerCase().includes(normalized));
  $('#object-table').innerHTML = objects.map((object) => `<tr>
    <td><span class="object-name"><span>◇</span><strong title="${escapeHtml(object.key)}">${escapeHtml(object.key)}</strong></span></td>
    <td><span class="health-tag ${escapeHtml(object.health)}"><i></i>${escapeHtml(object.health)}</span></td>
    <td>${formatBytes(object.size)}</td><td>v${object.version}</td><td>${object.replicas.length}/${object.policy.replicationFactor}</td>
    <td>${relativeTime(object.updatedAt)}</td><td><a class="download-link" href="/api/objects/${encodeURIComponent(object.key)}" title="Retrieve ${escapeHtml(object.key)}">↓</a></td>
  </tr>`).join('');
  $('#empty-state').classList.toggle('hidden', state.objects.length > 0);
}

const eventSymbol = (type) => ({ write: '＋', read: '↓', failure: '!', corruption: '!', repair: '✓', scan: '⌁', rebalance: '↻', recovery: '↑', cluster: '◇' }[type] || '·');

function renderEvents(events) {
  $('#event-list').innerHTML = events.slice(0, 8).map((event) => `<div class="event">
    <span class="event-icon">${eventSymbol(event.type)}</span><div><strong>${escapeHtml(event.title)}</strong><small>${escapeHtml(event.detail)}</small></div><time datetime="${escapeHtml(event.at)}">${relativeTime(event.at)}</time>
  </div>`).join('') || '<div class="empty-state"><p>Cluster events will appear here.</p></div>';
}

async function setNode(nodeId, status) {
  await request(`/api/nodes/${encodeURIComponent(nodeId)}`, { method: 'PATCH', body: JSON.stringify({ status }) });
  toast(status === 'healthy' ? 'Node recovered' : `Node ${status}`, `${nodeId} membership updated`);
  await refresh();
}

async function withBusy(action) {
  if (state.busy) return;
  state.busy = true;
  try { await action(); }
  catch (error) { toast('Operation failed', error.message, true); }
  finally { state.busy = false; }
}

$('#node-grid').addEventListener('click', (event) => {
  const card = event.target.closest('[data-node-id]');
  if (card) withBusy(() => setNode(card.dataset.nodeId, card.dataset.nextStatus));
});

$('#upload-button').addEventListener('click', () => $('#upload-dialog').showModal());
$('#file-input').addEventListener('change', () => {
  const file = $('#file-input').files[0];
  $('#file-label').textContent = file ? `${file.name} · ${formatBytes(file.size)}` : 'or drop it here · up to 30 MB';
  if (file && !$('#object-key').value) $('#object-key').value = file.name;
});

for (const selector of ['[name="replicationFactor"]', '[name="readQuorum"]', '[name="writeQuorum"]']) {
  $(selector).addEventListener('change', normalizePolicyControls);
}

const dropZone = $('#drop-zone');
for (const name of ['dragenter', 'dragover']) dropZone.addEventListener(name, (event) => { event.preventDefault(); dropZone.classList.add('dragging'); });
for (const name of ['dragleave', 'drop']) dropZone.addEventListener(name, (event) => { event.preventDefault(); dropZone.classList.remove('dragging'); });
dropZone.addEventListener('drop', (event) => {
  const transfer = new DataTransfer();
  if (event.dataTransfer.files[0]) transfer.items.add(event.dataTransfer.files[0]);
  $('#file-input').files = transfer.files;
  $('#file-input').dispatchEvent(new Event('change'));
});

$('#upload-form').addEventListener('submit', (event) => {
  event.preventDefault();
  withBusy(async () => {
    const form = new FormData(event.currentTarget);
    const file = form.get('file');
    if (!(file instanceof File) || !file.size) throw new Error('Choose a file to store');
    if (file.size > 30 * 1024 * 1024) throw new Error('Demo uploads are limited to 30 MB');
    normalizePolicyControls();
    const replicationFactor = Number(form.get('replicationFactor'));
    const readQuorum = Number(form.get('readQuorum'));
    const writeQuorum = Number(form.get('writeQuorum'));
    if (readQuorum + writeQuorum <= replicationFactor) throw new Error('Unsafe quorum: read + write must be greater than replicas');
    const data = await fileToBase64(file);
    const body = {
      key: form.get('key'), data, contentType: file.type || 'application/octet-stream', storageClass: form.get('storageClass'),
      policy: { replicationFactor, readQuorum, writeQuorum },
    };
    const result = await request('/api/objects', { method: 'POST', body: JSON.stringify(body) });
    $('#upload-dialog').close();
    event.currentTarget.reset();
    $('#file-label').textContent = 'or drop it here · up to 30 MB';
    toast('Object protected', `${result.object.key} · ${result.object.durability} replicas`);
    await refresh();
  });
});

$('#seed-button').addEventListener('click', () => withBusy(async () => {
  const result = await request('/api/demo/seed', { method: 'POST' });
  toast('Demo dataset loaded', `${result.seeded} objects replicated across the cluster`);
  await refresh();
}));

$('#scan-button').addEventListener('click', () => withBusy(async () => {
  const { report } = await request('/api/integrity-scan', { method: 'POST' });
  toast('Integrity scan complete', `${report.checked} checked · ${report.repaired} repaired · ${report.durationMs} ms`);
  await refresh();
}));

$('#heal-button').addEventListener('click', () => withBusy(async () => {
  const unhealthy = state.status.nodes.filter((node) => node.status !== 'healthy');
  await Promise.all(unhealthy.map((node) => request(`/api/nodes/${encodeURIComponent(node.id)}`, { method: 'PATCH', body: JSON.stringify({ status: 'healthy' }) })));
  await request('/api/integrity-scan', { method: 'POST' });
  toast('Cluster healed', `${unhealthy.length} nodes restored and replicas verified`);
  await refresh();
}));

$('#fail-node-button').addEventListener('click', () => withBusy(async () => {
  const node = state.status.nodes.find((item) => item.status === 'healthy' && item.replicas > 0) || state.status.nodes.find((item) => item.status === 'healthy');
  if (!node) throw new Error('No healthy node is available');
  if (state.status.storage?.mode === 'http') {
    await request('/api/faults/crash-node', { method: 'POST', body: JSON.stringify({ nodeId: node.id }) });
    toast('Storage process crashed', `${node.id} endpoint is unavailable; quorum reads continue`);
    await refresh();
  } else {
    await setNode(node.id, 'offline');
  }
}));

$('#partition-button').addEventListener('click', () => withBusy(async () => {
  const node = [...state.status.nodes].reverse().find((item) => item.status === 'healthy');
  if (!node) throw new Error('No healthy node is available');
  await setNode(node.id, 'partitioned');
}));

$('#corrupt-button').addEventListener('click', () => withBusy(async () => {
  const object = state.objects.find((item) => item.replicas.some((id) => state.status.nodes.find((node) => node.id === id)?.status === 'healthy'));
  if (!object) throw new Error('Load demo data or store an object first');
  const nodeId = object.replicas.find((id) => state.status.nodes.find((node) => node.id === id)?.status === 'healthy');
  await request('/api/faults/corrupt', { method: 'POST', body: JSON.stringify({ key: object.key, nodeId }) });
  toast('Replica corrupted', `${object.key} on ${nodeId}; run the integrity scan to repair it`);
  await refresh();
}));

$('#object-search').addEventListener('input', (event) => renderObjects(event.target.value));
$('#refresh-button').addEventListener('click', () => refresh({ quiet: false }));
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
setInterval(() => refresh(), 5000);
normalizePolicyControls();
refresh({ quiet: false });
