/* targetb0t — dashboard JS */

'use strict';

// ── State ──────────────────────────────────────────────────────────────────────
let selectedTaskId = null;
let proxyLists = [];
let accounts = [];

// ── Tab switching ──────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(`tab-${name}`).classList.add('active');
  const labels = { tasks: 'task', proxies: 'proxy', accounts: 'account' };
  document.querySelectorAll('.tab-btn').forEach(b => {
    if (b.textContent.toLowerCase().includes(labels[name] || name)) {
      b.classList.add('active');
    }
  });
}

// ── Toast ──────────────────────────────────────────────────────────────────────
let _toastTimer = null;
function toast(msg, type = 'info') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = `show ${type}`;
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => { el.className = ''; }, 3000);
}

// ── API helpers ────────────────────────────────────────────────────────────────
async function api(method, path, body) {
  const opts = {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  };
  const resp = await fetch(`/api${path}`, opts);
  if (resp.status === 204) return null;
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.detail || JSON.stringify(data));
  return data;
}

// ── Time formatting ────────────────────────────────────────────────────────────
function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' });
}

function timeAgo(iso) {
  if (!iso) return '—';
  const diff = Math.round((Date.now() - new Date(iso)) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

// ══════════════════════════════════════════════════════════════════════════════
// TASKS
// ══════════════════════════════════════════════════════════════════════════════

async function loadTasks() {
  try {
    const tasks = await api('GET', '/tasks');
    renderTasks(tasks);
  } catch (e) {
    console.error('loadTasks:', e);
  }
}

function renderTasks(tasks) {
  const tbody = document.getElementById('tasks-tbody');
  if (!tasks.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">No tasks yet. Add one above.</td></tr>';
    return;
  }

  tbody.innerHTML = tasks.map(t => {
    const proxyLabel = t.proxy_list_id
      ? (proxyLists.find(p => p.id === t.proxy_list_id)?.name || `List #${t.proxy_list_id}`)
      : '<span style="color:var(--muted)">Local</span>';
    const acctLabel = t.account_id
      ? (accounts.find(a => a.id === t.account_id)?.nickname || `Acct #${t.account_id}`)
      : '';

    const isPaused = t.status === 'paused';
    const isRunning = t.status === 'active';

    return `
      <tr class="clickable${selectedTaskId === t.id ? ' selected' : ''}" onclick="selectTask(${t.id}, '${escHtml(t.nickname)}')">
        <td><strong>${escHtml(t.nickname)}</strong></td>
        <td><code>${t.tcin}</code></td>
        <td>
          <span class="badge badge-${t.status}">${t.status.replace('_', ' ')}</span>
          ${t.live_status ? `<div class="live-status ${liveStatusClass(t.live_status)}">${escHtml(t.live_status)}</div>` : ''}
        </td>
        <td>${timeAgo(t.last_checked_at)}</td>
        <td>${t.interval_seconds}s</td>
        <td>${proxyLabel}${acctLabel ? `<br><span style="font-size:11px;color:var(--muted);">👤 ${escHtml(acctLabel)}</span>` : ''}</td>
        <td onclick="event.stopPropagation()" style="white-space:nowrap;">
          <button class="btn btn-ghost btn-sm" onclick="checkNow(${t.id})">Check Now</button>
          ${isRunning ? `<button class="btn btn-ghost btn-sm" onclick="toggleTask(${t.id}, '${t.status}')">Pause</button>` : ''}
          ${isPaused ? `<button class="btn btn-ghost btn-sm" onclick="toggleTask(${t.id}, '${t.status}')">Resume</button>` : ''}
          <button class="btn btn-danger btn-sm" onclick="deleteTask(${t.id})">Delete</button>
        </td>
      </tr>
    `;
  }).join('');
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function liveStatusClass(s) {
  if (!s) return '';
  if (s.includes('in stock')) return 'ls-in-stock';
  if (s.includes('checking')) return 'ls-checking';
  if (s.includes('adding') || s.includes('retrying cart')) return 'ls-carting';
  if (s.includes('added to cart')) return 'ls-success';
  if (s.includes('re-auth')) return 'ls-reauth';
  if (s.includes('cart failed') || s.includes('error') || s.includes('order failed')) return 'ls-error';
  if (s.includes('rate limited')) return 'ls-rate-limited';
  if (s.includes('out of stock')) return 'ls-oos';
  if (s.includes('order placed')) return 'ls-success';
  return 'ls-default';
}

async function createTask(e) {
  e.preventDefault();
  const form = e.target;
  const errEl = document.getElementById('task-form-error');
  errEl.textContent = '';

  const body = {
    url_or_tcin: form.url_or_tcin.value.trim(),
    nickname: form.nickname.value.trim(),
    interval_seconds: parseInt(form.interval_seconds.value) || 10,
    quantity: parseInt(form.quantity.value) || 1,
    store_id: form.store_id.value.trim() || null,
    proxy_list_id: form.proxy_list_id.value ? parseInt(form.proxy_list_id.value) : null,
    account_id: form.account_id.value ? parseInt(form.account_id.value) : null,
  };

  try {
    await api('POST', '/tasks', body);
    form.reset();
    toast('Task created — monitoring started.', 'success');
    await loadTasks();
  } catch (err) {
    errEl.textContent = err.message;
  }
}

async function toggleTask(id, currentStatus) {
  const newStatus = currentStatus === 'active' ? 'paused' : 'active';
  try {
    await api('PATCH', `/tasks/${id}`, { status: newStatus });
    toast(`Task ${newStatus === 'active' ? 'resumed' : 'paused'}.`, 'success');
    await loadTasks();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function deleteTask(id) {
  if (!confirm('Delete this monitoring task?')) return;
  try {
    await api('DELETE', `/tasks/${id}`);
    if (selectedTaskId === id) closeEventPanel();
    toast('Task deleted.', 'success');
    await loadTasks();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function checkNow(id) {
  try {
    const result = await api('POST', `/tasks/${id}/check-now`);
    const msg = result.available
      ? `IN STOCK! (${result.fulfillment_type || ''}) $${result.price || '?'}`
      : `Out of stock — ${result.raw_status}`;
    toast(msg, result.available ? 'success' : 'info');
    await loadTasks();
    if (selectedTaskId === id) await loadEvents(id);
  } catch (err) {
    toast(err.message, 'error');
  }
}

// ── Event log panel ────────────────────────────────────────────────────────────

async function selectTask(id, nickname) {
  selectedTaskId = id;
  document.getElementById('event-panel-title').textContent = nickname;
  document.getElementById('event-panel').style.display = 'block';
  await loadEvents(id);
  renderTasks(await api('GET', '/tasks')); // re-render to highlight selection
}

function closeEventPanel() {
  selectedTaskId = null;
  document.getElementById('event-panel').style.display = 'none';
}

async function loadEvents(taskId) {
  const events = await api('GET', `/tasks/${taskId}/events`);
  const el = document.getElementById('event-list');
  if (!events.length) {
    el.innerHTML = '<div class="empty">No events yet.</div>';
    return;
  }

  el.innerHTML = events.map(ev => {
    let detail = '';
    try {
      const d = JSON.parse(ev.detail || '{}');
      if (ev.event_type === 'in_stock' || ev.event_type === 'out_of_stock') {
        detail = `${d.raw_status || ''}`;
        if (d.fulfillment_type) detail += ` (${d.fulfillment_type})`;
        if (d.price) detail += ` · $${d.price}`;
        if (d._eligibility_detail) {
          const rules = Object.entries(d._eligibility_detail)
            .map(([k, v]) => `${k}=${v}`)
            .join(', ');
          detail += ` [${rules}]`;
        }
        if (d._debug) detail += ` [err: ${d._debug}]`;
      } else if (ev.event_type === 'added_to_cart') {
        detail = d.success ? `cart_id: ${d.cart_id}` : `failed: ${d.error}`;
        if (d._retry) detail += ` · ${d._retry}`;
      } else if (ev.event_type === 'order_placed') {
        detail = d.success ? `order #${d.order_id || '?'}` : `failed: ${d.error}`;
      } else if (ev.event_type === 'rate_limited') {
        detail = d.message || `Blocked (HTTP ${d.http_status || '?'})`;
      } else if (ev.event_type === 'error') {
        detail = d.message || '';
      }
    } catch {}

    return `
      <div class="event-item">
        <span class="event-time">${fmtTime(ev.occurred_at)}</span>
        <span class="badge badge-${ev.event_type}">${ev.event_type.replace(/_/g,' ')}</span>
        <span class="event-detail">${escHtml(detail)}</span>
      </div>
    `;
  }).join('');
}

// ══════════════════════════════════════════════════════════════════════════════
// PROXY LISTS
// ══════════════════════════════════════════════════════════════════════════════

async function loadProxyLists() {
  try {
    proxyLists = await api('GET', '/proxy-lists');
    renderProxyLists(proxyLists);
    populateProxySelect(proxyLists);
  } catch (e) {
    console.error('loadProxyLists:', e);
  }
}

function populateProxySelect(lists) {
  const sel = document.getElementById('task-proxy-select');
  const current = sel.value;
  sel.innerHTML = '<option value="">No Proxy (local)</option>';
  lists.forEach(l => {
    const opt = document.createElement('option');
    opt.value = l.id;
    opt.textContent = `${l.name} (${l.proxy_count} proxies)`;
    if (String(l.id) === current) opt.selected = true;
    sel.appendChild(opt);
  });
}

function renderProxyLists(lists) {
  const container = document.getElementById('proxy-lists-container');
  if (!lists.length) {
    container.innerHTML = '<div class="empty">No proxy lists yet. Create one above.</div>';
    return;
  }

  container.innerHTML = lists.map(l => `
    <div class="proxy-list-block" id="plist-${l.id}">
      <div class="proxy-list-header" onclick="toggleProxyBlock(${l.id})">
        <span class="proxy-list-name">${escHtml(l.name)}</span>
        <span class="proxy-list-meta">${l.proxy_count} proxies</span>
        <button class="btn btn-danger btn-sm" onclick="event.stopPropagation(); deleteProxyList(${l.id})">Delete</button>
      </div>
      <div class="proxy-list-body" id="plist-body-${l.id}">
        <div class="proxy-add-form">
          <label style="font-size:12px;color:var(--muted);">Add Proxies (one per line: http://[user:pass@]host:port)</label>
          <textarea id="proxy-input-${l.id}" placeholder="http://user:pass@1.2.3.4:8080&#10;http://5.6.7.8:3128"></textarea>
          <button class="btn btn-ghost btn-sm" onclick="addProxies(${l.id})">Add Proxies</button>
        </div>
        <div id="proxy-rows-${l.id}">Loading…</div>
      </div>
    </div>
  `).join('');
}

function toggleProxyBlock(listId) {
  const body = document.getElementById(`plist-body-${listId}`);
  const isOpen = body.classList.toggle('open');
  if (isOpen) loadProxiesForList(listId);
}

async function loadProxiesForList(listId) {
  const container = document.getElementById(`proxy-rows-${listId}`);
  try {
    const proxies = await api('GET', `/proxy-lists/${listId}/proxies`);
    if (!proxies.length) {
      container.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:4px 0;">No proxies added yet.</div>';
      return;
    }
    container.innerHTML = proxies.map(p => `
      <div class="proxy-row">
        <span class="proxy-url">${escHtml(p.url)}</span>
        <span class="badge badge-${p.enabled ? 'active' : 'paused'}">${p.enabled ? 'on' : 'off'}</span>
        ${p.fail_count > 0 ? `<span style="font-size:11px;color:var(--red);">${p.fail_count} fails</span>` : ''}
        <button class="btn btn-ghost btn-sm" onclick="toggleProxy(${p.id}, ${p.enabled}, ${listId})">
          ${p.enabled ? 'Disable' : 'Enable'}
        </button>
        <button class="btn btn-danger btn-sm" onclick="deleteProxy(${p.id}, ${listId})">✕</button>
      </div>
    `).join('');
  } catch (e) {
    container.innerHTML = `<div style="color:var(--red);font-size:12px;">${e.message}</div>`;
  }
}

async function createProxyList(e) {
  e.preventDefault();
  const form = e.target;
  try {
    await api('POST', '/proxy-lists', { name: form.name.value.trim() });
    form.reset();
    toast('Proxy list created.', 'success');
    await loadProxyLists();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function deleteProxyList(listId) {
  if (!confirm('Delete this proxy list? Tasks using it will switch to local.')) return;
  try {
    await api('DELETE', `/proxy-lists/${listId}`);
    toast('Proxy list deleted.', 'success');
    await loadProxyLists();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function addProxies(listId) {
  const ta = document.getElementById(`proxy-input-${listId}`);
  const urls = ta.value.split('\n').map(s => s.trim()).filter(Boolean);
  if (!urls.length) { toast('Enter at least one proxy URL.', 'error'); return; }
  try {
    await api('POST', `/proxy-lists/${listId}/proxies`, { urls });
    ta.value = '';
    toast(`${urls.length} proxy/proxies added.`, 'success');
    await loadProxiesForList(listId);
    await loadProxyLists(); // update count badge
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function toggleProxy(proxyId, currentEnabled, listId) {
  try {
    await api('PATCH', `/proxies/${proxyId}`, { enabled: !currentEnabled });
    await loadProxiesForList(listId);
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function deleteProxy(proxyId, listId) {
  try {
    await api('DELETE', `/proxies/${proxyId}`);
    toast('Proxy removed.', 'success');
    await loadProxiesForList(listId);
    await loadProxyLists();
  } catch (err) {
    toast(err.message, 'error');
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// ACCOUNTS
// ══════════════════════════════════════════════════════════════════════════════

async function loadAccounts() {
  try {
    accounts = await api('GET', '/accounts');
    renderAccounts(accounts);
    populateAccountSelect(accounts);
  } catch (e) {
    console.error('loadAccounts:', e);
  }
}

function populateAccountSelect(list) {
  const sel = document.getElementById('task-account-select');
  if (!sel) return;
  const current = sel.value;
  sel.innerHTML = '<option value="">Guest (no account)</option>';
  list.forEach(a => {
    const opt = document.createElement('option');
    opt.value = a.id;
    opt.textContent = a.nickname;
    if (String(a.id) === current) opt.selected = true;
    sel.appendChild(opt);
  });
}

function renderAccounts(list) {
  const container = document.getElementById('accounts-container');
  if (!list.length) {
    container.innerHTML = '<div class="empty">No accounts yet. Add one above.</div>';
    return;
  }
  const iStyle = `padding:4px 8px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:12px;`;
  container.innerHTML = list.map(a => {
    const ccvBadge = a.has_ccv
      ? '<span class="badge badge-active" style="font-size:10px;">💳 CCV saved</span>'
      : '<span class="badge badge-paused" style="font-size:10px;">no CCV</span>';
    return `
      <div class="account-block" style="flex-wrap:wrap;gap:10px;">
        <span class="account-name">👤 ${escHtml(a.nickname)}</span>
        <span style="font-size:12px;color:var(--muted);">${escHtml(a.email || '')}</span>
        ${ccvBadge}
        <input type="password" placeholder="new password" id="pw-${a.id}" style="width:120px;${iStyle}" />
        <input type="password" placeholder="CCV" maxlength="4" id="ccv-${a.id}" style="width:65px;letter-spacing:3px;text-align:center;${iStyle}" />
        <button class="btn btn-ghost btn-sm" onclick="saveAccountFields(${a.id})">Save</button>
        <button class="btn btn-danger btn-sm" onclick="deleteAccount(${a.id})">Delete</button>
      </div>
    `;
  }).join('');
}

async function saveAccountFields(id) {
  const pw  = document.getElementById(`pw-${id}`)?.value.trim();
  const ccv = document.getElementById(`ccv-${id}`)?.value.trim();
  if (!pw && !ccv) { toast('Enter a new password or CCV to save', 'error'); return; }
  const body = {};
  if (pw)  body.password = pw;
  if (ccv) body.ccv = ccv;
  try {
    await api('PATCH', `/accounts/${id}`, body);
    toast('Account updated!', 'success');
    await loadAccounts();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function addAccount() {
  const errEl = document.getElementById('account-form-error');
  const btn = document.getElementById('add-acct-btn');

  try {
    const nickname = (document.getElementById('acct-nickname').value || 'My Account').trim();
    const email = document.getElementById('acct-email').value.trim();
    const password = document.getElementById('acct-password').value;
    const ccv = document.getElementById('acct-ccv').value.trim();

    errEl.textContent = '';
    if (!email) { errEl.textContent = 'Email is required.'; return; }
    if (!password) { errEl.textContent = 'Password is required.'; return; }

    btn.disabled = true;
    btn.textContent = 'Saving…';

    await api('POST', '/accounts', { nickname, email, password, ccv: ccv || null });
    document.getElementById('acct-email').value = '';
    document.getElementById('acct-password').value = '';
    document.getElementById('acct-ccv').value = '';
    toast(`Account "${nickname}" saved!`, 'success');
    await loadAccounts();
  } catch (err) {
    if (errEl) errEl.textContent = err.message;
    else toast(err.message, 'error');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Add Account'; }
  }
}

async function deleteAccount(id) {
  if (!confirm('Delete this account? Tasks using it will fall back to guest mode.')) return;
  try {
    await api('DELETE', `/accounts/${id}`);
    toast('Account deleted.', 'success');
    await loadAccounts();
  } catch (err) {
    toast(err.message, 'error');
  }
}


// ══════════════════════════════════════════════════════════════════════════════
// Init + polling
// ══════════════════════════════════════════════════════════════════════════════

async function refresh() {
  await loadAccounts();
  await loadProxyLists();
  await loadTasks();
}

refresh();
setInterval(loadTasks, 5000);
setInterval(loadProxyLists, 15000);
setInterval(loadAccounts, 30000);
