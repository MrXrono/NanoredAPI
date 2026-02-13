const API = '/api/v1';
let token = localStorage.getItem('nanored_token');

// ========== AUTH ==========
async function api(path, opts = {}) {
    const headers = { 'Content-Type': 'application/json', ...opts.headers };
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const resp = await fetch(`${API}${path}`, { ...opts, headers });
    if (resp.status === 401) { logout(); throw new Error('Unauthorized'); }
    return resp;
}

function showApp() {
    document.getElementById('login-page').style.display = 'none';
    document.getElementById('app').style.display = 'block';
    document.getElementById('app').classList.add('app');
    loadDashboard();
}

function logout() {
    token = null;
    localStorage.removeItem('nanored_token');
    document.getElementById('login-page').style.display = 'flex';
    document.getElementById('app').style.display = 'none';
}

document.getElementById('login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const errEl = document.getElementById('login-error');
    try {
        const resp = await fetch(`${API}/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
        });
        if (!resp.ok) { errEl.textContent = 'Invalid credentials'; errEl.style.display = 'block'; return; }
        const data = await resp.json();
        token = data.access_token;
        localStorage.setItem('nanored_token', token);
        errEl.style.display = 'none';
        showApp();
    } catch (err) {
        errEl.textContent = 'Connection error'; errEl.style.display = 'block';
    }
});

// ========== NAVIGATION ==========
document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
        e.preventDefault();
        const section = link.dataset.section;
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
        link.classList.add('active');
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        document.getElementById(`section-${section}`).classList.add('active');
        if (section === 'dashboard') loadDashboard();
        else if (section === 'devices') loadDevices();
        else if (section === 'sessions') loadSessions();
        else if (section === 'sni') loadSNI();
        else if (section === 'dns') loadDNS();
        else if (section === 'apps') loadApps();
        else if (section === 'connections') loadConnections();
        else if (section === 'errors') loadErrors();
    });
});

// ========== HELPERS ==========
function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDate(iso) {
    if (!iso) return '-';
    const d = new Date(iso);
    return d.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function renderPagination(container, total, page, perPage, callback) {
    const pages = Math.ceil(total / perPage);
    let html = '';
    html += `<button ${page <= 1 ? 'disabled' : ''} onclick="${callback}(${page - 1})">&laquo;</button>`;
    for (let i = 1; i <= Math.min(pages, 7); i++) {
        html += `<button class="${i === page ? 'active' : ''}" onclick="${callback}(${i})">${i}</button>`;
    }
    if (pages > 7) html += `<button disabled>...</button><button onclick="${callback}(${pages})">${pages}</button>`;
    html += `<button ${page >= pages ? 'disabled' : ''} onclick="${callback}(${page + 1})">&raquo;</button>`;
    container.innerHTML = html;
}

// ========== DASHBOARD ==========
async function loadDashboard() {
    try {
        const resp = await api('/admin/dashboard');
        const d = await resp.json();

        document.getElementById('online-count').textContent = d.online_count;
        document.getElementById('total-devices').textContent = d.total_devices;
        document.getElementById('today-sessions').textContent = d.today_sessions;
        document.getElementById('today-download').textContent = formatBytes(d.today_downloaded);
        document.getElementById('today-upload').textContent = formatBytes(d.today_uploaded);
        document.getElementById('total-traffic').textContent = formatBytes(d.total_downloaded + d.total_uploaded);
        document.getElementById('errors-today').textContent = d.errors_today;

        // Top SNI
        const sniList = document.getElementById('top-sni-list');
        sniList.innerHTML = d.top_sni.map((s, i) =>
            `<li><span class="rank">${i + 1}</span><span class="domain">${s.domain}</span><span class="hits">${s.hits}</span></li>`
        ).join('') || '<li>No data</li>';

        // Chart
        renderChart(d.sessions_per_day);
    } catch (err) { console.error('Dashboard error:', err); }
}

function renderChart(data) {
    const canvas = document.getElementById('sessions-chart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const w = canvas.width = canvas.parentElement.clientWidth - 40;
    const h = canvas.height = 200;
    ctx.clearRect(0, 0, w, h);

    const max = Math.max(...data.map(d => d.count), 1);
    const barW = (w - 60) / data.length;
    const chartH = h - 40;

    ctx.fillStyle = '#a0a0b0';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'center';

    data.forEach((d, i) => {
        const barH = (d.count / max) * chartH;
        const x = 40 + i * barW;
        const y = chartH - barH + 10;

        ctx.fillStyle = '#e94560';
        ctx.fillRect(x + 4, y, barW - 8, barH);

        ctx.fillStyle = '#a0a0b0';
        ctx.fillText(d.date.slice(5), x + barW / 2, h - 4);
        if (d.count > 0) {
            ctx.fillText(d.count, x + barW / 2, y - 4);
        }
    });
}

// ========== DEVICES ==========
async function loadDevices(page = 1) {
    const search = document.getElementById('device-search')?.value || '';
    const resp = await api(`/admin/devices?page=${page}&per_page=50&search=${encodeURIComponent(search)}`);
    const d = await resp.json();
    const tbody = document.getElementById('devices-tbody');
    tbody.innerHTML = d.items.map(dev => `
        <tr>
            <td title="${dev.android_id}">${dev.android_id.slice(0, 12)}...</td>
            <td>${dev.manufacturer || ''} ${dev.device_model || ''}</td>
            <td>${dev.android_version || '-'}</td>
            <td>${dev.app_version || '-'}</td>
            <td>${dev.carrier || '-'}</td>
            <td>${dev.is_online ? '<span class="badge badge-green">Online</span>' : '<span class="badge badge-red">Offline</span>'}</td>
            <td>${dev.is_blocked ? '<span class="badge badge-red">Blocked</span>' : '<span class="badge badge-green">Active</span>'}</td>
            <td>${formatDate(dev.last_seen_at)}</td>
            <td>
                <button class="btn btn-sm ${dev.is_blocked ? 'btn-success' : 'btn-danger'}"
                    onclick="toggleBlock('${dev.id}', ${dev.is_blocked})">${dev.is_blocked ? 'Unblock' : 'Block'}</button>
                <button class="btn btn-sm btn-primary" onclick="viewDeviceSessions('${dev.id}')">Sessions</button>
            </td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('devices-pagination'), d.total, d.page, d.per_page, 'loadDevices');
}

async function toggleBlock(deviceId, isBlocked) {
    const action = isBlocked ? 'unblock' : 'block';
    await api(`/admin/devices/${deviceId}/${action}`, { method: 'POST' });
    loadDevices();
}

function viewDeviceSessions(deviceId) {
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    document.querySelector('[data-section="sessions"]').classList.add('active');
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.getElementById('section-sessions').classList.add('active');
    document.getElementById('session-device-filter').value = deviceId;
    loadSessions(1);
}

// ========== SESSIONS ==========
async function loadSessions(page = 1) {
    const deviceId = document.getElementById('session-device-filter')?.value || '';
    let url = `/admin/sessions?page=${page}&per_page=50`;
    if (deviceId) url += `&device_id=${deviceId}`;
    const resp = await api(url);
    const d = await resp.json();
    const tbody = document.getElementById('sessions-tbody');
    tbody.innerHTML = d.items.map(s => {
        const duration = s.disconnected_at
            ? Math.round((new Date(s.disconnected_at) - new Date(s.connected_at)) / 60000) + ' min'
            : '<span class="badge badge-green">Active</span>';
        return `<tr>
            <td title="${s.device_id}">${s.device_id.slice(0, 8)}...</td>
            <td>${s.protocol || '-'}</td>
            <td>${s.server_address || '-'}</td>
            <td>${s.client_ip || '-'}</td>
            <td>${s.client_country || '-'} ${s.client_city ? '/ ' + s.client_city : ''}</td>
            <td>${s.network_type || '-'}</td>
            <td>${formatBytes(s.bytes_downloaded)}</td>
            <td>${formatBytes(s.bytes_uploaded)}</td>
            <td>${duration}</td>
            <td>${formatDate(s.connected_at)}</td>
        </tr>`;
    }).join('');
    renderPagination(document.getElementById('sessions-pagination'), d.total, d.page, d.per_page, 'loadSessions');
}

// ========== SNI ==========
async function loadSNI(page = 1) {
    const domain = document.getElementById('sni-search')?.value || '';
    let url = `/admin/sni?page=${page}&per_page=100`;
    if (domain) url += `&domain=${encodeURIComponent(domain)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('sni-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${l.domain}</td>
            <td>${l.hit_count}</td>
            <td>${formatBytes(l.bytes_total)}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.last_seen)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('sni-pagination'), d.total, d.page, d.per_page, 'loadSNI');
}

// ========== DNS ==========
async function loadDNS(page = 1) {
    const domain = document.getElementById('dns-search')?.value || '';
    let url = `/admin/dns?page=${page}&per_page=100`;
    if (domain) url += `&domain=${encodeURIComponent(domain)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('dns-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${l.domain}</td>
            <td>${l.resolved_ip || '-'}</td>
            <td>${l.query_type || '-'}</td>
            <td>${l.hit_count}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('dns-pagination'), d.total, d.page, d.per_page, 'loadDNS');
}

// ========== APPS ==========
async function loadApps(page = 1) {
    const resp = await api(`/admin/app-traffic?page=${page}&per_page=100`);
    const d = await resp.json();
    document.getElementById('apps-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${l.package_name}</td>
            <td>${l.app_name || '-'}</td>
            <td>${formatBytes(l.bytes_downloaded)}</td>
            <td>${formatBytes(l.bytes_uploaded)}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('apps-pagination'), d.total, d.page, d.per_page, 'loadApps');
}

// ========== CONNECTIONS ==========
async function loadConnections(page = 1) {
    const search = document.getElementById('conn-search')?.value || '';
    let url = `/admin/connections?page=${page}&per_page=100`;
    if (search) url += `&dest_ip=${encodeURIComponent(search)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('connections-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${l.dest_ip}</td>
            <td>${l.dest_port}</td>
            <td>${l.protocol || '-'}</td>
            <td>${l.domain || '-'}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('connections-pagination'), d.total, d.page, d.per_page, 'loadConnections');
}

// ========== ERRORS ==========
async function loadErrors(page = 1) {
    const resp = await api(`/admin/errors?page=${page}&per_page=50`);
    const d = await resp.json();
    document.getElementById('errors-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td><span class="badge badge-${l.error_type === 'crash' ? 'red' : 'yellow'}">${l.error_type}</span></td>
            <td title="${l.message || ''}">${(l.message || '-').slice(0, 60)}</td>
            <td>${l.app_version || '-'}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('errors-pagination'), d.total, d.page, d.per_page, 'loadErrors');
}

// ========== EXPORT ==========
async function exportSNI() {
    const days = prompt('Export SNI for how many days?', '7');
    if (!days) return;
    window.open(`${API}/admin/export/sni?days=${days}`, '_blank');
}

// ========== INIT ==========
if (token) {
    api('/admin/dashboard').then(r => {
        if (r.ok) showApp(); else logout();
    }).catch(() => logout());
} else {
    document.getElementById('login-page').style.display = 'flex';
}
