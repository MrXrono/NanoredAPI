const API = '/api/v1';
let token = localStorage.getItem('nanored_token');
let accountsCache = [];
let journalInterval = null;
let remnawaveSelectedAccount = null;
let dbStatusInterval = null;
let remnawaveNodesSearchTimer = null;
let remnawaveAccountsSearchTimer = null;
let sqlBrowserState = { tableName: '', offset: 0, limit: 25, atStart: true, atEnd: false, search: '', mode: 'page', primaryKeys: [], rows: [] };

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
    loadAccounts().then(() => loadDashboard());
}

function logout() {
    token = null;
    localStorage.removeItem('nanored_token');
    document.getElementById('login-page').style.display = 'flex';
    document.getElementById('app').style.display = 'none';
    if (journalInterval) { clearInterval(journalInterval); journalInterval = null; }
    if (dbStatusInterval) { clearInterval(dbStatusInterval); dbStatusInterval = null; }
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
        if (!resp.ok) { errEl.textContent = 'Неверные учётные данные'; errEl.style.display = 'block'; return; }
        const data = await resp.json();
        token = data.access_token;
        localStorage.setItem('nanored_token', token);
        errEl.style.display = 'none';
        showApp();
    } catch (err) {
        errEl.textContent = 'Ошибка подключения'; errEl.style.display = 'block';
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

        if (dbStatusInterval) { clearInterval(dbStatusInterval); dbStatusInterval = null; }

        if (section === 'dashboard') loadDashboard();
        else if (section === 'devices') loadDevices();
        else if (section === 'sessions') loadSessions();
        else if (section === 'sni') loadSNI();
        else if (section === 'dns') loadDNS();
        else if (section === 'connections') loadConnections();
        else if (section === 'errors') loadErrors();
        else if (section === 'device-logs') loadDeviceLogs();
        else if (section === 'database-status') {
            loadDatabaseStatus();
            dbStatusInterval = setInterval(() => {
                if (document.getElementById('section-database-status').classList.contains('active')) {
                    loadDatabaseStatus();
                }
            }, 10000);
        }
        else if (section === 'sql-browser') {
            loadSqlBrowserTables();
        }
        else if (section === 'journal') refreshLogs();
        else if (section === 'remnawave-logs') {
            resetRemnawaveSelection();
            loadRemnawaveNodes();
            loadRemnawaveAccounts();
        }
        else if (section === 'remnawave-audit') {
            loadRemnawaveAudit(1);
        }
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

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ========== TABLE CELL FULL TEXT VIEWER ==========
const TABLE_CELL_VIEWER_ID = 'table-cell-viewer';

function ensureTableCellViewer() {
    let overlay = document.getElementById(TABLE_CELL_VIEWER_ID);
    if (overlay) return overlay;

    overlay = document.createElement('div');
    overlay.id = TABLE_CELL_VIEWER_ID;
    overlay.className = 'table-cell-viewer-overlay';
    overlay.style.display = 'none';
    overlay.innerHTML = `
        <div class="table-cell-viewer-modal">
            <div class="table-cell-viewer-header">
                <h3>Полный текст</h3>
                <button class="btn btn-sm btn-danger" id="table-cell-viewer-close">&times;</button>
            </div>
            <textarea id="table-cell-viewer-text" readonly></textarea>
            <div class="table-cell-viewer-actions">
                <button class="btn btn-sm btn-primary" id="table-cell-viewer-copy">Скопировать</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeTableCellViewer();
    });

    return overlay;
}

function openTableCellViewer(text) {
    const overlay = ensureTableCellViewer();
    const textarea = document.getElementById('table-cell-viewer-text');
    if (!textarea) return;
    textarea.value = text || '';
    overlay.style.display = 'flex';
    setTimeout(() => {
        textarea.focus();
        textarea.select();
    }, 0);
}

function closeTableCellViewer() {
    const overlay = document.getElementById(TABLE_CELL_VIEWER_ID);
    if (overlay) overlay.style.display = 'none';
}

async function copyTableCellViewerText() {
    const textarea = document.getElementById('table-cell-viewer-text');
    if (!textarea) return;
    const text = textarea.value || '';
    try {
        await navigator.clipboard.writeText(text);
    } catch (e) {
        textarea.focus();
        textarea.select();
        document.execCommand('copy');
    }
}

function getCellFullText(td) {
    if (!td) return '';
    const fromData = td.getAttribute('data-full-text');
    if (fromData) return fromData.trim();
    const fromTitle = td.getAttribute('title');
    if (fromTitle) return fromTitle.trim();
    return (td.innerText || td.textContent || '').trim();
}

function shouldOpenCellViewer(td, fullText) {
    if (!td || !fullText) return false;
    if (td.scrollWidth > td.clientWidth) return true;
    const shortText = (td.textContent || '').trim();
    return shortText !== fullText;
}

// ========== SQL BROWSER ==========
async function loadSqlBrowserTables() {
    const select = document.getElementById('sql-browser-table-select');
    const tbody = document.getElementById('sql-browser-tbody');
    const meta = document.getElementById('sql-browser-meta');
    if (!select || !tbody || !meta) return;

    try {
        const resp = await api('/admin/sql-browser/tables');
        const data = await resp.json();
        const items = Array.isArray(data?.items) ? data.items : [];
        if (!items.length) {
            select.innerHTML = '<option value="">Нет таблиц</option>';
            tbody.innerHTML = '<tr><td>Таблицы не найдены</td></tr>';
            meta.textContent = 'Таблицы не найдены';
            return;
        }

        const previous = sqlBrowserState.tableName;
        select.innerHTML = items.map(i => {
            const name = escapeHtml(String(i.table_name || ''));
            const approx = Number(i.approx_rows || 0).toLocaleString('ru-RU');
            return `<option value="${name}">${name} (~${approx} rows)</option>`;
        }).join('');

        const availableValues = new Set(items.map(i => String(i.table_name || '')));
        const selected = availableValues.has(previous) ? previous : String(items[0].table_name || '');
        select.value = selected;
        sqlBrowserState.tableName = selected;

        if (!select.dataset.bound) {
            select.addEventListener('change', () => {
                sqlBrowserState.tableName = select.value || '';
                sqlBrowserState.offset = 0;
                loadSqlBrowserRows('page', 0);
            });
            select.dataset.bound = '1';
        }

        await loadSqlBrowserRows('page', 0);
    } catch (err) {
        console.error('sql browser tables load error:', err);
        select.innerHTML = '<option value="">Ошибка загрузки</option>';
        tbody.innerHTML = '<tr><td>Не удалось загрузить таблицы</td></tr>';
        meta.textContent = 'Ошибка загрузки таблиц';
    }
}

function updateSqlBrowserNavButtons() {
    const prevBtn = document.getElementById('sql-browser-prev-btn');
    const nextBtn = document.getElementById('sql-browser-next-btn');
    if (prevBtn) prevBtn.disabled = !!sqlBrowserState.atStart;
    if (nextBtn) nextBtn.disabled = !!sqlBrowserState.atEnd;
}

function renderSqlBrowserTable(columns, rows, primaryKeys) {
    const thead = document.getElementById('sql-browser-thead');
    const tbody = document.getElementById('sql-browser-tbody');
    if (!thead || !tbody) return;

    const safeColumns = Array.isArray(columns) && columns.length ? columns : ['value'];
    const pkCols = Array.isArray(primaryKeys) ? primaryKeys : [];
    thead.innerHTML = `<tr>${safeColumns.map(c => `<th>${escapeHtml(String(c))}</th>`).join('')}<th style="width:96px;">Действия</th></tr>`;

    if (!Array.isArray(rows) || !rows.length) {
        tbody.innerHTML = `<tr><td colspan="${safeColumns.length + 1}">Нет данных</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map((row, rowIndex) => {
        const canDeleteByPk = pkCols.length > 0 && pkCols.every(pk => row && Object.prototype.hasOwnProperty.call(row, pk));
        const canDeleteByCtid = !!(row && row.__ctid);
        const canDelete = canDeleteByPk || canDeleteByCtid;
        const deleteBtn = canDelete
            ? `<button class="btn btn-sm btn-danger sql-browser-delete-row" data-row-index="${rowIndex}">Удалить</button>`
            : '<span style="color:var(--text-secondary);font-size:12px;">—</span>';
        return `<tr>${safeColumns.map(col => {
            const value = row?.[col];
            let text = '';
            if (value === null || value === undefined) text = '';
            else if (typeof value === 'object') text = JSON.stringify(value);
            else text = String(value);
            const escaped = escapeHtml(text);
            const fullAttr = escapeHtml(text).replace(/"/g, '&quot;');
            return `<td class="sql-browser-cell" data-full-text="${fullAttr}" title="${fullAttr}">${escaped}</td>`;
        }).join('')}<td>${deleteBtn}</td></tr>`;
    }).join('');
}

async function deleteSqlBrowserRowByIndex(rowIndex) {
    const tableName = sqlBrowserState.tableName;
    const rows = Array.isArray(sqlBrowserState.rows) ? sqlBrowserState.rows : [];
    const primaryKeys = Array.isArray(sqlBrowserState.primaryKeys) ? sqlBrowserState.primaryKeys : [];
    const idx = Number(rowIndex);
    if (!tableName || !Number.isInteger(idx) || idx < 0 || idx >= rows.length) return;

    const row = rows[idx] || {};
    const payload = {};

    if (primaryKeys.length > 0) {
        const pkValues = {};
        for (const key of primaryKeys) {
            if (!Object.prototype.hasOwnProperty.call(row, key)) {
                alert('Невозможно удалить строку: отсутствует значение первичного ключа.');
                return;
            }
            pkValues[key] = row[key];
        }
        payload.pk_values = pkValues;
    } else if (row.__ctid) {
        payload.ctid = row.__ctid;
    } else {
        alert('Невозможно удалить строку: таблица без PK и без ctid.');
        return;
    }

    if (!confirm('Удалить выбранную строку? Это действие необратимо.')) return;

    try {
        const resp = await api(`/admin/sql-browser/table/${encodeURIComponent(tableName)}/row`, {
            method: 'DELETE',
            body: JSON.stringify(payload),
        });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
            alert('Ошибка удаления: ' + (data.detail || 'Неизвестная ошибка'));
            return;
        }

        if (sqlBrowserState.mode === 'last') {
            await loadSqlBrowserRows('last');
        } else {
            const offset = typeof sqlBrowserState.offset === 'number' ? sqlBrowserState.offset : 0;
            await loadSqlBrowserRows('page', offset);
        }
    } catch (err) {
        console.error('sql browser row delete error:', err);
        alert('Ошибка удаления строки');
    }
}

async function loadSqlBrowserRows(mode = 'page', forcedOffset = null) {
    const select = document.getElementById('sql-browser-table-select');
    const meta = document.getElementById('sql-browser-meta');
    if (!select || !meta) return;

    const tableName = select.value || sqlBrowserState.tableName;
    if (!tableName) {
        meta.textContent = 'Выберите таблицу.';
        return;
    }
    sqlBrowserState.tableName = tableName;

    const searchInput = document.getElementById('sql-browser-search');
    const search = (searchInput?.value || '').trim();
    sqlBrowserState.search = search;

    let url = `/admin/sql-browser/table/${encodeURIComponent(tableName)}?limit=${sqlBrowserState.limit}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;
    if (mode === 'last') {
        url += '&mode=last';
    } else {
        const offset = forcedOffset !== null ? forcedOffset : sqlBrowserState.offset;
        url += `&mode=page&offset=${Math.max(0, Number(offset) || 0)}`;
    }

    try {
        const resp = await api(url);
        const data = await resp.json();
        const rows = Array.isArray(data?.rows) ? data.rows : [];
        const columns = Array.isArray(data?.columns) ? data.columns : [];

        const primaryKeys = Array.isArray(data?.primary_keys) ? data.primary_keys : [];

        sqlBrowserState.rows = rows;
        sqlBrowserState.primaryKeys = primaryKeys;
        sqlBrowserState.mode = data?.mode === 'last' ? 'last' : 'page';

        renderSqlBrowserTable(columns, rows, primaryKeys);

        if (data?.mode === 'last') {
            sqlBrowserState.offset = null;
            sqlBrowserState.atStart = false;
            sqlBrowserState.atEnd = true;
            meta.textContent = `${tableName}: последние ${rows.length} строк${sqlBrowserState.search ? ` (поиск: ${sqlBrowserState.search})` : ''}`;
        } else {
            sqlBrowserState.offset = Number(data?.offset || 0);
            sqlBrowserState.atStart = !!data?.at_start;
            sqlBrowserState.atEnd = !!data?.at_end;
            const from = rows.length ? sqlBrowserState.offset + 1 : sqlBrowserState.offset;
            const to = sqlBrowserState.offset + rows.length;
            meta.textContent = `${tableName}: строки ${from}–${to}${sqlBrowserState.search ? ` (поиск: ${sqlBrowserState.search})` : ''}`;
        }

        updateSqlBrowserNavButtons();
    } catch (err) {
        console.error('sql browser rows load error:', err);
        document.getElementById('sql-browser-tbody').innerHTML = '<tr><td>Ошибка загрузки данных таблицы</td></tr>';
        meta.textContent = `Ошибка чтения таблицы ${tableName}`;
    }
}

function loadSqlBrowserNext() {
    const baseOffset = typeof sqlBrowserState.offset === 'number' ? sqlBrowserState.offset : 0;
    const nextOffset = baseOffset + sqlBrowserState.limit;
    loadSqlBrowserRows('page', nextOffset);
}

function loadSqlBrowserPrev() {
    const baseOffset = typeof sqlBrowserState.offset === 'number' ? sqlBrowserState.offset : 0;
    const prevOffset = Math.max(0, baseOffset - sqlBrowserState.limit);
    loadSqlBrowserRows('page', prevOffset);
}

// ========== DATABASE STATUS ==========
async function runAdultSyncNow() {
    const btn = document.getElementById('run-adult-sync-btn');
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/adult-sync/run', { method: 'POST' });
        const data = await resp.json();
        if (data && data.message) alert(data.message);
        await loadDatabaseStatus();
    } catch (err) {
        console.error('run adult sync error:', err);
        alert('Не удалось запустить sync');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function runAdultRecheckAllNow() {
    const btn = document.getElementById('run-adult-recheck-btn');
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/adult-sync/recheck-all', { method: 'POST' });
        const data = await resp.json();
        if (data && data.message) alert(data.message);
        await loadDatabaseStatus();
    } catch (err) {
        console.error('run adult full recheck error:', err);
        alert('Не удалось запустить полную перепроверку');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function runAdultTxtSyncNow() {
    const btn = document.getElementById('run-adult-sync-txt-btn');
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/adult-sync/sync-from-txt', { method: 'POST' });
        const data = await resp.json();
        if (data && data.message) alert(data.message);
        await loadDatabaseStatus();
    } catch (err) {
        console.error('run adult txt sync error:', err);
        alert('Не удалось запустить TXT sync');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function runAdultCleanupNow() {
    const btn = document.getElementById('run-adult-cleanup-btn');
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/adult-sync/cleanup-garbage', { method: 'POST' });
        const data = await resp.json();
        if (data && data.message) alert(data.message);
        await loadDatabaseStatus();
    } catch (err) {
        console.error('run adult cleanup error:', err);
        alert('Не удалось запустить cleanup');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function stopBackgroundServices() {
    const btn = document.getElementById('services-stop-btn');
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/services/stop', { method: 'POST' });
        const data = await resp.json();
        alert(data?.message || 'Службы остановлены');
        await loadDatabaseStatus();
    } catch (err) {
        console.error('stop services error:', err);
        alert('Не удалось остановить службы');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function killBackgroundServices() {
    const btn = document.getElementById('services-kill-btn');
    if (btn) btn.disabled = true;
    try {
        if (!confirm('Убить фоновые службы? После этого для запуска потребуется рестарт приложения.')) return;
        const resp = await api('/admin/services/kill', { method: 'POST' });
        const data = await resp.json();
        alert(data?.message || 'Службы убиты');
        await loadDatabaseStatus();
    } catch (err) {
        console.error('kill services error:', err);
        alert('Не удалось убить службы');
    } finally {
        if (btn) btn.disabled = false;
    }
}


async function startBackgroundServices() {
    const btn = document.getElementById('services-start-btn');
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/services/start', { method: 'POST' });
        const data = await resp.json();
        alert(data?.message || 'Службы запущены');
        await loadDatabaseStatus();
    } catch (err) {
        console.error('start services error:', err);
        alert('Не удалось запустить службы');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function runDbIntegrityCheckRepair() {
    const buttons = [
        document.getElementById('db-integrity-check-btn'),
        document.getElementById('db-integrity-check-inline-btn'),
    ].filter(Boolean);
    for (const btn of buttons) {
        btn.disabled = true;
        btn.dataset.oldText = btn.textContent;
        btn.textContent = 'Проверка...';
    }
    try {
        const resp = await api('/admin/database/check-repair', { method: 'POST' });
        const data = await resp.json();
        const checks = data?.report?.checks || {};
        const miss = Array.isArray(checks.missing_tables_after) ? checks.missing_tables_after.length : (Array.isArray(checks.missing_tables) ? checks.missing_tables.length : 0);
        const badIdx = Array.isArray(checks.invalid_indexes_after) ? checks.invalid_indexes_after.length : (Array.isArray(checks.invalid_indexes) ? checks.invalid_indexes.length : 0);
        const missRecIdx = Array.isArray(checks.missing_recommended_indexes_after) ? checks.missing_recommended_indexes_after.length : (Array.isArray(checks.missing_recommended_indexes) ? checks.missing_recommended_indexes.length : 0);
        const failedRepairs = Array.isArray(data?.report?.repairs_failed) ? data.report.repairs_failed.length : 0;
        alert(`Проверка завершена. missing_tables=${miss}, invalid_indexes=${badIdx}, missing_recommended_indexes=${missRecIdx}, repair_errors=${failedRepairs}`);
        await loadDatabaseStatus();
    } catch (err) {
        console.error('db integrity check error:', err);
        alert('Не удалось выполнить проверку целостности БД');
    } finally {
        for (const btn of buttons) {
            btn.disabled = false;
            btn.textContent = btn.dataset.oldText || 'Проверить целостность БД и восстановить';
        }
    }
}

function updateServicesControlButtons(servicesControl, taskDetails) {
    const enabled = Boolean(servicesControl?.services_enabled ?? true);
    const killed = Boolean(servicesControl?.services_killed ?? false);
    const stopBtn = document.getElementById('services-stop-btn');
    const killBtn = document.getElementById('services-kill-btn');
    const startBtn = document.getElementById('services-start-btn');
    if (stopBtn) stopBtn.disabled = !enabled || killed;
    if (killBtn) killBtn.disabled = killed;
    if (startBtn) startBtn.disabled = enabled || killed;

    const hasRunningTasks = ['sync', 'recheck', 'txt_sync', 'cleanup'].some((k) => Boolean(taskDetails?.[k]?.running));
    if (killed && startBtn) {
        startBtn.textContent = 'Нужен рестарт (службы убиты)';
    } else if (!enabled && hasRunningTasks && startBtn) {
        startBtn.textContent = 'Запустить службы (задачи остановлены)';
    } else if (startBtn) {
        startBtn.textContent = 'Запустить службы';
    }
}

async function setAdultWeeklySchedule() {
    const btn = document.getElementById('run-adult-schedule-btn');
    const weekdayEl = document.getElementById('adult-sync-weekday');
    const timeEl = document.getElementById('adult-sync-time');
    if (!weekdayEl || !timeEl) return;
    const weekday = Number(weekdayEl.value || 6);
    const raw = String(timeEl.value || '03:00');
    const [h, m] = raw.split(':');
    const hour = Number(h || 3);
    const minute = Number(m || 0);
    if (btn) btn.disabled = true;
    try {
        const resp = await api('/admin/adult-sync/schedule', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ weekday, hour, minute }),
        });
        const data = await resp.json();
        if (data && data.message) alert(data.message);
        await loadDatabaseStatus();
    } catch (err) {
        console.error('set adult schedule error:', err);
        alert('Не удалось сохранить weekly расписание');
    } finally {
        if (btn) btn.disabled = false;
    }
}

function formatTaskProgress(task) {
    if (!task) return '-';
    const percentRaw = Number(task.progress_percent || 0);
    const cur = Number(task.progress_current || 0);
    const total = Number(task.progress_total || 0);
    const shownTotal = Math.max(total, cur, 0);
    const percent = Math.min(100, Math.max(0, percentRaw));
    if (shownTotal > 0) return `${cur}/${shownTotal} (${percent.toFixed(1)}%)`;
    return `${percent.toFixed(1)}%`;
}

function updateAdultButtonsByTaskState(tasks, servicesControl) {
    const map = [
        ['sync', 'run-adult-sync-btn', 'Запустить sync'],
        ['recheck', 'run-adult-recheck-btn', 'Перепроверить все'],
        ['txt_sync', 'run-adult-sync-txt-btn', 'TXT → DB'],
        ['cleanup', 'run-adult-cleanup-btn', 'Удалить мусор'],
    ];
    const enabled = Boolean(servicesControl?.services_enabled ?? true);
    for (const [taskKey, btnId, baseLabel] of map) {
        const btn = document.getElementById(btnId);
        if (!btn) continue;
        const t = tasks?.[taskKey] || {};
        const running = Boolean(t.running);
        btn.disabled = running || !enabled;
        btn.textContent = running ? `${baseLabel} (${formatTaskProgress(t)})` : baseLabel;
    }
}

async function loadDatabaseStatus() {
    try {
        const resp = await api('/admin/database-status');
        const d = await resp.json();
        const pg = d.postgres || {};
        const pgConn = pg.connections || {};
        const pgPerf = pg.performance || {};
        const redis = d.redis || {};
        const queue = redis.command_queue || {};
        const rsyslog = d.rsyslog || {};
        const adult = d.adult_sync || {};
        const ingest = d.api_ingest || {};
        const ingestRsyslog = ingest.rsyslog || {};
        const ingestNanoredvpn = ingest.nanoredvpn || {};
        const system = d.system || {};

        const maxConn = Number(pg.max_connections || 0);
        const activeConn = Number(pgConn.active || 0);
        document.getElementById('db-pg-active-ratio').textContent = `${activeConn} / ${maxConn}`;
        document.getElementById('db-pg-total-connections').textContent = `${pgConn.total || 0}`;
        document.getElementById('db-pg-size').textContent = formatBytes(Number(pg.size_bytes || 0));
        const pgUtil = maxConn > 0 ? ((activeConn / maxConn) * 100) : 0;
        document.getElementById('db-pg-utilization').textContent = `${pgUtil.toFixed(2)}%`;
        document.getElementById('db-pg-cache-hit').textContent = `${Number(pgPerf.cache_hit_ratio || 0).toFixed(2)}%`;
        document.getElementById('db-pg-xact').textContent = `${Number(pgPerf.xact_commit || 0)} / ${Number(pgPerf.xact_rollback || 0)}`;
        document.getElementById('db-pg-deadlocks').textContent = Number(pgPerf.deadlocks || 0);
        document.getElementById('db-pg-temp-bytes').textContent = formatBytes(Number(pgPerf.temp_bytes || 0));
        document.getElementById('db-sys-cpu').textContent = `${Number(system.cpu_percent || 0).toFixed(2)}%`;
        document.getElementById('db-sys-memory').textContent = `${Number(system.memory_percent || 0).toFixed(2)}%`;
        document.getElementById('db-redis-online').textContent = Number(redis.online_devices || 0);
        document.getElementById('db-redis-commands').textContent = Number(queue.total || 0);
        document.getElementById('db-redis-devices').textContent = Number(queue.devices_with_pending || 0);
        document.getElementById('db-rsyslog-requests-1m').textContent = Number(rsyslog.count_1m || 0);
        document.getElementById('db-rsyslog-bytes-1m').textContent = formatBytes(Number(rsyslog.bytes_1m || 0));
        document.getElementById('db-rsyslog-avg-1m').textContent = formatBytes(Number(rsyslog.bytes_per_entry_1m || 0));
        const rsyslogQueue = ingest.rsyslog_queue || {};
        const rsReceived = Number(ingestRsyslog.received || 0);
        const rsValidated = Number(ingestRsyslog.validated_ok || 0);
        const rsProcessed = Number(ingestRsyslog.processed_ok || ingestRsyslog.processed || 0);
        const rsInserted = Number(ingestRsyslog.inserted_new || 0);
        const rsDedup = Number(ingestRsyslog.deduplicated || 0);
        const rsRejected = Number(ingestRsyslog.rejected || 0);
        const rsFailed = Number(ingestRsyslog.failed || 0);
        const rsRetried = Number(ingestRsyslog.retried || 0);
        const rsLag = Number(rsyslogQueue.lag_estimate || 0);
        const rsPending = Number(rsyslogQueue.pending || 0);
        const rsDead = Number(rsyslogQueue.dead_len || 0);
        const rsReasonTop = (ingestRsyslog.reject_reasons || []).slice(0, 3).map(x => `${x.reason}:${x.count}`).join(', ');
        const rsBaseText = `${rsReceived} / ${rsProcessed} (valid ${rsValidated}, new ${rsInserted}, dedup ${rsDedup}, rej ${rsRejected}, fail ${rsFailed}, retry ${rsRetried}, lag ${rsLag}, pend ${rsPending}, dead ${rsDead})`;
        const rsText = rsReasonTop ? `${rsBaseText} [rej_top: ${rsReasonTop}]` : rsBaseText;

        const nvReceived = Number(ingestNanoredvpn.received || 0);
        const nvProcessed = Number(ingestNanoredvpn.processed || 0);
        const nvFailed = Number(ingestNanoredvpn.failed || 0);
        document.getElementById('db-nanoredvpn-ingest-ok').textContent = `${nvReceived} / ${nvProcessed} (fail ${nvFailed})`;
        document.getElementById('db-api-avg-ms').textContent = `${Number(ingestRsyslog.avg_latency_ms || 0).toFixed(1)}ms / ${Number(ingestNanoredvpn.avg_latency_ms || 0).toFixed(1)}ms`;
        document.getElementById('db-redis-memory').textContent = `Redis memory: ${redis.memory_used_human || '-'}, clients: ${redis.connected_clients || 0}`;

        const adultCatalogEnabled = Number(adult.catalog_domains_enabled || 0);
        const adultCatalogTotal = Number(adult.catalog_domains_total || 0);
        const schedule = adult.schedule || {};
        const services = adult.services || {};
        const taskDetails = adult.task_details || {};
        const servicesControl = d.services_control || {};
        const syncTask = taskDetails.sync || {};
        const recheckTask = taskDetails.recheck || {};
        const txtTask = taskDetails.txt_sync || {};
        const cleanupTask = taskDetails.cleanup || {};
        const weekdaySelect = document.getElementById('adult-sync-weekday');
        const timeInput = document.getElementById('adult-sync-time');
        if (weekdaySelect && Number.isFinite(Number(schedule.weekday))) weekdaySelect.value = String(Number(schedule.weekday));
        if (timeInput && Number.isFinite(Number(schedule.hour)) && Number.isFinite(Number(schedule.minute))) {
            const h = String(Number(schedule.hour)).padStart(2, '0');
            const m = String(Number(schedule.minute)).padStart(2, '0');
            timeInput.value = `${h}:${m}`;
        }
        updateAdultButtonsByTaskState(taskDetails, servicesControl);
        updateServicesControlButtons(servicesControl, taskDetails);
        const adultSyncRows = [
            ['Статус', String(adult.status || 'unknown')],
            ['Комментарий', String(adult.status_hint || '-')],
            ['Службы', `scheduler:${services.scheduler || 'unknown'}, recheck:${services.recheck_worker || 'unknown'}, catalog:${services.catalog_sync_lock || 'unknown'}`],
            ['Service control', `enabled:${servicesControl.services_enabled ? 'yes' : 'no'}, reason:${String(servicesControl.reason || '-')}, updated:${formatDate(servicesControl.updated_at)}`],
            ['Фоновые задачи', `sync:${adult.manual_tasks?.sync ? 'ON' : 'off'}, recheck:${adult.manual_tasks?.recheck ? 'ON' : 'off'}, txt:${adult.manual_tasks?.txt_sync ? 'ON' : 'off'}, cleanup:${adult.manual_tasks?.cleanup ? 'ON' : 'off'}`],
            ['Scheduler loop', formatDate(services.last_loop_at)],
            ['Scheduler error', formatDate(services.last_error_at)],
            ['Расписание weekly (UTC)', `${String(schedule.weekday_label || '?')} ${String(schedule.hour ?? '--').padStart(2, '0')}:${String(schedule.minute ?? '--').padStart(2, '0')} [${String(schedule.source || '-')}]`],
            ['Последний запуск', formatDate(adult.last_run_at)],
            ['Версия листа', String(adult.last_version || '-')],
            ['Обновлено доменов', Number(adult.last_updated_rows || 0)],
            ['Следующий sync (ETA)', formatDate(adult.next_sync_eta)],
            ['Task sync', `${String(syncTask.status || '-')}; ${formatTaskProgress(syncTask)}; ${String(syncTask.message || '-')}`],
            ['Task recheck', `${String(recheckTask.status || '-')}; ${formatTaskProgress(recheckTask)}; ${String(recheckTask.message || '-')}`],
            ['Task TXT→DB', `${String(txtTask.status || '-')}; ${formatTaskProgress(txtTask)}; ${String(txtTask.message || '-')}`],
            ['Task cleanup', `${String(cleanupTask.status || '-')}; ${formatTaskProgress(cleanupTask)}; ${String(cleanupTask.message || '-')}`],
            ['API rsyslog: получено / успешно', rsText],
            ['Catalog (enabled / total)', `${adultCatalogEnabled} / ${adultCatalogTotal}`],
            ['Catalog by source', `BLP: ${Number((adult.catalog_sources || {}).blocklistproject || 0)}, OISD: ${Number((adult.catalog_sources || {}).oisd || 0)}, V2Fly: ${Number((adult.catalog_sources || {}).v2fly || 0)}, TXT: ${Number((adult.catalog_sources || {}).txt_import || 0)}`],
            ['Unique 18+ / matched / total', `${Number(adult.unique_adult_total || 0)} / ${Number(adult.unique_matched_total || 0)} / ${Number(adult.unique_domains_total || 0)}`],
            ['Need recheck', Number(adult.unique_need_recheck || 0)],
            ['Coverage', `${Number(adult.adult_coverage_percent || 0)}%`],
        ];
        document.getElementById('db-adult-sync-tbody').innerHTML = adultSyncRows
            .map(([k, v]) => {
                const key = String(k);
                const val = String(v);
                return `<tr><td>${escapeHtml(key)}</td><td class="cell-expandable" data-full-text="${escapeHtml(val)}">${escapeHtml(val)}</td></tr>`;
            })
            .join('');

        document.getElementById('db-pg-states-tbody').innerHTML = `
            <tr><td>active</td><td>${Number(pgConn.active || 0)}</td></tr>
            <tr><td>idle</td><td>${Number(pgConn.idle || 0)}</td></tr>
            <tr><td>idle in transaction</td><td>${Number(pgConn.idle_in_transaction || 0)}</td></tr>
            <tr><td>waiting</td><td>${Number(pgConn.waiting || 0)}</td></tr>
        `;

        const topQueues = (queue.top_devices || []);
        if (topQueues.length === 0) {
            document.getElementById('db-queue-top-tbody').innerHTML = '<tr><td>Нет активных очередей</td><td>0</td></tr>';
        } else {
            document.getElementById('db-queue-top-tbody').innerHTML = topQueues.map(item => `
                <tr><td title="${escapeHtml(item.device_id)}">${escapeHtml(item.device_id.slice(0, 8))}...</td><td>${Number(item.pending || 0)}</td></tr>
            `).join('');
        }

        const tableRows = (d.database_tables || []);
        if (tableRows.length === 0) {
            document.getElementById('db-table-sizes-tbody').innerHTML = '<tr><td>Нет данных</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td></tr>';
        } else {
            document.getElementById('db-table-sizes-tbody').innerHTML = tableRows.map(item => {
                const ins = Number(item.tup_ins || 0);
                const upd = Number(item.tup_upd || 0);
                const del = Number(item.tup_del || 0);
                const hotUpd = Number(item.tup_hot_upd || 0);
                const writeOps = `${ins}/${upd}/${del}`;
                const vacuumInfo = `v:${Number(item.vacuum_count || 0)}, av:${Number(item.autovacuum_count || 0)}${item.last_vacuum_at ? `, last:${formatDate(item.last_vacuum_at)}` : ''}`;
                return `
                <tr>
                    <td>${escapeHtml(item.name || '-')}</td>
                    <td>${formatBytes(Number(item.size_bytes || 0))}</td>
                    <td>${Number(item.live_rows || 0)}</td>
                    <td>${Number(item.dead_rows || 0)}</td>
                    <td>${Number(item.dead_ratio_percent || 0).toFixed(2)}%</td>
                    <td title="HOT updates: ${hotUpd}">${writeOps}</td>
                    <td>${escapeHtml(vacuumInfo)}</td>
                </tr>`;
            }).join('');
        }
    } catch (err) {
        console.error('Database status error:', err);
    }
}

// ========== ACCOUNTS ==========
async function loadAccounts() {
    try {
        const resp = await api('/admin/accounts');
        const d = await resp.json();
        accountsCache = d.items || [];
        populateAccountFilters();
    } catch (err) { console.error('Accounts error:', err); }
}

function populateAccountFilters() {
    const selectors = [
        'device-account-filter', 'session-account-filter', 'sni-account-filter',
        'dns-account-filter', 'conn-account-filter', 'errors-account-filter',
        'devlogs-account-filter'
    ];
    selectors.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        const currentVal = el.value;
        el.innerHTML = '<option value="">Все аккаунты</option>';
        accountsCache.forEach(a => {
            const label = a.description ? `${a.account_id} (${a.description})` : a.account_id;
            el.innerHTML += `<option value="${escapeHtml(a.account_id)}">${escapeHtml(label)}</option>`;
        });
        el.value = currentVal;
    });
}

function getAccountFilter(selectId) {
    const el = document.getElementById(selectId);
    return el ? el.value : '';
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

        renderChart(d.sessions_per_day);

        loadTopSNI(1);
        loadAccountStats(1);
    } catch (err) { console.error('Dashboard error:', err); }
}

async function loadTopSNI(page = 1) {
    try {
        const resp = await api(`/admin/dashboard/top-sni?page=${page}&per_page=25`);
        const d = await resp.json();
        const sniList = document.getElementById('top-sni-list');
        const offset = (d.page - 1) * d.per_page;
        sniList.innerHTML = d.items.map((s, i) =>
            `<li><span class="rank">${offset + i + 1}</span><span class="domain">${escapeHtml(s.domain_display || s.domain)}</span><span class="hits">${s.hits}</span></li>`
        ).join('') || '<li>Нет данных</li>';
        renderDashPagination(document.getElementById('top-sni-pagination'), d.total, d.page, d.per_page, 'loadTopSNI');
    } catch (err) { console.error('Top SNI error:', err); }
}

async function loadAccountStats(page = 1) {
    try {
        const resp = await api(`/admin/dashboard/account-stats?page=${page}&per_page=25`);
        const d = await resp.json();
        const tbody = document.getElementById('account-stats-tbody');
        tbody.innerHTML = d.items.map(a => `
            <tr>
                <td title="${escapeHtml(a.description || '')}">${escapeHtml(a.account_id)}${a.description ? ' (' + escapeHtml(a.description) + ')' : ''}</td>
                <td>${formatBytes(a.today_downloaded)}</td>
                <td>${formatBytes(a.total_downloaded)}</td>
                <td>${formatBytes(a.today_uploaded)}</td>
                <td>${formatBytes(a.total_uploaded)}</td>
            </tr>
        `).join('') || '<tr><td colspan="5">Нет данных</td></tr>';
        renderDashPagination(document.getElementById('account-stats-pagination'), d.total, d.page, d.per_page, 'loadAccountStats');
    } catch (err) { console.error('Account stats error:', err); }
}

function renderDashPagination(container, total, page, perPage, callback) {
    const pages = Math.ceil(total / perPage);
    if (pages <= 1) { container.innerHTML = ''; return; }
    let html = `<button ${page <= 1 ? 'disabled' : ''} onclick="${callback}(${page - 1})">&laquo;</button>`;
    for (let i = 1; i <= Math.min(pages, 5); i++) {
        html += `<button class="${i === page ? 'active' : ''}" onclick="${callback}(${i})">${i}</button>`;
    }
    if (pages > 5) html += `<button disabled>...</button><button onclick="${callback}(${pages})">${pages}</button>`;
    html += `<button ${page >= pages ? 'disabled' : ''} onclick="${callback}(${page + 1})">&raquo;</button>`;
    container.innerHTML = html;
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
    const accountId = getAccountFilter('device-account-filter');
    let url = `/admin/devices?page=${page}&per_page=50&search=${encodeURIComponent(search)}`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    const resp = await api(url);
    const d = await resp.json();
    const tbody = document.getElementById('devices-tbody');
    tbody.innerHTML = d.items.map(dev => {
        const accLabel = dev.account_description
            ? `${dev.account_id || '-'} (${escapeHtml(dev.account_description)})`
            : (dev.account_id || '-');
        return `<tr>
            <td title="${escapeHtml(dev.account_id || '')}">${escapeHtml(accLabel)}</td>
            <td>${escapeHtml(dev.android_id)}</td>
            <td>${escapeHtml(dev.manufacturer || '')} ${escapeHtml(dev.device_model || '')}</td>
            <td>${dev.android_version || '-'}</td>
            <td>${dev.app_version || '-'}</td>
            <td>${escapeHtml(dev.carrier || '-')}</td>
            <td>${dev.is_online ? '<span class="badge badge-green">Онлайн</span>' : '<span class="badge badge-red">Оффлайн</span>'}</td>
            <td>${formatDate(dev.last_seen_at)}</td>
            <td>
                <button class="btn btn-sm btn-primary" onclick="viewDeviceDetail('${dev.id}')">Детали</button>
                <button class="btn btn-sm btn-primary" onclick="viewDeviceSessions('${dev.id}')">Сессии</button>
                <button class="btn btn-sm btn-primary" onclick="viewDeviceChanges('${dev.id}')">Изменения</button>
            </td>
        </tr>`;
    }).join('');
    renderPagination(document.getElementById('devices-pagination'), d.total, d.page, d.per_page, 'loadDevices');
}

function viewDeviceSessions(deviceId) {
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    document.querySelector('[data-section="sessions"]').classList.add('active');
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.getElementById('section-sessions').classList.add('active');
    document.getElementById('session-device-filter').value = deviceId;
    loadSessions(1);
}

async function viewDeviceDetail(deviceId) {
    try {
        const resp = await api(`/admin/devices/${deviceId}`);
        const dev = await resp.json();
        const body = document.getElementById('device-detail-body');

        const permissionCatalog = [
            { key: 'android.permission.CAMERA', label: 'Камера' },
            { key: 'android.permission.READ_MEDIA_IMAGES', label: 'Фото/изображения' },
            { key: 'android.permission.READ_MEDIA_VIDEO', label: 'Видео' },
            { key: 'android.permission.READ_MEDIA_AUDIO', label: 'Аудио' },
            { key: 'android.permission.READ_EXTERNAL_STORAGE', label: 'Файлы (чтение)' },
            { key: 'android.permission.WRITE_EXTERNAL_STORAGE', label: 'Файлы (запись)' },
            { key: 'android.permission.POST_NOTIFICATIONS', label: 'Уведомления' },
            { key: 'android.permission.REQUEST_INSTALL_PACKAGES', label: 'Установка APK' },
            { key: 'android.permission.ACCESS_NETWORK_STATE', label: 'Состояние сети' },
            { key: 'android.permission.INTERNET', label: 'Интернет' },
            { key: 'android.permission.QUERY_ALL_PACKAGES', label: 'Список приложений' },
        ];
        const permsMap = {};
        (dev.permissions || []).forEach(p => { permsMap[p.name] = p; });

        let permHtml = '<h4 style="margin-top:16px;">Разрешения приложения</h4><table><thead><tr><th>Право</th><th>Статус</th><th>Android permission</th></tr></thead><tbody>';
        permissionCatalog.forEach(item => {
            const p = permsMap[item.key];
            const granted = p?.granted === true;
            const status = p ? (granted ? '<span class="badge badge-green">Разрешено</span>' : '<span class="badge badge-red">Запрещено</span>') : '<span class="badge badge-yellow">Нет данных</span>';
            permHtml += `<tr><td>${escapeHtml(item.label)}</td><td>${status}</td><td><code>${escapeHtml(item.key)}</code></td></tr>`;
        });

        // Show extra permissions requested by app but not included in the base catalog.
        const extras = (dev.permissions || []).filter(p => !permissionCatalog.some(c => c.key === p.name));
        extras.forEach(p => {
            permHtml += `<tr><td>${escapeHtml(p.label || p.name)}</td><td>${p.granted ? '<span class="badge badge-green">Разрешено</span>' : '<span class="badge badge-red">Запрещено</span>'}</td><td><code>${escapeHtml(p.name)}</code></td></tr>`;
        });
        permHtml += '</tbody></table>';

        // Battery bar
        let batteryHtml = '';
        if (dev.battery_level != null) {
            const lvl = dev.battery_level;
            const color = lvl > 50 ? 'var(--green)' : lvl > 20 ? 'var(--yellow)' : 'var(--red)';
            batteryHtml = `<div class="battery-bar-wrap"><div class="battery-bar-track"><div class="battery-bar" style="width:${lvl}%;background:${color};"></div></div><span class="battery-label">${lvl}%</span></div>`;
        }

        // Update modal header with battery
        const modalHeader = document.querySelector('#device-detail-modal .modal-header h3');
        modalHeader.innerHTML = `Детали устройства ${batteryHtml}`;

        body.innerHTML = `
            <div class="detail-grid">
                <div><strong>ID:</strong> ${dev.id}</div>
                <div><strong>Android ID:</strong> ${escapeHtml(dev.android_id)}</div>
                <div><strong>Аккаунт:</strong> ${escapeHtml(dev.account_id || '-')} ${dev.account_description ? '(' + escapeHtml(dev.account_description) + ')' : ''}</div>
                <div><strong>Устройство:</strong> ${escapeHtml(dev.manufacturer || '')} ${escapeHtml(dev.device_model || '')}</div>
                <div><strong>Android:</strong> ${dev.android_version || '-'} (API ${dev.api_level || '-'})</div>
                <div><strong>Версия приложения:</strong> ${dev.app_version || '-'}</div>
                <div><strong>Разрешение экрана:</strong> ${dev.screen_resolution || '-'}</div>
                <div><strong>DPI:</strong> ${dev.dpi || '-'}</div>
                <div><strong>Язык:</strong> ${dev.language || '-'}</div>
                <div><strong>Часовой пояс:</strong> ${escapeHtml(dev.timezone || '-')}</div>
                <div><strong>Root:</strong> ${dev.is_rooted ? 'Да' : 'Нет'}</div>
                <div><strong>Оператор:</strong> ${escapeHtml(dev.carrier || '-')}</div>
                <div><strong>RAM:</strong> ${dev.ram_total_mb ? dev.ram_total_mb + ' MB' : '-'}</div>
                <div><strong>Статус:</strong> ${dev.is_online ? '<span class="badge badge-green">Онлайн</span>' : '<span class="badge badge-red">Оффлайн</span>'}</div>
                <div><strong>Заметка:</strong> ${escapeHtml(dev.note || '-')}</div>
                <div><strong>Создан:</strong> ${formatDate(dev.created_at)}</div>
                <div><strong>Посл. активность:</strong> ${formatDate(dev.last_seen_at)}</div>
            </div>
            ${permHtml}
            <div style="margin-top:16px;display:flex;gap:8px;">
                <button class="btn btn-primary btn-sm" onclick="requestDeviceLogs('${dev.id}')">Запросить журнал</button>
                <button class="btn btn-primary btn-sm" onclick="closeDeviceDetail();viewDeviceChanges('${dev.id}')">Изменения</button>
            </div>
        `;
        document.getElementById('device-detail-modal').style.display = 'flex';
    } catch (err) { console.error('Device detail error:', err); }
}

async function requestDeviceLogs(deviceId) {
    try {
        const resp = await api(`/admin/devices/${deviceId}/request-logs`, { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
            alert('Команда отправлена. Устройство отправит журнал при следующем подключении.');
        } else {
            alert('Ошибка: ' + (data.detail || 'Неизвестная ошибка'));
        }
    } catch (err) {
        alert('Ошибка отправки команды');
        console.error('Request logs error:', err);
    }
}

function closeDeviceDetail() {
    document.getElementById('device-detail-modal').style.display = 'none';
}

// ========== DEVICE CHANGES ==========
let _changesDeviceId = null;

async function viewDeviceChanges(deviceId, page = 1) {
    _changesDeviceId = deviceId;
    try {
        const resp = await api(`/admin/devices/${deviceId}/changes?page=${page}&per_page=50`);
        const d = await resp.json();
        const tbody = document.getElementById('device-changes-tbody');
        tbody.innerHTML = d.items.map(c => `
            <tr>
                <td>${escapeHtml(c.field_name)}</td>
                <td>${escapeHtml(c.old_value || '-')}</td>
                <td>${escapeHtml(c.new_value || '-')}</td>
                <td>${formatDate(c.changed_at)}</td>
            </tr>
        `).join('') || '<tr><td colspan="4">Нет изменений</td></tr>';
        renderPagination(document.getElementById('device-changes-pagination'), d.total, d.page, d.per_page, 'viewDeviceChangesPage');
        document.getElementById('device-changes-modal').style.display = 'flex';
    } catch (err) { console.error('Device changes error:', err); }
}

function viewDeviceChangesPage(page) {
    if (_changesDeviceId) viewDeviceChanges(_changesDeviceId, page);
}

function closeDeviceChanges() {
    document.getElementById('device-changes-modal').style.display = 'none';
}

// ========== SESSIONS ==========
async function loadSessions(page = 1) {
    const deviceId = document.getElementById('session-device-filter')?.value || '';
    const accountId = getAccountFilter('session-account-filter');
    let url = `/admin/sessions?page=${page}&per_page=50`;
    if (deviceId) url += `&device_id=${deviceId}`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    const resp = await api(url);
    const d = await resp.json();
    const tbody = document.getElementById('sessions-tbody');
    tbody.innerHTML = d.items.map(s => {
        let duration;
        if (s.disconnected_at) {
            const mins = Math.round((new Date(s.disconnected_at) - new Date(s.connected_at)) / 60000);
            duration = mins >= 60 ? Math.floor(mins / 60) + ' ч ' + (mins % 60) + ' мин' : mins + ' мин';
        } else {
            duration = '<span class="badge badge-green">Активна</span>';
        }
        let ipChange = 'Нет';
        if (s.server_ip_changes) {
            try {
                const changes = JSON.parse(s.server_ip_changes);
                if (changes.length > 0) {
                    ipChange = changes.map(c => {
                        const d = new Date(c.changed_at);
                        const dt = d.toLocaleString('ru-RU', {day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
                        return `Да в ${dt} на: ${escapeHtml(c.ip)}`;
                    }).join('<br>');
                }
            } catch(e) { ipChange = 'Нет'; }
        }
        return `<tr>
            <td title="${s.device_id}">${s.device_id.slice(0, 8)}...</td>
            <td>${s.protocol || '-'}</td>
            <td>${escapeHtml(s.server_address || '-')}</td>
            <td>${s.server_ip || '-'}</td>
            <td>${s.client_ip || '-'}</td>
            <td>${ipChange}</td>
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
    const accountId = getAccountFilter('sni-account-filter');
    let url = `/admin/sni?page=${page}&per_page=100`;
    if (domain) url += `&domain=${encodeURIComponent(domain)}`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('sni-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${escapeHtml(l.domain_display || l.domain)}</td>
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
    const accountId = getAccountFilter('dns-account-filter');
    let url = `/admin/dns?page=${page}&per_page=100`;
    if (domain) url += `&domain=${encodeURIComponent(domain)}`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('dns-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${escapeHtml(l.domain)}</td>
            <td>${l.resolved_ip || '-'}</td>
            <td>${l.query_type || '-'}</td>
            <td>${l.hit_count}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('dns-pagination'), d.total, d.page, d.per_page, 'loadDNS');
}

// ========== CONNECTIONS ==========
async function loadConnections(page = 1) {
    const search = document.getElementById('conn-search')?.value || '';
    const accountId = getAccountFilter('conn-account-filter');
    let url = `/admin/connections?page=${page}&per_page=100`;
    if (search) url += `&dest_ip=${encodeURIComponent(search)}`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('connections-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td>${l.dest_ip}</td>
            <td>${l.dest_port}</td>
            <td>${l.protocol || '-'}</td>
            <td>${escapeHtml(l.domain || '-')}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('connections-pagination'), d.total, d.page, d.per_page, 'loadConnections');
}

// ========== ERRORS ==========
async function loadErrors(page = 1) {
    const accountId = getAccountFilter('errors-account-filter');
    let url = `/admin/errors?page=${page}&per_page=50`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    const resp = await api(url);
    const d = await resp.json();
    document.getElementById('errors-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td><span class="badge badge-${l.error_type === 'crash' ? 'red' : 'yellow'}">${escapeHtml(l.error_type)}</span></td>
            <td title="${escapeHtml(l.message || '')}">${escapeHtml((l.message || '-').slice(0, 60))}</td>
            <td>${l.app_version || '-'}</td>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td>${formatDate(l.timestamp)}</td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('errors-pagination'), d.total, d.page, d.per_page, 'loadErrors');
}

// ========== DEVICE LOGS ==========
async function loadDeviceLogs(page = 1) {
    const accountId = getAccountFilter('devlogs-account-filter');
    const logType = document.getElementById('devlogs-type-filter')?.value || '';
    let url = `/admin/device-logs?page=${page}&per_page=50`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    if (logType) url += `&log_type=${encodeURIComponent(logType)}`;
    const resp = await api(url);
    const d = await resp.json();
    const typeLabels = { logcat: 'Приложение', full_log: 'Полный журнал', xray_access: 'Xray access', xray_error: 'Xray error', crash: 'Crash' };
    document.getElementById('devlogs-tbody').innerHTML = d.items.map(l => `
        <tr>
            <td title="${l.device_id}">${l.device_id.slice(0, 8)}...</td>
            <td><span class="badge badge-blue">${escapeHtml(typeLabels[l.log_type] || l.log_type)}</span></td>
            <td>${formatBytes(l.content_size)}</td>
            <td>${formatDate(l.uploaded_at)}</td>
            <td>
                <button class="btn btn-sm btn-primary" onclick="viewDevLog('${l.id}')">Открыть</button>
                <button class="btn btn-sm btn-danger" onclick="deleteDevLog('${l.id}')">Удалить</button>
            </td>
        </tr>
    `).join('');
    renderPagination(document.getElementById('devlogs-pagination'), d.total, d.page, d.per_page, 'loadDeviceLogs');
}

async function viewDevLog(logId) {
    try {
        const resp = await api(`/admin/device-logs/${logId}`);
        const d = await resp.json();
        document.getElementById('devlog-content').textContent = d.content;
        document.getElementById('devlog-download-btn').onclick = () => downloadDevLog(logId);
        document.getElementById('devlog-upload-btn').onclick = () => uploadDevLog(logId);
        document.getElementById('devlog-modal').style.display = 'flex';
    } catch (err) { console.error('View device log error:', err); }
}

function closeDevLogModal() {
    document.getElementById('devlog-modal').style.display = 'none';
}

function copyDevLogContent() {
    const content = document.getElementById('devlog-content').textContent;
    navigator.clipboard.writeText(content).then(() => {
        const btn = event.target;
        const orig = btn.textContent;
        btn.textContent = 'Скопировано!';
        setTimeout(() => btn.textContent = orig, 1500);
    }).catch(() => alert('Не удалось скопировать'));
}

async function downloadDevLog(logId) {
    try {
        const resp = await api(`/admin/device-logs/${logId}`);
        const d = await resp.json();
        const blob = new Blob([d.content], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `device_log_${logId}.txt`;
        a.click();
        URL.revokeObjectURL(url);
    } catch (err) { console.error('Download error:', err); }
}

async function uploadDevLog(logId) {
    const btn = document.getElementById('devlog-upload-btn');
    const origText = btn.textContent;
    btn.textContent = 'Загрузка...';
    btn.disabled = true;
    try {
        const resp = await api(`/admin/device-logs/${logId}/upload`, { method: 'POST' });
        const d = await resp.json();
        if (resp.ok && d.url) {
            // Show result in a modal-like dialog
            const overlay = document.createElement('div');
            overlay.className = 'modal-overlay';
            overlay.style.display = 'flex';
            overlay.innerHTML = `
                <div class="modal-content" style="max-width:500px;">
                    <div class="modal-header">
                        <h3>Файл загружен</h3>
                        <button class="btn btn-sm btn-danger" onclick="this.closest('.modal-overlay').remove()">&times;</button>
                    </div>
                    <div style="padding:16px;">
                        <p>Ссылка на скачивание:</p>
                        <input type="text" value="${d.url}" readonly style="width:100%;padding:8px;background:#1a1a2e;color:#e0e0ff;border:1px solid #333;border-radius:4px;font-size:13px;" onclick="this.select()">
                        <button class="btn btn-sm btn-primary" style="margin-top:10px;" onclick="navigator.clipboard.writeText('${d.url}');this.textContent='Скопировано!';setTimeout(()=>this.textContent='Скопировать ссылку',1500)">Скопировать ссылку</button>
                    </div>
                </div>
            `;
            overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
            document.body.appendChild(overlay);
        } else {
            alert('Ошибка: ' + (d.detail || 'Неизвестная ошибка'));
        }
    } catch (err) {
        alert('Ошибка загрузки');
        console.error('Upload error:', err);
    } finally {
        btn.textContent = origText;
        btn.disabled = false;
    }
}

async function deleteDevLog(logId) {
    if (!confirm('Удалить этот лог?')) return;
    await api(`/admin/device-logs/${logId}`, { method: 'DELETE' });
    loadDeviceLogs();
}

async function clearAllDeviceLogs() {
    if (!confirm('Удалить ВСЕ логи устройств? Это действие необратимо.')) return;
    await api('/admin/device-logs', { method: 'DELETE' });
    loadDeviceLogs();
}

// ========== JOURNAL (LIVE LOGS) ==========
async function startLogs() {
    await api('/admin/logs/start', { method: 'POST' });
    document.getElementById('btn-log-start').disabled = true;
    document.getElementById('btn-log-stop').disabled = false;
    document.getElementById('journal-status').textContent = 'Запись...';
    document.getElementById('journal-status').classList.add('active');
    if (!journalInterval) {
        journalInterval = setInterval(refreshLogs, 2000);
    }
    refreshLogs();
}

async function stopLogs() {
    await api('/admin/logs/stop', { method: 'POST' });
    document.getElementById('btn-log-start').disabled = false;
    document.getElementById('btn-log-stop').disabled = true;
    document.getElementById('journal-status').textContent = 'Остановлен';
    document.getElementById('journal-status').classList.remove('active');
    if (journalInterval) { clearInterval(journalInterval); journalInterval = null; }
}

async function clearLogs() {
    await api('/admin/logs/clear', { method: 'POST' });
    document.getElementById('journal-terminal').innerHTML = '<div class="journal-empty">Журнал очищен</div>';
}

async function refreshLogs() {
    try {
        const logType = document.getElementById('log-type-filter')?.value || 'all';
        const resp = await api(`/admin/logs?log_type=${logType}&limit=200`);
        const d = await resp.json();

        if (d.enabled) {
            document.getElementById('btn-log-start').disabled = true;
            document.getElementById('btn-log-stop').disabled = false;
            document.getElementById('journal-status').textContent = 'Запись...';
            document.getElementById('journal-status').classList.add('active');
            if (!journalInterval) {
                journalInterval = setInterval(refreshLogs, 2000);
            }
        }

        const terminal = document.getElementById('journal-terminal');
        if (!d.items || d.items.length === 0) {
            terminal.innerHTML = '<div class="journal-empty">Нет записей</div>';
            return;
        }

        const typeLabels = { request: 'ЗАПРОС', processing: 'ОБРАБОТКА', error: 'ОШИБКА' };
        const typeClasses = { request: 'log-request', processing: 'log-processing', error: 'log-error' };

        terminal.innerHTML = d.items.map(log => {
            const ts = log.timestamp ? new Date(log.timestamp).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
            const typeLabel = typeLabels[log.type] || log.type.toUpperCase();
            const typeClass = typeClasses[log.type] || '';
            let details = '';
            if (log.details && Object.keys(log.details).length > 0) {
                if (log.details.body) details += `\n    Тело: ${escapeHtml(log.details.body.slice(0, 200))}`;
                if (log.details.ip) details += `\n    IP: ${log.details.ip}`;
                if (log.details.api_key) details += `\n    Ключ: ${log.details.api_key}`;
                if (log.details.traceback) details += `\n    ${escapeHtml(log.details.traceback.slice(0, 500))}`;
            }
            return `<div class="log-entry ${typeClass}"><span class="log-time">${ts}</span> <span class="log-type">[${typeLabel}]</span> ${escapeHtml(log.message)}${details}</div>`;
        }).join('');

        terminal.scrollTop = 0;
    } catch (err) { console.error('Journal error:', err); }
}



// ========== REMNAWAVE LOGS ==========
async function loadRemnawaveNodes(page = 1) {
    const search = document.getElementById('rnw-node-search')?.value || '';
    const resp = await api(`/admin/remnawave-logs/nodes?page=${page}&per_page=50&search=${encodeURIComponent(search)}`);
    const d = await resp.json();
    const tbody = document.getElementById('rnw-nodes-tbody');
    tbody.innerHTML = d.items.map(i => `
        <tr>
            <td>${escapeHtml(i.node)}</td>
            <td>${formatDate(i.last_message)}</td>
        </tr>
    `).join('') || '<tr><td colspan="2">Нет данных</td></tr>';
    renderPagination(document.getElementById('rnw-nodes-pagination'), d.total, d.page, d.per_page, 'loadRemnawaveNodes');
}

function loadRemnawaveNodesDebounced(page = 1) {
    if (remnawaveNodesSearchTimer) clearTimeout(remnawaveNodesSearchTimer);
    remnawaveNodesSearchTimer = setTimeout(() => loadRemnawaveNodes(page), 350);
}

function resetRemnawaveSelection() {
    remnawaveSelectedAccount = null;
    const topTitle = document.getElementById('rnw-top-title');
    const lastTitle = document.getElementById('rnw-last-title');
    const topBody = document.getElementById('rnw-top-tbody');
    const recentBody = document.getElementById('rnw-recent-tbody');
    const recentPagination = document.getElementById('rnw-recent-pagination');
    if (topTitle) topTitle.textContent = 'Топ 25 доменов';
    if (lastTitle) lastTitle.textContent = 'Последние запросы';
    if (topBody) topBody.innerHTML = '<tr><td colspan="2">Выберите аккаунт</td></tr>';
    if (recentBody) recentBody.innerHTML = '<tr><td colspan="2">Выберите аккаунт</td></tr>';
    if (recentPagination) recentPagination.innerHTML = '';
}

async function loadRemnawaveAccounts(page = 1) {
    const search = document.getElementById('rnw-account-search')?.value || '';
    const resp = await api(`/admin/remnawave-logs/accounts?page=${page}&per_page=50&search=${encodeURIComponent(search)}`);
    const d = await resp.json();
    const tbody = document.getElementById('rnw-accounts-tbody');
    tbody.innerHTML = d.items.map(i => {
        const account = String(i.account || '');
        const accountEsc = escapeHtml(account);
        const accountAttr = encodeURIComponent(account);
        return `
        <tr>
            <td>${accountEsc}</td>
            <td>${formatDate(i.last_activity)}</td>
            <td>${Number(i.total_requests || 0).toLocaleString('ru-RU')}</td>
            <td><button class="btn btn-sm btn-primary rnw-select-account-btn" data-account-enc="${accountAttr}">Открыть</button></td>
        </tr>
    `;
    }).join('') || '<tr><td colspan="4">Нет данных</td></tr>';
    renderPagination(document.getElementById('rnw-accounts-pagination'), d.total, d.page, d.per_page, 'loadRemnawaveAccounts');
}

function loadRemnawaveAccountsDebounced(page = 1) {
    if (remnawaveAccountsSearchTimer) clearTimeout(remnawaveAccountsSearchTimer);
    remnawaveAccountsSearchTimer = setTimeout(() => loadRemnawaveAccounts(page), 350);
}

async function selectRemnawaveAccount(account) {
    remnawaveSelectedAccount = account;
    document.getElementById('rnw-top-title').textContent = `Топ 25 доменов: ${account}`;
    document.getElementById('rnw-last-title').textContent = `Последние запросы: ${account}`;
    await loadRemnawaveTop();
    await loadRemnawaveRecent(1);
}

async function loadRemnawaveTop() {
    if (!remnawaveSelectedAccount) return;
    const resp = await api(`/admin/remnawave-logs/${encodeURIComponent(remnawaveSelectedAccount)}/top-domains?selected=1&limit=25&days=365`);
    const d = await resp.json();
    const tbody = document.getElementById('rnw-top-tbody');
    tbody.innerHTML = d.items.map(i => `
        <tr>
            <td>${escapeHtml(i.dns)}</td>
            <td>${i.hits}</td>
        </tr>
    `).join('') || '<tr><td colspan="2">Нет данных</td></tr>';
}

async function loadRemnawaveRecent(page = 1) {
    if (!remnawaveSelectedAccount) return;
    const fromVal = document.getElementById('rnw-from')?.value || '';
    const toVal = document.getElementById('rnw-to')?.value || '';
    const qVal = document.getElementById('rnw-q')?.value || '';

    let url = `/admin/remnawave-logs/${encodeURIComponent(remnawaveSelectedAccount)}/queries?selected=1&page=${page}&per_page=50`;
    if (fromVal) url += `&from_ts=${encodeURIComponent(new Date(fromVal).toISOString())}`;
    if (toVal) url += `&to_ts=${encodeURIComponent(new Date(toVal).toISOString())}`;
    if (qVal) url += `&q=${encodeURIComponent(qVal)}`;

    const resp = await api(url);
    const d = await resp.json();
    const tbody = document.getElementById('rnw-recent-tbody');
    tbody.innerHTML = d.items.map(i => `
        <tr>
            <td>${escapeHtml(i.dns)}</td>
            <td>${formatDate(i.requested_at)}</td>
        </tr>
    `).join('') || '<tr><td colspan="2">Нет данных</td></tr>';

    renderPagination(document.getElementById('rnw-recent-pagination'), d.total, d.page, d.per_page, 'loadRemnawaveRecent');
}

// ========== REMNAWAVE AUDIT ==========
async function loadRemnawaveAudit(page = 1) {
    const account = document.getElementById('rnw-audit-account')?.value || '';
    const search = document.getElementById('rnw-audit-search')?.value || '';
    let url = `/admin/remnawave-audit?page=${page}&per_page=50`;
    if (account) url += `&account=${encodeURIComponent(account)}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;

    const resp = await api(url);
    const d = await resp.json();
    const tbody = document.getElementById('rnw-audit-tbody');
    tbody.innerHTML = d.items.map(i => {
        const domain = escapeHtml(i.dns_root || '');
        const rawDomainJson = JSON.stringify(String(i.dns_root || ''));
        return `
        <tr>
            <td>${formatDate(i.time)}</td>
            <td>${escapeHtml(i.account_login)}</td>
            <td>${domain}</td>
            <td><button class="btn btn-danger btn-sm" onclick='excludeRemnawaveDomain(${rawDomainJson})'>Исключить</button></td>
        </tr>
    `;
    }).join('') || '<tr><td colspan="4">Нет данных</td></tr>';
    renderPagination(document.getElementById('rnw-audit-pagination'), d.total, d.page, d.per_page, 'loadRemnawaveAudit');
}

async function excludeRemnawaveDomain(domain) {
    if (!domain) return;
    if (!confirm(`Исключить домен ${domain} из DNS 18+?`)) return;
    const resp = await api('/admin/remnawave-audit/exclude', {
        method: 'POST',
        body: JSON.stringify({ domain, reason: 'manual_exclude_ui' }),
    });
    if (!resp.ok) {
        alert('Не удалось исключить домен');
        return;
    }
    loadRemnawaveAudit(1);
}

// ========== EXPORT ==========
async function exportSNI() {
    const days = prompt('Экспорт SNI за сколько дней?', '7');
    if (!days) return;
    const accountId = getAccountFilter('sni-account-filter');
    let url = `/admin/export/sni?days=${days}`;
    if (accountId) url += `&account_id=${encodeURIComponent(accountId)}`;
    try {
        const resp = await api(url);
        const blob = await resp.blob();
        const blobUrl = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = `sni_export_${days}d.csv`;
        a.click();
        URL.revokeObjectURL(blobUrl);
    } catch (err) { console.error('Export error:', err); }
}

// ========== CLICK-OUTSIDE TO CLOSE MODALS ==========
document.addEventListener('click', (e) => {
    if (!e.target.classList.contains('modal-overlay')) return;
    const modals = {
        'device-detail-modal': closeDeviceDetail,
        'device-changes-modal': closeDeviceChanges,
        'devlog-modal': closeDevLogModal,
    };
    const fn = modals[e.target.id];
    if (fn) fn();
});

document.addEventListener('click', (e) => {
    const accountBtn = e.target.closest('.rnw-select-account-btn');
    if (accountBtn) {
        e.preventDefault();
        const accountEncoded = String(accountBtn.getAttribute('data-account-enc') || '').trim();
        const account = accountEncoded ? decodeURIComponent(accountEncoded) : '';
        if (account) {
            selectRemnawaveAccount(account);
        }
        return;
    }

    const deleteBtn = e.target.closest('.sql-browser-delete-row');
    if (deleteBtn) {
        e.preventDefault();
        const rowIndex = Number(deleteBtn.getAttribute('data-row-index'));
        deleteSqlBrowserRowByIndex(rowIndex);
        return;
    }

    if (e.target.id === 'table-cell-viewer-close') {
        closeTableCellViewer();
        return;
    }

    if (e.target.id === 'table-cell-viewer-copy') {
        copyTableCellViewerText();
        return;
    }

    const td = e.target.closest('table tbody td');
    if (!td) return;

    const anchor = e.target.closest('a');
    if (anchor && String(anchor.getAttribute('href') || '').startsWith('#')) {
        e.preventDefault();
    }

    if (e.target.closest('button, input, select, textarea, label')) return;

    const fullText = getCellFullText(td);
    if (!shouldOpenCellViewer(td, fullText)) return;

    openTableCellViewer(fullText);
});

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeTableCellViewer();
});

// ========== INIT ==========
if (token) {
    api('/admin/dashboard').then(r => {
        if (r.ok) showApp(); else logout();
    }).catch(() => logout());
} else {
    document.getElementById('login-page').style.display = 'flex';
}
