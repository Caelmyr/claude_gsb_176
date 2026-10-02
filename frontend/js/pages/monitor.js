/* 作业监控 Monitor */
Components.init('monitor');
const C = Components;

let currentJob = '';
let kindFilter = '';
let lastData = null;
let editingDeps = false;

function setupFilters() {
  const kinds = [['', '全部 All'], ['map', 'Map'], ['reduce', 'Reduce']];
  document.getElementById('kind-filter').innerHTML = kinds.map(([k, label]) =>
    `<span class="pill${kindFilter === k ? ' active' : ''}" data-k="${k}">${label}</span>`).join('');
  document.querySelectorAll('#kind-filter .pill').forEach(p => {
    p.addEventListener('click', () => { kindFilter = p.getAttribute('data-k'); setupFilters(); render(); });
  });
}

async function render() {
  if (!currentJob) return;
  try {
    lastData = await API.get('/api/jobs/' + currentJob);
  } catch (e) { return; }
  const job = lastData.job;
  const tasks = lastData.tasks || [];

  // Summary + stage progress
  const sp = job.stage_progress || {};
  const sh = job.shuffle || {};
  const stageBar = (key, label) => {
    const st = sp[key] || {};
    let pct = st.pct || 0;
    if (key === 'shuffle') pct = sh.pct || 0;
    return C.progress(pct, label + (key === 'shuffle' ? ` (${sh.done||0}/${sh.total||0})` : ` (${st.done||0}/${st.total||0})`));
  };
  document.getElementById('summary').innerHTML = `
    <div class="stat-tiles">
      <div class="stat"><div class="label">作业 Job</div><div class="value" style="font-size:20px">${C.esc(job.name)}</div>
        <div class="delta mono">${C.esc(job.job_id)}</div></div>
      <div class="stat"><div class="label">状态 Status</div><div class="value" style="font-size:20px">${C.stateBadge(job.status, true)}</div></div>
      <div class="stat"><div class="label">Map 任务 Tasks</div><div class="value">${job.num_map_tasks}</div></div>
      <div class="stat"><div class="label">Reduce 任务 Tasks</div><div class="value">${job.num_reduce_tasks}</div></div>
      <div class="stat"><div class="label">输入记录 Records</div><div class="value">${C.fmtNum(job.input_rows)}</div></div>
      <div class="stat"><div class="label">故障事件 Faults</div><div class="value ${job.fault_count ? 'bad' : ''}">${job.fault_count || 0}</div></div>
    </div>
    <div class="card mt">
      <div class="grid cols-3">
        <div>${stageBar('map', 'Map')}</div>
        <div>${stageBar('shuffle', 'Shuffle')}</div>
        <div>${stageBar('reduce', 'Reduce')}</div>
      </div>
    </div>`;

  if (!editingDeps) renderDeps(job);

  // Tasks table
  const filtered = tasks.filter(t => !kindFilter || t.kind === kindFilter);
  document.getElementById('tasks').innerHTML = filtered.length ? C.table([
    { key: 'task_id', label: '任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
    { key: 'kind', label: '类型 Kind', render: r => r.kind },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'worker_name', label: 'Worker', render: r => C.esc(r.worker_name || r.worker_id || '-') },
    { key: 'attempts', label: '尝试 Att', render: r => r.attempts, num: true },
    { key: 'progress', label: '进度 Progress', render: r => `<div style="min-width:120px">${C.progress((r.progress||0)*100)}</div>` },
    { key: 'records_processed', label: '处理 Records', render: r => C.fmtNum(r.records_processed), num: true },
    { key: 'duration_ms', label: '耗时 Dur', render: r => C.fmtDur(r.duration_ms), num: true },
  ], filtered) : C.empty();
}

// ------------------------------------------------------------------
// Dependency card: upstream status, downstream dependents, mid-flight edits.
// ------------------------------------------------------------------
async function renderDeps(job) {
  const host = document.getElementById('deps');
  const deps = job.dependency_status || [];
  const dependents = job.dependents || [];
  if (!deps.length && !dependents.length && job.status !== 'WAITING' && job.status !== 'SKIPPED') {
    host.innerHTML = '';
    return;
  }

  let banner = '';
  if (job.status === 'SKIPPED') {
    banner = `<div class="badge bad" style="font-size:13px;padding:6px 10px">已跳过 Skipped — ${C.esc(job.skip_reason || 'dependency did not succeed')}</div>`;
  } else if (job.status === 'WAITING') {
    const unmet = deps.filter(d => !d.met).length;
    banner = `<div class="badge warn" style="font-size:13px;padding:6px 10px">等待依赖 Waiting — ${unmet} 个前置作业未成功 / ${unmet} dependencies not yet succeeded</div>`;
  }

  const depRows = deps.length ? C.table([
    { key: 'name', label: '前置作业 Dependency', render: r => `${C.esc(r.name)} <span class="mono muted">${C.esc(r.job_id)}</span>` },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    {
      key: 'met', label: '是否满足 Met', render: r => r.met
        ? '<span class="badge good">满足 Met</span>'
        : (['FAILED', 'CANCELLED', 'SKIPPED', 'MISSING'].includes(r.status)
          ? '<span class="badge bad">未满足 Unmet</span>'
          : '<span class="badge muted">等待中 Pending</span>'),
    },
  ], deps) : '<div class="empty">无前置依赖 No upstream dependencies</div>';

  const depChips = dependents.length
    ? dependents.map(d => `<span class="badge ${C.CLASS[d.status] || 'muted'}" title="${C.esc(d.job_id)}">${C.esc(d.name)}</span>`).join(' ')
    : '<span class="muted small">无 None</span>';

  const editBtn = job.status === 'WAITING'
    ? '<button class="btn small" id="deps-edit">修改依赖 Edit dependencies</button>' : '';

  host.innerHTML = `
    <div class="card mt">
      <h2>作业依赖 <span class="sub">Dependencies</span></h2>
      ${banner}
      <div class="mt">${depRows}</div>
      <div class="mt small"><span class="muted">被以下作业依赖 Depended on by:</span> ${depChips}</div>
      <div class="mt" id="deps-editor">${editBtn}</div>
    </div>`;

  const btn = document.getElementById('deps-edit');
  if (btn) btn.addEventListener('click', () => openDepsEditor(job));
}

async function openDepsEditor(job) {
  editingDeps = true;
  const host = document.getElementById('deps-editor');
  let jobs = [];
  try { jobs = (await API.get('/api/jobs')).jobs || []; } catch (e) { /* fall through */ }
  const candidates = jobs.filter(j => j.job_id !== job.job_id);
  const opts = candidates.map(j => {
    const sel = (job.depends_on || []).includes(j.job_id) ? ' selected' : '';
    return `<option value="${j.job_id}"${sel}>${C.esc(j.name)} — ${j.job_id} (${j.status})</option>`;
  }).join('');
  host.innerHTML = `
    <label class="small muted">前置作业 Depends on（保存后立即重新判定 re-evaluated on save）</label>
    <select id="deps-select" multiple size="6" style="width:100%">${opts}</select>
    <div class="flex mt">
      <button class="btn primary small" id="deps-save">保存 Save</button>
      <button class="btn small" id="deps-cancel">取消 Cancel</button>
      <span class="hint">清空全部选项表示立即放行本作业。Clearing all releases the job immediately.</span>
    </div>`;
  document.getElementById('deps-save').addEventListener('click', async () => {
    const deps = Array.from(document.getElementById('deps-select').selectedOptions).map(o => o.value);
    try {
      await API.put('/api/jobs/' + job.job_id + '/deps', { depends_on: deps });
      C.toast('依赖已更新 Dependencies updated', 'ok');
    } catch (e) {
      C.toast('更新失败 ' + e.message, 'error');
    }
    editingDeps = false;
    render();
  });
  document.getElementById('deps-cancel').addEventListener('click', () => {
    editingDeps = false;
    render();
  });
}


C.jobPicker('job-picker', (id) => { currentJob = id; editingDeps = false; render(); });
document.getElementById('refresh').addEventListener('click', render);
setupFilters();
C.poll(render, 2000).start();
