/* 作业监控 Monitor */
Components.init('monitor');
const C = Components;

let currentJob = '';
let kindFilter = '';
let lastData = null;

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
    const [jobsData, detail] = await Promise.all([
      API.get('/api/jobs'),
      API.get('/api/jobs/' + currentJob),
    ]);
    window.ALL_JOBS = jobsData.jobs || [];
    lastData = detail;
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
  renderDependencies(job);

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

function renderDependencies(job) {
  const d = job.dependencies || { upstream: [], downstream: [], depends_on: [], editable: false };
  const upstream = d.upstream || [];
  const downstream = d.downstream || [];
  const editor = d.editable ? `
    <div class="mt">
      <label>修改前置作业（Ctrl/⌘ 多选；清空表示无依赖）Edit upstream jobs</label>
      <select id="dep-edit" multiple size="5" style="min-height:120px">${depOptions(job)}</select>
      <div class="mt flex">
        <button class="btn small primary" id="dep-save" type="button">保存依赖 Save</button>
        <span class="hint">仍在等待时可中途修改；已开始后为保证结果一致不可修改。</span>
      </div>
    </div>` : `<div class="hint mt">依赖只能在 WAITING（等待中）状态修改。</div>`;
  const upstreamHtml = upstream.length ? C.table([
    { key: 'name', label: '前置 Upstream', render: r => `${C.esc(r.name)}<div class="small mono">${C.esc(r.job_id)}</div>` },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
  ], upstream) : '<div class="empty">无前置作业 No upstream jobs</div>';
  const downstreamHtml = downstream.length ? C.table([
    { key: 'name', label: '后续 Downstream', render: r => `${C.esc(r.name)}<div class="small mono">${C.esc(r.job_id)}</div>` },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
  ], downstream) : '<div class="empty">暂无后续作业 No downstream jobs</div>';
  document.getElementById('dependencies').innerHTML = `
    ${job.skip_reason ? `<div class="alert warn mb"><b>已跳过：</b>${C.esc(job.skip_reason)}</div>` : ''}
    ${job.status === 'WAITING' ? `<div class="hint mb">${(d.blocking || []).length} 个前置作业尚未成功，作业不会启动。</div>` : ''}
    <div class="grid cols-2">
      <div><h3 class="sub">等待全部成功 All must succeed</h3>${upstreamHtml}</div>
      <div><h3 class="sub">将被本作业影响 Downstream</h3>${downstreamHtml}</div>
    </div>
    ${editor}`;
  const btn = document.getElementById('dep-save');
  if (btn) btn.addEventListener('click', saveDependencies);
}

function depOptions(job) {
  const selected = new Set(job.depends_on || []);
  return (window.ALL_JOBS || []).filter(j => j.job_id !== job.job_id).map(j =>
    `<option value="${j.job_id}"${selected.has(j.job_id) ? ' selected' : ''}>` +
    `${C.esc(j.name)} — ${j.status}</option>`).join('');
}

async function saveDependencies() {
  const ids = Array.from(document.getElementById('dep-edit').selectedOptions).map(o => o.value);
  const btn = document.getElementById('dep-save');
  btn.disabled = true;
  try {
    await API.put('/api/jobs/' + currentJob + '/dependencies', { depends_on: ids });
    C.toast('依赖已更新 Dependencies updated', 'ok');
    await render();
  } catch (e) {
    C.toast('依赖更新失败 ' + e.message, 'error');
    btn.disabled = false;
  }
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
document.getElementById('refresh').addEventListener('click', render);
setupFilters();
C.poll(render, 2000).start();
