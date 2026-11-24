import {heading, panel} from '../components/common/heading.js';
import {records} from '../components/common/records.js';
import {badge} from '../components/common/status.js';
import {auditLabels} from '../constants/workflow.js';
import {topic} from '../constants/project.js';
import {date, esc} from '../utils/dom.js';
import {submit} from '../hooks/submit.js';

const bytes = value => `${Number(value || 0).toLocaleString('zh-CN')} B`;

export function runsPage(state, identity) {
  const selected = state.runs.find(row=>row.id===identity) || state.runs[0];
  const cases = new Map(state.cases.map(row=>[row.id, row.title]));
  const rows = state.runs.map(row=>[`<a href="#runs/${row.id}">${esc(cases.get(row.case_id) || row.case_id)}</a>`, badge(row.status), esc(row.owner), esc(date(row.created_at))]);
  const result = state.evidence.find(row=>row.run_id===selected?.id);
  const active = selected && ['queued','running'].includes(selected.status);
  const plan = selected?.peak_bytes != null
    ? `<dl><dt>预留段容量</dt><dd>${bytes(selected.peak_bytes)}</dd><dt>初始段数</dt><dd>${esc(selected.initial_runs)}</dd>${selected.overtakes?`<dt>被越过</dt><dd>${esc(selected.overtakes)} 次</dd>`:''}</dl>`
    : '';
  const detail = selected ? `<h2>${esc(cases.get(selected.case_id) || selected.case_id)}</h2>
    <p data-testid="run-status">${badge(selected.status)}</p>
    <dl><dt>启动时间</dt><dd>${esc(date(selected.started_at))}</dd><dt>完成时间</dt><dd>${esc(date(selected.finished_at))}</dd></dl>
    ${plan}
    ${selected.error ? `<p class="error">${esc(selected.error)}</p>` : ''}
    ${active?'<p class="note">后台处理中，记录会自动刷新。</p>':''}
    ${result?`<a class="button" href="#evidence/${result.id}">查看业务证据</a>`:''}` : '<h2>执行记录</h2><p>从档案创建一条执行记录后，可在此查看进度。</p>';
  const audit = state.audit.filter(x=>selected && x.reference===String(selected.id) && ['enqueue','admit','complete','failed','reclaimed'].includes(x.action));
  return `${heading(topic.modules[2], `${state.spec.entity_labels[1]}与后台处理进度`, '<button class="secondary" type="button" id="refresh">刷新记录</button>')}
    <div class="spread"><section>${records(['档案','状态','经办','提交时间'], rows, '暂无执行记录。')}</section><aside class="brief">${detail}</aside></div>
    ${capacityPanel(state)}
    ${audit.length?panel('操作记录', records(['操作者','操作','时间'], audit.map(x=>[esc(x.actor),esc(auditLabels[x.action]||x.action),esc(date(x.created_at))]))):''}`;
}

function capacityPanel(state) {
  const cap = state.capacity;
  if (!cap) return '';
  const queued = cap.queued.map(row=>[`执行 ${row.id}`, bytes(row.peak_bytes), esc(row.reason)]);
  const leases = cap.leases.map(row=>[`执行 ${row.run_id}`, bytes(row.bytes), esc(date(row.created_at))]);
  const form = state.session.role==='operator'
    ? `<form id="capacity-form"><label class="field"><span>共享归并段容量（字节）</span><input name="limit_bytes" type="number" min="0" step="1" value="${esc(cap.limit_bytes)}" required /></label><div class="actions"><button>保存容量</button></div><p class="note">调低额度不会打断已开跑的批次；等位批次按新额度重新排队。一个大批最多被越过 ${esc(cap.max_overtakes)} 次。</p></form>`
    : '<p class="note">只读：容量由经办员调整。</p>';
  return panel('共享临时盘容量', `<dl class="metrics">
      <div><dt>额度</dt><dd>${bytes(cap.limit_bytes)}</dd></div>
      <div><dt>已占（新旧段同时）</dt><dd>${bytes(cap.used_bytes)}</dd></div>
      <div><dt>余量</dt><dd>${bytes(cap.available_bytes)}</dd></div>
    </dl>${form}
    ${panel('占用中的租约', records(['执行','占用字节','开始'], leases, '当前没有批次占用容量。'))}
    ${panel('等位批次', records(['执行','预留峰值','放行判断'], queued, '没有等位批次。'))}`);
}

export function bindRuns(state, api, update, fail) {
  document.getElementById('refresh')?.addEventListener('click', update);
  submit('capacity-form', async data=>{
    const limit_bytes = Number(data.get('limit_bytes'));
    await api('/capacity', {method:'PUT', data:{limit_bytes}});
    await update();
  }, fail);
}
