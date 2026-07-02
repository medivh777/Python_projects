/* Общие помощники: палитра, форматтеры, фабрика графиков, рендер плана. */

function cssVar(name) {
  return getComputedStyle(document.body).getPropertyValue(name).trim();
}
const PAL = () => [1, 2, 3, 4, 5, 6, 7, 8].map(i => cssVar(`--series-${i}`));

/* Цвет закреплён за сущностью (типом ожидания), а не за порядком в выборке */
const WAIT_COLORS = {
  'CPU':       () => cssVar('--series-4'),
  'IO':        () => cssVar('--series-1'),
  'Lock':      () => cssVar('--series-6'),
  'LWLock':    () => cssVar('--series-8'),
  'Client':    () => cssVar('--series-3'),
  'IPC':       () => cssVar('--series-5'),
  'BufferPin': () => cssVar('--series-7'),
  'Timeout':   () => cssVar('--series-2'),
  'Activity':  () => cssVar('--text-muted'),
  'Extension': () => cssVar('--series-5'),
};
function waitColor(name, idx) {
  return (WAIT_COLORS[name] || (() => PAL()[idx % 8]))();
}

/* Выбранный кластер добавляется ко всем запросам API автоматически. */
function currentCluster() {
  return localStorage.getItem('pgmon-cluster') || '';
}
async function getJSON(url) {
  const c = currentCluster();
  if (c && url.startsWith('/api/')) {
    url += (url.includes('?') ? '&' : '?') + 'cluster=' + encodeURIComponent(c);
  }
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

/* Селектор кластера в шапке: виден только когда кластеров больше одного. */
async function initClusterSelect() {
  const sel = document.getElementById('cluster-select');
  if (!sel) return;
  try {
    const clusters = await (await fetch('/api/clusters')).json();
    if (!Array.isArray(clusters) || clusters.length < 2) {
      localStorage.removeItem('pgmon-cluster');
      return;
    }
    let saved = currentCluster();
    if (!clusters.includes(saved)) saved = clusters[0];
    localStorage.setItem('pgmon-cluster', saved);
    sel.innerHTML = clusters.map(c =>
      `<option value="${esc(c)}" ${c === saved ? 'selected' : ''}>кластер: ${esc(c)}</option>`).join('');
    sel.hidden = false;
    sel.addEventListener('change', () => {
      localStorage.setItem('pgmon-cluster', sel.value);
      location.reload();
    });
  } catch (e) { /* API недоступен — селектор не показываем */ }
}
initClusterSelect();

const fmtMs = v => {
  if (v == null) return '—';
  if (v < 1) return (v * 1000).toFixed(0) + ' мкс';
  if (v < 1000) return v.toFixed(1) + ' мс';
  return (v / 1000).toFixed(2) + ' с';
};
const fmtNum = v => {
  if (v == null) return '—';
  v = Number(v);
  if (v >= 1e9) return (v / 1e9).toFixed(1) + ' млрд';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + ' млн';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + ' тыс';
  // дробные тики оси не должны схлопываться в одинаковые подписи
  if (v > 0 && v < 10 && !Number.isInteger(v)) return v.toFixed(1);
  return Math.round(v).toString();
};
const fmtBytes = v => {
  if (v == null) return '—';
  const units = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ'];
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return v.toFixed(i ? 1 : 0) + ' ' + units[i];
};
const esc = s => (s || '').replace(/[&<>"]/g,
  c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c]));
/* CH отдаёт "2026-07-02 04:40:00" (UTC) — приводим к ISO с зоной */
const isoUTC = t => {
  t = String(t).replace(' ', 'T');
  return t.endsWith('Z') ? t : t + 'Z';
};
const fmtTs = t => t ? new Date(isoUTC(t)).toLocaleString('ru-RU') : '—';

Chart.defaults.font.family = 'system-ui, -apple-system, "Segoe UI", sans-serif';
Chart.defaults.color = cssVar('--text-muted');

function baseOptions(yFormat) {
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { labels: { boxWidth: 12, boxHeight: 12, color: cssVar('--text-secondary') } },
      tooltip: {
        callbacks: yFormat ? {
          label: ctx => `${ctx.dataset.label}: ${yFormat(ctx.parsed.y)}`,
        } : {},
      },
    },
    scales: {
      x: {
        type: 'time',
        time: { tooltipFormat: 'dd.MM HH:mm', displayFormats: { hour: 'HH:mm', minute: 'HH:mm', day: 'dd.MM' } },
        grid: { color: cssVar('--grid') },
        ticks: { maxTicksLimit: 8 },
      },
      y: {
        beginAtZero: true,
        grid: { color: cssVar('--grid') },
        ticks: yFormat ? { callback: v => yFormat(v) } : {},
        border: { color: cssVar('--baseline') },
      },
    },
  };
}

/* Линейный график: series = [{label, data:[{x,y}], color}], один Y. */
function lineChart(canvasId, series, yFormat) {
  const el = document.getElementById(canvasId);
  if (!el) return null;
  const opts = baseOptions(yFormat);
  if (series.length < 2) opts.plugins.legend.display = false;
  return new Chart(el, {
    type: 'line',
    data: {
      datasets: series.map((s, i) => ({
        label: s.label,
        data: s.data,
        borderColor: s.color || PAL()[i % 8],
        backgroundColor: (s.color || PAL()[i % 8]) + '33',
        borderWidth: 2, pointRadius: 0, pointHoverRadius: 5,
        fill: !!s.fill, tension: 0.15,
      })),
    },
    options: opts,
  });
}

/* Стековая гистограмма по времени (ASH по типам ожиданий). */
function stackedBarChart(canvasId, labels, datasets, yFormat) {
  const el = document.getElementById(canvasId);
  if (!el) return null;
  const opts = baseOptions(yFormat);
  opts.scales.x.stacked = true;
  opts.scales.y.stacked = true;
  return new Chart(el, {
    type: 'bar',
    data: {
      labels,
      datasets: datasets.map(d => ({
        ...d,
        borderColor: cssVar('--surface-1'),
        borderWidth: 1,           /* 2px визуального зазора между сегментами */
        borderRadius: 2,
        maxBarThickness: 24,
      })),
    },
    options: opts,
  });
}

/* Бейдж количества алертов за сутки в навигации. */
async function loadAlertBadge() {
  try {
    const b = await getJSON('/api/alerts/badge');
    const n = (b.critical || 0) + (b.warning || 0);
    const el = document.getElementById('alert-badge');
    if (el && n > 0) { el.textContent = n; el.hidden = false; }
  } catch (e) { /* нет данных — нет бейджа */ }
}
loadAlertBadge();

/* Текстовый рендер плана из EXPLAIN (FORMAT JSON). */
function renderPlanText(planJson) {
  const root = Array.isArray(planJson) ? planJson[0].Plan : (planJson.Plan || planJson);
  const lines = [];
  function walk(node, depth) {
    const pad = '  '.repeat(depth);
    let head = node['Node Type'] || '?';
    if (node['Join Type']) head += ` (${node['Join Type']})`;
    let cls = '';
    if (head.startsWith('Seq Scan')) cls = 'seq';
    else if (head.includes('Index')) cls = 'idx';
    let line = `${pad}-> <span class="${cls}">${esc(head)}</span>`;
    if (node['Relation Name']) line += ` on ${esc(node['Relation Name'])}`;
    if (node['Index Name']) line += ` using ${esc(node['Index Name'])}`;
    line += `  <span class="muted">(cost=${node['Startup Cost']}..${node['Total Cost']} rows=${node['Plan Rows']})</span>`;
    lines.push(line);
    ['Index Cond', 'Filter', 'Hash Cond', 'Merge Cond', 'Join Filter', 'Sort Key'].forEach(k => {
      if (node[k]) lines.push(`${pad}     ${esc(k)}: ${esc(String(node[k]))}`);
    });
    (node.Plans || []).forEach(ch => walk(ch, depth + 1));
  }
  walk(root, 0);
  return lines.join('\n');
}

function copyText(text, btn) {
  navigator.clipboard.writeText(text).then(() => {
    const old = btn.textContent;
    btn.textContent = '✓ скопировано';
    setTimeout(() => { btn.textContent = old; }, 1200);
  });
}

/* Пары {x,y} из массива строк API. */
const toXY = (rows, xKey, yKey) => rows.map(r => ({ x: isoUTC(r[xKey]), y: Number(r[yKey]) }));
