let data = { runs: [], points: [] };
const dataUrl = 'https://raw.githubusercontent.com/winrarr/runnel/benchmark-history/site/data.json';
const definitions = [
  { metric: 'throughput_messages_per_second', title: 'Throughput', unit: 'messages/s', higherBetter: true },
  { metric: 'latency_p50', title: 'p50 latency', unit: 'µs', higherBetter: false },
  { metric: 'latency_p99', title: 'p99 latency', unit: 'µs', higherBetter: false },
  { metric: 'latency_p999', title: 'p99.9 latency', unit: 'µs', higherBetter: false },
  { metric: 'cpu_efficiency_messages_per_cpu_second', title: 'CPU efficiency', unit: 'messages/CPU-second', higherBetter: true },
  { metric: 'cpu_percent_max', title: 'Peak broker CPU', unit: '%', higherBetter: false },
  { metric: 'memory_bytes_max', title: 'Peak broker memory', unit: 'bytes', higherBetter: false },
];
const colors = ['#75c7ff', '#ff9f68', '#9be28f', '#d7a5ff', '#ffd166', '#ff7aa2', '#76e4d1'];

const profileSelect = document.getElementById('profile');
const operationSelect = document.getElementById('operation');
const sizeSelect = document.getElementById('size');
const charts = document.getElementById('charts');

function unique(values) {
  return [...new Set(values)].sort((a, b) => String(a).localeCompare(String(b)));
}

function escapeText(value) {
  return String(value).replace(/[&<>"']/g, (character) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[character]));
}

function formatValue(value, unit) {
  if (unit === 'bytes') {
    const units = ['B', 'KiB', 'MiB', 'GiB'];
    let number = value;
    let unitIndex = 0;
    while (number >= 1024 && unitIndex < units.length - 1) {
      number /= 1024;
      unitIndex += 1;
    }
    return `${number.toFixed(number >= 100 ? 0 : 1)} ${units[unitIndex]}`;
  }
  if (Math.abs(value) >= 1000) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function filteredPoints(metric, operation, size, profile) {
  const resourceMetric = [
    'cpu_efficiency_messages_per_cpu_second',
    'cpu_percent_max',
    'memory_bytes_max',
  ].includes(metric);

  return data.points.filter((point) => point.metric === metric
    && (!resourceMetric
      || (point.message_size_bytes === null ? size === null : point.operation === operation))
    && (!resourceMetric
      || point.message_size_bytes === null
      || size === null
      || point.message_size_bytes === size)
    && (profile === 'all' || point.profile === profile));
}

function renderEmptyChart(card) {
  const empty = document.createElement('p');
  empty.className = 'muted';
  empty.textContent = 'No measurements for this selection.';
  card.appendChild(empty);
}

function chartGeometry(points) {
  const width = 900;
  const height = 330;
  const left = 70;
  const right = 20;
  const top = 20;
  const bottom = 48;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const values = points.map((point) => point.value);
  const times = points.map((point) => point.timestamp_ms);
  const min = Math.min(0, ...values);
  const max = Math.max(...values, min + 1);
  const timeMin = Math.min(...times);
  const timeMax = Math.max(...times);

  return {
    width,
    height,
    left,
    right,
    top,
    bottom,
    plotWidth,
    plotHeight,
    min,
    max,
    x: (value) => (timeMax === timeMin
      ? left + plotWidth / 2
      : left + ((value - timeMin) / (timeMax - timeMin)) * plotWidth),
    y: (value) => top + plotHeight - ((value - min) / (max - min)) * plotHeight,
  };
}

function renderChart(definition, operation, size, profile) {
  const points = filteredPoints(definition.metric, operation, size, profile);
  const card = document.createElement('article');
  card.className = 'chart-card';

  const heading = document.createElement('h3');
  heading.textContent = definition.title;
  card.appendChild(heading);

  if (!points.length) {
    renderEmptyChart(card);
    return card;
  }

  const grouped = {};
  points.forEach((point) => (grouped[point.backend] ||= []).push(point));
  Object.values(grouped).forEach((series) => series.sort((a, b) => a.timestamp_ms - b.timestamp_ms));

  const geometry = chartGeometry(points);
  const ticks = [0, 0.25, 0.5, 0.75, 1];
  let svg = `<svg viewBox="0 0 ${geometry.width} ${geometry.height}" role="img" aria-label="${escapeText(definition.title)} over time">`;

  ticks.forEach((tick) => {
    const value = geometry.min + (geometry.max - geometry.min) * tick;
    const position = geometry.y(value);
    svg += `<line class="gridline" x1="${geometry.left}" x2="${geometry.width - geometry.right}" y1="${position}" y2="${position}"/>`;
    svg += `<text class="axis-label" x="${geometry.left - 8}" y="${position + 4}" text-anchor="end">${escapeText(formatValue(value, definition.unit))}</text>`;
  });

  svg += `<line class="axis" x1="${geometry.left}" x2="${geometry.left}" y1="${geometry.top}" y2="${geometry.height - geometry.bottom}"/>`;
  svg += `<line class="axis" x1="${geometry.left}" x2="${geometry.width - geometry.right}" y1="${geometry.height - geometry.bottom}" y2="${geometry.height - geometry.bottom}"/>`;

  Object.entries(grouped).forEach(([backend, series], index) => {
    const color = colors[index % colors.length];
    const path = series.map((point, pointIndex) => `${pointIndex ? 'L' : 'M'} ${geometry.x(point.timestamp_ms)} ${geometry.y(point.value)}`).join(' ');
    svg += `<path d="${path}" fill="none" stroke="${color}" stroke-width="2"/>`;

    series.forEach((point) => {
      const detail = `${new Date(point.timestamp).toLocaleString()} · ${backend} · ${formatValue(point.value, definition.unit)}`;
      svg += `<circle cx="${geometry.x(point.timestamp_ms)}" cy="${geometry.y(point.value)}" r="3.5" fill="${color}"><title>${escapeText(detail)}</title></circle>`;
    });
  });

  svg += `<text class="axis-label" x="${geometry.left + geometry.plotWidth / 2}" y="${geometry.height - 8}" text-anchor="middle">time</text></svg>`;
  card.insertAdjacentHTML('beforeend', svg);

  const legend = document.createElement('div');
  legend.className = 'legend';
  Object.keys(grouped).forEach((backend, index) => {
    const item = document.createElement('span');
    item.innerHTML = `<span class="swatch" style="background:${colors[index % colors.length]}"></span>${escapeText(backend)}`;
    legend.appendChild(item);
  });
  card.appendChild(legend);
  return card;
}

function refresh() {
  const profile = profileSelect.value;
  const operation = operationSelect.value;
  const sizeValue = sizeSelect.value;
  const size = sizeValue === 'all' ? null : Number(sizeValue);
  charts.replaceChildren(...definitions.map((definition) => renderChart(definition, operation, size, profile)));
}

function populateSizes() {
  const sizes = unique(data.points
    .filter((point) => point.operation === operationSelect.value && point.message_size_bytes !== null)
    .map((point) => point.message_size_bytes));
  const oldValue = sizeSelect.value;
  sizeSelect.replaceChildren(
    new Option('All sizes', 'all'),
    ...sizes.map((value) => new Option(`${value} bytes`, value)),
  );
  if ([...sizeSelect.options].some((option) => option.value === oldValue)) {
    sizeSelect.value = oldValue;
  } else if (sizes.length) {
    sizeSelect.value = String(sizes[0]);
  }
}

function populate() {
  const profiles = unique(data.points.map((point) => point.profile));
  profileSelect.replaceChildren(
    new Option('All profiles', 'all'),
    ...profiles.map((value) => new Option(value, value)),
  );

  const operations = unique(data.points
    .filter((point) => point.metric === 'throughput_messages_per_second')
    .map((point) => point.operation));
  operationSelect.replaceChildren(...operations.map((value) => new Option(value, value)));
  if (operations.includes('publish')) operationSelect.value = 'publish';

  populateSizes();
  profileSelect.addEventListener('change', refresh);
  operationSelect.addEventListener('change', () => {
    populateSizes();
    refresh();
  });
  sizeSelect.addEventListener('change', refresh);
  refresh();
}

function renderRuns() {
  const tbody = document.getElementById('runs');
  [...data.runs].reverse().slice(0, 50).forEach((run) => {
    const row = document.createElement('tr');
    const time = new Date(run.timestamp).toLocaleString();
    const commit = run.run_url
      ? `<a href="${escapeText(run.run_url)}">${escapeText(run.revision.slice(0, 12))}</a>`
      : `<code>${escapeText(run.revision.slice(0, 12))}</code>`;
    row.innerHTML = `<td>${escapeText(time)}</td><td>${escapeText(run.profile)}</td><td>${commit}</td><td>${escapeText(run.event || '')}</td><td>${escapeText(run.backends.join(', '))}</td><td>${escapeText(JSON.stringify(run.resource_limits || {}))}</td>`;
    tbody.appendChild(row);
  });
}

async function loadData() {
  const separator = dataUrl.includes('?') ? '&' : '?';
  const response = await fetch(`${dataUrl}${separator}v=${Date.now()}`, { cache: 'no-store' });
  if (!response.ok) throw new Error(`benchmark data request failed: ${response.status}`);
  data = await response.json();
  populate();
  renderRuns();
}

loadData().catch((error) => {
  const notice = document.createElement('p');
  notice.className = 'notice';
  notice.textContent = `Unable to load benchmark history: ${error.message}`;
  document.querySelector('main').appendChild(notice);
});
