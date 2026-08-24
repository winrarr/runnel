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
const suiteSelect = document.getElementById('suite');
const operationSelect = document.getElementById('operation');
const sizeSelect = document.getElementById('size');
const charts = document.getElementById('charts');
const changes = document.getElementById('changes');
const changesNote = document.getElementById('changes-note');

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

function definitionFor(metric) {
  return definitions.find((definition) => definition.metric === metric);
}

function pointSuite(point) {
  if (point.benchmark_suite) return point.benchmark_suite;
  return point.comparison_mode === 'cluster-baseline' ? 'cluster' : 'native-comparison';
}

function pointSeries(point) {
  if (point.benchmark_series) return point.benchmark_series;
  if (point.backend === 'runnel' && ['runnel', 'native-comparison'].includes(pointSuite(point))) {
    return 'runnel-single-node';
  }
  return pointSuite(point);
}

function matchesSuite(point, suite) {
  return suite === 'all' || pointSeries(point) === suite;
}

function runSuite(run) {
  if (run.benchmark_suite) return run.benchmark_suite;
  return run.comparison_mode === 'cluster-baseline' ? 'cluster' : 'native-comparison';
}

function filteredPoints(metric, operation, size, profile, suite) {
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
    && (profile === 'all' || point.profile === profile)
    && matchesSuite(point, suite));
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

function seriesKey(point) {
  return `${pointSeries(point)}:${point.backend}:${point.message_size_bytes ?? 'broker'}`;
}

function seriesLabel(point, suite) {
  const prefix = suite === 'all' ? `${suiteLabel(pointSeries(point))} · ` : '';
  const size = point.message_size_bytes === null
    ? 'broker'
    : `${point.message_size_bytes} bytes`;
  return `${prefix}${point.backend} · ${size}`;
}

function xTickPoints(points) {
  const byTimestamp = new Map();
  points.forEach((point) => {
    if (!byTimestamp.has(point.timestamp_ms)) byTimestamp.set(point.timestamp_ms, point);
  });
  const uniquePoints = [...byTimestamp.values()].sort((a, b) => a.timestamp_ms - b.timestamp_ms);
  const tickCount = Math.min(6, uniquePoints.length);
  if (tickCount <= 1) return uniquePoints;
  return Array.from({ length: tickCount }, (_, index) => {
    const position = Math.round(index * (uniquePoints.length - 1) / (tickCount - 1));
    return uniquePoints[position];
  });
}

function renderChart(definition, operation, size, profile, suite) {
  const points = filteredPoints(definition.metric, operation, size, profile, suite);
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
  points.forEach((point) => {
    const key = seriesKey(point);
    if (!grouped[key]) grouped[key] = { label: seriesLabel(point, suite), points: [] };
    grouped[key].points.push(point);
  });
  Object.values(grouped).forEach((series) => series.points.sort((a, b) => a.timestamp_ms - b.timestamp_ms));

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

  xTickPoints(points).forEach((point) => {
    const position = geometry.x(point.timestamp_ms);
    const revision = (point.revision || 'unknown').slice(0, 7);
    svg += `<line class="tick" x1="${position}" x2="${position}" y1="${geometry.height - geometry.bottom}" y2="${geometry.height - geometry.bottom + 5}"/>`;
    svg += `<text class="axis-label" x="${position}" y="${geometry.height - geometry.bottom + 17}" text-anchor="middle"><title>${escapeText(new Date(point.timestamp).toLocaleString())}</title>${escapeText(revision)}</text>`;
  });

  Object.values(grouped).forEach((series, index) => {
    const color = colors[index % colors.length];
    const path = series.points.map((point, pointIndex) => `${pointIndex ? 'L' : 'M'} ${geometry.x(point.timestamp_ms)} ${geometry.y(point.value)}`).join(' ');
    svg += `<path d="${path}" fill="none" stroke="${color}" stroke-width="2"/>`;

    series.points.forEach((point) => {
      const range = point.range
        ? ` · range ${formatValue(point.range.min, definition.unit)}–${formatValue(point.range.max, definition.unit)}`
        : '';
      const change = point.delta_percent === undefined
        ? ''
        : ` · change ${point.delta_percent.toFixed(1)}%`;
      const detail = `${new Date(point.timestamp).toLocaleString()} · ${series.label} · ${formatValue(point.value, definition.unit)}${range}${change}`;
      svg += `<circle cx="${geometry.x(point.timestamp_ms)}" cy="${geometry.y(point.value)}" r="3.5" fill="${color}"><title>${escapeText(detail)}</title></circle>`;
    });
  });

  svg += `<text class="axis-label" x="${geometry.left + geometry.plotWidth / 2}" y="${geometry.height - 8}" text-anchor="middle">time</text></svg>`;
  card.insertAdjacentHTML('beforeend', svg);

  const legend = document.createElement('div');
  legend.className = 'legend';
  Object.values(grouped).forEach((series, index) => {
    const item = document.createElement('span');
    item.innerHTML = `<span class="swatch" style="background:${colors[index % colors.length]}"></span>${escapeText(series.label)}`;
    legend.appendChild(item);
  });
  card.appendChild(legend);
  return card;
}

function refresh() {
  const profile = profileSelect.value;
  const suite = suiteSelect.value;
  const operation = operationSelect.value;
  const sizeValue = sizeSelect.value;
  const size = sizeValue === 'all' ? null : Number(sizeValue);
  charts.replaceChildren(...definitions.map((definition) => renderChart(definition, operation, size, profile, suite)));
  renderChanges(operation, size, profile, suite);
}

function populateSizes() {
  const suite = suiteSelect.value;
  const sizes = unique(data.points
    .filter((point) => point.operation === operationSelect.value
      && matchesSuite(point, suite)
      && point.message_size_bytes !== null)
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

function populateOperations() {
  const suite = suiteSelect.value;
  const oldValue = operationSelect.value;
  const operations = unique(data.points
    .filter((point) => point.metric === 'throughput_messages_per_second'
      && matchesSuite(point, suite))
    .map((point) => point.operation));
  operationSelect.replaceChildren(...operations.map((value) => new Option(value, value)));
  if (operations.includes(oldValue)) {
    operationSelect.value = oldValue;
  } else if (operations.includes('publish')) {
    operationSelect.value = 'publish';
  } else if (operations.length) {
    operationSelect.value = operations[0];
  }
}

function suiteLabel(value) {
  return {
    runnel: 'Runnel benchmark',
    'runnel-single-node': 'Runnel single-node history',
    'native-comparison': 'Native broker comparison',
    'cluster-comparison': 'Three-node competitor comparison',
    cluster: 'Runnel cluster',
  }[value] || value;
}

function populate() {
  const suites = unique(data.points.map(pointSeries));
  suiteSelect.replaceChildren(
    new Option('All suites', 'all'),
    ...suites.map((value) => new Option(suiteLabel(value), value)),
  );
  if (suites.includes('runnel-single-node')) suiteSelect.value = 'runnel-single-node';
  else if (suites.includes('native-comparison')) suiteSelect.value = 'native-comparison';

  const profiles = unique(data.points.map((point) => point.profile));
  profileSelect.replaceChildren(
    new Option('All profiles', 'all'),
    ...profiles.map((value) => new Option(value, value)),
  );

  populateOperations();
  populateSizes();
  suiteSelect.addEventListener('change', () => {
    populateOperations();
    populateSizes();
    refresh();
  });
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
    row.innerHTML = `<td>${escapeText(time)}</td><td>${escapeText(suiteLabel(runSuite(run)))}</td><td>${escapeText(run.profile)}</td><td>${escapeText(run.repetitions || 1)}</td><td>${commit}</td><td>${escapeText(run.event || '')}</td><td>${escapeText(run.backends.join(', '))}</td><td>${escapeText(JSON.stringify(run.resource_limits || {}))}</td>`;
    tbody.appendChild(row);
  });
}

function renderChanges(operation, size, profile, suite) {
  const candidates = data.points.filter((point) => point.previous_value !== undefined
    && point.operation === operation
    && (size === null || point.message_size_bytes === size)
    && (profile === 'all' || point.profile === profile)
    && matchesSuite(point, suite));
  changes.replaceChildren();
  if (!candidates.length) {
    changesNote.textContent = 'No previous comparable run is available for this selection.';
    return;
  }

  const latestTimestamp = Math.max(...candidates.map((point) => point.timestamp_ms));
  const latest = candidates.filter((point) => point.timestamp_ms === latestTimestamp);
  const latestPoint = latest[0];
  changesNote.textContent = `Compared with the previous ${suiteLabel(pointSeries(latestPoint))} run for commit ${latestPoint.revision.slice(0, 12)}. Each value is the median of ${latestPoint.repetitions || 1} repetition(s).`;
  latest.forEach((point) => {
    const definition = definitionFor(point.metric);
    const row = document.createElement('tr');
    const percent = point.delta_percent === undefined ? 'n/a' : `${point.delta_percent.toFixed(1)}%`;
    const result = point.improved ? 'Improved' : 'Regressed';
    row.innerHTML = `<td>${escapeText(point.backend)}</td><td>${escapeText(definition ? definition.title : point.metric)}</td><td>${escapeText(formatValue(point.value, definition ? definition.unit : ''))}</td><td>${escapeText(formatValue(point.previous_value, definition ? definition.unit : ''))}</td><td>${escapeText(percent)}</td><td class="${point.improved ? 'improved' : 'regressed'}">${result}</td>`;
    changes.appendChild(row);
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
