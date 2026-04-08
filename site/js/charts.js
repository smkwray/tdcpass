/* charts.js — theme-aware Chart.js visualizations for TDC Pass-Through */
(function () {
  'use strict';

  var instances = [];
  var _data = null;

  /* ---- helpers ---- */

  function el(id) { return document.getElementById(id); }
  function fmt(v) { return (v >= 0 ? '+' : '') + v.toFixed(1); }
  function fmtPct(v) { return (v * 100).toFixed(1) + '%'; }

  function parseCSV(text) {
    var lines = text.trim().split('\n');
    var headers = lines[0].split(',');
    var rows = [];
    for (var i = 1; i < lines.length; i++) {
      var vals = lines[i].split(',');
      var obj = {};
      for (var j = 0; j < headers.length; j++) {
        var v = vals[j];
        obj[headers[j]] = isNaN(+v) ? v : +v;
      }
      rows.push(obj);
    }
    return rows;
  }

  /* ---- chart defaults ---- */

  function applyDefaults() {
    var c = tpColors();
    Chart.defaults.color = c.text;
    Chart.defaults.borderColor = c.grid;
    Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.plugins.tooltip.backgroundColor = c.tooltipBg;
    Chart.defaults.plugins.tooltip.titleColor = c.tooltipTitle;
    Chart.defaults.plugins.tooltip.bodyColor = c.tooltipBody;
    Chart.defaults.plugins.tooltip.cornerRadius = 6;
    Chart.defaults.plugins.tooltip.padding = 10;
    Chart.defaults.plugins.tooltip.displayColors = true;
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.animation = false;
  }

  /* ---- data loading ---- */

  function loadAll() {
    var base = 'data/';
    Promise.all([
      fetch(base + 'lp_irf_identity_baseline.csv').then(function (r) { return r.text(); }),
      fetch(base + 'overview.json').then(function (r) { return r.json(); }),
      fetch(base + 'period_sensitivity_summary.json').then(function (r) { return r.json(); }),
      fetch(base + 'counterpart_channel_scorecard.json').then(function (r) { return r.json(); }),
      fetch(base + 'result_readiness_summary.json').then(function (r) { return r.json(); }),
      fetch(base + 'deposit_type_side_read.csv').then(function (r) { return r.text(); })
    ]).then(function (results) {
      _data = {
        irf: parseCSV(results[0]),
        overview: results[1],
        periodSummary: results[2],
        counterpart: results[3],
        readiness: results[4],
        depositTypes: parseCSV(results[5])
      };
      populateHero(_data);
      populateChannels(_data);
      populateRobustness(_data);
      populatePeriodTable(_data);
      buildAll(_data);
    });
  }

  /* ---- hero stats ---- */

  function populateHero(d) {
    var ke = d.readiness.key_estimates;
    el('stat-tdc').textContent = '+$' + ke.tdc_h0.beta.toFixed(0) + 'B';
    el('stat-total').textContent = '+$' + ke.total_deposits_h0.beta.toFixed(0) + 'B';
    el('stat-other').textContent = '-$' + Math.abs(ke.other_component_h0.beta).toFixed(0) + 'B';
    el('stat-share').textContent = fmtPct(d.overview.headline_metrics.share_other_negative);
  }

  /* ---- counterpart channel cards ---- */

  function populateChannels(d) {
    var h0 = d.counterpart.horizons.h0;
    if (h0.legacy_private_credit_proxy && h0.legacy_private_credit_proxy.snapshot) {
      var lp = h0.legacy_private_credit_proxy.snapshot;
      el('ch-creator').textContent = 'beta=' + fmt(lp.beta) + ', 95% CI [' + fmt(lp.lower95) + ', ' + fmt(lp.upper95) + ']';
    }
    if (h0.external_escape_channels && h0.external_escape_channels.foreign_nonts_qoq) {
      var fn = h0.external_escape_channels.foreign_nonts_qoq;
      el('ch-foreign').textContent = 'beta=' + fmt(fn.beta) + ', 95% CI [' + fmt(fn.lower95) + ', ' + fmt(fn.upper95) + ']';
    }
    if (h0.deposit_retention_support_channels && h0.deposit_retention_support_channels.domestic_nonfinancial_mmf_reallocation_qoq) {
      var mm = h0.deposit_retention_support_channels.domestic_nonfinancial_mmf_reallocation_qoq;
      el('ch-mmf').textContent = 'MMF beta=' + fmt(mm.beta) + ', 95% CI [' + fmt(mm.lower95) + ', ' + fmt(mm.upper95) + ']';
    }
  }

  /* ---- robustness gate grid & table ---- */

  function populateRobustness(d) {
    var checks = d.readiness.treatment_quality_gate.checks;
    var grid = el('gateGrid');
    var items = [
      { label: 'Usable Obs', key: 'min_usable_observations' },
      { label: 'Shock-Target Corr', key: 'min_shock_target_correlation' },
      { label: 'Flagged Share', key: 'max_flagged_share' },
      { label: 'Scale Ratio p95', key: 'max_realized_scale_ratio_p95' },
      { label: 'Scale Ratio p99', key: 'max_realized_scale_ratio_p99' },
      { label: 'Train/Usable Vol', key: 'max_initial_train_to_usable_volatility_ratio' }
    ];
    var html = '';
    for (var i = 0; i < items.length; i++) {
      var ck = checks[items[i].key];
      var val = typeof ck.observed === 'number'
        ? (ck.observed < 1 && ck.observed > 0 ? ck.observed.toFixed(3) : ck.observed.toFixed(1))
        : ck.observed;
      html += '<div class="gate-item"><div class="gate-label">' + items[i].label +
        '</div><div class="gate-value ' + (ck.passed ? 'pass' : 'fail') + '">' +
        val + ' ' + (ck.passed ? '\u2713' : '\u2717') + '</div></div>';
    }
    grid.innerHTML = html;

    var diag = d.readiness.diagnostics;
    var tbody = el('robustnessTable').querySelector('tbody');
    var rows = [
      ['Primary decomposition', diag.primary_decomposition_mode, ''],
      ['Shock usable obs', diag.shock_usable_obs, ''],
      ['Shock window', diag.shock_start_quarter + ' \u2013 ' + diag.shock_end_quarter, ''],
      ['Flagged shock obs', diag.flagged_shock_obs + ' (' + fmtPct(diag.flagged_shock_share) + ')', diag.flagged_shock_share < 0.2 ? 'pass' : 'caution'],
      ['Sensitivity variants', diag.sensitivity_variant_count, ''],
      ['Sign disagreement (exploratory)', diag.exploratory_variant_sign_disagreement ? 'Yes' : 'No', diag.exploratory_variant_sign_disagreement ? 'fail' : 'pass'],
      ['Sign disagreement (control set)', diag.control_set_sign_disagreement ? 'Yes' : 'No', diag.control_set_sign_disagreement ? 'fail' : 'pass'],
      ['Sample sign disagreement', diag.sample_variant_sign_disagreement ? 'Yes' : 'No', diag.sample_variant_sign_disagreement ? 'fail' : 'pass'],
      ['Magnitude instability', diag.sample_variant_magnitude_instability ? 'Yes' : 'No', diag.sample_variant_magnitude_instability ? 'fail' : 'pass'],
      ['Ratio reporting gate', 'Out of scope', 'caution'],
      ['Release status', d.readiness.status, d.readiness.status === 'provisional' ? 'caution' : 'pass']
    ];
    var rhtml = '';
    for (var j = 0; j < rows.length; j++) {
      var cls = rows[j][2] === 'pass' ? ' style="color:var(--c-green)"' :
                rows[j][2] === 'fail' ? ' style="color:var(--c-red)"' :
                rows[j][2] === 'caution' ? ' style="color:var(--c-amber)"' : '';
      rhtml += '<tr><td style="font-family:inherit">' + rows[j][0] + '</td><td>' + rows[j][1] + '</td><td' + cls + '>' +
        (rows[j][2] === 'pass' ? '\u2713' : rows[j][2] === 'fail' ? '\u2717' : rows[j][2] === 'caution' ? '\u26A0' : '') + '</td></tr>';
    }
    tbody.innerHTML = rhtml;
  }

  /* ---- period sensitivity table ---- */

  function populatePeriodTable(d) {
    var periods = d.periodSummary.periods;
    var container = el('periodTable');
    var html = '<div class="table-wrap"><table class="data-table"><thead><tr>' +
      '<th>Period</th><th>Window</th><th>n</th><th>h0 Assessment</th>' +
      '<th>TDC h0</th><th>Total h0</th><th>Other h0</th>' +
      '<th>h4 Assessment</th></tr></thead><tbody>';
    for (var i = 0; i < periods.length; i++) {
      var p = periods[i];
      var h0 = p.key_horizons.h0;
      var h4 = p.key_horizons.h4;
      var label = p.period_variant.replace(/_/g, ' ');
      if (p.period_role === 'headline') label += ' *';
      var tdcCls = h0.tdc.ci_excludes_zero ? 'sig' : 'not-sig';
      var totCls = h0.total_deposits.ci_excludes_zero ? 'sig' : 'not-sig';
      var othCls = h0.other_component.ci_excludes_zero ? 'sig' : 'not-sig';
      html += '<tr><td style="font-family:inherit;text-transform:capitalize">' + label + '</td>' +
        '<td>' + p.start_quarter + '&ndash;' + p.end_quarter + '</td>' +
        '<td>' + h0.tdc.n + '</td>' +
        '<td style="font-family:inherit">' + h0.assessment.replace(/_/g, ' ') + '</td>' +
        '<td class="' + tdcCls + '">' + fmt(h0.tdc.beta) + '</td>' +
        '<td class="' + totCls + '">' + fmt(h0.total_deposits.beta) + '</td>' +
        '<td class="' + othCls + '">' + fmt(h0.other_component.beta) + '</td>' +
        '<td style="font-family:inherit">' + h4.assessment.replace(/_/g, ' ') + '</td></tr>';
    }
    html += '</tbody></table></div>';
    container.innerHTML = html;
  }

  /* ---- chart builders ---- */

  /* Helper: build a hidden band dataset pair (upper fills down to lower) */
  function bandPair(label, upper, lower, color) {
    return [
      {
        label: label + ' upper',
        data: upper,
        borderColor: 'transparent',
        backgroundColor: color,
        pointRadius: 0,
        borderWidth: 0,
        fill: '+1',
        tension: 0.3
      },
      {
        label: label + ' lower',
        data: lower,
        borderColor: 'transparent',
        backgroundColor: 'transparent',
        pointRadius: 0,
        borderWidth: 0,
        fill: false,
        tension: 0.3
      }
    ];
  }

  function buildIRF(d) {
    var c = tpColors();
    var irf = d.irf;

    var tdc = irf.filter(function (r) { return r.outcome === 'tdc_bank_only_qoq'; });
    var total = irf.filter(function (r) { return r.outcome === 'total_deposits_bank_qoq'; });
    var other = irf.filter(function (r) { return r.outcome === 'other_component_qoq'; });
    var horizons = tdc.map(function (r) { return 'h' + r.horizon; });

    /* Compute y-axis range from all CI bounds with 10% padding */
    var allVals = [].concat(
      tdc.map(function (r) { return r.upper95; }), tdc.map(function (r) { return r.lower95; }),
      total.map(function (r) { return r.upper95; }), total.map(function (r) { return r.lower95; }),
      other.map(function (r) { return r.upper95; }), other.map(function (r) { return r.lower95; })
    );
    var yMin = Math.min.apply(null, allVals);
    var yMax = Math.max.apply(null, allVals);
    var pad = (yMax - yMin) * 0.08;

    /* Band datasets: upper fills to lower for each series */
    var datasets = [].concat(
      bandPair('TDC CI', tdc.map(function (r) { return r.upper95; }), tdc.map(function (r) { return r.lower95; }), c.bandTdc),
      bandPair('Total CI', total.map(function (r) { return r.upper95; }), total.map(function (r) { return r.lower95; }), c.bandTotal),
      bandPair('Other CI', other.map(function (r) { return r.upper95; }), other.map(function (r) { return r.lower95; }), c.bandOther),
      [
        {
          label: 'TDC (bank-only)',
          data: tdc.map(function (r) { return r.beta; }),
          borderColor: c.teal, backgroundColor: c.teal,
          borderWidth: 2.5, pointRadius: 4, pointBackgroundColor: c.teal,
          tension: 0.3, fill: false
        },
        {
          label: 'Total deposits',
          data: total.map(function (r) { return r.beta; }),
          borderColor: c.blue, backgroundColor: c.blue,
          borderWidth: 2.5, pointRadius: 4, pointBackgroundColor: c.blue,
          tension: 0.3, fill: false
        },
        {
          label: 'Non-TDC residual',
          data: other.map(function (r) { return r.beta; }),
          borderColor: c.red, backgroundColor: c.red,
          borderWidth: 2.5, pointRadius: 4, pointBackgroundColor: c.red,
          tension: 0.3, fill: false
        },
        {
          label: 'Zero',
          data: horizons.map(function () { return 0; }),
          borderColor: c.textMuted, borderWidth: 1, borderDash: [4, 4],
          pointRadius: 0, fill: false
        }
      ]
    );

    var visibleLabels = ['TDC (bank-only)', 'Total deposits', 'Non-TDC residual'];

    /* Map each main-line dataset index to its CI band pair indices */
    var lineToBand = { 6: [0, 1], 7: [2, 3], 8: [4, 5] };

    instances.push(new Chart(el('irfChart'), {
      type: 'line',
      data: { labels: horizons, datasets: datasets },
      options: {
        responsive: true,
        aspectRatio: 2.2,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            position: 'top',
            align: 'center',
            onClick: function (evt, item, legend) {
              var chart = legend.chart;
              var lineIdx = item.datasetIndex;
              /* Toggle the main line using default behaviour */
              var meta = chart.getDatasetMeta(lineIdx);
              meta.hidden = meta.hidden === null ? true : !meta.hidden;
              var hide = !!meta.hidden;
              /* Toggle the paired CI bands to match */
              var bands = lineToBand[lineIdx];
              if (bands) {
                for (var i = 0; i < bands.length; i++) {
                  chart.getDatasetMeta(bands[i]).hidden = hide;
                }
              }
              chart.update();
            },
            labels: {
              padding: 16,
              filter: function (item) { return visibleLabels.indexOf(item.text) >= 0; },
              color: c.text
            }
          },
          tooltip: {
            filter: function (item) { return visibleLabels.indexOf(item.dataset.label) >= 0; },
            callbacks: {
              label: function (ctx) { return ctx.dataset.label + ': ' + fmt(ctx.parsed.y) + ' $B'; }
            }
          }
        },
        scales: {
          x: {
            title: { display: true, text: 'Horizon (quarters)', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          },
          y: {
            min: Math.floor((yMin - pad) / 50) * 50,
            max: Math.ceil((yMax + pad) / 50) * 50,
            title: { display: true, text: 'Cumulative response ($B per 1\u03C3)', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          }
        }
      }
    }));
  }

  function buildImpactBar(d) {
    var c = tpColors();
    var ke = d.readiness.key_estimates;

    instances.push(new Chart(el('impactBar'), {
      type: 'bar',
      data: {
        labels: ['TDC', 'Total Deposits', 'Non-TDC'],
        datasets: [{
          data: [ke.tdc_h0.beta, ke.total_deposits_h0.beta, ke.other_component_h0.beta],
          _ciLower: [ke.tdc_h0.lower95, ke.total_deposits_h0.lower95, ke.other_component_h0.lower95],
          _ciUpper: [ke.tdc_h0.upper95, ke.total_deposits_h0.upper95, ke.other_component_h0.upper95],
          backgroundColor: [c.teal, c.blue, c.red],
          borderColor: [c.teal, c.blue, c.red],
          borderWidth: 1,
          borderRadius: 4
        }]
      },
      options: {
        responsive: true,
        aspectRatio: 1.4,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var lo = ctx.dataset._ciLower[ctx.dataIndex];
                var hi = ctx.dataset._ciUpper[ctx.dataIndex];
                return fmt(ctx.parsed.y) + ' $B  [' + fmt(lo) + ', ' + fmt(hi) + ']';
              }
            }
          }
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: c.text } },
          y: {
            title: { display: true, text: '$B per 1\u03C3', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          }
        }
      }
    }));
  }

  function buildHorizonBar(d) {
    var c = tpColors();
    var ke = d.readiness.key_estimates;
    var irf = d.irf;
    var tdcH8 = irf.filter(function (r) { return r.outcome === 'tdc_bank_only_qoq' && r.horizon === 8; })[0];
    var totH8 = irf.filter(function (r) { return r.outcome === 'total_deposits_bank_qoq' && r.horizon === 8; })[0];
    var othH8 = irf.filter(function (r) { return r.outcome === 'other_component_qoq' && r.horizon === 8; })[0];

    instances.push(new Chart(el('horizonBar'), {
      type: 'bar',
      data: {
        labels: ['h0', 'h4', 'h8'],
        datasets: [
          {
            label: 'TDC',
            data: [ke.tdc_h0.beta, ke.tdc_h4.beta, tdcH8 ? tdcH8.beta : 0],
            _ciLower: [ke.tdc_h0.lower95, ke.tdc_h4.lower95, tdcH8 ? tdcH8.lower95 : 0],
            _ciUpper: [ke.tdc_h0.upper95, ke.tdc_h4.upper95, tdcH8 ? tdcH8.upper95 : 0],
            backgroundColor: c.teal,
            borderRadius: 3
          },
          {
            label: 'Total Deposits',
            data: [ke.total_deposits_h0.beta, ke.total_deposits_h4.beta, totH8 ? totH8.beta : 0],
            _ciLower: [ke.total_deposits_h0.lower95, ke.total_deposits_h4.lower95, totH8 ? totH8.lower95 : 0],
            _ciUpper: [ke.total_deposits_h0.upper95, ke.total_deposits_h4.upper95, totH8 ? totH8.upper95 : 0],
            backgroundColor: c.blue,
            borderRadius: 3
          },
          {
            label: 'Non-TDC',
            data: [ke.other_component_h0.beta, ke.other_component_h4.beta, othH8 ? othH8.beta : 0],
            _ciLower: [ke.other_component_h0.lower95, ke.other_component_h4.lower95, othH8 ? othH8.lower95 : 0],
            _ciUpper: [ke.other_component_h0.upper95, ke.other_component_h4.upper95, othH8 ? othH8.upper95 : 0],
            backgroundColor: c.red,
            borderRadius: 3
          }
        ]
      },
      options: {
        responsive: true,
        aspectRatio: 1.4,
        plugins: {
          legend: { labels: { color: c.text, usePointStyle: true } },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var lo = ctx.dataset._ciLower[ctx.dataIndex];
                var hi = ctx.dataset._ciUpper[ctx.dataIndex];
                return ctx.dataset.label + ': ' + fmt(ctx.parsed.y) + ' $B  [' + fmt(lo) + ', ' + fmt(hi) + ']';
              }
            }
          }
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: c.text } },
          y: {
            title: { display: true, text: '$B', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          }
        }
      }
    }));
  }

  function buildPeriodH0(d) {
    var c = tpColors();
    var periods = d.periodSummary.periods;
    var labels = periods.map(function (p) {
      return p.period_variant.replace(/_/g, ' ');
    });

    instances.push(new Chart(el('periodH0Chart'), {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [
          {
            label: 'TDC',
            data: periods.map(function (p) { return p.key_horizons.h0.tdc.beta; }),
            _ciLower: periods.map(function (p) { return p.key_horizons.h0.tdc.lower95; }),
            _ciUpper: periods.map(function (p) { return p.key_horizons.h0.tdc.upper95; }),
            backgroundColor: c.teal,
            borderRadius: 3
          },
          {
            label: 'Total Deposits',
            data: periods.map(function (p) { return p.key_horizons.h0.total_deposits.beta; }),
            _ciLower: periods.map(function (p) { return p.key_horizons.h0.total_deposits.lower95; }),
            _ciUpper: periods.map(function (p) { return p.key_horizons.h0.total_deposits.upper95; }),
            backgroundColor: c.blue,
            borderRadius: 3
          },
          {
            label: 'Non-TDC',
            data: periods.map(function (p) { return p.key_horizons.h0.other_component.beta; }),
            _ciLower: periods.map(function (p) { return p.key_horizons.h0.other_component.lower95; }),
            _ciUpper: periods.map(function (p) { return p.key_horizons.h0.other_component.upper95; }),
            backgroundColor: c.red,
            borderRadius: 3
          }
        ]
      },
      options: {
        responsive: true,
        aspectRatio: 2.2,
        plugins: {
          legend: { labels: { color: c.text, usePointStyle: true } },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var lo = ctx.dataset._ciLower[ctx.dataIndex];
                var hi = ctx.dataset._ciUpper[ctx.dataIndex];
                return ctx.dataset.label + ': ' + fmt(ctx.parsed.y) + ' $B  [' + fmt(lo) + ', ' + fmt(hi) + ']';
              }
            }
          }
        },
        scales: {
          x: { grid: { display: false }, ticks: { color: c.text, maxRotation: 30 } },
          y: {
            title: { display: true, text: 'Impact response ($B)', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          }
        }
      }
    }));
  }

  /* ---- counterpart channel charts ---- */

  /* Extract a channel result safely from a horizon object */
  function chVal(horizonObj, path) {
    var parts = path.split('.');
    var cur = horizonObj;
    for (var i = 0; i < parts.length; i++) {
      if (!cur) return null;
      cur = cur[parts[i]];
    }
    return cur;
  }

  function buildChannelH0(d) {
    var c = tpColors();
    var h0 = d.counterpart.horizons.h0;

    /* Gather all channels with beta/CI at h0 */
    var channels = [
      { label: 'Foreign nontx', data: chVal(h0, 'external_escape_channels.foreign_nonts_qoq') },
      { label: 'CB nontx (asset)', data: chVal(h0, 'asset_purchase_plumbing_context.channels.cb_nonts_qoq') },
      { label: 'Legacy credit proxy', data: h0.legacy_private_credit_proxy ? h0.legacy_private_credit_proxy.snapshot : null },
      { label: 'MMF reallocation', data: chVal(h0, 'deposit_retention_support_channels.domestic_nonfinancial_mmf_reallocation_qoq') },
      { label: 'Repo reallocation', data: chVal(h0, 'deposit_retention_support_channels.domestic_nonfinancial_repo_reallocation_qoq') }
    ].filter(function (ch) { return ch.data && typeof ch.data.beta === 'number'; });

    var labels = channels.map(function (ch) { return ch.label; });
    var betas = channels.map(function (ch) { return ch.data.beta; });
    var lowers = channels.map(function (ch) { return ch.data.lower95; });
    var uppers = channels.map(function (ch) { return ch.data.upper95; });
    var sigFlags = channels.map(function (ch) { return ch.data.ci_excludes_zero; });

    /* Horizontal bar with error bars drawn via plugin */
    var ciPlugin = {
      id: 'hBarCI',
      afterDatasetsDraw: function (chart) {
        var ctx = chart.ctx;
        var meta = chart.getDatasetMeta(0);
        if (meta.hidden) return;
        ctx.save();
        ctx.lineWidth = 1.5;
        ctx.strokeStyle = c.text;
        meta.data.forEach(function (bar, i) {
          if (!sigFlags[i]) return;
          var loPx = chart.scales.x.getPixelForValue(lowers[i]);
          var hiPx = chart.scales.x.getPixelForValue(uppers[i]);
          var y = bar.y;
          ctx.beginPath();
          ctx.moveTo(loPx, y); ctx.lineTo(hiPx, y);
          ctx.stroke();
          ctx.beginPath();
          ctx.moveTo(loPx, y - 4); ctx.lineTo(loPx, y + 4);
          ctx.moveTo(hiPx, y - 4); ctx.lineTo(hiPx, y + 4);
          ctx.stroke();
        });
        ctx.restore();
      }
    };

    /* Zero-line plugin for horizontal chart */
    var zeroPlugin = {
      id: 'hBarZero',
      beforeDatasetsDraw: function (chart) {
        var ctx = chart.ctx;
        var xPx = chart.scales.x.getPixelForValue(0);
        var area = chart.chartArea;
        ctx.save();
        ctx.strokeStyle = c.textMuted;
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(xPx, area.top); ctx.lineTo(xPx, area.bottom);
        ctx.stroke();
        ctx.restore();
      }
    };

    /* Color each bar by significance */
    var bgColors = channels.map(function (ch, i) {
      if (!sigFlags[i]) return c.slate;
      return betas[i] >= 0 ? c.teal : c.red;
    });

    instances.push(new Chart(el('channelH0Chart'), {
      type: 'bar',
      plugins: [ciPlugin, zeroPlugin],
      data: {
        labels: labels,
        datasets: [{
          data: betas,
          backgroundColor: bgColors,
          borderColor: bgColors,
          borderWidth: 1,
          borderRadius: 3,
          barThickness: 18
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        aspectRatio: 1.3,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var i = ctx.dataIndex;
                return fmt(betas[i]) + ' $B  [' + fmt(lowers[i]) + ', ' + fmt(uppers[i]) + ']' +
                  (sigFlags[i] ? '  *' : '');
              }
            }
          }
        },
        scales: {
          x: {
            title: { display: true, text: '$B per 1\u03C3 (h0)', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          },
          y: {
            grid: { display: false },
            ticks: { color: c.text, font: { size: 11 } }
          }
        }
      }
    }));
  }

  function buildChannelHorizon(d) {
    var c = tpColors();
    var hz = d.counterpart.horizons;
    var labels = ['h0', 'h4', 'h8'];

    function extract(path) {
      return labels.map(function (h) {
        var obj = chVal(hz[h], path);
        return obj ? obj.beta : null;
      });
    }
    function extractCI(path) {
      return labels.map(function (h) {
        var obj = chVal(hz[h], path);
        return obj ? { lo: obj.lower95, hi: obj.upper95, sig: obj.ci_excludes_zero } : null;
      });
    }

    var series = [
      { label: 'Foreign nontx', path: 'external_escape_channels.foreign_nonts_qoq', color: c.teal },
      { label: 'Legacy credit proxy', path: null, color: c.blue },
      { label: 'CB nontx (asset)', path: 'asset_purchase_plumbing_context.channels.cb_nonts_qoq', color: c.amber },
      { label: 'MMF reallocation', path: 'deposit_retention_support_channels.domestic_nonfinancial_mmf_reallocation_qoq', color: c.purple }
    ];

    var datasets = series.map(function (s) {
      var data;
      var ci;
      if (s.path) {
        data = extract(s.path);
        ci = extractCI(s.path);
      } else {
        /* Legacy credit proxy is nested differently */
        data = labels.map(function (h) {
          var lp = hz[h].legacy_private_credit_proxy;
          return lp && lp.snapshot ? lp.snapshot.beta : null;
        });
        ci = labels.map(function (h) {
          var lp = hz[h].legacy_private_credit_proxy;
          return lp && lp.snapshot ? { lo: lp.snapshot.lower95, hi: lp.snapshot.upper95, sig: lp.snapshot.ci_excludes_zero } : null;
        });
      }
      return {
        label: s.label,
        data: data,
        _ci: ci,
        borderColor: s.color,
        backgroundColor: s.color,
        borderWidth: 2,
        pointRadius: function (ctx) {
          var ci2 = ci[ctx.dataIndex];
          return ci2 && ci2.sig ? 6 : 4;
        },
        pointStyle: function (ctx) {
          var ci2 = ci[ctx.dataIndex];
          return ci2 && ci2.sig ? 'circle' : 'crossRot';
        },
        pointBackgroundColor: function (ctx) {
          var ci2 = ci[ctx.dataIndex];
          return ci2 && ci2.sig ? s.color : 'transparent';
        },
        pointBorderColor: s.color,
        tension: 0.3,
        fill: false
      };
    });

    instances.push(new Chart(el('channelHorizonChart'), {
      type: 'line',
      data: { labels: labels, datasets: datasets },
      options: {
        responsive: true,
        aspectRatio: 1.3,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            position: 'top',
            labels: { color: c.text, padding: 12, usePointStyle: true, font: { size: 11 } }
          },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var ci2 = ctx.dataset._ci[ctx.dataIndex];
                var base = ctx.dataset.label + ': ' + fmt(ctx.parsed.y) + ' $B';
                if (ci2) base += '  [' + fmt(ci2.lo) + ', ' + fmt(ci2.hi) + ']' + (ci2.sig ? ' *' : '');
                return base;
              }
            }
          }
        },
        scales: {
          x: {
            title: { display: true, text: 'Horizon', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          },
          y: {
            title: { display: true, text: '$B per 1\u03C3', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          }
        }
      }
    }));
  }

  /* ---- deposit-type side-read charts ---- */

  function buildDepositTypeHorizon(d) {
    var c = tpColors();
    var dt = d.depositTypes;
    var labels = ['h0', 'h4', 'h8'];
    var horizonNums = [0, 4, 8];

    var seriesDefs = [
      { outcome: 'checkable_deposits_bank_qoq', label: 'Checkable deposits', color: c.blue },
      { outcome: 'interbank_transactions_bank_qoq', label: 'Interbank transactions', color: c.teal },
      { outcome: 'time_savings_deposits_bank_qoq', label: 'Time & savings', color: c.amber },
      { outcome: 'checkable_private_domestic_bank_qoq', label: 'Checkable: priv. domestic', color: c.slate }
    ];

    var datasets = seriesDefs.map(function (s) {
      var rows = dt.filter(function (r) { return r.outcome === s.outcome; });
      var data = horizonNums.map(function (h) {
        var row = rows.filter(function (r) { return r.horizon === h; })[0];
        return row ? row.beta : null;
      });
      var ci = horizonNums.map(function (h) {
        var row = rows.filter(function (r) { return r.horizon === h; })[0];
        return row ? { lo: row.lower95, hi: row.upper95, sig: row.ci_excludes_zero } : null;
      });
      var muted = s.outcome === 'checkable_private_domestic_bank_qoq';
      return {
        label: s.label,
        data: data,
        _ci: ci,
        borderColor: s.color,
        backgroundColor: s.color,
        borderWidth: muted ? 1.5 : 2.5,
        borderDash: muted ? [4, 3] : [],
        pointRadius: function (ctx) {
          var c2 = ci[ctx.dataIndex];
          return c2 && c2.sig ? 6 : 4;
        },
        pointStyle: function (ctx) {
          var c2 = ci[ctx.dataIndex];
          return c2 && c2.sig ? 'circle' : 'crossRot';
        },
        pointBackgroundColor: function (ctx) {
          var c2 = ci[ctx.dataIndex];
          return c2 && c2.sig ? s.color : 'transparent';
        },
        pointBorderColor: s.color,
        tension: 0.3,
        fill: false
      };
    });

    /* Zero line */
    datasets.push({
      label: 'Zero',
      data: labels.map(function () { return 0; }),
      borderColor: c.textMuted, borderWidth: 1, borderDash: [4, 4],
      pointRadius: 0, fill: false
    });

    instances.push(new Chart(el('depositTypeChart'), {
      type: 'line',
      data: { labels: labels, datasets: datasets },
      options: {
        responsive: true,
        aspectRatio: 1.3,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            position: 'top',
            labels: {
              color: c.text, padding: 12, usePointStyle: true, font: { size: 11 },
              filter: function (item) { return item.text !== 'Zero'; }
            }
          },
          tooltip: {
            filter: function (item) { return item.dataset.label !== 'Zero'; },
            callbacks: {
              label: function (ctx) {
                var ci2 = ctx.dataset._ci ? ctx.dataset._ci[ctx.dataIndex] : null;
                var base = ctx.dataset.label + ': ' + fmt(ctx.parsed.y) + ' $B';
                if (ci2) base += '  [' + fmt(ci2.lo) + ', ' + fmt(ci2.hi) + ']' + (ci2.sig ? ' *' : '');
                return base;
              }
            }
          }
        },
        scales: {
          x: {
            title: { display: true, text: 'Horizon', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          },
          y: {
            title: { display: true, text: '$B per 1\u03C3', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          }
        }
      }
    }));
  }

  function buildDepositTypeH0(d) {
    var c = tpColors();
    var dt = d.depositTypes;

    /* Pick the three main categories + one muted sub at h0 */
    var picks = [
      'checkable_deposits_bank_qoq',
      'interbank_transactions_bank_qoq',
      'time_savings_deposits_bank_qoq',
      'checkable_private_domestic_bank_qoq'
    ];

    var rows = picks.map(function (o) {
      return dt.filter(function (r) { return r.outcome === o && r.horizon === 0; })[0];
    }).filter(Boolean);

    var labels = rows.map(function (r) { return r.display_name; });
    var betas = rows.map(function (r) { return r.beta; });
    var lowers = rows.map(function (r) { return r.lower95; });
    var uppers = rows.map(function (r) { return r.upper95; });
    var sigFlags = rows.map(function (r) { return r.ci_excludes_zero; });

    var bgColors = rows.map(function (r, i) {
      if (!sigFlags[i]) return c.slate;
      return betas[i] >= 0 ? c.teal : c.red;
    });

    /* CI whisker plugin (only for significant bars) */
    var ciPlugin = {
      id: 'dtBarCI',
      afterDatasetsDraw: function (chart) {
        var ctx = chart.ctx;
        var meta = chart.getDatasetMeta(0);
        if (meta.hidden) return;
        ctx.save();
        ctx.lineWidth = 1.5;
        ctx.strokeStyle = c.text;
        meta.data.forEach(function (bar, i) {
          if (!sigFlags[i]) return;
          var loPx = chart.scales.x.getPixelForValue(lowers[i]);
          var hiPx = chart.scales.x.getPixelForValue(uppers[i]);
          var y = bar.y;
          ctx.beginPath();
          ctx.moveTo(loPx, y); ctx.lineTo(hiPx, y);
          ctx.stroke();
          ctx.beginPath();
          ctx.moveTo(loPx, y - 4); ctx.lineTo(loPx, y + 4);
          ctx.moveTo(hiPx, y - 4); ctx.lineTo(hiPx, y + 4);
          ctx.stroke();
        });
        ctx.restore();
      }
    };

    /* Zero-line plugin */
    var zeroPlugin = {
      id: 'dtBarZero',
      beforeDatasetsDraw: function (chart) {
        var ctx = chart.ctx;
        var xPx = chart.scales.x.getPixelForValue(0);
        var area = chart.chartArea;
        ctx.save();
        ctx.strokeStyle = c.textMuted;
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(xPx, area.top); ctx.lineTo(xPx, area.bottom);
        ctx.stroke();
        ctx.restore();
      }
    };

    instances.push(new Chart(el('depositTypeH0Chart'), {
      type: 'bar',
      plugins: [ciPlugin, zeroPlugin],
      data: {
        labels: labels,
        datasets: [{
          data: betas,
          backgroundColor: bgColors,
          borderColor: bgColors,
          borderWidth: 1,
          borderRadius: 3,
          barThickness: 18
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        aspectRatio: 1.3,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var i = ctx.dataIndex;
                return fmt(betas[i]) + ' $B  [' + fmt(lowers[i]) + ', ' + fmt(uppers[i]) + ']' +
                  (sigFlags[i] ? '  *' : '');
              }
            }
          }
        },
        scales: {
          x: {
            title: { display: true, text: '$B per 1\u03C3 (h0)', color: c.textMuted },
            grid: { color: c.grid },
            ticks: { color: c.text }
          },
          y: {
            grid: { display: false },
            ticks: { color: c.text, font: { size: 11 } }
          }
        }
      }
    }));
  }

  /* ---- orchestration ---- */

  function destroyAll() {
    instances.forEach(function (ch) { ch.destroy(); });
    instances.length = 0;
  }

  function buildAll(d) {
    destroyAll();
    applyDefaults();
    buildIRF(d);
    buildImpactBar(d);
    buildHorizonBar(d);
    buildPeriodH0(d);
    buildChannelH0(d);
    buildChannelHorizon(d);
    buildDepositTypeHorizon(d);
    buildDepositTypeH0(d);
  }

  window.tpRebuildCharts = function () {
    if (_data) buildAll(_data);
  };

  /* ---- mobile nav toggle ---- */

  document.addEventListener('DOMContentLoaded', function () {
    var navBtn = el('nav-toggle');
    var navLinks = document.querySelector('.nav-links');
    if (navBtn && navLinks) {
      navBtn.addEventListener('click', function () {
        navLinks.classList.toggle('open');
      });
      navLinks.addEventListener('click', function () {
        navLinks.classList.remove('open');
      });
    }
  });

  /* ---- init ---- */

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadAll);
  } else {
    loadAll();
  }
})();
