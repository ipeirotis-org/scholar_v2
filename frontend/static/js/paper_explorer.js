/**
 * PiP Score — paper explorer for the author results page.
 *
 * One chart with up to three views that share a selection (opens on "standing" when available):
 *   - "standing": the 40-class peer standing curve g(theta) across citation-percentile
 *     thresholds theta, shaded above cutoff tau (default 0.50) whose average height is
 *     the PiP score, with an interactive tau cutoff control.
 *   - "year": each work at (publication year, citation percentile for that year),
 *     with reference lines at the 90th, 75th and 50th percentiles. The axis
 *     starts at the author's career-start year (opts.startYear); works dated
 *     earlier (in GS, stray misdated items or namesakes' papers) are left off
 *     this view only, and the finding text says how many.
 *   - "pip":  the legacy PiP-AUC curve (works in percentile order against the author's
 *     cited-publication count percentile), with the PiP-AUC area shaded.
 * Hover previews a work; click/tap pins it in the side panel. Clicking a spot
 * where several works overlap lists them. A sortable list below the chart is
 * the keyboard- and screen-reader-friendly way to reach every work.
 *
 * Titles and venues are untrusted text: DOM insertion uses textContent only,
 * and hover labels are HTML-escaped (Plotly renders a subset of HTML).
 */
(function (window, document) {
    'use strict';

    // Dots are shaded by tier: one violet getting lighter as the tier widens
    // (OKLab L 0.37 / 0.55 / 0.71), then a light neutral gray below the 50th percentile.
    var COLOR = {
        accent: '#57068c',      // works at the 90th percentile or above for their year; standing/PiP curves
        top25: '#8a55b8',       // 75th-89th percentile
        top50: '#b48fd4',       // 50th-74th percentile
        lowCounted: '#9b86b3',  // counted works below 50th percentile when tau < 0.5
        muted: '#bdb8c4',       // below the 50th percentile / uncounted works
        uncountedLine: '#9e97a8',
        uncountedFill: 'rgba(189, 184, 196, 0.14)',
        selected: '#007a6d',    // selection ring
        ink: '#333333',
        inkMuted: '#6c6675',
        reference: '#330662',
        grid: '#ececec',
        areaFill: 'rgba(87, 6, 140, 0.12)',
        surface: '#ffffff'
    };
    // Highest first. A work belongs to the first tier whose min it reaches;
    // counts on the reference lines are cumulative (the 75th-percentile line counts
    // works at the 90th and above too).
    var TIERS = [
        { min: 90, label: '90.0th percentile and up', color: COLOR.accent, barClass: 'is-top10' },
        { min: 75, label: '75.0th\u201389.9th percentile', color: COLOR.top25, barClass: 'is-top25' },
        { min: 50, label: '50.0th\u201374.9th percentile', color: COLOR.top50, barClass: 'is-top50' }
    ];
    var LIST_PREVIEW = 12;
    var OVERLAP_PX = 7;

    // ------------------------------------------------------------------
    // Formatting (kept in step with frontend/formatting.py)
    // ------------------------------------------------------------------

    function fmtInt(n) {
        return Math.round(Number(n) || 0).toLocaleString('en-US');
    }

    function trim1(v) {
        var t = Number(v).toFixed(1);
        return t.slice(-2) === '.0' ? t.slice(0, -2) : t;
    }

    function fmtTau(tau) {
        var k = Math.round(tau * 40);
        return (k % 2 === 0) ? (k / 40).toFixed(2) : (k / 40).toFixed(3);
    }

    // Percentile with one decimal (0-100 in), clamped to 0.1..99.9, as in
    // formatting.percentile_value: 89.94 -> 89.9, 100 -> 99.9.
    function percentileValue(v) {
        return Math.min(99.9, Math.max(0.1, Math.round(Number(v) * 10) / 10));
    }

    // '77.1st', '86.0th': always one decimal, as formatting.ordinal_percentile.
    function ordinal(v) {
        var t = percentileValue(v).toFixed(1);
        return t + ({ '1': 'st', '2': 'nd', '3': 'rd' }[t.charAt(t.length - 1)] || 'th');
    }

    function standingForYear(pct, year) {
        return ordinal(pct) + ' percentile among cited works published in ' + year;
    }

    function plural(n, one, many) {
        return fmtInt(n) + ' ' + (n === 1 ? one : many);
    }

    function shortTitle(title, max) {
        title = title || 'Untitled';
        if (title.length <= max) return title;
        var cut = title.slice(0, max);
        var space = cut.lastIndexOf(' ');
        return (space > max * 0.6 ? cut.slice(0, space) : cut) + '…';
    }

    function escapeHtml(s) {
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function el(tag, className, text) {
        var node = document.createElement(tag);
        if (className) node.className = className;
        if (text !== undefined && text !== null) node.textContent = text;
        return node;
    }

    // Index into TIERS for a percentile, or -1 for the bottom half.
    function tierIndex(pct) {
        for (var i = 0; i < TIERS.length; i++) {
            if (pct >= TIERS[i].min) return i;
        }
        return -1;
    }

    function countAtLeast(papers, min) {
        return papers.filter(function (p) { return p.pct >= min; }).length;
    }

    // Focus the element in container whose attr equals value, else the fallback
    // selector's match, else the container itself (which has tabindex=-1).
    function focusWithin(container, attr, value, fallbackSelector) {
        var target = null;
        if (value) {
            var nodes = container.querySelectorAll('[' + attr + ']');
            for (var i = 0; i < nodes.length && !target; i++) {
                if (nodes[i].getAttribute(attr) === value) target = nodes[i];
            }
        }
        if (!target && fallbackSelector) target = container.querySelector(fallbackSelector);
        (target || container).focus({ preventScroll: true });
    }

    // ------------------------------------------------------------------
    // Explorer
    // ------------------------------------------------------------------

    function PaperExplorer(rawPapers, opts) {
        this.opts = opts;
        this.chart = document.getElementById(opts.chartId);
        this.panel = document.getElementById(opts.panelId);
        this.list = document.getElementById(opts.listId);
        this.listMore = document.getElementById(opts.listMoreId);
        this.listNote = document.getElementById(opts.listNoteId);
        this.finding = document.getElementById(opts.findingId);
        this.tauControls = opts.tauControlsId ? document.getElementById(opts.tauControlsId) : null;
        this.tauSlider = document.getElementById('tauSlider');
        this.tauReadout = document.getElementById('tauReadout');
        this.tauPctNote = document.getElementById('tauPctNote');
        this.tauScoreReadout = document.getElementById('tauScoreReadout');

        this.standingCurve = (Array.isArray(opts.standingCurve) && opts.standingCurve.length === 40)
            ? opts.standingCurve.map(function (v) { return Math.max(0, Math.min(1, Number(v) || 0)); })
            : null;
        this.hasStanding = Boolean(this.standingCurve);

        var papers = rawPapers.map(function (p, i) {
            var pct = Number(p.num_citations_percentile) || 0;
            var pipClass = (p.pip_class !== undefined && p.pip_class !== null)
                ? Math.min(39, Math.max(0, Number(p.pip_class) | 0))
                : Math.min(39, Math.max(0, Math.floor(pct / 2.5)));
            return {
                id: p.author_pub_id || ('pub-' + i),
                pubId: p.author_pub_id || null,
                title: p.title || 'Untitled',
                venue: p.venue || '',
                year: p.pub_year,
                citations: Number(p.num_citations) || 0,
                pct: pct,
                pipClass: pipClass,
                x: Number(p.num_papers_percentile) || 0,
                sourceRank: Number(p.publication_rank) || (i + 1)
            };
        });
        // Percentile order (the order of the PiP-AUC curve); rank = position in it.
        papers.sort(function (a, b) { return a.sourceRank - b.sourceRank; });
        papers.forEach(function (p, i) { p.rank = i + 1; });

        this.papers = papers;
        this.byId = {};
        for (var i = 0; i < papers.length; i++) this.byId[papers[i].id] = papers[i];

        // Count of author's cited works in class >= j, for j = 0..39.
        this.countsByClass = [];
        for (var j = 0; j < 40; j++) {
            var c = 0;
            for (var m = 0; m < papers.length; m++) {
                if (papers[m].pipClass >= j) c++;
            }
            this.countsByClass.push(c);
        }

        // Career start (stats.year_of_first_pub). It only ever cuts the by-year
        // view; ignored when missing or when no work would be left to plot.
        var start = (opts.startYear === null || opts.startYear === undefined) ? NaN : Number(opts.startYear);
        var onAxis = isFinite(start) ? papers.filter(function (p) { return p.year >= start; }) : [];
        this.startYear = onAxis.length ? start : null;
        this.yearPapers = onAxis.length ? onAxis : papers;
        this.earlierCount = papers.length - this.yearPapers.length;

        // Opens on the standing curve when available (else the legacy PiP-AUC curve).
        this.state = {
            view: this.hasStanding ? 'standing' : 'pip',
            tau: 0.5,
            selectedId: null,
            group: null,
            sort: 'percentile',
            expanded: false
        };
        this.narrow = this._isNarrow();
        this.touch = window.matchMedia && window.matchMedia('(pointer: coarse)').matches;
    }

    PaperExplorer.prototype.init = function () {
        var self = this;
        this.panel.setAttribute('tabindex', '-1');
        this._bindControls();
        this._syncTauUi();
        this._renderFinding();
        this.renderChart();
        this.renderPanel();
        this.renderList();

        this.chart.on('plotly_click', function (ev) { self._onChartClick(ev); });

        var resizeTimer = null;
        window.addEventListener('resize', function () {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(function () {
                var narrow = self._isNarrow();
                if (narrow !== self.narrow) {
                    self.narrow = narrow;
                    self._renderFinding();
                    self.renderChart();
                }
            }, 150);
        });
    };

    PaperExplorer.prototype._isNarrow = function () {
        return (this.chart && this.chart.clientWidth || window.innerWidth) < 560;
    };

    PaperExplorer.prototype._tauClass = function () {
        return Math.min(38, Math.max(0, Math.round(this.state.tau * 40)));
    };

    PaperExplorer.prototype._pipAtTau = function (k) {
        if (!this.hasStanding) return null;
        if (k === 20 && this.opts.pipScore !== null && this.opts.pipScore !== undefined &&
            isFinite(Number(this.opts.pipScore))) {
            return Number(this.opts.pipScore);
        }
        var sum = 0;
        for (var j = k; j < 40; j++) sum += this.standingCurve[j];
        return sum / (40 - k);
    };

    PaperExplorer.prototype._aucScoreValue = function () {
        if (this.opts.pipAucScore !== null && this.opts.pipAucScore !== undefined &&
            isFinite(Number(this.opts.pipAucScore))) {
            return Number(this.opts.pipAucScore);
        }
        if (!this.hasStanding && this.opts.pipScore !== null && this.opts.pipScore !== undefined &&
            isFinite(Number(this.opts.pipScore))) {
            return Number(this.opts.pipScore);
        }
        return null;
    };

    PaperExplorer.prototype._ensureTauReadoutDom = function () {
        if (!this.tauScoreReadout || this._tauScoreNodes) return;
        this.tauScoreReadout.textContent = '';

        var primary = el('span', 'tau-score-primary');
        var swatch = el('span', 'tau-mean-swatch');
        swatch.setAttribute('aria-hidden', 'true');
        primary.appendChild(swatch);
        primary.appendChild(document.createTextNode('Mean height:'));
        var scoreVal = el('strong', 'tau-num-score');
        primary.appendChild(scoreVal);

        var sep = el('span', 'tau-sep', '·');
        sep.setAttribute('aria-hidden', 'true');
        primary.appendChild(sep);

        var countedWrap = el('span', 'tau-counted');
        var countVal = el('span', 'tau-num-count');
        countedWrap.appendChild(countVal);
        countedWrap.appendChild(document.createTextNode(
            ' / ' + fmtInt(this.papers.length) + ' cited works counted'));
        primary.appendChild(countedWrap);

        var note = el('span', 'tau-official-note', 'Official PiP & rankings use τ = 0.50');

        this.tauScoreReadout.appendChild(primary);
        this.tauScoreReadout.appendChild(note);
        this._tauScoreNodes = { scoreVal: scoreVal, countVal: countVal, note: note };
    };

    PaperExplorer.prototype._syncTauUi = function () {
        if (!this.tauControls) return;
        this.tauControls.hidden = (this.state.view !== 'standing');
        if (!this.hasStanding) return;
        var k = this._tauClass();
        var tau = k / 40;
        var tauPct = k * 2.5;
        var pipTau = this._pipAtTau(k);

        if (this.tauSlider) this.tauSlider.value = String(tau);
        if (this.tauReadout) this.tauReadout.textContent = fmtTau(tau);
        if (this.tauPctNote) {
            this.tauPctNote.textContent = '(\u2265 ' + tauPct.toFixed(1) + '%)';
        }
        var chips = this.tauControls.querySelectorAll('[data-tau-preset]');
        Array.prototype.forEach.call(chips, function (chip) {
            var presetK = Math.round(Number(chip.getAttribute('data-tau-preset')) * 40);
            var active = presetK === k;
            chip.classList.toggle('is-active', active);
            chip.setAttribute('aria-pressed', active ? 'true' : 'false');
        });
        if (this.tauScoreReadout && pipTau !== null) {
            this._ensureTauReadoutDom();
            this._tauScoreNodes.scoreVal.textContent = pipTau.toFixed(4);
            this._tauScoreNodes.countVal.textContent = fmtInt(this.countsByClass[k]);
            this._tauScoreNodes.note.classList.toggle('is-custom', k !== 20);
        }
    };

    PaperExplorer.prototype.setTau = function (rawTau) {
        if (!this.hasStanding) return;
        var k = Math.min(38, Math.max(0, Math.round(Number(rawTau) * 40)));
        var prevK = this._tauClass();
        this.state.tau = k / 40;
        this._syncTauUi();
        if (k === prevK) return;
        this.renderChart();
        if (this.state.selectedId) {
            this.renderPanel();
        }
    };

    PaperExplorer.prototype._bindControls = function () {
        var self = this;
        var viewButtons = document.querySelectorAll('[data-explorer-view]');
        Array.prototype.forEach.call(viewButtons, function (btn) {
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                self.setView(btn.getAttribute('data-explorer-view'));
                if (btn.hasAttribute('data-explorer-scroll')) {
                    document.getElementById(self.opts.sectionId).scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            });
        });
        if (this.tauSlider) {
            this.tauSlider.addEventListener('input', function () {
                self.setTau(self.tauSlider.value);
            });
        }
        if (this.tauControls) {
            var chips = this.tauControls.querySelectorAll('[data-tau-preset]');
            Array.prototype.forEach.call(chips, function (chip) {
                chip.addEventListener('click', function () {
                    self.setTau(chip.getAttribute('data-tau-preset'));
                });
            });
        }
        var sortButtons = document.querySelectorAll('[data-explorer-sort]');
        Array.prototype.forEach.call(sortButtons, function (btn) {
            btn.addEventListener('click', function () {
                self.state.sort = btn.getAttribute('data-explorer-sort');
                Array.prototype.forEach.call(sortButtons, function (b) {
                    var on = b === btn;
                    b.classList.toggle('active', on);
                    b.setAttribute('aria-pressed', on ? 'true' : 'false');
                });
                self.renderList();
            });
        });
        if (this.listMore) {
            this.listMore.addEventListener('click', function () {
                self.state.expanded = !self.state.expanded;
                self.renderList();
            });
        }
    };

    PaperExplorer.prototype.setView = function (view) {
        if (view === 'standing' && !this.hasStanding) view = 'pip';
        if (view !== 'standing' && view !== 'year' && view !== 'pip') return;
        this.state.view = view;
        var buttons = document.querySelectorAll('.view-toggle [data-explorer-view]');
        Array.prototype.forEach.call(buttons, function (b) {
            var on = b.getAttribute('data-explorer-view') === view;
            b.classList.toggle('active', on);
            b.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
        this._syncTauUi();
        this._renderFinding();
        this.renderChart();
        this.renderPanel();
    };

    PaperExplorer.prototype.select = function (id, keepGroup) {
        this.state.selectedId = id;
        if (!keepGroup) this.state.group = null;
        this.renderChart();
        this.renderPanel();
        this.renderList();
    };

    PaperExplorer.prototype.clearSelection = function () {
        this.state.selectedId = null;
        this.state.group = null;
        this.renderChart();
        this.renderPanel();
        this.renderList();
    };

    // ------------------------------------------------------------------
    // Chart
    // ------------------------------------------------------------------

    PaperExplorer.prototype._coords = function (p) {
        if (this.state.view === 'year') return { x: p.year, y: p.pct };
        if (this.state.view === 'standing' && this.hasStanding) {
            var b = p.pipClass;
            var x = Math.min(2.5 * (b + 1) - 0.05, Math.max(2.5 * b + 0.05, p.pct));
            return { x: x, y: 100 * this.standingCurve[b] };
        }
        return { x: p.x, y: p.pct };
    };

    // Before the career start: kept everywhere except the by-year view.
    PaperExplorer.prototype._isEarlier = function (p) {
        return this.startYear !== null && p.year < this.startYear;
    };

    PaperExplorer.prototype._onChart = function (p) {
        return this.state.view !== 'year' || !this._isEarlier(p);
    };

    PaperExplorer.prototype._hoverText = function (p) {
        var lines = '<b>' + escapeHtml(shortTitle(p.title, 60)) + '</b><br>' +
            p.year + ' · ' + plural(p.citations, 'citation', 'citations');
        if (this.state.view === 'standing' && this.hasStanding) {
            var b = p.pipClass;
            lines += '<br>' + ordinal(p.pct) + ' pct for its year · peer standing ' +
                trim1(100 * this.standingCurve[b]) + '%';
        } else if (this.state.view === 'pip') {
            lines += ' · #' + p.rank;
        }
        return lines;
    };

    PaperExplorer.prototype._dotTrace = function (papers, color, name) {
        var self = this;
        return {
            type: 'scatter',
            mode: 'markers',
            name: name,
            x: papers.map(function (p) { return self._coords(p).x; }),
            y: papers.map(function (p) { return self._coords(p).y; }),
            customdata: papers.map(function (p) { return p.id; }),
            hovertext: papers.map(function (p) { return self._hoverText(p); }),
            hovertemplate: '%{hovertext}<extra></extra>',
            cliponaxis: false,
            marker: {
                size: this.state.view === 'year' ? 10 : 8,
                color: color,
                line: { color: COLOR.surface, width: 1.5 }
            },
            showlegend: false
        };
    };

    // One dot trace per tier, bottom half first so higher tiers draw on top.
    PaperExplorer.prototype._tierTraces = function (papers) {
        var groups = TIERS.map(function () { return []; });
        var rest = [];
        papers.forEach(function (p) {
            var t = tierIndex(p.pct);
            (t < 0 ? rest : groups[t]).push(p);
        });
        var traces = [this._dotTrace(rest, COLOR.muted, 'Below the 50.0th percentile for its year')];
        for (var i = TIERS.length - 1; i >= 0; i--) {
            traces.push(this._dotTrace(groups[i], TIERS[i].color, TIERS[i].label + ' for its year'));
        }
        return traces;
    };

    PaperExplorer.prototype._ringTrace = function () {
        var self = this;
        var ids = this.state.selectedId ? [this.state.selectedId] : (this.state.group || []);
        var pts = ids.map(function (id) { return self.byId[id]; })
            .filter(function (p) { return p && self._onChart(p); });
        return {
            type: 'scatter',
            mode: 'markers',
            x: pts.map(function (p) { return self._coords(p).x; }),
            y: pts.map(function (p) { return self._coords(p).y; }),
            hoverinfo: 'skip',
            cliponaxis: false,
            marker: {
                size: 22,
                symbol: 'circle-open',
                color: COLOR.selected,
                line: { color: COLOR.selected, width: 2.5 }
            },
            showlegend: false
        };
    };

    PaperExplorer.prototype._baseLayout = function () {
        return {
            font: { family: 'Montserrat, sans-serif', size: this.narrow ? 11 : 12, color: COLOR.ink },
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            margin: { t: this.touch ? 16 : 34, r: this.narrow ? 12 : 24, b: 56, l: this.narrow ? 48 : 64 },
            hovermode: 'closest',
            hoverdistance: 24,
            hoverlabel: {
                bgcolor: COLOR.surface,
                bordercolor: '#d6d0de',
                font: { family: 'Montserrat, sans-serif', size: 12, color: COLOR.ink },
                align: 'left'
            },
            dragmode: this.touch ? false : 'zoom',
            showlegend: false,
            shapes: [],
            annotations: []
        };
    };

    PaperExplorer.prototype._yAxis = function () {
        return {
            title: { text: this.narrow ? 'Percentile for its year' : 'Citation percentile among works from the same year', standoff: 8 },
            range: [0, 100],
            tickvals: [0, 25, 50, 75, 90, 100],
            gridcolor: COLOR.grid,
            gridwidth: 1,
            zeroline: false,
            linecolor: '#d6d0de',
            fixedrange: this.touch
        };
    };

    PaperExplorer.prototype._yearFigure = function () {
        var papers = this.papers;
        // Axis from the first work on or after the career start.
        var years = this.yearPapers.map(function (p) { return p.year; });
        var minYear = Math.min.apply(null, years);
        var maxYear = Math.max.apply(null, years);

        var layout = this._baseLayout();
        layout.xaxis = {
            title: { text: 'Publication year', standoff: 8 },
            range: [minYear - 0.8, maxYear + 0.8],
            tickformat: 'd',
            gridcolor: COLOR.grid,
            gridwidth: 1,
            zeroline: false,
            linecolor: '#d6d0de',
            fixedrange: this.touch
        };
        layout.yaxis = this._yAxis();
        // One dashed line per tier, fainter as the tier widens; counts are cumulative.
        TIERS.forEach(function (tier, i) {
            layout.shapes.push({
                type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: tier.min, y1: tier.min,
                line: { color: COLOR.reference, width: 1, dash: 'dash' },
                opacity: [1, 0.6, 0.4][i], layer: 'below'
            });
            layout.annotations.push({
                xref: 'paper', x: 0, xanchor: 'left', yref: 'y', y: tier.min, yanchor: 'bottom',
                yshift: 1, showarrow: false,
                text: ordinal(tier.min) + ' percentile' + (i === 0 ? ' for its year' : '') + ': ' +
                      countAtLeast(papers, tier.min) + ' of ' + papers.length,
                font: { size: 11, color: COLOR.inkMuted },
                bgcolor: 'rgba(255,255,255,0.8)', borderpad: 1
            });
        });

        return {
            data: this._tierTraces(this.yearPapers).concat([this._ringTrace()]),
            layout: layout
        };
    };

    PaperExplorer.prototype._standingDotTraces = function (papers, k) {
        var uncounted = [];
        var lowCounted = [];
        var groups = TIERS.map(function () { return []; });
        papers.forEach(function (p) {
            if (p.pipClass < k) {
                uncounted.push(p);
                return;
            }
            var t = tierIndex(p.pct);
            (t < 0 ? lowCounted : groups[t]).push(p);
        });
        var uncountedTrace = this._dotTrace(uncounted, COLOR.muted, 'Below cutoff τ');
        uncountedTrace.marker.opacity = 0.6;
        var traces = [
            uncountedTrace,
            this._dotTrace(lowCounted, COLOR.lowCounted, 'Counted (<50th percentile)')
        ];
        for (var i = TIERS.length - 1; i >= 0; i--) {
            traces.push(this._dotTrace(groups[i], TIERS[i].color, TIERS[i].label + ' for its year'));
        }
        return traces;
    };

    // Standing curve g(theta) across the 40 citation-percentile classes (Figure 2 of the QSS paper).
    PaperExplorer.prototype._standingFigure = function () {
        var g = this.standingCurve;
        var k = this._tauClass();
        var tauPct = k * 2.5;
        var pipTau = this._pipAtTau(k);
        var avgHeight = pipTau !== null ? 100 * pipTau : 0;
        var narrow = this.narrow;

        var layout = this._baseLayout();
        layout.uirevision = 'standing';
        layout.xaxis = {
            title: {
                text: narrow ? 'Paper percentile threshold θ'
                             : 'Paper citation percentile threshold θ (among works from the same year)',
                standoff: 8
            },
            range: [0, 100],
            autorange: false,
            automargin: false,
            tickvals: [0, 25, 50, 75, 90, 100],
            gridcolor: COLOR.grid,
            gridwidth: 1,
            zeroline: false,
            linecolor: '#d6d0de',
            fixedrange: this.touch
        };
        layout.yaxis = {
            title: {
                text: narrow ? 'Peer standing g(θ)'
                             : 'Standing among cohort peers g(θ) (% with fewer works ≥ θ)',
                standoff: 8
            },
            range: [0, 100],
            autorange: false,
            automargin: false,
            tickvals: [0, 25, 50, 75, 90, 100],
            gridcolor: COLOR.grid,
            gridwidth: 1,
            zeroline: false,
            linecolor: '#d6d0de',
            fixedrange: this.touch
        };

        // Fixed shape slots: muted curtain [0, tauPct], vertical cutoff line, and horizontal mean line.
        layout.shapes.push({
            type: 'rect', xref: 'x', x0: 0, x1: tauPct, yref: 'y', y0: 0, y1: 100,
            fillcolor: COLOR.uncountedFill, line: { width: 0 }, layer: 'below'
        });
        layout.shapes.push({
            type: 'line', xref: 'x', x0: tauPct, x1: tauPct, yref: 'y', y0: 0, y1: 100,
            line: { color: COLOR.reference, width: k > 0 ? 1.25 : 0, dash: 'dot' }
        });
        if (pipTau !== null) {
            layout.shapes.push({
                type: 'line', xref: 'x', x0: tauPct, x1: 100, yref: 'y', y0: avgHeight, y1: avgHeight,
                line: { color: COLOR.reference, width: 1.6, dash: 'dash' }
            });
        }

        // Fixed curve trace slots: uncounted [0..tauPct] + counted [tauPct..100].
        var ux = [], uy = [];
        if (k > 0) {
            for (var j = 0; j < k; j++) {
                ux.push(j * 2.5);
                uy.push(100 * g[j]);
            }
            ux.push(tauPct);
            uy.push(100 * g[k - 1]);
        }
        var cx = [], cy = [];
        for (var m = k; m < 40; m++) {
            cx.push(m * 2.5);
            cy.push(100 * g[m]);
        }
        cx.push(100);
        cy.push(100 * g[39]);

        var traces = [
            {
                type: 'scatter', mode: 'lines', x: ux, y: uy,
                line: { color: COLOR.uncountedLine, width: 1.75, shape: 'hv' },
                hoverinfo: 'skip', showlegend: false
            },
            {
                type: 'scatter', mode: 'lines', x: cx, y: cy,
                fill: 'tozeroy', fillcolor: COLOR.areaFill,
                line: { color: COLOR.accent, width: 2.2, shape: 'hv' },
                hoverinfo: 'skip', showlegend: false
            }
        ];

        return {
            data: traces.concat(this._standingDotTraces(this.papers, k), [this._ringTrace()]),
            layout: layout
        };
    };

    // The legacy PiP-AUC curve as stats_author_pip_scores_current integrates it (step form):
    // works in percentile order, work k held at its percentile from the previous work's x to its own,
    // starting at (0, first work's percentile). Drawn with line shape 'vh'.
    PaperExplorer.prototype._curve = function () {
        var xs = [0], ys = [this.papers.length ? this.papers[0].pct : 0];
        this.papers.forEach(function (p) { xs.push(p.x); ys.push(p.pct); });
        return { x: xs, y: ys };
    };

    // Height of the step curve at x: the first work whose x is at or past it.
    PaperExplorer.prototype._curveYAt = function (curve, x) {
        for (var i = 1; i < curve.x.length; i++) {
            if (curve.x[i] >= x) return curve.y[i];
        }
        return curve.y[curve.y.length - 1];
    };

    // The first work at or past the cohort median (x = 50), if the curve gets there.
    PaperExplorer.prototype._medianWork = function () {
        for (var i = 0; i < this.papers.length; i++) {
            if (this.papers[i].x >= 50) return this.papers[i];
        }
        return null;
    };

    PaperExplorer.prototype._pipFigure = function () {
        var papers = this.papers;
        var curve = this._curve();
        var last = papers[papers.length - 1];
        var xEnd = last ? last.x : 0;
        var narrow = this.narrow;
        var aucScore = this._aucScoreValue();

        var layout = this._baseLayout();
        layout.xaxis = {
            title: {
                text: narrow ? 'Cited-publication percentile'
                             : 'Cited-publication count, percentile among peers who started the same year',
                standoff: 8
            },
            range: [0, 100],
            tickvals: [0, 25, 50, 75, 100],
            gridcolor: COLOR.grid,
            gridwidth: 1,
            zeroline: false,
            linecolor: '#d6d0de',
            fixedrange: this.touch
        };
        layout.yaxis = this._yAxis();

        var area = {
            type: 'scatter', mode: 'lines', x: curve.x, y: curve.y,
            fill: 'tozeroy', fillcolor: COLOR.areaFill,
            line: { color: COLOR.accent, width: 2, shape: 'vh' },
            hoverinfo: 'skip', showlegend: false
        };

        // Where the curve ends: the author's own cited-publication percentile.
        if (last) {
            layout.shapes.push({
                type: 'line', xref: 'x', x0: xEnd, x1: xEnd, yref: 'y', y0: 0, y1: last.pct,
                line: { color: COLOR.reference, width: 1, dash: 'dot' }
            });
            layout.annotations.push(narrow ? {
                xref: 'x', x: xEnd, yref: 'paper', y: 0, yanchor: 'bottom', xanchor: 'left', xshift: 3,
                showarrow: false, text: trim1(xEnd),
                font: { size: 10, color: COLOR.inkMuted }
            } : {
                xref: 'x', x: xEnd, yref: 'y', y: last.pct,
                showarrow: true, arrowhead: 0, arrowwidth: 1, arrowcolor: COLOR.inkMuted,
                ax: xEnd > 72 ? -48 : 44, ay: -44,
                xanchor: xEnd > 72 ? 'right' : 'left', align: 'left',
                text: 'Ends at ' + trim1(xEnd) + '<br>' + plural(papers.length, 'cited publication', 'cited publications'),
                font: { size: 11, color: COLOR.ink },
                bgcolor: 'rgba(255,255,255,0.85)'
            });
        }

        // Halfway across: the median peer's count, and this author's work at that rank.
        var median = this._medianWork();
        if (median && xEnd >= 50) {
            var about = Math.abs(median.x - 50) < 0.5 ? '' : 'about ';
            layout.shapes.push({
                type: 'line', xref: 'x', x0: 50, x1: 50, yref: 'y', y0: 0, y1: median.pct,
                line: { color: COLOR.reference, width: 1, dash: 'dot' }
            });
            // On narrow screens the finding text above the chart carries this example.
            if (!narrow) layout.annotations.push({
                xref: 'x', x: median.x, yref: 'y', y: median.pct,
                showarrow: true, arrowhead: 0, arrowwidth: 1, arrowcolor: COLOR.inkMuted,
                ax: 36, ay: -86, xanchor: 'left', align: 'left',
                text: 'Median peer: ' + about + median.rank + ' cited publications<br>Work #' + median.rank +
                      ' here: ' + ordinal(median.pct) + ' percentile for its year',
                font: { size: 11, color: COLOR.ink },
                bgcolor: 'rgba(255,255,255,0.85)'
            });
        }

        // Label the shaded area: inside it when there is room, else just right of the end line.
        if (aucScore !== null && last) {
            var scoreText = aucScore.toFixed(4);
            var label = (narrow ? 'Area = ' : 'Shaded area = PiP-AUC ') + scoreText;
            if (xEnd >= 45) {
                var midY = this._curveYAt(curve, xEnd / 2);
                layout.annotations.push({
                    xref: 'x', x: xEnd * (narrow ? 0.32 : 0.5), yref: 'y',
                    y: narrow ? 10 : Math.max(6, midY * 0.4), showarrow: false, text: label,
                    font: { size: narrow ? 11 : 12, color: COLOR.reference }
                });
            } else {
                layout.annotations.push({
                    xref: 'x', x: xEnd, xanchor: 'left', xshift: narrow ? 28 : 8, yref: 'y', y: 6,
                    showarrow: false, text: '\u2190 ' + label,
                    font: { size: narrow ? 11 : 12, color: COLOR.reference }
                });
            }
        }

        return {
            data: [area].concat(this._tierTraces(papers), [this._ringTrace()]),
            layout: layout
        };
    };

    PaperExplorer.prototype.renderChart = function () {
        var fig;
        var ariaLabel;
        if (this.state.view === 'standing' && this.hasStanding) {
            fig = this._standingFigure();
            ariaLabel = 'Standing curve: peer standing across paper citation-percentile thresholds, shaded above cutoff tau';
        } else if (this.state.view === 'year') {
            fig = this._yearFigure();
            ariaLabel = 'Scatter plot of each work by publication year and citation percentile for that year';
        } else {
            fig = this._pipFigure();
            ariaLabel = 'PiP-AUC curve: works in percentile order against cited-publication count percentile, with the area shaded';
        }
        var config = {
            responsive: true,
            displaylogo: false,
            displayModeBar: this.touch ? false : 'hover',
            modeBarButtonsToRemove: ['lasso2d', 'select2d', 'autoScale2d', 'toggleSpikelines',
                                     'hoverClosestCartesian', 'hoverCompareCartesian']
        };
        Plotly.react(this.chart, fig.data, fig.layout, config);
        this.chart.setAttribute('aria-label', ariaLabel);
    };

    PaperExplorer.prototype._renderFinding = function () {
        if (!this.finding) return;
        var papers = this.papers;
        var n = papers.length;
        this.finding.textContent = '';
        if (this.state.view === 'standing' && this.hasStanding) {
            var stdScore = this._pipAtTau(20);
            var counted50 = this.countsByClass[20];
            if (stdScore !== null) {
                this.finding.appendChild(el('strong', null,
                    'The average height above τ = 0.50 (dashed line) is the PiP score, ' + stdScore.toFixed(4) + '.'));
                this.finding.appendChild(document.createTextNode(' '));
            }
            this.finding.appendChild(document.createTextNode(
                fmtInt(counted50) + ' of ' + fmtInt(n) + ' cited works reach at least the 50th percentile for their year.' +
                ' At each threshold θ, the curve shows the share of career-stage peers with strictly fewer works' +
                ' at or above θ; between works it rises as peers drop out, and at each work’s threshold it steps down.'));
        } else if (this.state.view === 'year') {
            var lead = el('strong', null, countAtLeast(papers, TIERS[0].min) + ' of ' + n +
                ' scored works reached the ' + ordinal(TIERS[0].min) + ' percentile for their publication year.');
            this.finding.appendChild(lead);
            this.finding.appendChild(document.createTextNode(
                ' ' + countAtLeast(papers, TIERS[1].min) + ' reached the ' + ordinal(TIERS[1].min) + ' and ' +
                countAtLeast(papers, TIERS[2].min) + ' the ' + ordinal(TIERS[2].min) + '.' +
                ' Each dot is one work, placed by when it appeared and by how its citations compare' +
                ' with other cited works from that year; darker dots rank higher.'));
            if (this.earlierCount) {
                this.finding.appendChild(document.createTextNode(
                    ' The chart starts at ' + this.startYear + ', the career start used for the comparison;' +
                    ' ' + plural(this.earlierCount, 'work', 'works') + ' dated earlier ' +
                    (this.earlierCount === 1 ? 'is' : 'are') + ' left off this view but ' +
                    (this.earlierCount === 1 ? 'stays' : 'stay') + ' in the list below.'));
            }
        } else {
            var last = papers[n - 1];
            var score = this._aucScoreValue();
            if (score !== null) {
                this.finding.appendChild(el('strong', null, 'The shaded area is the PiP-AUC score, ' + Number(score).toFixed(4) + '.'));
                this.finding.appendChild(document.createTextNode(' '));
            }
            var text = 'Works run left to right from highest to lowest percentile for their year.';
            if (last) {
                text += ' The curve stops at ' + trim1(last.x) + ', where ' +
                    plural(n, 'cited publication places ', 'cited publications place ') +
                    this.opts.authorName + ' among peers.';
            }
            var median = this._medianWork();
            if (this.narrow && median && last && last.x >= 50) {
                text += ' Halfway across, the median peer has ' + (Math.abs(median.x - 50) < 0.5 ? '' : 'about ') +
                    plural(median.rank, 'cited publication', 'cited publications') + '; work #' + median.rank +
                    ' here sits at the ' + ordinal(median.pct) + ' percentile for its year.';
            }
            this.finding.appendChild(document.createTextNode(text));
        }
    };

    PaperExplorer.prototype._overlapping = function (id) {
        var target = this.byId[id];
        if (!target) return [id];
        var self = this;
        var full = this.chart._fullLayout;
        var c0 = this._coords(target);
        var drawn = this.papers.filter(function (p) { return self._onChart(p); });
        try {
            var xa = full.xaxis, ya = full.yaxis;
            var px = xa.l2p(c0.x), py = ya.l2p(c0.y);
            return drawn.filter(function (p) {
                var c = self._coords(p);
                var dx = xa.l2p(c.x) - px, dy = ya.l2p(c.y) - py;
                return dx * dx + dy * dy <= OVERLAP_PX * OVERLAP_PX;
            }).map(function (p) { return p.id; });
        } catch (e) {
            return drawn.filter(function (p) {
                var c = self._coords(p);
                return c.x === c0.x && c.y === c0.y;
            }).map(function (p) { return p.id; });
        }
    };

    PaperExplorer.prototype._onChartClick = function (ev) {
        var pt = ev && ev.points && ev.points[0];
        if (!pt || pt.customdata === undefined) return;
        var ids = this._overlapping(pt.customdata);
        if (ids.length > 1) {
            this.state.group = ids;
            this.state.selectedId = null;
            this.renderChart();
            this.renderPanel();
            this.renderList();
        } else {
            this.select(pt.customdata);
        }
        if (this.panel.scrollIntoView) this.panel.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    };

    // ------------------------------------------------------------------
    // Side panel
    // ------------------------------------------------------------------

    // Re-rendering replaces the panel's buttons. When focus was inside the panel,
    // move it to the matching control in the new content (same data-focus-key),
    // else to the work's title, else to the panel, so keyboard users keep their place.
    PaperExplorer.prototype.renderPanel = function () {
        var active = document.activeElement;
        var hadFocus = active && active !== this.panel && this.panel.contains(active);
        var key = this._focusKeyOverride || (hadFocus ? active.getAttribute('data-focus-key') : null);
        this._focusKeyOverride = null;

        this.panel.textContent = '';
        if (this.state.selectedId && this.byId[this.state.selectedId]) {
            this._panelPaper(this.byId[this.state.selectedId]);
        } else if (this.state.group && this.state.group.length) {
            this._panelGroup(this.state.group);
        } else {
            this._panelEmpty();
        }
        if (hadFocus) focusWithin(this.panel, 'data-focus-key', key, '[data-focus-fallback]');
    };

    PaperExplorer.prototype._panelHeader = function (label) {
        var self = this;
        var head = el('div', 'paper-panel-head');
        head.appendChild(el('span', 'paper-panel-eyebrow', label));
        var close = el('button', 'paper-panel-close', '×');
        close.type = 'button';
        close.setAttribute('aria-label', 'Clear selection');
        close.setAttribute('data-focus-key', 'close');
        close.addEventListener('click', function () { self.clearSelection(); });
        head.appendChild(close);
        this.panel.appendChild(head);
    };

    PaperExplorer.prototype._panelEmpty = function () {
        var self = this;
        this.panel.appendChild(el('span', 'paper-panel-eyebrow', 'Explore the works'));
        this.panel.appendChild(el('p', 'paper-panel-hint', (this.touch
            ? 'Tap a dot to open a work here, or pick one from the list below. '
            : 'Hover a dot for a preview. Click to keep a work open here, or pick one from the list below. ') +
            'It stays selected when you switch views.'));
        if (!this.papers.length) return;

        var bestPct = this.papers[0];
        var mostCited = this.papers.reduce(function (a, b) { return b.citations > a.citations ? b : a; });
        var picks = el('div', 'paper-panel-picks');
        var add = function (label, p) {
            var btn = el('button', 'paper-pick');
            btn.type = 'button';
            btn.appendChild(el('span', 'paper-pick-label', label));
            btn.appendChild(el('span', 'paper-pick-title', shortTitle(p.title, 80)));
            btn.appendChild(el('span', 'paper-pick-meta',
                p.year + ' · ' + plural(p.citations, 'citation', 'citations') + ' · ' + ordinal(p.pct) + ' percentile'));
            btn.setAttribute('data-focus-key', 'pick-' + p.id);
            btn.addEventListener('click', function () { self.select(p.id); });
            picks.appendChild(btn);
        };
        add('Highest for its year', bestPct);
        if (mostCited.id !== bestPct.id) add('Most cited', mostCited);
        this.panel.appendChild(picks);
    };

    PaperExplorer.prototype._panelPaper = function (p) {
        var self = this;
        this._panelHeader('Selected work');

        if (this.state.group && this.state.group.length > 1) {
            var back = el('button', 'paper-panel-back', '← ' + this.state.group.length + ' works at this spot');
            back.type = 'button';
            back.setAttribute('data-focus-key', 'back');
            back.addEventListener('click', function () {
                // Land back on the work that was open.
                self._focusKeyOverride = 'pick-' + self.state.selectedId;
                self.state.selectedId = null;
                self.renderChart();
                self.renderPanel();
                self.renderList();
            });
            this.panel.appendChild(back);
        }

        var title = el('h4', 'paper-panel-title', p.title);
        title.setAttribute('tabindex', '-1');
        title.setAttribute('data-focus-fallback', '');
        this.panel.appendChild(title);
        if (p.venue) this.panel.appendChild(el('p', 'paper-panel-venue', p.venue));

        var stat = el('p', 'paper-panel-stat');
        stat.appendChild(el('strong', null, plural(p.citations, 'citation', 'citations')));
        stat.appendChild(document.createTextNode(' · ' + standingForYear(p.pct, p.year) + '.'));
        this.panel.appendChild(stat);

        var dl = el('dl', 'paper-panel-facts');
        var fact = function (k, v) {
            dl.appendChild(el('dt', null, k));
            dl.appendChild(el('dd', null, v));
        };
        fact('Percentile for its year', ordinal(p.pct));
        fact('Rank among scored works', '#' + p.rank + ' of ' + this.papers.length);
        if (this.hasStanding) {
            var b = p.pipClass;
            var k = this._tauClass();
            var nAt = this.countsByClass[b];
            fact('Standing at its threshold',
                plural(nAt, 'work', 'works') + ' ≥ ' + trim1(b * 2.5) + 'th pct · beats ' +
                trim1(100 * this.standingCurve[b]) + '% of peers');
            fact('Current cutoff',
                b >= k ? 'Counted in score' : 'Below cutoff (not counted)');
        }
        if (this.state.view === 'pip' || !this.hasStanding) {
            fact('Place on PiP-AUC curve', 'x = ' + trim1(p.x) + ', y = ' + trim1(p.pct));
        }
        this.panel.appendChild(dl);

        var earlier = this._isEarlier(p);
        if (earlier && this.state.view === 'year') {
            this.panel.appendChild(el('p', 'paper-panel-hint',
                'Dated before ' + this.startYear + ', the career start used for the comparison,' +
                ' so it is not plotted by year.'));
        }

        var actions = el('div', 'paper-panel-actions');
        var nextView = this.hasStanding
            ? (this.state.view === 'standing' ? 'year' : 'standing')
            : (this.state.view === 'year' ? 'pip' : 'year');
        if (earlier && nextView === 'year') {
            nextView = 'pip';
        }
        if (nextView !== this.state.view) {
            var swapLabel = nextView === 'standing'
                ? 'Show it on the standing curve'
                : (nextView === 'year' ? 'Show it by publication year' : 'Show it on the PiP-AUC curve');
            var swap = el('button', 'btn btn-sm btn-outline-violet', swapLabel);
            swap.type = 'button';
            swap.setAttribute('data-focus-key', 'swap');
            swap.addEventListener('click', function () { self.setView(nextView); });
            actions.appendChild(swap);
        }

        var link = this._paperLink(p);
        if (link) actions.appendChild(link);
        this.panel.appendChild(actions);
    };

    PaperExplorer.prototype._paperLink = function (p) {
        if (!p.pubId) return null;
        var a = el('a', 'paper-panel-link');
        if (this.opts.dataSource === 'gs') {
            if (p.pubId.indexOf(':') < 0) return null;
            a.href = 'https://scholar.google.com/citations?view_op=view_citation&hl=en&citation_for_view=' +
                encodeURIComponent(p.pubId);
            a.target = '_blank';
            a.rel = 'noopener';
            a.textContent = 'Authors and full record on Google Scholar ↗';
        } else {
            a.href = '/publication/' + encodeURIComponent(this.opts.authorId) + '/' + encodeURIComponent(p.pubId);
            a.textContent = 'Citation history';
        }
        return a;
    };

    PaperExplorer.prototype._panelGroup = function (ids) {
        var self = this;
        var papers = ids.map(function (id) { return self.byId[id]; }).filter(Boolean);
        this._panelHeader(papers.length + ' works at this spot');
        var first = papers[0];
        var where;
        if (this.state.view === 'year') {
            where = first.year + ', near the ' + ordinal(first.pct) + ' percentile';
        } else if (this.state.view === 'standing' && this.hasStanding) {
            where = 'near θ = ' + trim1(first.pct) + ', peer standing ' +
                trim1(100 * this.standingCurve[first.pipClass]) + '%';
        } else {
            where = 'near x = ' + trim1(first.x) + ', ' + ordinal(first.pct) + ' percentile';
        }
        this.panel.appendChild(el('p', 'paper-panel-hint', 'These works overlap ' + where + '. Pick one:'));
        var ul = el('ul', 'paper-panel-group');
        papers.forEach(function (p) {
            var li = el('li');
            var btn = el('button', 'paper-pick');
            btn.type = 'button';
            btn.appendChild(el('span', 'paper-pick-title', shortTitle(p.title, 90)));
            btn.appendChild(el('span', 'paper-pick-meta',
                p.year + ' · ' + plural(p.citations, 'citation', 'citations') + ' · ' + ordinal(p.pct)));
            btn.setAttribute('data-focus-key', 'pick-' + p.id);
            btn.addEventListener('click', function () { self.select(p.id, true); });
            li.appendChild(btn);
            ul.appendChild(li);
        });
        this.panel.appendChild(ul);
    };

    // ------------------------------------------------------------------
    // Sortable list
    // ------------------------------------------------------------------

    PaperExplorer.prototype._sorted = function () {
        var papers = this.papers.slice();
        var sort = this.state.sort;
        papers.sort(function (a, b) {
            if (sort === 'citations') return (b.citations - a.citations) || (a.rank - b.rank);
            if (sort === 'year') return (b.year - a.year) || (a.rank - b.rank);
            return a.rank - b.rank;
        });
        return papers;
    };

    PaperExplorer.prototype._renderListNote = function () {
        if (!this.listNote) return;
        this.listNote.hidden = true;
        this.listNote.textContent = '';
        if (this.state.sort !== 'citations' || this.papers.length < 2) return;
        var best = this.papers[0];
        var most = this.papers.reduce(function (a, b) { return b.citations > a.citations ? b : a; });
        if (most.id === best.id || best.citations >= most.citations || best.pct <= most.pct) return;
        this.listNote.appendChild(el('strong', null, 'Why percentiles: '));
        this.listNote.appendChild(document.createTextNode(
            '“' + shortTitle(best.title, 60) + '” (' + best.year + ') has fewer citations than “' +
            shortTitle(most.title, 60) + '” (' + most.year + '), ' + fmtInt(best.citations) + ' vs ' +
            fmtInt(most.citations) + ', yet ranks higher for its year (' + ordinal(best.pct) + ' vs ' +
            ordinal(most.pct) + ' percentile). Older work has had longer to collect citations, so each' +
            ' work is compared only with works published the same year.'));
        this.listNote.hidden = false;
    };

    PaperExplorer.prototype.renderList = function () {
        if (!this.list) return;
        var self = this;
        var sorted = this._sorted();
        var shown = this.state.expanded ? sorted : sorted.slice(0, LIST_PREVIEW);
        var highlight = {};
        if (this.state.selectedId) highlight[this.state.selectedId] = true;
        (this.state.group || []).forEach(function (id) { highlight[id] = true; });

        var active = document.activeElement;
        var focusId = active && this.list.contains(active) ? active.getAttribute('data-paper-id') : null;

        this.list.textContent = '';
        shown.forEach(function (p) {
            var li = el('li');
            var btn = el('button', 'paper-row' + (highlight[p.id] ? ' is-selected' : ''));
            btn.type = 'button';
            btn.setAttribute('aria-pressed', p.id === self.state.selectedId ? 'true' : 'false');
            btn.setAttribute('data-paper-id', p.id);

            var pct = el('span', 'paper-row-pct');
            pct.appendChild(el('span', 'paper-row-pct-value', percentileValue(p.pct).toFixed(1)));
            var bar = el('span', 'paper-row-bar');
            var tier = tierIndex(p.pct);
            var fill = el('span', 'paper-row-bar-fill' + (tier >= 0 ? ' ' + TIERS[tier].barClass : ''));
            fill.style.width = Math.max(0, Math.min(100, p.pct)) + '%';
            bar.appendChild(fill);
            pct.appendChild(bar);

            var main = el('span', 'paper-row-main');
            main.appendChild(el('span', 'paper-row-title', p.title));
            main.appendChild(el('span', 'paper-row-meta', (p.venue ? p.venue + ' · ' : '') + p.year));

            var cites = el('span', 'paper-row-cites');
            cites.appendChild(el('span', 'paper-row-cites-value', fmtInt(p.citations)));
            cites.appendChild(el('span', 'paper-row-cites-label', p.citations === 1 ? 'citation' : 'citations'));

            btn.appendChild(pct);
            btn.appendChild(main);
            btn.appendChild(cites);
            btn.addEventListener('click', function () {
                self.select(p.id);
                if (self.panel.scrollIntoView) self.panel.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            });
            li.appendChild(btn);
            self.list.appendChild(li);
        });

        if (this.listMore) {
            var hidden = sorted.length - LIST_PREVIEW;
            this.listMore.hidden = hidden <= 0;
            this.listMore.textContent = this.state.expanded
                ? 'Show the first ' + LIST_PREVIEW
                : 'Show all ' + fmtInt(sorted.length) + ' works';
        }
        this._renderListNote();
        if (focusId) focusWithin(this.list, 'data-paper-id', focusId, null);
    };

    window.initPaperExplorer = function (papers, opts) {
        if (!papers || !papers.length || typeof Plotly === 'undefined') return null;
        var explorer = new PaperExplorer(papers, opts);
        if (!explorer.chart || !explorer.panel) return null;
        explorer.init();
        return explorer;
    };
})(window, document);
