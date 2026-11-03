/**
 * PiP Score — paper explorer for the author results page.
 *
 * One chart with two views that share a selection:
 *   - "year": each work at (publication year, citation percentile for that year),
 *     with a reference line at the 90th percentile.
 *   - "pip":  the PiP curve (works in percentile order against the author's
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

    var COLOR = {
        accent: '#57068c',      // works in the top 10% for their year
        muted: '#a49cb0',       // all other works
        selected: '#007a6d',    // selection ring
        ink: '#333333',
        inkMuted: '#6c6675',
        reference: '#330662',
        grid: '#ececec',
        areaFill: 'rgba(87, 6, 140, 0.10)',
        surface: '#ffffff'
    };
    var TOP_THRESHOLD = 90;
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

    function ordinal(v) {
        var t = trim1(v);
        if (t.indexOf('.') < 0) {
            var n = parseInt(t, 10) % 100;
            if (n >= 10 && n <= 20) return t + 'th';
        }
        return t + ({ '1': 'st', '2': 'nd', '3': 'rd' }[t.charAt(t.length - 1)] || 'th');
    }

    function topShare(pct) {
        var share = Math.max(0.1, 100 - pct);
        return share >= 10 ? String(Math.round(share)) : trim1(share);
    }

    function standingForYear(pct, year) {
        if (pct >= 50) return 'Top ' + topShare(pct) + '% among cited works published in ' + year;
        return 'Ahead of ' + trim1(pct) + '% of cited works published in ' + year;
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

        var papers = rawPapers.map(function (p, i) {
            return {
                id: p.author_pub_id || ('pub-' + i),
                pubId: p.author_pub_id || null,
                title: p.title || 'Untitled',
                venue: p.venue || '',
                year: p.pub_year,
                citations: Number(p.num_citations) || 0,
                pct: Number(p.num_citations_percentile) || 0,
                x: Number(p.num_papers_percentile) || 0,
                sourceRank: Number(p.publication_rank) || (i + 1)
            };
        });
        // Percentile order (the order of the PiP curve); rank = position in it.
        papers.sort(function (a, b) { return a.sourceRank - b.sourceRank; });
        papers.forEach(function (p, i) { p.rank = i + 1; });

        this.papers = papers;
        this.byId = {};
        for (var i = 0; i < papers.length; i++) this.byId[papers[i].id] = papers[i];

        this.state = { view: 'year', selectedId: null, group: null, sort: 'percentile', expanded: false };
        this.narrow = this._isNarrow();
        this.touch = window.matchMedia && window.matchMedia('(pointer: coarse)').matches;
    }

    PaperExplorer.prototype.init = function () {
        var self = this;
        this.panel.setAttribute('tabindex', '-1');
        this._bindControls();
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
                    self.renderChart();
                }
            }, 150);
        });
    };

    PaperExplorer.prototype._isNarrow = function () {
        return (this.chart && this.chart.clientWidth || window.innerWidth) < 560;
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
        if (view !== 'year' && view !== 'pip') return;
        this.state.view = view;
        var buttons = document.querySelectorAll('.view-toggle [data-explorer-view]');
        Array.prototype.forEach.call(buttons, function (b) {
            var on = b.getAttribute('data-explorer-view') === view;
            b.classList.toggle('active', on);
            b.setAttribute('aria-pressed', on ? 'true' : 'false');
        });
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
        return this.state.view === 'year' ? { x: p.year, y: p.pct } : { x: p.x, y: p.pct };
    };

    PaperExplorer.prototype._hoverText = function (p) {
        var lines = '<b>' + escapeHtml(shortTitle(p.title, 60)) + '</b><br>' +
            p.year + ' · ' + plural(p.citations, 'citation', 'citations');
        if (this.state.view === 'pip') lines += ' · #' + p.rank;
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

    PaperExplorer.prototype._ringTrace = function () {
        var self = this;
        var ids = this.state.selectedId ? [this.state.selectedId] : (this.state.group || []);
        var pts = ids.map(function (id) { return self.byId[id]; }).filter(Boolean);
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
        var top = papers.filter(function (p) { return p.pct >= TOP_THRESHOLD; });
        var rest = papers.filter(function (p) { return p.pct < TOP_THRESHOLD; });
        var years = papers.map(function (p) { return p.year; });
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
        layout.shapes.push({
            type: 'line', xref: 'paper', x0: 0, x1: 1, yref: 'y', y0: TOP_THRESHOLD, y1: TOP_THRESHOLD,
            line: { color: COLOR.reference, width: 1, dash: 'dash' }, layer: 'below'
        });
        layout.annotations.push({
            xref: 'paper', x: 0, xanchor: 'left', yref: 'y', y: TOP_THRESHOLD, yanchor: 'bottom',
            yshift: 1, showarrow: false,
            text: 'Top 10% for its year: ' + top.length + ' of ' + papers.length,
            font: { size: 11, color: COLOR.inkMuted }
        });

        return {
            data: [this._dotTrace(rest, COLOR.muted, 'Other works'),
                   this._dotTrace(top, COLOR.accent, 'Top 10% for its year'),
                   this._ringTrace()],
            layout: layout
        };
    };

    // The PiP curve exactly as stats_author_pip_scores_current integrates it:
    // works in percentile order, starting at (0, first work's percentile).
    PaperExplorer.prototype._curve = function () {
        var xs = [0], ys = [this.papers.length ? this.papers[0].pct : 0];
        this.papers.forEach(function (p) { xs.push(p.x); ys.push(p.pct); });
        return { x: xs, y: ys };
    };

    PaperExplorer.prototype._curveYAt = function (curve, x) {
        for (var i = 1; i < curve.x.length; i++) {
            if (curve.x[i] >= x) {
                var x0 = curve.x[i - 1], x1 = curve.x[i];
                if (x1 === x0) return curve.y[i];
                return curve.y[i - 1] + (curve.y[i] - curve.y[i - 1]) * (x - x0) / (x1 - x0);
            }
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
            line: { color: COLOR.accent, width: 2, shape: 'linear' },
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
        if (this.opts.pipScore !== null && this.opts.pipScore !== undefined && last) {
            var scoreText = Number(this.opts.pipScore).toFixed(4);
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

        var top = papers.filter(function (p) { return p.pct >= TOP_THRESHOLD; });
        var rest = papers.filter(function (p) { return p.pct < TOP_THRESHOLD; });
        return {
            data: [area,
                   this._dotTrace(rest, COLOR.muted, 'Other works'),
                   this._dotTrace(top, COLOR.accent, 'Top 10% for its year'),
                   this._ringTrace()],
            layout: layout
        };
    };

    PaperExplorer.prototype.renderChart = function () {
        var fig = this.state.view === 'year' ? this._yearFigure() : this._pipFigure();
        var config = {
            responsive: true,
            displaylogo: false,
            displayModeBar: this.touch ? false : 'hover',
            modeBarButtonsToRemove: ['lasso2d', 'select2d', 'autoScale2d', 'toggleSpikelines',
                                     'hoverClosestCartesian', 'hoverCompareCartesian']
        };
        Plotly.react(this.chart, fig.data, fig.layout, config);
        this.chart.setAttribute('aria-label', this.state.view === 'year'
            ? 'Scatter plot of each work by publication year and citation percentile for that year'
            : 'PiP curve: works in percentile order against cited-publication count percentile, with the area shaded');
        this._renderFinding();
    };

    PaperExplorer.prototype._renderFinding = function () {
        if (!this.finding) return;
        var papers = this.papers;
        var n = papers.length;
        var top = papers.filter(function (p) { return p.pct >= TOP_THRESHOLD; }).length;
        this.finding.textContent = '';
        if (this.state.view === 'year') {
            var lead = el('strong', null, top + ' of ' + n + ' scored works reached the top 10% for their publication year.');
            this.finding.appendChild(lead);
            this.finding.appendChild(document.createTextNode(
                ' Each dot is one work, placed by when it appeared and by how its citations compare' +
                ' with other cited works from that year.'));
        } else {
            var last = papers[n - 1];
            var score = this.opts.pipScore;
            if (score !== null && score !== undefined) {
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
        try {
            var xa = full.xaxis, ya = full.yaxis;
            var px = xa.l2p(c0.x), py = ya.l2p(c0.y);
            return this.papers.filter(function (p) {
                var c = self._coords(p);
                var dx = xa.l2p(c.x) - px, dy = ya.l2p(c.y) - py;
                return dx * dx + dy * dy <= OVERLAP_PX * OVERLAP_PX;
            }).map(function (p) { return p.id; });
        } catch (e) {
            return this.papers.filter(function (p) {
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
        fact('Place on the PiP curve', 'x = ' + trim1(p.x) + ', y = ' + trim1(p.pct));
        this.panel.appendChild(dl);

        var actions = el('div', 'paper-panel-actions');
        var other = this.state.view === 'year' ? 'pip' : 'year';
        var swap = el('button', 'btn btn-sm btn-outline-violet',
            other === 'pip' ? 'Show it on the PiP curve' : 'Show it by publication year');
        swap.type = 'button';
        swap.setAttribute('data-focus-key', 'swap');
        swap.addEventListener('click', function () { self.setView(other); });
        actions.appendChild(swap);

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
        var where = this.state.view === 'year'
            ? first.year + ', near the ' + ordinal(first.pct) + ' percentile'
            : 'near x = ' + trim1(first.x) + ', ' + ordinal(first.pct) + ' percentile';
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
            pct.appendChild(el('span', 'paper-row-pct-value', p.pct.toFixed(1)));
            var bar = el('span', 'paper-row-bar');
            var fill = el('span', 'paper-row-bar-fill' + (p.pct >= TOP_THRESHOLD ? ' is-top' : ''));
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
