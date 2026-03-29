/**
 * PiP Score — Client-side chart rendering with Plotly.js
 *
 * All chart functions accept a DOM element ID and data array,
 * and render interactive Plotly charts.
 */

// NYU brand colors
var COLORS = {
    violet: '#57068c',
    violetLight: '#ab82c5',
    teal: '#009b8a',
    tealDeep: '#007a6d',
    blue: 'rgb(31, 119, 180)',
    red: 'rgb(214, 39, 40)',
    orange: 'rgb(255, 127, 14)',
    gray: '#e0e0e0'
};

var PLOT_LAYOUT_DEFAULTS = {
    font: { family: 'Montserrat, sans-serif', size: 13 },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { t: 50, r: 30, b: 60, l: 60 },
    hoverlabel: { font: { family: 'Montserrat, sans-serif', size: 12 } }
};

var PLOT_CONFIG = {
    responsive: true,
    displayModeBar: true,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    displaylogo: false
};


/**
 * Percentile Rank Plot: paper rank (X) vs citation percentile (Y), colored by age.
 */
function renderPercentileRankPlot(elementId, data, authorName) {
    if (!data || data.length === 0) return;

    var ages = data.map(function(d) { return d.age; });
    var maxAge = Math.max.apply(null, ages);
    var minAge = Math.min.apply(null, ages);

    var trace = {
        x: data.map(function(d) { return d.publication_rank; }),
        y: data.map(function(d) { return d.num_citations_percentile; }),
        mode: 'markers',
        type: 'scatter',
        marker: {
            size: 8,
            color: ages,
            colorscale: 'Blues',
            reversescale: true,
            cmin: minAge,
            cmax: maxAge,
            colorbar: {
                title: { text: 'Years since<br>Publication', font: { size: 12 } },
                thickness: 15,
                len: 0.6
            },
            line: { width: 0.5, color: 'rgba(0,0,0,0.2)' }
        },
        text: data.map(function(d) {
            return d.title + '<br>Year: ' + d.pub_year +
                   '<br>Citations: ' + d.num_citations +
                   '<br>Percentile: ' + d.num_citations_percentile.toFixed(1) + '%';
        }),
        hoverinfo: 'text'
    };

    var layout = Object.assign({}, PLOT_LAYOUT_DEFAULTS, {
        title: { text: 'Paper Percentile Scores for ' + authorName, font: { size: 16 } },
        xaxis: {
            title: 'Paper Rank',
            gridcolor: COLORS.gray,
            gridwidth: 1,
            zeroline: false
        },
        yaxis: {
            title: 'Paper Percentile Score',
            range: [0, 105],
            dtick: 10,
            gridcolor: COLORS.gray,
            gridwidth: 1,
            zeroline: false
        },
        margin: { t: 60, r: 80, b: 60, l: 60 }
    });

    Plotly.newPlot(elementId, [trace], layout, PLOT_CONFIG);
}


/**
 * PiP Plot: publication count percentile (X) vs citation percentile (Y), colored by age.
 */
function renderPipPlot(elementId, data, authorName) {
    if (!data || data.length === 0) return;

    var ages = data.map(function(d) { return d.age; });
    var maxAge = Math.max.apply(null, ages);
    var minAge = Math.min.apply(null, ages);

    var trace = {
        x: data.map(function(d) { return d.num_papers_percentile; }),
        y: data.map(function(d) { return d.num_citations_percentile; }),
        mode: 'markers',
        type: 'scatter',
        marker: {
            size: 8,
            color: ages,
            colorscale: 'Blues',
            reversescale: true,
            cmin: minAge,
            cmax: maxAge,
            colorbar: {
                title: { text: 'Years since<br>Publication', font: { size: 12 } },
                thickness: 15,
                len: 0.6
            },
            line: { width: 0.5, color: 'rgba(0,0,0,0.2)' }
        },
        text: data.map(function(d) {
            return d.title + '<br>Year: ' + d.pub_year +
                   '<br>Citations: ' + d.num_citations +
                   '<br>Citation %ile: ' + d.num_citations_percentile.toFixed(1) + '%' +
                   '<br>Papers %ile: ' + d.num_papers_percentile.toFixed(1) + '%';
        }),
        hoverinfo: 'text'
    };

    var layout = Object.assign({}, PLOT_LAYOUT_DEFAULTS, {
        title: { text: 'Paper Percentile Scores vs #Papers Percentile for ' + authorName, font: { size: 15 } },
        xaxis: {
            title: 'Number of Papers Published Percentile',
            range: [0, 105],
            dtick: 10,
            gridcolor: COLORS.gray,
            gridwidth: 1,
            zeroline: false
        },
        yaxis: {
            title: 'Paper Percentile Score',
            range: [0, 105],
            dtick: 10,
            gridcolor: COLORS.gray,
            gridwidth: 1,
            zeroline: false
        },
        margin: { t: 60, r: 80, b: 60, l: 60 }
    });

    Plotly.newPlot(elementId, [trace], layout, PLOT_CONFIG);
}


/**
 * Temporal metric plot: dual-axis with metric value (left) and percentile (right).
 */
function renderTemporalPlot(elementId, data, metricKey, percentileKey, title, yLabel) {
    if (!data || data.length === 0) return;

    var years = data.map(function(d) { return d.state_year; });
    var values = data.map(function(d) { return d[metricKey]; });
    var percentiles = data.map(function(d) { return d[percentileKey]; });

    // Check that we have valid data
    var hasValues = values.some(function(v) { return v !== null && v !== undefined; });
    if (!hasValues) return;

    var traceValue = {
        x: years,
        y: values,
        type: 'scatter',
        mode: 'lines+markers',
        name: yLabel,
        marker: { color: COLORS.blue, size: 6 },
        line: { color: COLORS.blue, width: 2 },
        hovertemplate: '%{x}: %{y:,.0f}<extra>' + yLabel + '</extra>'
    };

    var tracePercentile = {
        x: years,
        y: percentiles,
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Percentile',
        yaxis: 'y2',
        marker: { color: COLORS.red, size: 6, symbol: 'x' },
        line: { color: COLORS.red, width: 2, dash: 'dash' },
        hovertemplate: '%{x}: %{y:.1f}%<extra>Percentile</extra>'
    };

    var layout = Object.assign({}, PLOT_LAYOUT_DEFAULTS, {
        title: { text: title, font: { size: 16 } },
        xaxis: {
            title: 'Year',
            gridcolor: COLORS.gray,
            gridwidth: 1,
            zeroline: false,
            dtick: Math.max(1, Math.round(years.length / 10))
        },
        yaxis: {
            title: { text: yLabel, font: { color: COLORS.blue } },
            tickfont: { color: COLORS.blue },
            gridcolor: COLORS.gray,
            gridwidth: 1,
            zeroline: false,
            rangemode: 'tozero'
        },
        yaxis2: {
            title: { text: 'Percentile', font: { color: COLORS.red } },
            tickfont: { color: COLORS.red },
            overlaying: 'y',
            side: 'right',
            range: [0, 100],
            zeroline: false
        },
        legend: {
            orientation: 'h',
            yanchor: 'top',
            y: -0.2,
            xanchor: 'center',
            x: 0.5
        },
        margin: { t: 50, r: 60, b: 80, l: 70 }
    });

    Plotly.newPlot(elementId, [traceValue, tracePercentile], layout, PLOT_CONFIG);
}


/**
 * Publication citation plot: yearly citations (bar) + percentile lines.
 */
function renderCitationPlot(elementId, data) {
    if (!data || data.length === 0) return;

    var years = data.map(function(d) { return d.citation_year; });
    var yearlyCitations = data.map(function(d) { return d.yearly_citations; });
    var percYearly = data.map(function(d) { return (d.perc_yearly_citations || 0) * 100; });
    var percCumulative = data.map(function(d) { return (d.perc_cumulative_citations || 0) * 100; });

    var traceBars = {
        x: years,
        y: yearlyCitations,
        type: 'bar',
        name: 'Yearly Citations',
        marker: { color: COLORS.blue, opacity: 0.7 },
        hovertemplate: '%{x}: %{y} citations<extra>Yearly</extra>'
    };

    var tracePercYearly = {
        x: years,
        y: percYearly,
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Yearly Citations Percentile',
        yaxis: 'y2',
        marker: { color: COLORS.orange, size: 6 },
        line: { color: COLORS.orange, width: 2 },
        hovertemplate: '%{x}: %{y:.1f}%<extra>Yearly %ile</extra>'
    };

    var tracePercCumul = {
        x: years,
        y: percCumulative,
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Cumulative Citations Percentile',
        yaxis: 'y2',
        marker: { color: COLORS.red, size: 6 },
        line: { color: COLORS.red, width: 2 },
        hovertemplate: '%{x}: %{y:.1f}%<extra>Cumulative %ile</extra>'
    };

    var layout = Object.assign({}, PLOT_LAYOUT_DEFAULTS, {
        title: { text: 'Citations Over Time', font: { size: 16 } },
        xaxis: {
            title: 'Citation Year',
            gridcolor: COLORS.gray,
            gridwidth: 1
        },
        yaxis: {
            title: { text: 'Yearly Citations', font: { color: COLORS.blue } },
            tickfont: { color: COLORS.blue },
            gridcolor: COLORS.gray,
            gridwidth: 1,
            rangemode: 'tozero'
        },
        yaxis2: {
            title: { text: '% Citations', font: { color: COLORS.red } },
            tickfont: { color: COLORS.red },
            overlaying: 'y',
            side: 'right',
            range: [0, 100],
            zeroline: false
        },
        legend: {
            orientation: 'h',
            yanchor: 'top',
            y: -0.2,
            xanchor: 'center',
            x: 0.5
        },
        barmode: 'group',
        margin: { t: 50, r: 60, b: 80, l: 70 }
    });

    Plotly.newPlot(elementId, [traceBars, tracePercYearly, tracePercCumul], layout, PLOT_CONFIG);
}
