"""Behavioral tests for frontend/static/js/paper_explorer.js via Node."""

import os
import shutil
import subprocess

import pytest


JS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "static", "js", "paper_explorer.js")
)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_paper_explorer_standing_tau_and_three_views():
    harness = r"""
const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

function makeNode(tag, id) {
    const attrs = {};
    const classes = new Set();
    const listeners = {};
    const node = {
        tagName: (tag || 'div').toUpperCase(),
        id: id || '',
        type: '',
        value: '',
        hidden: false,
        style: {},
        children: [],
        parentNode: null,
        clientWidth: 800,
        _text: '',
        get className() { return Array.from(classes).join(' '); },
        set className(v) {
            classes.clear();
            String(v || '').split(/\s+/).filter(Boolean).forEach(c => classes.add(c));
        },
        classList: {
            add(c) { classes.add(c); },
            remove(c) { classes.delete(c); },
            toggle(c, on) {
                if (on === undefined) {
                    if (classes.has(c)) { classes.delete(c); return false; }
                    classes.add(c); return true;
                }
                if (on) classes.add(c); else classes.delete(c);
                return Boolean(on);
            },
            contains(c) { return classes.has(c); }
        },
        get textContent() {
            if (node.children.length === 0) return node._text;
            return node._text + node.children.map(c => c.textContent).join('');
        },
        set textContent(v) {
            node.children = [];
            node._text = v === null || v === undefined ? '' : String(v);
        },
        appendChild(child) {
            child.parentNode = node;
            node.children.push(child);
            return child;
        },
        setAttribute(k, v) { attrs[k] = String(v); if (k === 'id') node.id = String(v); },
        getAttribute(k) { return Object.prototype.hasOwnProperty.call(attrs, k) ? attrs[k] : null; },
        hasAttribute(k) { return Object.prototype.hasOwnProperty.call(attrs, k); },
        addEventListener(ev, fn) { (listeners[ev] = listeners[ev] || []).push(fn); },
        trigger(ev, payload) { (listeners[ev] || []).forEach(fn => fn(payload || { preventDefault() {} })); },
        on() {},
        focus() {},
        scrollIntoView() {},
        contains(other) {
            let cur = other;
            while (cur) { if (cur === node) return true; cur = cur.parentNode; }
            return false;
        },
        querySelectorAll(sel) {
            const out = [];
            function walk(n) {
                for (const c of n.children) {
                    if (matches(c, sel)) out.push(c);
                    walk(c);
                }
            }
            walk(node);
            return out;
        },
        querySelector(sel) { return node.querySelectorAll(sel)[0] || null; }
    };
    return node;
}

function matches(node, sel) {
    if (sel.startsWith('.view-toggle ')) {
        let p = node.parentNode, inToggle = false;
        while (p) { if (p.classList && p.classList.contains('view-toggle')) inToggle = true; p = p.parentNode; }
        return inToggle && matches(node, sel.slice('.view-toggle '.length));
    }
    const m = sel.match(/^\[([a-zA-Z0-9_-]+)\]$/);
    if (m) return node.hasAttribute(m[1]);
    return false;
}

function buildDom(withStanding) {
    const root = makeNode('div', 'explore');
    const byId = { explore: root };
    const register = (n) => { if (n.id) byId[n.id] = n; root.appendChild(n); return n; };

    const toggle = register(makeNode('div', 'viewToggle'));
    toggle.className = 'view-toggle';
    const views = withStanding ? ['standing', 'year', 'pip'] : ['pip', 'year'];
    views.forEach((v, idx) => {
        const b = makeNode('button');
        b.className = 'btn btn-outline-violet' + (idx === 0 ? ' active' : '');
        b.setAttribute('data-explorer-view', v);
        b.setAttribute('aria-pressed', idx === 0 ? 'true' : 'false');
        toggle.appendChild(b);
    });

    register(makeNode('p', 'exploreFinding'));
    if (withStanding) {
        const tc = register(makeNode('div', 'tauControls'));
        const readout = makeNode('strong', 'tauReadout'); byId.tauReadout = readout; tc.appendChild(readout);
        const pctNote = makeNode('span', 'tauPctNote'); byId.tauPctNote = pctNote; tc.appendChild(pctNote);
        const slider = makeNode('input', 'tauSlider'); slider.value = '0.5'; byId.tauSlider = slider; tc.appendChild(slider);
        const scoreOut = makeNode('p', 'tauScoreReadout'); byId.tauScoreReadout = scoreOut; tc.appendChild(scoreOut);
        [0, 0.25, 0.5, 0.75, 0.9].forEach(val => {
            const chip = makeNode('button');
            chip.setAttribute('data-tau-preset', String(val));
            chip.setAttribute('aria-pressed', val === 0.5 ? 'true' : 'false');
            tc.appendChild(chip);
        });
    }
    register(makeNode('div', 'paperChart'));
    register(makeNode('aside', 'paperPanel'));
    register(makeNode('ol', 'paperList'));
    register(makeNode('button', 'paperListMore'));
    register(makeNode('p', 'paperListNote'));

    const doc = {
        activeElement: null,
        getElementById(id) { return byId[id] || null; },
        createElement(tag) { return makeNode(tag); },
        createTextNode(txt) {
            return { _text: String(txt), children: [], textContent: String(txt), hasAttribute() { return false; } };
        },
        querySelectorAll(sel) { return root.querySelectorAll(sel); }
    };
    const plotlyCalls = [];
    const win = {
        innerWidth: 1024,
        addEventListener() {},
        matchMedia() { return { matches: false }; },
        Plotly: {
            react(el, data, layout, config) {
                plotlyCalls.push({ data, layout, config });
            }
        }
    };
    win.window = win;
    win.document = doc;
    vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), win);
    return { win, doc, byId, plotlyCalls };
}

const curve = Array.from({ length: 40 }, (_, j) => Number((0.9 - j * 0.02).toFixed(4)));
const papers = [
    { author_pub_id: 'a:1', title: 'Top <script>Paper</script>', venue: 'QSS', pub_year: 2018,
      num_citations: 120, num_citations_percentile: 96.0, pip_class: 38, num_papers_percentile: 40.0, publication_rank: 1 },
    { author_pub_id: 'a:2', title: 'Mid Paper', venue: 'JASIST', pub_year: 2020,
      num_citations: 25, num_citations_percentile: 62.0, pip_class: 24, num_papers_percentile: 70.0, publication_rank: 2 },
    { author_pub_id: 'a:3', title: 'Early Note', venue: 'Workshop', pub_year: 2021,
      num_citations: 2, num_citations_percentile: 20.0, pip_class: 8, num_papers_percentile: 85.0, publication_rank: 3 }
];

// 1. Standing curve mode
{
    const { win, byId, plotlyCalls } = buildDom(true);
    const exp = win.initPaperExplorer(papers, {
        sectionId: 'explore', chartId: 'paperChart', panelId: 'paperPanel',
        listId: 'paperList', listMoreId: 'paperListMore', listNoteId: 'paperListNote',
        findingId: 'exploreFinding', tauControlsId: 'tauControls',
        authorId: 'abc123def456', authorName: 'Ada Lovelace',
        pipScore: 0.3100, pipAucScore: 0.5421, standingCurve: curve,
        startYear: 2015, dataSource: 'gs'
    });
    assert.strictEqual(exp.state.view, 'standing');
    assert.strictEqual(byId.tauControls.hidden, false);
    assert.strictEqual(byId.tauReadout.textContent, '0.50');
    assert(byId.tauScoreReadout.textContent.includes('PiP = 0.3100'));
    assert(byId.tauScoreReadout.textContent.includes('2 works above the 50th percentile counted'));

    // Escaped hover HTML (no raw <script>)
    const allHover = JSON.stringify(plotlyCalls[plotlyCalls.length - 1].data);
    assert(!allHover.includes('<script>'));
    assert(allHover.includes('&lt;script&gt;'));

    // Select mid paper (class 24 -> 60th pct) and shift tau above it (0.75 -> class 30)
    exp.select('a:2');
    assert(byId.paperPanel.textContent.includes('Counted in score'));
    exp.setTau(0.75);
    assert.strictEqual(byId.tauReadout.textContent, '0.75');
    assert(byId.paperPanel.textContent.includes('Below cutoff (not counted)'));
    assert(byId.tauScoreReadout.textContent.includes('Cohort & field rankings use τ = 0.50'));

    // Switch to year and pip views -> tauControls hides, 3rd tab shows PiP-AUC
    exp.setView('year');
    assert.strictEqual(byId.tauControls.hidden, true);
    exp.setView('pip');
    assert.strictEqual(byId.tauControls.hidden, true);
    assert(byId.exploreFinding.textContent.includes('PiP-AUC score, 0.5421'));

    // Back to standing -> tauControls visible again
    exp.setView('standing');
    assert.strictEqual(byId.tauControls.hidden, false);
}

// 2. Legacy / S2 fallback without standingCurve
{
    const { win } = buildDom(false);
    const exp = win.initPaperExplorer(papers, {
        sectionId: 'explore', chartId: 'paperChart', panelId: 'paperPanel',
        listId: 'paperList', listMoreId: 'paperListMore', listNoteId: 'paperListNote',
        findingId: 'exploreFinding', tauControlsId: 'tauControls',
        authorId: '2942126', authorName: 'Legacy Author',
        pipScore: 0.4500, pipAucScore: 0.4500, standingCurve: null,
        startYear: 2015, dataSource: 's2'
    });
    assert.strictEqual(exp.state.view, 'pip');
    exp.setView('standing');
    assert.strictEqual(exp.state.view, 'pip');
}
"""
    proc = subprocess.run(
        ["node", "-e", harness, JS_PATH],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
