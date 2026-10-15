/* Reusable controls for C4Diagram. Each [data-c4] host is independent. */
(function () {
  'use strict';
  const escape = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  let instance = 0;
  function mount(host, model, initialView) {
    C4Diagram.validate(model);
    const id = 'explorer_' + instance++;
    const viewId = initialView || model.initialView || Object.keys(model.views)[0];
    if (!model.views[viewId]) throw new Error('Unknown initial view: ' + viewId);
    const flows = model.flows || [];
    const tagNames = [...new Set(Object.values(model.elements).flatMap((entry) => entry.tags || []))];
    const palette = ['#8a6200', '#5b3f8c', '#b3261e', '#2f6b4f', '#1f5560'];
    const tagColor = (tag) => /^#[0-9a-f]{6}$/i.test(model.tags?.[tag]?.color || '') ? model.tags[tag].color : palette[tagNames.indexOf(tag) % palette.length];
    host.classList.add('explorer');
    host.innerHTML = `
      <div class="ex-top">
        <nav class="crumbs" aria-label="Diagram levels"></nav><span class="spacer"></span>
        <div class="seg" aria-label="Diagram vocabulary"><button data-vocab="c4" class="on" aria-pressed="true">C4 terms</button><button data-vocab="icepanel" aria-pressed="false">App / Store terms</button></div>
        <button class="btn sm" data-action="support" aria-pressed="false">Supporting systems</button>
        <button class="btn sm" data-action="aloud" aria-expanded="false" aria-controls="${id}_aloud">Read arrows</button>
        <button class="btn sm" data-action="dsl">Export DSL</button>
        <button class="btn sm" data-action="fullscreen">Full screen</button>
      </div>
      <div class="ex-tools">
        ${tagNames.length ? '<span class="lbl">Tags</span>' : ''}
        ${tagNames.map((tag) => `<button class="tagchip" data-tag="${escape(tag)}" style="--c:${tagColor(tag)}" aria-pressed="false"><i aria-hidden="true"></i>${escape(model.tags?.[tag]?.label || tag)}</button>`).join('')}
        <span class="spacer"></span>
        ${flows.length ? `<label class="lbl" for="${id}_flow">Flow</label><select class="sel" id="${id}_flow"><option value="">Choose a flow…</option>${flows.map((flow) => `<option value="${escape(flow.id)}">${escape(flow.name)} (${flow.steps.length})</option>`).join('')}</select><button class="btn sm primary" data-action="play">Play flow</button>` : ''}
      </div>
      <div class="ex-body">
        <div class="c4">
          <div class="overlay-title"><div class="lvl"></div><div class="ttl"></div><div class="date">${escape([model.status, model.date].filter(Boolean).join(' · '))}</div></div>
          <div class="zoombar"><button data-action="zoom-in" aria-label="Zoom in">+</button><button data-action="zoom-out" aria-label="Zoom out">−</button><button data-action="fit" aria-label="Fit diagram">⤢</button><button data-action="up" aria-label="Up a level">↑</button></div>
          <div class="hint">Click or Space to inspect · double-click or Enter to drill in · drag to pan · Ctrl/⌘ + scroll to zoom</div>
        </div>
        <aside class="inspector" aria-label="Element details" hidden></aside>
      </div>
      <div class="legend-strip" aria-label="Diagram key">
        <span class="ttl">Key</span><span><span class="sw person"></span>Person / Actor</span><span><span class="sw scope"></span>System in scope</span><span><span class="sw ext"></span>External system</span><span><span class="sw app"></span>Container / App</span><span><span class="sw store"></span>Data store</span><span><span class="sw comp"></span>Component</span><span><span class="ln"></span>Sync</span><span><span class="ln dash"></span>Async</span><span><span class="ln dot"></span>Rolled up</span><span>⤢ Zoomable</span><button class="btn sm" data-action="labels" aria-pressed="false">Show all labels</button>
      </div>
      <div class="flowbar" hidden><div class="ctrl"><button class="btn sm" data-action="prev" aria-label="Previous flow step">◀</button><button class="btn sm" data-action="auto">Auto</button><button class="btn sm" data-action="next" aria-label="Next flow step">▶</button><button class="btn sm" data-action="stop">Stop</button></div><div class="cap" aria-live="polite"><div class="fn"></div><div class="tx"></div><div class="dots"></div></div></div>
      <div class="aloud" id="${id}_aloud" hidden></div>
      <p class="ex-status" role="status" hidden></p>
      <dialog aria-label="Structurizr DSL export"><div class="hd"><b>Structurizr DSL</b><span class="spacer"></span><button class="btn sm" data-action="copy">Copy</button><button class="btn sm" data-action="download">Download</button><button class="btn sm" data-action="close-dsl">Close</button></div><pre tabindex="0"></pre><p class="small muted" style="padding:0 16px">Model and static views only. Manual positions and animated flows are not exported.</p></dialog>`;
    const $ = (selector) => host.querySelector(selector);
    const $$ = (selector) => [...host.querySelectorAll(selector)];
    const root = $('.c4'), inspector = $('.inspector'), flowbar = $('.flowbar'), select = $('.sel'), status = $('.ex-status');
    let busy = false, auto = null, diagram;
    const message = (text) => { status.textContent = text; status.hidden = !text; };
    async function run(action) {
      if (busy) return;
      busy = true; host.setAttribute('aria-busy', 'true');
      try { await action(); } catch (error) { message(error.message); }
      finally { busy = false; host.removeAttribute('aria-busy'); }
    }
    function stopAuto() { clearInterval(auto); auto = null; $('[data-action="auto"]').textContent = 'Auto'; }
    function stopFlow() {
      stopAuto();
      if (diagram.flow) diagram.stopFlow();
      flowbar.hidden = true;
      if (select) select.value = '';
    }
    function renderAloud() {
      $('.aloud').innerHTML = diagram.sentences().map((sentence) => `<div><b>${escape(sentence.from)}</b> <i>${escape(sentence.label)}</i> <b>${escape(sentence.to)}</b> <span class="tech">${escape(sentence.tech || '')}${sentence.implied ? ` (rolled up from ${sentence.n})` : ''}</span></div>`).join('');
    }
    function renderInspector(info) {
      inspector.hidden = !info;
      if (!info) { inspector.replaceChildren(); return; }
      const entry = info.e;
      const notes = entry.notes || {};
      inspector.innerHTML = `<button class="btn sm x" data-close>Close</button><div class="tl">${escape(info.typeLine)}</div><h3>${escape(entry.name)}</h3>
        ${entry.status ? `<span class="badge amber">${escape(entry.status)}</span>` : ''}
        <p class="desc">${escape(entry.desc)}</p>
        ${info.drill ? `<button class="btn primary sm" data-drill>Zoom into ${escape(entry.name)}</button>` : ''}
        ${entry.facts?.length ? `<div class="blk"><h4>Facts</h4><ul class="small">${entry.facts.map((fact) => `<li>${escape(fact)}</li>`).join('')}</ul></div>` : ''}
        ${Object.keys(notes).length ? `<div class="blk"><h4>Design notes</h4>${Object.entries(notes).map(([key, text]) => `<div class="note ${['why', 'tradeoff', 'improve'].includes(key) ? key : ''}"><b>${escape(key)}</b>${escape(text)}</div>`).join('')}</div>` : ''}
        ${info.rels.length ? `<div class="blk"><h4>Relationships</h4>${info.rels.map((relation) => `<div class="rel"><span class="dir">${relation.dir === 'out' ? '→' : '←'}</span>${escape(relation.label)} <b>${escape(relation.other.name)}</b> <code>${escape(relation.tech || '')}</code>${relation.count > 1 || relation.implied ? `<details><summary>${relation.count} underlying relationship(s)</summary>${relation.details.map((r) => `<p>${escape(model.elements[r.from].name)} → ${escape(model.elements[r.to].name)}: ${escape(r.label)}</p>`).join('')}</details>` : ''}</div>`).join('')}</div>` : ''}
        ${entry.files?.length ? `<div class="blk files"><h4>Source paths</h4>${entry.files.map((file) => `<code>${escape(file)}</code>`).join('')}</div>` : ''}`;
      inspector.querySelector('[data-close]').addEventListener('click', () => {
        diagram.clear(); root.querySelector(`[data-id="${info.id}"]`)?.focus();
      });
      inspector.querySelector('[data-drill]')?.addEventListener('click', () => diagram.drill(info.id));
    }
    function renderView(currentId, view) {
      const title = diagram.vocab === 'icepanel' ? view.ipTitle || view.title : view.title;
      $('.overlay-title .lvl').textContent = 'Level ' + view.level;
      $('.overlay-title .ttl').textContent = title + ': ' + model.elements[view.scope].name;
      diagram.svg.setAttribute('aria-label', title + ': ' + model.elements[view.scope].name);
      const chain = [];
      for (let current = currentId; current; current = model.views[current].parent) chain.unshift(current);
      $('.crumbs').innerHTML = chain.map((key, index) => `${index ? '<span class="sep">›</span>' : ''}<button data-view="${key}" class="${key === currentId ? 'cur' : ''}" ${key === currentId ? 'aria-current="page"' : ''}>C${model.views[key].level} ${escape(model.views[key].crumb || model.elements[model.views[key].scope].name)}</button>`).join('');
      $$('[data-view]').forEach((button) => button.addEventListener('click', () => run(async () => { stopFlow(); await diagram.show(button.dataset.view); })));
      $('[data-action="up"]').disabled = !view.parent;
      if (!$('.aloud').hidden) renderAloud();
    }
    function renderStep(flow, index) {
      flowbar.hidden = false;
      $('.flowbar .fn').textContent = `${flow.name} · step ${index + 1} of ${flow.steps.length}`;
      $('.flowbar .tx').textContent = flow.steps[index].text;
      $('.flowbar .dots').innerHTML = flow.steps.map((step, i) => `<button data-step="${i}" class="${i === index ? 'on' : ''}" aria-label="Flow step ${i + 1}" ${i === index ? 'aria-current="step"' : ''}>${i + 1}</button>`).join('');
      $$('[data-step]').forEach((button) => button.addEventListener('click', () => run(async () => { stopAuto(); await diagram.step(+button.dataset.step); })));
      $('[data-action="prev"]').disabled = index === 0;
      $('[data-action="next"]').disabled = index === flow.steps.length - 1;
    }
    diagram = new C4Diagram(root, model, {
      onSelect: renderInspector, onView: renderView, onStep: renderStep,
      navigate: (target, how) => run(async () => { stopFlow(); await diagram.show(target, how); }),
    });
    const pressed = (button, on) => { button.classList.toggle('on', on); button.setAttribute('aria-pressed', String(on)); };
    const resetTags = () => $$('[data-tag]').forEach((button) => pressed(button, false));
    function playFlow() {
      if (!flows.length) return;
      stopAuto(); resetTags();
      if (!select.value) select.value = flows[0].id;
      return diagram.startFlow(select.value);
    }
    const actions = {
      'zoom-in': () => diagram.zoomBy(.8), 'zoom-out': () => diagram.zoomBy(1.25), fit: () => diagram.fit(true),
      up: async () => { stopFlow(); await diagram.up(); },
      support: async () => { stopFlow(); const on = !diagram.showSupporting; pressed($('[data-action="support"]'), on); await diagram.setSupporting(on); },
      labels: () => { const on = !diagram.allLabels; diagram.setAllLabels(on); const button = $('[data-action="labels"]'); pressed(button, on); button.textContent = on ? 'Labels on selection' : 'Show all labels'; },
      aloud: () => { const panel = $('.aloud'); panel.hidden = !panel.hidden; $('[data-action="aloud"]').setAttribute('aria-expanded', String(!panel.hidden)); if (!panel.hidden) renderAloud(); },
      play: playFlow,
      prev: async () => { stopAuto(); await diagram.step(diagram.stepIdx - 1); },
      next: async () => { stopAuto(); await diagram.step(diagram.stepIdx + 1); },
      stop: () => { stopFlow(); return diagram.fit(true); },
      auto: () => {
        if (auto) { stopAuto(); return; }
        if (diagram.reduced) { message('Automatic playback is disabled for reduced motion. Use the step buttons.'); return; }
        $('[data-action="auto"]').textContent = 'Pause';
        auto = setInterval(() => {
          if (!diagram.flow || diagram.reduced || diagram.stepIdx >= diagram.flow.steps.length - 1) { stopAuto(); return; }
          run(() => diagram.step(diagram.stepIdx + 1));
        }, 4200);
      },
      dsl: () => { $('dialog pre').textContent = diagram.toDSL(); $('dialog').showModal(); },
      'close-dsl': () => $('dialog').close(),
      copy: async () => {
        try {
          if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable');
          await navigator.clipboard.writeText($('dialog pre').textContent);
          $('[data-action="copy"]').textContent = 'Copied';
        } catch {
          const range = document.createRange(); range.selectNodeContents($('dialog pre'));
          const selection = window.getSelection(); selection.removeAllRanges(); selection.addRange(range);
          $('[data-action="copy"]').textContent = 'Selected: Ctrl/Cmd+C';
        }
      },
      download: () => {
        const url = URL.createObjectURL(new Blob([diagram.toDSL()], { type: 'text/plain;charset=utf-8' }));
        const link = document.createElement('a'); link.href = url; link.download = 'architecture.dsl'; link.click();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      },
      fullscreen: async () => {
        if (document.fullscreenElement) await document.exitFullscreen(); else await host.requestFullscreen();
        diagram.fit(false);
      },
    };
    $$('[data-action]').forEach((button) => button.addEventListener('click', () => run(actions[button.dataset.action])));
    $$('[data-vocab]').forEach((button) => button.addEventListener('click', () => run(() => {
      stopFlow(); diagram.setVocab(button.dataset.vocab);
      $$('[data-vocab]').forEach((item) => pressed(item, item === button));
    })));
    $$('[data-tag]').forEach((button) => button.addEventListener('click', () => run(() => {
      stopFlow(); diagram.clear(); const tag = diagram.tag === button.dataset.tag ? null : button.dataset.tag;
      resetTags(); if (tag) pressed(button, true);
      root.style.setProperty('--tagc', tag ? tagColor(tag) : 'var(--ink)'); diagram.setTag(tag);
    })));
    select?.addEventListener('change', () => run(() => select.value ? playFlow() : stopFlow()));
    host.addEventListener('keydown', (event) => { if (event.key === 'Escape' && !$('dialog').open) diagram.clear(); });
    $('[data-action="support"]').hidden = !Object.values(model.elements).some((entry) => entry.supporting);
    $('[data-action="fullscreen"]').hidden = !document.fullscreenEnabled || !host.requestFullscreen;
    if (!tagNames.length && !flows.length) $('.ex-tools').hidden = true;
    host.addEventListener('fullscreenchange', () => diagram.fit(false));
    run(() => diagram.show(viewId));
    document.fonts?.ready.then(() => run(async () => {
      if (diagram.flow) await diagram.step(diagram.stepIdx); else { const box = { ...diagram.vb }; diagram.render(); diagram.setVB(box); }
    }));
    host.c4Diagram = diagram;
    return diagram;
  }
  window.PaperDocC4 = { mount };
  document.querySelectorAll('[data-c4]').forEach((host) => {
    try {
      const model = window.PAPER_DOC_MODELS?.[host.dataset.c4];
      if (!model) throw new Error('No model registered for ' + host.dataset.c4);
      mount(host, model, host.dataset.view);
    } catch (error) {
      const warning = document.createElement('p'); warning.className = 'card warn'; warning.setAttribute('role', 'alert');
      warning.textContent = error.message; host.replaceChildren(warning);
      console.error(error);
    }
  });
})();
