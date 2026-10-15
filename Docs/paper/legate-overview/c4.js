/* Model-driven C4 viewer extracted from the paper-themed interview brief.
   Native SVG, one model, many views; hidden descendants roll up to ancestors. */
(function () {
  'use strict';
  const NS = 'http://www.w3.org/2000/svg';
  const SIZE = {
    person: [200, 150], system: [290, 150], external: [215, 124], sm: [178, 92],
    app: [210, 124], store: [210, 124], component: [205, 116],
  };
  let uid = 0;
  function el(tag, attrs, parent) {
    const node = document.createElementNS(NS, tag);
    if (attrs) for (const key in attrs) if (attrs[key] != null) node.setAttribute(key, attrs[key]);
    if (parent) parent.appendChild(node);
    return node;
  }
  function wrap(text, max) {
    const lines = [];
    let current = '';
    for (const word of String(text || '').split(/\s+/)) {
      if (current && (current + ' ' + word).length > max) { lines.push(current); current = word; }
      else current = current ? current + ' ' + word : word;
    }
    if (current) lines.push(current);
    return lines;
  }
  const ease = (t) => t < .5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  function validate(model) {
    const fail = (message) => { throw new Error('C4 model: ' + message); };
    if (!model?.elements || !model?.views || !Array.isArray(model.rels)) fail('elements, views, and rels are required');
    const elements = model.elements, views = model.views;
    const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;
    for (const [id, entry] of Object.entries(elements)) {
      if (!identifier.test(id)) fail('invalid element identifier ' + id);
      if (!['person', 'system', 'app', 'store', 'component'].includes(entry.kind) || !entry.name) fail('invalid kind/name on ' + id);
      const seen = new Set([id]);
      for (let parent = entry.parent; parent; parent = elements[parent].parent) {
        if (!elements[parent]) fail('missing parent ' + parent);
        if (seen.has(parent)) fail('parent cycle at ' + id);
        seen.add(parent);
      }
      const expected = { app: ['system'], store: ['system'], component: ['app', 'store'] }[entry.kind];
      if (expected && !expected.includes(elements[entry.parent]?.kind)) fail('invalid hierarchy at ' + id);
      if (!expected && entry.parent) fail('people and systems must be top-level: ' + id);
    }
    if (!Object.keys(views).length) fail('at least one view is required');
    for (const [id, view] of Object.entries(views)) {
      if (!identifier.test(id)) fail('invalid view identifier ' + id);
      if (![1, 2, 3].includes(view.level) || !elements[view.scope]) fail('invalid level/scope in ' + id);
      const scopeKind = elements[view.scope].kind;
      if ((view.level < 3 && scopeKind !== 'system') || (view.level === 3 && !['app', 'store'].includes(scopeKind))) fail('scope does not match C4 level in ' + id);
      if (!view.layout || !Object.keys(view.layout).length) fail('empty layout in ' + id);
      for (const [element, position] of Object.entries(view.layout)) {
        if (!elements[element] || !Array.isArray(position) || !Number.isFinite(position[0]) || !Number.isFinite(position[1])) fail('invalid layout entry ' + element);
        if (position[2] && !SIZE[position[2]]) fail('unknown box size ' + position[2]);
      }
      if (!Object.keys(view.layout).some((key) => !elements[key].supporting)) fail('view contains only hidden supporting elements: ' + id);
      const seen = new Set([id]);
      for (let parent = view.parent; parent; parent = views[parent].parent) {
        if (!views[parent]) fail('missing parent view ' + parent);
        if (seen.has(parent)) fail('view cycle at ' + id);
        seen.add(parent);
      }
      for (const [element, target] of Object.entries(view.drill || {})) {
        if (!view.layout[element] || !views[target]) fail('invalid drill target in ' + id);
      }
    }
    for (const relation of model.rels) {
      if (!elements[relation.from] || !elements[relation.to] || !relation.label) fail('relationship needs valid endpoints and a label');
      if (relation.only?.some((id) => !views[id])) fail('relationship has unknown view restriction');
    }
    const flowIds = new Set();
    for (const flow of model.flows || []) {
      if (!identifier.test(flow.id) || flowIds.has(flow.id) || !flow.steps?.length) fail('flow needs unique id and steps');
      flowIds.add(flow.id);
      for (const step of flow.steps) {
        const view = views[step.view || flow.view];
        if (!view || !elements[step.from] || !elements[step.to] || !step.text) fail('invalid flow step in ' + flow.id);
        const visibleAncestor = (id) => { while (id && !view.layout[id]) id = elements[id].parent; return id; };
        if (!visibleAncestor(step.from) || !visibleAncestor(step.to)) fail('flow endpoints are not represented in its view: ' + flow.id);
      }
    }
    return model;
  }

  class C4Diagram {
    constructor(root, model, options) {
      validate(model);
      this.root = root;
      this.m = model;
      this.o = Object.assign({ vocab: 'c4', onSelect() {}, onView() {}, onStep() {} }, options);
      this.vocab = this.o.vocab;
      this.showSupporting = false;
      this.forced = new Set();
      this.tag = null;
      this.selected = null;
      this.motion = matchMedia('(prefers-reduced-motion: reduce)');
      this.id = 'c4_' + uid++;
      this.svg = el('svg', { class: 'c4-svg', preserveAspectRatio: 'xMidYMid meet', role: 'group', 'aria-label': model.name + ' architecture diagram' }, root);
      const defs = el('defs', null, this.svg);
      for (const [id, cls] of [['arr', 'arrow'], ['arrH', 'arrow hl'], ['arrF', 'arrow flow']]) {
        const marker = el('marker', { id: this.id + id, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' }, defs);
        el('path', { d: 'M0,0 L10,5 L0,10 z', class: cls }, marker);
      }
      this.gBound = el('g', { class: 'layer bounds' }, this.svg);
      this.gEdges = el('g', { class: 'layer edges' }, this.svg);
      this.gNodes = el('g', { class: 'layer nodes' }, this.svg);
      this.gLabels = el('g', { class: 'layer labels' }, this.svg);
      this.gFx = el('g', { class: 'layer fx' }, this.svg);
      this.vb = { x: 0, y: 0, w: 1200, h: 800 };
      this.bindPanZoom();
    }
    get reduced() { return this.motion.matches; }
    E(id) { return this.m.elements[id]; }
    view() { return this.m.views[this.viewId]; }
    typeName(entry) {
      const ip = this.vocab === 'icepanel';
      return { person: ip ? 'Actor' : 'Person', system: ip ? 'System' : 'Software System', app: ip ? 'App' : 'Container', store: ip ? 'Store' : 'Container: Data store', component: 'Component' }[entry.kind];
    }
    typeLine(entry) {
      const type = this.typeName(entry);
      return this.vocab === 'icepanel' ? [type + (entry.external ? ' · external' : ''), entry.tech].filter(Boolean).join(' · ') : '[' + type + (entry.tech ? ': ' + entry.tech : '') + ']';
    }
    within(id, scope) { for (let current = id; current; current = this.E(current)?.parent) if (current === scope) return true; return false; }
    visibleSet() { return new Set(Object.keys(this.view().layout).filter((id) => !this.E(id).supporting || this.showSupporting || this.forced.has(id))); }
    visAnc(id, visible) { for (let current = id; current; current = this.E(current)?.parent) if (visible.has(current)) return current; return null; }
    boxFor(id) {
      const layout = this.view().layout[id], entry = this.E(id);
      const key = layout[2] || (entry.kind === 'system' ? entry.external ? 'external' : 'system' : entry.kind);
      const [w, h] = SIZE[key];
      return { x: layout[0], y: layout[1], w, h, sm: key === 'sm' };
    }
    deriveEdges(visible) {
      const view = this.view(), groups = new Map();
      for (const relation of this.m.rels) {
        if (relation.only && !relation.only.includes(this.viewId)) continue;
        const a = this.visAnc(relation.from, visible), b = this.visAnc(relation.to, visible);
        if (!a || !b || a === b) continue;
        if (view.level >= 2 && !this.within(a, view.scope) && !this.within(b, view.scope)) continue;
        const key = a + '>' + b;
        if (!groups.has(key)) groups.set(key, { key, a, b, rels: [], explicit: [] });
        const group = groups.get(key);
        group.rels.push(relation);
        if (relation.from === a && relation.to === b) group.explicit.push(relation);
      }
      for (const group of groups.values()) {
        const main = group.explicit[0] || group.rels[0];
        group.implied = !group.explicit.length;
        group.label = main.label; group.tech = main.tech;
        group.async = (group.explicit.length ? group.explicit : group.rels).every((relation) => relation.async);
      }
      return groups;
    }
    union(boxes) {
      if (!boxes.length) return { x: 0, y: 0, w: 800, h: 500 };
      const x = Math.min(...boxes.map((box) => box.x)), y = Math.min(...boxes.map((box) => box.y));
      return { x, y, w: Math.max(...boxes.map((box) => box.x + box.w)) - x, h: Math.max(...boxes.map((box) => box.y + box.h)) - y };
    }
    render() {
      const view = this.view();
      [this.gBound, this.gEdges, this.gNodes, this.gLabels, this.gFx].forEach((group) => group.replaceChildren());
      this.vis = this.visibleSet(); this.boxes = {};
      for (const id of this.vis) this.boxes[id] = this.boxFor(id);
      const inner = [...this.vis].filter((id) => id !== view.scope && this.within(id, view.scope));
      this.boundBox = null;
      if (inner.length) {
        const bounds = this.union(inner.map((id) => this.boxes[id]));
        const box = this.boundBox = { x: bounds.x - 26, y: bounds.y - 48, w: bounds.w + 52, h: bounds.h + 74 };
        el('rect', { x: box.x, y: box.y, width: box.w, height: box.h, rx: 18, class: 'boundary' }, this.gBound);
        const scope = this.E(view.scope);
        const title = el('text', { x: box.x + 18, y: box.y + 26, class: 'boundary-label' }, this.gBound);
        title.textContent = scope.name + '  ';
        el('tspan', { class: 'boundary-type' }, title).textContent = '[' + this.typeName(scope) + ']';
      }
      this.edges = this.deriveEdges(this.vis);
      if (view.hideImplied) for (const [key, group] of this.edges) if (group.implied) this.edges.delete(key);
      this.root.classList.toggle('labels-select', !this.allLabels && view.labels === 'select');
      this.edgeEls = {};
      for (const group of this.edges.values()) this.renderEdge(group);
      for (const id of this.vis) this.renderNode(id);
      this.applyHighlight();
      this.o.onView(this.viewId, view);
    }
    renderNode(id) {
      const entry = this.E(id), box = this.boxes[id], view = this.view();
      const classes = ['node', 'k-' + entry.kind, entry.external ? 'ext' : '', id === view.scope ? 'scope' : ''];
      const canDrill = !!view.drill?.[id];
      const group = el('g', { class: classes.join(' '), 'data-id': id, transform: `translate(${box.x},${box.y})`, tabindex: 0, role: 'button', 'aria-label': entry.name + ', ' + this.typeLine(entry) + (canDrill ? '. Enter to drill in; Space to inspect.' : '. Enter to inspect.') }, this.gNodes);
      const { w, h } = box;
      let top = 26;
      if (entry.kind === 'person') {
        el('circle', { cx: w / 2, cy: 24, r: 22, class: 'shape' }, group);
        el('rect', { x: 0, y: 40, width: w, height: h - 40, rx: 26, class: 'shape' }, group); top = 66;
      } else if (entry.kind === 'store') {
        el('path', { d: `M0,12 A${w / 2},12 0 0,0 ${w},12 V${h - 12} A${w / 2},12 0 0,1 0,${h - 12} Z`, class: 'shape' }, group);
        el('ellipse', { cx: w / 2, cy: 12, rx: w / 2, ry: 12, class: 'shape cap' }, group); top = 42;
      } else el('rect', { width: w, height: h, rx: 12, class: 'shape' }, group);
      el('text', { x: w / 2, y: top, class: 'n-name' }, group).textContent = entry.name;
      let y = top + 16;
      wrap(this.typeLine(entry), Math.floor((w - 16) / 5.9)).slice(0, 2).forEach((line) => {
        el('text', { x: w / 2, y, class: 'n-type' }, group).textContent = line; y += 13;
      });
      if (!box.sm) {
        y += 5;
        const maxLines = Math.max(0, Math.floor((h - y - 6) / 14));
        let lines = wrap(entry.desc, Math.floor((w - 20) / 6.15));
        if (lines.length > maxLines) {
          lines = lines.slice(0, maxLines);
          if (lines.length) lines[lines.length - 1] = lines.at(-1).replace(/[\s,.;:]*\S*$/, '') + '…';
        }
        lines.forEach((line) => { el('text', { x: w / 2, y, class: 'n-desc' }, group).textContent = line; y += 14; });
      }
      if (entry.status) {
        const label = entry.status.toUpperCase(), width = label.length * 6.2 + 14;
        const status = el('g', { class: 'status', transform: `translate(${w - width - 10},-8)` }, group);
        el('rect', { width, height: 16, rx: 8 }, status);
        el('text', { x: width / 2, y: 11.5 }, status).textContent = label;
      }
      if (canDrill) {
        const drill = el('g', { class: 'drill', transform: `translate(${w - 22},${h - 22})` }, group);
        el('circle', { r: 13 }, drill);
        el('path', { d: 'M-5,-1 L-5,-5 L-1,-5 M5,1 L5,5 L1,5 M-5,-5 L-1,-1 M5,5 L1,1', class: 'drill-ico' }, drill);
      }
      el('title', null, group).textContent = entry.name + ': ' + this.typeLine(entry) + '\n' + (entry.desc || '');
      group.addEventListener('click', (event) => { event.stopPropagation(); if (!this.dragMoved) this.select(id); });
      group.addEventListener('dblclick', (event) => { event.stopPropagation(); if (canDrill) this.drill(id); });
      group.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          if (event.key === 'Enter' && canDrill) this.drill(id); else this.select(id);
        }
      });
    }
    anchor(box, point) {
      const cx = box.x + box.w / 2, cy = box.y + box.h / 2, dx = point.x - cx, dy = point.y - cy;
      if (!dx && !dy) return { x: cx, y: cy };
      const scale = Math.min((box.w / 2 + 3) / Math.abs(dx || 1e-9), (box.h / 2 + 3) / Math.abs(dy || 1e-9));
      return { x: cx + dx * scale, y: cy + dy * scale };
    }
    geom(a, b, offset) {
      const A = this.boxes[a], B = this.boxes[b];
      const ca = { x: A.x + A.w / 2, y: A.y + A.h / 2 }, cb = { x: B.x + B.w / 2, y: B.y + B.h / 2 };
      const dx = cb.x - ca.x, dy = cb.y - ca.y, length = Math.hypot(dx, dy) || 1;
      const c = { x: (ca.x + cb.x) / 2 - dy / length * offset, y: (ca.y + cb.y) / 2 + dx / length * offset };
      const p1 = this.anchor(A, offset ? c : cb), p2 = this.anchor(B, offset ? c : ca);
      return { p1, p2, c, d: `M${p1.x},${p1.y} Q${c.x},${c.y} ${p2.x},${p2.y}` };
    }
    at(geometry, t) {
      const u = 1 - t;
      return { x: u * u * geometry.p1.x + 2 * u * t * geometry.c.x + t * t * geometry.p2.x, y: u * u * geometry.p1.y + 2 * u * t * geometry.c.y + t * t * geometry.p2.y };
    }
    renderEdge(group) {
      const view = this.view();
      const offset = view.curves?.[group.key] ?? (this.edges.has(group.b + '>' + group.a) ? 30 : 0);
      const geometry = this.geom(group.a, group.b, offset);
      const classes = ['edge', group.implied ? 'implied' : '', group.async ? 'async' : ''].join(' ');
      const path = el('path', { d: geometry.d, class: classes, 'marker-end': `url(#${this.id}arr)`, 'data-key': group.key }, this.gEdges);
      const hit = el('path', { d: geometry.d, class: 'edge-hit' }, this.gEdges);
      hit.addEventListener('click', (event) => { event.stopPropagation(); this.select(group.a, group.key); });
      for (const [event, on] of [['mouseenter', true], ['mouseleave', false]]) hit.addEventListener(event, () => {
        this.edgeEls[group.key]?.label.classList.toggle('hov', on); path.classList.toggle('hov', on);
      });
      const point = this.at(geometry, view.labelT?.[group.key] ?? .5);
      const label = el('g', { class: 'elabel' + (group.implied ? ' implied' : ''), 'data-key': group.key }, this.gLabels);
      const lines = wrap(group.label, 26);
      const total = lines.length + (group.tech ? 1 : 0);
      let y = point.y - total * 12.5 / 2 + 10;
      lines.forEach((line) => { el('text', { x: point.x, y, class: 'el-text' }, label).textContent = line; y += 12.5; });
      if (group.tech) { el('text', { x: point.x, y, class: 'el-tech' }, label).textContent = '[' + group.tech + ']'; y += 13; }
      if (group.rels.length > 1) el('text', { x: point.x, y, class: 'el-tech' }, label).textContent = '+' + (group.rels.length - 1) + (group.implied ? ' lower' : ' related');
      let bounds;
      try { bounds = label.getBBox(); } catch { bounds = null; }
      if (!bounds?.width) bounds = { x: point.x - 80, y: point.y - 16, width: 160, height: total * 13 };
      const background = el('rect', { x: bounds.x - 6, y: bounds.y - 3, width: bounds.width + 12, height: bounds.height + 6, rx: 6, class: 'el-bg' });
      label.prepend(background);
      label.addEventListener('click', (event) => { event.stopPropagation(); this.select(group.a, group.key); });
      this.edgeEls[group.key] = { path, label, geometry, group };
    }
    select(id, edgeKey) { this.selected = id; this.selEdge = edgeKey || null; this.applyHighlight(); this.o.onSelect(id ? this.info(id) : null); }
    clear() { this.select(null); }
    info(id) {
      const rels = [];
      for (const group of this.edges.values()) {
        const add = (dir, otherId) => rels.push({ dir, other: this.E(otherId), otherId, label: group.label, tech: group.tech, implied: group.implied, count: group.rels.length, details: group.rels });
        if (group.a === id) add('out', group.b);
        if (group.b === id) add('in', group.a);
      }
      return { id, e: this.E(id), typeLine: this.typeLine(this.E(id)), rels, drill: this.view().drill?.[id], viewId: this.viewId };
    }
    hasTag(id, tag) {
      if (this.E(id).tags?.includes(tag)) return true;
      return Object.keys(this.m.elements).some((key) => key !== id && this.within(key, id) && !this.vis.has(key) && this.E(key).tags?.includes(tag));
    }
    applyHighlight() {
      const near = new Set([this.selected]);
      for (const group of this.edges.values()) {
        if (group.a === this.selected) near.add(group.b);
        if (group.b === this.selected) near.add(group.a);
      }
      this.gNodes.querySelectorAll('.node').forEach((node) => {
        const id = node.dataset.id;
        const dim = this.tag ? !this.hasTag(id, this.tag) : !!this.selected && !near.has(id);
        node.classList.toggle('dim', dim); node.classList.toggle('sel', id === this.selected);
        node.classList.toggle('tagged', !!this.tag && !dim);
      });
      for (const [key, entry] of Object.entries(this.edgeEls)) {
        const { path, label, group } = entry;
        let on = !!this.selected && (group.a === this.selected || group.b === this.selected), dim = !!this.selected && !on;
        if (this.tag) { dim = !(this.hasTag(group.a, this.tag) && this.hasTag(group.b, this.tag)); on = !dim && !!this.selected; }
        if (this.selEdge) { on = key === this.selEdge; dim = !on; }
        [path, label].forEach((node) => { node.classList.toggle('hl', on); node.classList.toggle('dim', dim); });
        path.setAttribute('marker-end', `url(#${this.id}${on ? 'arrH' : 'arr'})`);
      }
    }
    setTag(tag) { this.tag = tag; this.applyHighlight(); }
    setAllLabels(on) { this.allLabels = on; this.root.classList.toggle('labels-select', !on && this.view().labels === 'select'); }
    setVocab(vocab) { this.vocab = vocab; const box = { ...this.vb }; this.render(); this.setVB(box); if (this.selected) this.o.onSelect(this.info(this.selected)); }
    setSupporting(on) { this.showSupporting = on; this.clear(); this.render(); return this.fit(true); }
    setVB(box) { this.vb = { ...box }; this.svg.setAttribute('viewBox', `${box.x} ${box.y} ${box.w} ${box.h}`); }
    fitBox() {
      const boxes = Object.values(this.boxes);
      if (this.boundBox) boxes.push(this.boundBox);
      for (const entry of Object.values(this.edgeEls)) {
        const box = entry.label.getBBox();
        boxes.push({ x: box.x, y: box.y, w: box.width, h: box.height });
      }
      const union = this.union(boxes), top = this.o.topPad ?? 90;
      return { x: union.x - 40, y: union.y - 40 - top, w: union.w + 80, h: union.h + 80 + top };
    }
    animateVB(to, ms = 380) {
      const token = this.zoomToken = (this.zoomToken || 0) + 1;
      if (this.reduced) { this.setVB(to); return Promise.resolve(); }
      const from = { ...this.vb };
      return new Promise((resolve) => {
        const start = performance.now();
        const step = (now) => {
          if (token !== this.zoomToken) return resolve();
          const t = this.reduced ? 1 : Math.min(1, (now - start) / ms), k = ease(t);
          this.setVB({ x: from.x + (to.x - from.x) * k, y: from.y + (to.y - from.y) * k, w: from.w + (to.w - from.w) * k, h: from.h + (to.h - from.h) * k });
          if (t < 1) requestAnimationFrame(step); else resolve();
        };
        requestAnimationFrame(step);
      });
    }
    fit(animate) { return animate ? this.animateVB(this.fitBox()) : this.setVB(this.fitBox()); }
    zoomBy(factor) {
      const box = this.vb, width = Math.max(200, Math.min(6000, box.w * factor)), height = box.h * width / box.w;
      return this.animateVB({ x: box.x + (box.w - width) / 2, y: box.y + (box.h - height) / 2, w: width, h: height }, 220);
    }
    pad(box, padding) { return { x: box.x - padding, y: box.y - padding, w: box.w + padding * 2, h: box.h + padding * 2 }; }
    async show(viewId, how = {}) {
      if (!this.m.views[viewId]) throw new Error('Unknown C4 view: ' + viewId);
      const token = this.showToken = (this.showToken || 0) + 1;
      this.clearFx(); this.selected = null; this.selEdge = null; this.o.onSelect(null);
      if (how.zoomInto && this.boxes?.[how.zoomInto] && !this.reduced) {
        this.gNodes.querySelector(`[data-id="${how.zoomInto}"]`)?.classList.add('focus');
        this.root.classList.add('zooming');
        await this.animateVB(this.pad(this.boxes[how.zoomInto], 20), 460);
      } else if (this.viewId && this.viewId !== viewId && !this.reduced) {
        this.root.classList.add('fading'); await sleep(180);
      }
      if (token !== this.showToken) return;
      this.viewId = viewId; this.render();
      const fit = this.fitBox();
      this.root.classList.remove('zooming', 'fading');
      if (how.zoomInto && !this.reduced) {
        this.setVB({ x: fit.x + fit.w * .325, y: fit.y + fit.h * .325, w: fit.w * .35, h: fit.h * .35 });
        await this.animateVB(fit, 560);
      } else if (how.zoomOutFrom && this.boxes[how.zoomOutFrom] && !this.reduced) {
        this.setVB(this.pad(this.boxes[how.zoomOutFrom], 20)); await this.animateVB(fit, 560);
      } else this.setVB(fit);
    }
    drill(id) { const target = this.view().drill?.[id]; if (target) return this.o.navigate ? this.o.navigate(target, { zoomInto: id }) : this.show(target, { zoomInto: id }); }
    up() { const view = this.view(); if (view.parent) return this.show(view.parent, { zoomOutFrom: view.scope }); }
    bindPanZoom() {
      const svg = this.svg;
      let start;
      svg.addEventListener('pointerdown', (event) => {
        if (event.button !== 0) return;
        start = { x: event.clientX, y: event.clientY, vb: { ...this.vb }, pointer: event.pointerId };
        this.dragMoved = false;
      });
      svg.addEventListener('pointermove', (event) => {
        if (!start || event.pointerId !== start.pointer) return;
        const dx = event.clientX - start.x, dy = event.clientY - start.y;
        if (!this.dragMoved && Math.hypot(dx, dy) < 4) return;
        if (!this.dragMoved) { svg.setPointerCapture(event.pointerId); this.zoomToken = (this.zoomToken || 0) + 1; }
        this.dragMoved = true; svg.classList.add('panning');
        const matrix = svg.getScreenCTM();
        if (matrix) this.setVB({ ...start.vb, x: start.vb.x - dx / matrix.a, y: start.vb.y - dy / matrix.d });
      });
      const end = () => { start = null; svg.classList.remove('panning'); setTimeout(() => { this.dragMoved = false; }, 0); };
      svg.addEventListener('pointerup', end); svg.addEventListener('pointercancel', end);
      svg.addEventListener('pointerleave', () => { if (!this.dragMoved) start = null; });
      svg.addEventListener('click', (event) => { if (!this.dragMoved && (event.target === svg || event.target.closest('.bounds'))) this.clear(); });
      svg.addEventListener('wheel', (event) => {
        if (!(event.ctrlKey || event.metaKey)) return;
        event.preventDefault(); this.zoomToken = (this.zoomToken || 0) + 1;
        const matrix = svg.getScreenCTM(); if (!matrix) return;
        const point = svg.createSVGPoint(); point.x = event.clientX; point.y = event.clientY;
        const p = point.matrixTransform(matrix.inverse()), box = this.vb;
        const width = Math.max(200, Math.min(6000, box.w * Math.exp(event.deltaY * .01))), ratio = width / box.w;
        this.setVB({ x: p.x - (p.x - box.x) * ratio, y: p.y - (p.y - box.y) * ratio, w: width, h: box.h * ratio });
      }, { passive: false });
    }
    async startFlow(flowId) {
      const flow = (this.m.flows || []).find((entry) => entry.id === flowId);
      if (!flow) return;
      this.flow = flow; this.forced = new Set(); this.tag = null; this.selected = null; this.selEdge = null;
      for (const step of flow.steps) for (const id of [step.from, step.to]) {
        for (let parent = id; parent; parent = this.E(parent).parent) if (this.E(parent).supporting) this.forced.add(parent);
      }
      return this.step(0);
    }
    async step(index) {
      if (!this.flow) return;
      const flow = this.flow;
      index = Math.max(0, Math.min(flow.steps.length - 1, index));
      this.stepIdx = index;
      const step = flow.steps[index], viewId = step.view || flow.view;
      this.selected = null; this.selEdge = null; this.o.onSelect(null);
      if (viewId !== this.viewId) await this.show(viewId); else { this.render(); this.fit(false); }
      this.clearFx();
      const a = this.visAnc(step.from, this.vis), b = this.visAnc(step.to, this.vis);
      this.gNodes.querySelectorAll('.node').forEach((node) => {
        const on = node.dataset.id === a || node.dataset.id === b;
        node.classList.toggle('dim', !on); node.classList.toggle('flow-on', on); node.classList.toggle('flow-target', node.dataset.id === b);
      });
      for (const [key, entry] of Object.entries(this.edgeEls)) {
        const on = key === a + '>' + b;
        entry.path.classList.toggle('dim', !on); entry.label.classList.add('dim'); entry.path.classList.toggle('flow-active', on);
        if (on) entry.path.setAttribute('marker-end', `url(#${this.id}arrF)`);
      }
      this.o.onStep(flow, index);
      if (!a || !b || a === b) return;
      const path = this.edgeEls[a + '>' + b]?.path || el('path', { d: this.geom(a, b, this.edges.has(b + '>' + a) ? -36 : 0).d, class: 'edge flow-active', 'marker-end': `url(#${this.id}arrF)` }, this.gFx);
      const A = this.boxes[a];
      const badge = el('g', { class: 'step-badge', transform: `translate(${A.x + 6},${A.y - 6})` }, this.gFx);
      el('circle', { r: 13 }, badge); el('text', { y: 4.5 }, badge).textContent = index + 1;
      this.animateDot(path);
    }
    animateDot(path) {
      const dot = el('circle', { r: 7, class: 'flow-dot' }, this.gFx), length = path.getTotalLength();
      const token = this.dotToken = (this.dotToken || 0) + 1;
      const place = (t) => { const p = path.getPointAtLength(length * t); dot.setAttribute('cx', p.x); dot.setAttribute('cy', p.y); };
      if (this.reduced) { place(.5); return; }
      const run = () => {
        const start = performance.now();
        const tick = (now) => {
          if (token !== this.dotToken || !dot.isConnected) return;
          if (this.reduced) { place(.5); return; }
          const t = Math.min(1, (now - start) / 1100); place(ease(t));
          if (t < 1) requestAnimationFrame(tick); else setTimeout(() => { if (token === this.dotToken) run(); }, 650);
        };
        requestAnimationFrame(tick);
      };
      run();
    }
    clearFx() { this.gFx.replaceChildren(); this.dotToken = (this.dotToken || 0) + 1; }
    stopFlow() { this.flow = null; this.forced = new Set(); this.clearFx(); this.render(); }
    sentences() { return [...this.edges.values()].map((group) => ({ from: this.E(group.a).name, label: group.label, to: this.E(group.b).name, tech: group.tech, implied: group.implied, n: group.rels.length })); }
    toDSL() {
      const q = (value) => JSON.stringify(String(value || ''));
      const elements = this.m.elements, ids = Object.keys(elements);
      const children = (parent) => ids.filter((id) => elements[id].parent === parent);
      const lines = [`workspace ${q(this.m.name)} ${q(this.m.description || 'Architecture model')} {`, '', '  model {'];
      for (const id of ids.filter((key) => !elements[key].parent)) {
        const entry = elements[id];
        if (entry.kind === 'person') { lines.push(`    ${id} = person ${q(entry.name)} ${q(entry.desc)}`); continue; }
        const tag = entry.external ? ' "External"' : '';
        const kids = children(id);
        lines.push(`    ${id} = softwareSystem ${q(entry.name)} ${q(entry.desc)}${tag}${kids.length ? ' {' : ''}`);
        for (const child of kids) {
          const container = elements[child], components = children(child), dbTag = container.kind === 'store' ? ' "Database"' : '';
          lines.push(`      ${child} = container ${q(container.name)} ${q(container.desc)} ${q(container.tech)}${dbTag}${components.length ? ' {' : ''}`);
          for (const component of components) lines.push(`        ${component} = component ${q(elements[component].name)} ${q(elements[component].desc)} ${q(elements[component].tech)}`);
          if (components.length) lines.push('      }');
        }
        if (kids.length) lines.push('    }');
      }
      for (const relation of this.m.rels) lines.push(`    ${relation.from} -> ${relation.to} ${q(relation.label)} ${q(relation.tech)}`);
      lines.push('  }', '', '  views {');
      for (const [id, view] of Object.entries(this.m.views)) {
        lines.push(`    ${{ 1: 'systemContext', 2: 'container', 3: 'component' }[view.level]} ${view.scope} ${q(id)} {`, '      include ' + Object.keys(view.layout).join(' '), '      autolayout lr', '    }');
      }
      lines.push('    styles {', '      element "Element" {', '        background #fbf9f4', '        color #1d1b18', '        stroke #4b463e', '      }', '      element "Person" {', '        shape Person', '        background #e6ecf6', '      }', '      element "Database" {', '        shape Cylinder', '        background #e5efe6', '      }', '      element "External" {', '        background #ece8df', '      }', '    }', '  }', '}');
      return lines.join('\n');
    }
  }
  C4Diagram.validate = validate;
  window.C4Diagram = C4Diagram;
})();
