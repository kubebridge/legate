// SPDX-License-Identifier: Apache-2.0
(function () {
  'use strict';

  const hero = document.querySelector('.scale-hero');
  const heroCanvas = document.getElementById('agent-field');
  const populationCanvas = document.getElementById('population-field');
  const panel = document.querySelector('.population-panel');
  const count = document.querySelector('.population-count');
  const label = document.getElementById('population-label');
  const phaseLabel = document.querySelector('.population-phase');
  const steps = Array.from(document.querySelectorAll('.story-step'));
  const toggle = document.querySelector('.motion-toggle');
  if (!heroCanvas || !populationCanvas) return;
  const heroContext = heroCanvas.getContext('2d');
  const populationContext = populationCanvas.getContext('2d');
  if (!heroContext || !populationContext) return;

  const reduced = matchMedia('(prefers-reduced-motion: reduce)');
  const light = matchMedia('(prefers-color-scheme: light)');
  let paused = false;
  let frame = 0;
  let lastTime = 0;
  let elapsed = 0;
  let phase = -1;
  let density = 1;
  let targetDensity = 1;
  let heroVisible = true;
  let populationVisible = false;
  let heroSize = { width: 0, height: 0 };
  let populationSize = { width: 0, height: 0 };
  let colors = [];
  const counts = ['1', '1,000', '1,000,000'];
  const labels = ['One identity. One inbox.', 'Independent sessions. Shared runtime.', 'Distributed across cluster nodes.'];
  const motion = () => !paused && !reduced.matches;

  function readColors() {
    const style = getComputedStyle(document.documentElement);
    colors = ['--v', '--c', '--p'].map(name => style.getPropertyValue(name).trim());
    requestDraw();
  }

  function resize(canvas, context) {
    const rect = canvas.getBoundingClientRect();
    const ratio = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.round(rect.width * ratio);
    canvas.height = Math.round(rect.height * ratio);
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    return { width: rect.width, height: rect.height };
  }

  function measure() {
    heroSize = resize(heroCanvas, heroContext);
    populationSize = resize(populationCanvas, populationContext);
    updatePhase();
    requestDraw();
  }

  function drawHero(time) {
    const { width, height } = heroSize;
    heroContext.clearRect(0, 0, width, height);
    const spacing = width < 640 ? 19 : 24;
    const wave = time * .00013;
    for (let row = 0; row < height / spacing; row++) {
      for (let col = 0; col < width / spacing; col++) {
        const x = col * spacing + spacing / 2;
        const y = row * spacing + spacing / 2;
        // Quiet centre keeps the headline legible; the population fills the edges.
        const edge = Math.min(1, Math.pow(Math.abs(x - width / 2) / (width * .45), 3));
        const horizon = Math.max(0, (y / height - .8) * 4);
        const signal = (Math.sin(col * .31 + row * .27 - wave * 5) + 1) / 2;
        const alpha = (edge * .25 + horizon * .12) * (.25 + signal * .75);
        if (alpha < .012) continue;
        heroContext.globalAlpha = alpha;
        heroContext.fillStyle = colors[(col + row) % 11 === 0 ? 1 : 0];
        const size = signal > .92 ? 3 : 2;
        heroContext.fillRect(x, y, size, size);
      }
    }
    heroContext.globalAlpha = 1;
  }

  function drawPopulation(time) {
    const { width, height } = populationSize;
    populationContext.clearRect(0, 0, width, height);
    const compact = height < 150;
    const columns = compact ? 21 : 42;
    const rows = compact ? 9 : 21;
    const total = columns * rows;
    const visible = Math.max(1, Math.round(1 + (total - 1) * density));
    const cellW = (width - 26) / columns;
    const cellH = (height - (phase === 2 ? 30 : 20)) / rows;
    const size = Math.max(2, Math.min(cellW, cellH) * .58);
    // Centre-out fill makes the population grow from the first session.
    for (let index = 0; index < total; index++) {
      const col = index % columns;
      const row = Math.floor(index / columns);
      const distance = Math.max(Math.abs(col - (columns - 1) / 2) / columns, Math.abs(row - (rows - 1) / 2) / rows) * 2;
      if (visible <= 1 || distance > Math.sqrt(visible / total)) continue;
      const node = Math.min(2, Math.floor(col / (columns / 3)));
      const pulse = .55 + .45 * (Math.sin(index * 2.39 + time * .0015) + 1) / 2;
      populationContext.globalAlpha = pulse;
      populationContext.fillStyle = colors[phase === 2 ? node : 0];
      populationContext.fillRect(13 + col * cellW, 10 + row * cellH, size, size);
    }
    if (visible <= 1) {
      populationContext.globalAlpha = .12;
      populationContext.fillStyle = colors[0];
      populationContext.fillRect(width / 2 - 23, height / 2 - 23, 46, 46);
      populationContext.globalAlpha = 1;
      populationContext.fillRect(width / 2 - 7, height / 2 - 7, 14, 14);
      populationContext.strokeStyle = colors[0];
      populationContext.strokeRect(width / 2 - 23, height / 2 - 23, 46, 46);
    }
    populationContext.globalAlpha = 1;
  }

  function updatePhase() {
    const line = innerWidth <= 640 ? innerHeight * .73 : innerHeight * .55;
    let next = 0;
    steps.forEach((step, index) => { if (step.getBoundingClientRect().top < line) next = index; });
    if (next !== phase) {
      phase = next;
      targetDensity = [0, .24, 1][phase];
      if (!motion()) density = targetDensity;
      count.replaceChildren(document.createTextNode(counts[phase]));
      const unit = document.createElement('span');
      unit.className = 'count-unit';
      unit.textContent = phase === 0 ? 'session' : 'illustrative population';
      count.append(unit);
      label.textContent = labels[phase];
      phaseLabel.textContent = `0${phase + 1} / 03`;
      panel.dataset.phase = String(phase);
    }
  }

  function render(now) {
    frame = 0;
    const delta = lastTime ? Math.min(now - lastTime, 50) : 16;
    lastTime = now;
    if (motion()) elapsed += delta;
    updatePhase();
    density = motion() ? density + (targetDensity - density) * (1 - Math.exp(-delta / 180)) : targetDensity;
    if (Math.abs(density - targetDensity) < .001) density = targetDensity;
    if (heroVisible) drawHero(elapsed);
    if (populationVisible) drawPopulation(elapsed);
    if (motion() && !document.hidden && (heroVisible || populationVisible)) frame = requestAnimationFrame(render);
  }

  function requestDraw() {
    if (!frame && !document.hidden) frame = requestAnimationFrame(render);
  }

  function syncMotion() {
    document.documentElement.dataset.motion = motion() ? 'running' : 'paused';
    toggle.textContent = reduced.matches ? 'Reduced motion' : paused ? 'Resume motion' : 'Pause motion';
    toggle.setAttribute('aria-pressed', String(paused || reduced.matches));
    toggle.disabled = reduced.matches;
    lastTime = 0;
    requestDraw();
  }

  toggle.hidden = false;
  toggle.addEventListener('click', () => { paused = !paused; syncMotion(); });
  reduced.addEventListener('change', syncMotion);
  light.addEventListener('change', readColors);
  new MutationObserver(readColors).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  const visibility = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.target === hero) heroVisible = entry.isIntersecting;
      else populationVisible = entry.isIntersecting;
    });
    requestDraw();
  });
  visibility.observe(hero);
  visibility.observe(panel);
  const reveals = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.isIntersecting) { entry.target.classList.add('is-revealed'); reveals.unobserve(entry.target); }
    });
  }, { threshold: .15 });
  document.querySelectorAll('[data-reveal]').forEach(element => reveals.observe(element));
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) { cancelAnimationFrame(frame); frame = 0; }
    else { lastTime = 0; requestDraw(); }
  });
  window.addEventListener('scroll', requestDraw, { passive: true });
  new ResizeObserver(measure).observe(document.querySelector('.scale-story'));
  window.addEventListener('resize', measure, { passive: true });
  readColors();
  measure();
  syncMotion();
})();
