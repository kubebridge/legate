// SPDX-License-Identifier: Apache-2.0
(function () {
  'use strict';

  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

  // Compact event and recovery examples play once and advance only while visible.
  function createDemo(root, frameCount, render, reducedFrame) {
    if (!root) return;

    const toggle = root.querySelector('.demo-toggle');
    const replay = root.querySelector('.demo-replay');
    let frame = reducedMotion.matches ? reducedFrame : 0;
    let playing = !reducedMotion.matches;
    let visible = false;
    let timer = null;

    function updatePlayback() {
      if (timer !== null) {
        window.clearTimeout(timer);
        timer = null;
      }

      const active = playing && visible && !document.hidden;
      const completed = frame === frameCount - 1 && !playing;
      root.dataset.state = completed ? 'complete' : (active ? 'playing' : 'paused');
      if (toggle) toggle.textContent = playing ? 'Pause' : 'Play';

      if (active) {
        timer = window.setTimeout(function () {
          frame += 1;
          if (frame === frameCount - 1) playing = false;
          paint();
        }, 1100);
      }
    }

    function paint() {
      root.dataset.frame = String(frame);
      render(frame);
      updatePlayback();
    }

    if (toggle) {
      toggle.addEventListener('click', function () {
        if (!playing && frame === frameCount - 1) frame = 0;
        playing = !playing;
        paint();
      });
    }

    if (replay) {
      replay.addEventListener('click', function () {
        frame = 0;
        playing = !reducedMotion.matches;
        paint();
      });
    }

    const observer = new IntersectionObserver(function (entries) {
      visible = entries[0].isIntersecting && entries[0].intersectionRatio >= 0.15;
      updatePlayback();
    }, { threshold: [0, 0.15] });
    observer.observe(root);

    document.addEventListener('visibilitychange', updatePlayback);
    reducedMotion.addEventListener('change', function () {
      if (reducedMotion.matches) playing = false;
      updatePlayback();
    });

    const controls = root.querySelector('.demo-controls');
    if (controls) controls.hidden = false;
    paint();
  }

  const events = document.querySelector('#event-demo');
  const eventRows = events.querySelectorAll('.event-row');
  createDemo(events, eventRows.length, function (frame) {
    eventRows.forEach(function (row, index) {
      row.dataset.reached = String(index <= frame);
      row.dataset.current = String(index === frame);
    });
  }, eventRows.length - 1);

  // Restore the original Sessions flow: moving connection highlights, a
  // filling journal strip, and a six-step panel that advances every third tick.
  function createSessionDemo(root) {
    const steps = root.querySelectorAll('[data-step]');
    const panels = root.querySelectorAll('[data-step-panel]');
    const toggles = root.querySelectorAll('.demo-toggle');
    const statuses = root.querySelectorAll('.demo-status');
    const edges = root.querySelectorAll('.session-edge');
    const band = root.querySelector('#journal-band');
    const sequence = root.querySelector('#band-sequence');
    const stepNumber = root.querySelector('#session-step-number');
    const palette = ['var(--v)', 'var(--c)', 'var(--c)', 'var(--a)', 'var(--a)', 'var(--c)', 'var(--c)', 'var(--g)'];
    const cells = Array.from({ length: 32 }, function () {
      const cell = document.createElement('span');
      cell.className = 'journal-cell';
      band.appendChild(cell);
      return cell;
    });
    const observed = new Map([
      [root.querySelector('#session-flow'), false],
      [root.querySelector('#session-walkthrough'), false]
    ]);
    let tick = 0;
    let step = 0;
    let playing = !reducedMotion.matches;
    let timer = null;

    function updatePlayback() {
      if (timer !== null) {
        window.clearTimeout(timer);
        timer = null;
      }

      const visible = Array.from(observed.values()).some(Boolean);
      const active = playing && visible && !document.hidden;
      root.dataset.state = active ? 'playing' : 'paused';
      toggles.forEach(function (button) { button.textContent = playing ? 'Pause' : 'Play'; });
      statuses.forEach(function (status) {
        status.textContent = 'Step ' + (step + 1) + ' of ' + steps.length +
          (active ? ' · Playing' : ' · Paused');
      });

      if (active) {
        timer = window.setTimeout(function () {
          tick += 1;
          if (tick % 3 === 0) step = (step + 1) % steps.length;
          paint();
        }, 1100);
      }
    }

    function paint() {
      root.dataset.frame = String(step);
      root.dataset.tick = String(tick);
      steps.forEach(function (button, index) {
        button.setAttribute('aria-pressed', String(index === step));
      });
      panels.forEach(function (panel, index) { panel.hidden = index !== step; });
      stepNumber.textContent = String(step + 1).padStart(2, '0');
      edges.forEach(function (edge, index) {
        edge.dataset.active = String(index === tick % edges.length);
      });

      const filled = tick % (cells.length + 1);
      sequence.textContent = 'seq ' + filled;
      cells.forEach(function (cell, index) {
        cell.dataset.filled = String(index < filled);
        cell.style.backgroundColor = index < filled
          ? (index === filled - 1 ? 'var(--ink)' : palette[index % palette.length])
          : 'var(--empty)';
      });
      updatePlayback();
    }

    toggles.forEach(function (button) {
      button.addEventListener('click', function () {
        playing = !playing;
        updatePlayback();
      });
    });
    root.querySelectorAll('.demo-replay').forEach(function (button) {
      button.addEventListener('click', function () {
        tick = 0;
        step = 0;
        playing = !reducedMotion.matches;
        paint();
      });
    });
    steps.forEach(function (button, index) {
      button.addEventListener('click', function () {
        step = index;
        tick = index * 3;
        playing = false;
        paint();
      });
    });

    const observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        observed.set(entry.target, entry.isIntersecting && entry.intersectionRatio >= 0.15);
      });
      updatePlayback();
    }, { threshold: [0, 0.15] });
    observed.forEach(function (_, element) { observer.observe(element); });
    document.addEventListener('visibilitychange', updatePlayback);
    reducedMotion.addEventListener('change', function () {
      if (reducedMotion.matches) playing = false;
      updatePlayback();
    });
    root.querySelectorAll('.demo-controls').forEach(function (controls) { controls.hidden = false; });
    paint();
  }

  createSessionDemo(document.querySelector('#session-demo'));

  const recovery = document.querySelector('#recovery-demo');
  const owner = recovery.querySelector('[data-node="owner"]');
  const survivor = recovery.querySelector('[data-node="survivor"]');
  const terminal = recovery.querySelector('[data-terminal]');
  const caption = recovery.querySelector('.recovery-caption');
  const progress = recovery.querySelector('.recovery-progress');
  const ticks = Array.from({ length: 12 }, function () {
    const tick = document.createElement('span');
    tick.className = 'recovery-tick';
    progress.appendChild(tick);
    return tick;
  });
  const recoveryPhases = [
    { owner: 'running', ownerLabel: 'Running turn', survivor: 'available', survivorLabel: 'Standby', caption: 'Node 1 is inside a model call; the turn start is recorded.' },
    { owner: 'stopped', ownerLabel: 'Stopped', survivor: 'available', survivorLabel: 'Waiting', caption: 'Node 1 stops before the call returns. Its lease has not expired.' },
    { owner: 'stopped', ownerLabel: 'Stopped', survivor: 'claiming', survivorLabel: 'Claiming turn', caption: 'The lease expires. Node 2 claims the turn and reads its journal.' },
    { owner: 'stopped', ownerLabel: 'Stopped', survivor: 'ready', survivorLabel: 'Failure recorded', caption: 'FailAttempt records the interrupted turn as failed. The session can take another prompt.' }
  ];

  createDemo(recovery, ticks.length, function (frame) {
    const phase = frame < 3 ? 0 : (frame < 5 ? 1 : (frame < 7 ? 2 : 3));
    const state = recoveryPhases[phase];
    recovery.dataset.phase = String(phase);
    owner.dataset.state = state.owner;
    owner.querySelector('[data-node-status]').textContent = state.ownerLabel;
    survivor.dataset.state = state.survivor;
    survivor.querySelector('[data-node-status]').textContent = state.survivorLabel;
    terminal.dataset.reached = String(phase === 3);
    caption.textContent = state.caption;
    ticks.forEach(function (tick, index) { tick.dataset.reached = String(index <= frame); });
  }, ticks.length - 1);
})();
