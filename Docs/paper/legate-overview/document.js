/* Optional document behaviors. Core prose is already present in the HTML. */
(function () {
  'use strict';
  const $ = (s, root = document) => root.querySelector(s);
  const $$ = (s, root = document) => [...root.querySelectorAll(s)];
  const reduced = matchMedia('(prefers-reduced-motion: reduce)');
  const links = $$('.nav-links a[href^="#"]');
  const sections = links.map((a) => document.getElementById(a.hash.slice(1))).filter(Boolean);
  const progress = $('[data-progress]');
  let scheduled = false;
  function scrollState() {
    scheduled = false;
    const h = document.documentElement;
    const distance = h.scrollHeight - h.clientHeight;
    if (progress) progress.style.width = (distance > 0 ? Math.min(100, Math.max(0, h.scrollTop / distance * 100)) : 0) + '%';
    const navHeight = $('.nav')?.offsetHeight || 54;
    const current = sections.filter((s) => s.getBoundingClientRect().top <= navHeight + 50).at(-1);
    links.forEach((a) => {
      const active = !!current && a.hash === '#' + current.id;
      a.classList.toggle('active', active);
      if (active) a.setAttribute('aria-current', 'location'); else a.removeAttribute('aria-current');
    });
  }
  function scheduleScroll() { if (!scheduled) { scheduled = true; requestAnimationFrame(scrollState); } }
  window.addEventListener('scroll', scheduleScroll, { passive: true });
  window.addEventListener('resize', scheduleScroll);
  scrollState();

  if ('IntersectionObserver' in window && !reduced.matches) {
    const observer = new IntersectionObserver((entries) => entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.classList.remove('reveal-pending');
        entry.target.classList.add('in');
        observer.unobserve(entry.target);
      }
    }), { threshold: .08 });
    $$('.reveal').forEach((item) => { observer.observe(item); item.classList.add('reveal-pending'); });
  }

  const storagePrefix = 'paper-doc:' + location.pathname + ':' + (document.body.dataset.docId || document.title) + ':';
  $$('input[type="checkbox"][data-check]').forEach((checkbox) => {
    const key = storagePrefix + checkbox.dataset.check;
    try { const value = localStorage.getItem(key); if (value !== null) checkbox.checked = value === '1'; } catch { /* Storage may be disabled on file origins. */ }
    checkbox.addEventListener('change', () => {
      try { localStorage.setItem(key, checkbox.checked ? '1' : '0'); } catch { /* The checkbox remains functional for this visit. */ }
    });
  });
  const notesButton = $('[data-notes-toggle]');
  if (notesButton) {
    notesButton.hidden = !$('.doc-note');
    notesButton.addEventListener('click', () => {
      const hidden = document.body.classList.toggle('hide-notes');
      notesButton.textContent = hidden ? 'Show notes' : 'Hide notes';
      notesButton.setAttribute('aria-pressed', String(hidden));
      scheduleScroll();
    });
  }

  const figures = $$('[data-lightbox]');
  if (figures.length) {
    const dialog = document.createElement('dialog');
    dialog.className = 'lightbox';
    dialog.setAttribute('aria-label', 'Expanded figure');
    const close = document.createElement('button');
    close.className = 'btn'; close.textContent = 'Close figure';
    const image = document.createElement('img');
    const caption = document.createElement('p');
    dialog.append(close, image, caption); document.body.append(dialog);
    close.addEventListener('click', () => dialog.close());
    figures.forEach((figure) => figure.addEventListener('click', () => {
      const thumbnail = $('img', figure);
      if (!thumbnail) return;
      image.src = figure.dataset.fullSrc || thumbnail.src;
      image.alt = thumbnail.alt;
      caption.textContent = figure.dataset.caption || figure.textContent.trim();
      dialog.showModal();
    }));
  }
  let closedDetails = [];
  window.addEventListener('beforeprint', () => {
    closedDetails = $$('details:not([open])');
    closedDetails.forEach((item) => { item.open = true; });
  });
  window.addEventListener('afterprint', () => closedDetails.forEach((item) => { item.open = false; }));
})();
