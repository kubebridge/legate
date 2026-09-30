// SPDX-License-Identifier: Apache-2.0
(function () {
  'use strict';

  const sun = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="4"></circle><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"></path></svg>';
  const moon = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"></path></svg>';
  const root = document.documentElement;
  const systemTheme = window.matchMedia('(prefers-color-scheme: light)');
  const buttons = document.querySelectorAll('.theme-btn');
  let preference = 'auto';

  try {
    const saved = localStorage.getItem('legate-theme');
    if (saved === 'light' || saved === 'dark') preference = saved;
  } catch (_) { /* Theme selection still works when storage is unavailable. */ }

  function resolvedTheme() {
    return preference === 'auto' ? (systemTheme.matches ? 'light' : 'dark') : preference;
  }

  function apply() {
    if (preference === 'auto') root.removeAttribute('data-theme');
    else root.setAttribute('data-theme', preference);

    const dark = resolvedTheme() === 'dark';
    buttons.forEach(function (button) {
      const label = dark ? 'Switch to light mode' : 'Switch to dark mode';
      button.setAttribute('aria-label', label);
      button.setAttribute('title', label);
      button.innerHTML = dark ? sun : moon;
    });
  }

  buttons.forEach(function (button) {
    button.addEventListener('click', function () {
      preference = resolvedTheme() === 'dark' ? 'light' : 'dark';
      try { localStorage.setItem('legate-theme', preference); } catch (_) {}
      apply();
    });
  });
  systemTheme.addEventListener('change', apply);
  apply();
})();
