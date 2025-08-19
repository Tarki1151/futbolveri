async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

function setupSuggest(inputId, listId, pickedId) {
  const input = document.getElementById(inputId);
  const list = document.getElementById(listId);
  const picked = document.getElementById(pickedId);
  let timer = null;
  let last = '';

  async function search(v) {
    if (!v || v.length < 1) { list.style.display = 'none'; return; }
    try {
      const res = await fetchJSON(`/teams?q=${encodeURIComponent(v)}&limit=8`);
      list.innerHTML = '';
      res.forEach(row => {
        const div = document.createElement('div');
        div.className = 'suggest-item';
        div.textContent = row.name;
        div.onclick = () => {
          input.value = row.name;
          picked.textContent = `Seçildi: ${row.name}`;
          list.style.display = 'none';
        };
        list.appendChild(div);
      });
      list.style.display = res.length ? 'block' : 'none';
    } catch (e) {
      list.style.display = 'none';
    }
  }

  input.addEventListener('input', () => {
    const v = input.value;
    if (v === last) return;
    last = v;
    clearTimeout(timer);
    timer = setTimeout(() => search(v), 200);
  });

  document.addEventListener('click', (e) => {
    if (!list.contains(e.target) && e.target !== input) list.style.display = 'none';
  });
}

async function predict() {
  const home = document.getElementById('home').value.trim();
  const away = document.getElementById('away').value.trim();
  const out = document.getElementById('out');
  const btn = document.getElementById('go');
  if (!home || !away) { out.textContent = 'Lütfen iki takım giriniz'; return; }
  btn.disabled = true; out.textContent = 'Hesaplanıyor...';
  try {
    const res = await fetchJSON(`/predict?home=${encodeURIComponent(home)}&away=${encodeURIComponent(away)}`);
    const p = res.prediction;
    const picksP = new Set(p.top_picks_poisson || []);
    const picksD = new Set(p.top_picks_dc || []);
    out.innerHTML = `
      <div class="grid">
        <div class="pill"><div class="k">Lambda Ev</div><div class="v">${p.lambda_home}</div></div>
        <div class="pill"><div class="k">Lambda Dep</div><div class="v">${p.lambda_away}</div></div>
        <div class="pill"><div class="k">Kaynak</div><div class="v">${p.sources.api_football ? 'API-Football' : 'Tahmin'}</div></div>
      </div>
      <div class="section-title">Poisson</div>
      <div class="grid">
        <div class="pill ${picksP.has('MS1') ? 'highlight' : ''}"><div class="k">MS1</div><div class="v">${p.markets_poisson.MS.MS1}%</div></div>
        <div class="pill ${picksP.has('MS0') ? 'highlight' : ''}"><div class="k">MS0</div><div class="v">${p.markets_poisson.MS.MS0}%</div></div>
        <div class="pill ${picksP.has('MS2') ? 'highlight' : ''}"><div class="k">MS2</div><div class="v">${p.markets_poisson.MS.MS2}%</div></div>
      </div>
      <div class="grid">
        <div class="pill ${picksP.has('UST25') ? 'highlight' : ''}"><div class="k">ÜST 2.5</div><div class="v">${p.markets_poisson.OU25.UST25}%</div></div>
        <div class="pill ${picksP.has('ALT25') ? 'highlight' : ''}"><div class="k">ALT 2.5</div><div class="v">${p.markets_poisson.OU25.ALT25}%</div></div>
        <div class="pill ${picksP.has('KGVAR') ? 'highlight' : ''}"><div class="k">KG VAR</div><div class="v">${p.markets_poisson.BTTS.KGVAR}%</div></div>
      </div>
      <div class="grid">
        <div class="pill ${picksP.has('KGYOK') ? 'highlight' : ''}"><div class="k">KG YOK</div><div class="v">${p.markets_poisson.BTTS.KGYOK}%</div></div>
        <div class="pill" style="grid-column: span 2"><div class="k">En Güçlü Seçimler</div><div class="top">${p.top_picks_poisson.map(x=>`<span class=\"chip ${picksP.has(x)?'highlight':''}\">${x}</span>`).join('')}</div></div>
      </div>
      <div class="section-title">Dixon–Coles (ρ=${typeof p.params?.rho==='number' ? p.params.rho : '?'} )</div>
      <div class="grid">
        <div class="pill ${picksD.has('MS1') ? 'highlight' : ''}"><div class="k">MS1</div><div class="v">${p.markets_dc.MS.MS1}%</div></div>
        <div class="pill ${picksD.has('MS0') ? 'highlight' : ''}"><div class="k">MS0</div><div class="v">${p.markets_dc.MS.MS0}%</div></div>
        <div class="pill ${picksD.has('MS2') ? 'highlight' : ''}"><div class="k">MS2</div><div class="v">${p.markets_dc.MS.MS2}%</div></div>
      </div>
      <div class="grid">
        <div class="pill ${picksD.has('UST25') ? 'highlight' : ''}"><div class="k">ÜST 2.5</div><div class="v">${p.markets_dc.OU25.UST25}%</div></div>
        <div class="pill ${picksD.has('ALT25') ? 'highlight' : ''}"><div class="k">ALT 2.5</div><div class="v">${p.markets_dc.OU25.ALT25}%</div></div>
        <div class="pill ${picksD.has('KGVAR') ? 'highlight' : ''}"><div class="k">KG VAR</div><div class="v">${p.markets_dc.BTTS.KGVAR}%</div></div>
      </div>
      <div class="grid">
        <div class="pill ${picksD.has('KGYOK') ? 'highlight' : ''}"><div class="k">KG YOK</div><div class="v">${p.markets_dc.BTTS.KGYOK}%</div></div>
        <div class="pill" style="grid-column: span 2"><div class="k">En Güçlü Seçimler</div><div class="top">${p.top_picks_dc.map(x=>`<span class=\"chip ${picksD.has(x)?'highlight':''}\">${x}</span>`).join('')}</div></div>
      </div>
    `;
  } catch (e) {
    out.textContent = `Hata: ${e.message}`;
  } finally {
    btn.disabled = false;
  }
}

window.addEventListener('DOMContentLoaded', () => {
  setupSuggest('home','home-list','home-picked');
  setupSuggest('away','away-list','away-picked');
  document.getElementById('go').addEventListener('click', predict);
  const swap = document.getElementById('swap');
  if (swap) {
    swap.addEventListener('click', () => {
      const home = document.getElementById('home');
      const away = document.getElementById('away');
      const hp = document.getElementById('home-picked');
      const ap = document.getElementById('away-picked');
      const hv = home.value; home.value = away.value; away.value = hv;
      const ht = hp.textContent; hp.textContent = ap.textContent; ap.textContent = ht;
      document.getElementById('home-list').style.display = 'none';
      document.getElementById('away-list').style.display = 'none';
    });
  }
});
