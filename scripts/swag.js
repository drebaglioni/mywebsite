(() => {
    const collection = document.getElementById('swag-collection');
    const template = document.getElementById('swag-spread-template');
    if (!collection || !template) return;

    const renderItem = (item, total) => {
        const node = template.content.firstElementChild.cloneNode(true);

        const editionLabel = `${item.edition} / ${String(total).padStart(2, '0')}`;
        node.querySelector('[data-edition]').textContent = editionLabel;
        node.querySelector('[data-name]').textContent = item.name;
        node.querySelector('[data-subtitle]').textContent = item.subtitle || '';
        node.querySelector('[data-caption]').textContent = item.caption || '';
        node.querySelector('[data-material]').textContent = item.material || '—';
        node.querySelector('[data-construction]').textContent = item.construction || '—';
        node.querySelector('[data-sizes]').textContent = (item.sizes || []).join(' · ') || '—';

        const colorways = Array.isArray(item.colorways) ? item.colorways : [];
        const swatchList = node.querySelector('[data-swatches]');
        const activeLabel = node.querySelector('[data-active-colorway]');
        const heroFrame = node.querySelector('[data-hero]');
        const heroImg = node.querySelector('[data-hero-img]');
        const viewToggle = node.querySelectorAll('.swag-view-toggle button');

        const state = { colorIdx: 0, view: 'front' };
        let emptyNote = null;

        const sync = () => {
            const cw = colorways[state.colorIdx];
            if (!cw) return;
            activeLabel.textContent = cw.name;

            const src = cw[state.view];
            const backdrop = cw[`${state.view}_backdrop`];
            const altPiece = state.view === 'front' ? 'front view' : 'back view';

            if (backdrop) {
                heroFrame.style.setProperty('--hero-bg', backdrop);
            } else {
                heroFrame.style.removeProperty('--hero-bg');
            }

            if (src) {
                heroFrame.classList.remove('is-empty');
                heroImg.style.display = '';
                heroImg.src = src;
                heroImg.alt = `${item.name} in ${cw.name}, ${altPiece}`;
                if (emptyNote) emptyNote.style.display = 'none';
            } else {
                heroFrame.classList.add('is-empty');
                heroImg.removeAttribute('src');
                heroImg.style.display = 'none';
                if (!emptyNote) {
                    emptyNote = document.createElement('span');
                    heroFrame.appendChild(emptyNote);
                }
                emptyNote.style.display = '';
                emptyNote.textContent = `Sample shot pending — ${cw.name} / ${altPiece}`;
            }

            const hasFront = !!cw.front;
            const hasBack = !!cw.back;
            viewToggle.forEach((btn) => {
                const v = btn.dataset.view;
                btn.setAttribute('aria-pressed', v === state.view ? 'true' : 'false');
                if (v === 'back') btn.disabled = !hasBack;
                if (v === 'front') btn.disabled = !hasFront;
            });

            swatchList.querySelectorAll('button').forEach((btn, i) => {
                btn.setAttribute('aria-pressed', i === state.colorIdx ? 'true' : 'false');
            });
        };

        colorways.forEach((cw, i) => {
            const li = document.createElement('li');
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'swag-swatch';
            btn.style.setProperty('--dot', cw.swatch || '#999');
            btn.setAttribute('aria-label', `Show ${cw.name}`);
            btn.setAttribute('aria-pressed', i === 0 ? 'true' : 'false');
            btn.title = cw.print_color ? `${cw.name} · ${cw.print_color} print` : cw.name;
            btn.addEventListener('click', () => {
                state.colorIdx = i;
                if (state.view === 'back' && !colorways[i].back) state.view = 'front';
                if (state.view === 'front' && !colorways[i].front) state.view = colorways[i].back ? 'back' : 'front';
                sync();
            });
            li.appendChild(btn);
            swatchList.appendChild(li);
        });

        viewToggle.forEach((btn) => {
            btn.addEventListener('click', () => {
                if (btn.disabled) return;
                state.view = btn.dataset.view;
                sync();
            });
        });

        const cta = node.querySelector('[data-cta]');
        if (item.buy_url) {
            const a = document.createElement('a');
            a.className = 'swag-buy';
            a.href = item.buy_url;
            a.target = '_blank';
            a.rel = 'noopener';
            a.textContent = item.price_usd ? `Acquire — $${item.price_usd} ↗` : 'Acquire ↗';
            cta.appendChild(a);
        } else {
            const status = document.createElement('span');
            status.className = 'swag-status';
            status.textContent = item.status || 'Not yet for sale';
            cta.appendChild(status);
        }

        sync();
        return node;
    };

    const renderEmpty = (message) => {
        const p = document.createElement('p');
        p.style.cssText = 'padding: 80px 24px; text-align: center; font-family: "IBM Plex Mono", monospace; font-size: 11px; letter-spacing: 0.16em; text-transform: uppercase; color: rgba(43,38,34,0.50);';
        p.textContent = message;
        collection.appendChild(p);
    };

    fetch('data/swag.json', { cache: 'no-cache' })
        .then((r) => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
        })
        .then((items) => {
            if (!Array.isArray(items) || items.length === 0) {
                renderEmpty('Coming soon');
                return;
            }
            items.forEach((item) => collection.appendChild(renderItem(item, items.length)));
        })
        .catch((err) => {
            console.error('swag.json failed to load', err);
            renderEmpty('Catalog unavailable');
        });
})();
