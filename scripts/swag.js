(() => {
    const collection = document.getElementById('swag-collection');
    const template = document.getElementById('swag-spread-template');
    if (!collection || !template) return;

    const reducedMotionQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
    const mathEntries = [];
    let mathFrame = null;
    let mathObserver = null;

    const hashCode = (input) => {
        let hash = 0;
        for (let i = 0; i < input.length; i += 1) {
            hash = ((hash << 5) - hash) + input.charCodeAt(i);
            hash |= 0;
        }
        return Math.abs(hash);
    };

    const parseHexColor = (value) => {
        const hex = (value || '').trim().replace('#', '');
        if (/^[a-fA-F0-9]{3}$/.test(hex)) {
            return {
                r: parseInt(hex.charAt(0) + hex.charAt(0), 16),
                g: parseInt(hex.charAt(1) + hex.charAt(1), 16),
                b: parseInt(hex.charAt(2) + hex.charAt(2), 16)
            };
        }
        if (/^[a-fA-F0-9]{6}$/.test(hex)) {
            return {
                r: parseInt(hex.slice(0, 2), 16),
                g: parseInt(hex.slice(2, 4), 16),
                b: parseInt(hex.slice(4, 6), 16)
            };
        }
        return { r: 43, g: 38, b: 34 };
    };

    const resizeMathCanvas = (entry) => {
        const rect = entry.canvas.getBoundingClientRect();
        const dpr = Math.min(window.devicePixelRatio || 1, 1.35);
        const width = Math.max(1, Math.round(rect.width));
        const height = Math.max(1, Math.round(rect.height));
        const pixelWidth = Math.round(width * dpr);
        const pixelHeight = Math.round(height * dpr);

        if (entry.canvas.width !== pixelWidth || entry.canvas.height !== pixelHeight) {
            entry.canvas.width = pixelWidth;
            entry.canvas.height = pixelHeight;
            entry.width = width;
            entry.height = height;
            entry.dpr = dpr;
            entry.needsStaticDraw = true;
        }
    };

    const drawSwagMath = (entry, timestamp, isStatic = false) => {
        resizeMathCanvas(entry);
        if (entry.width <= 1 || entry.height <= 1) {
            return;
        }

        const ctx = entry.ctx;
        const width = entry.width;
        const height = entry.height;
        const dpr = entry.dpr;
        const time = isStatic ? entry.seed * 0.013 : timestamp * 0.001;
        const accent = parseHexColor(entry.canvas.dataset.mathAccent);
        let ink = parseHexColor(entry.canvas.dataset.mathInk || '#f2ead7');
        const inkLuminance = (ink.r * 0.299) + (ink.g * 0.587) + (ink.b * 0.114);
        if (inkLuminance < 80) {
            ink = { r: 242, g: 234, b: 215 };
        }
        const spreadRect = entry.spread.getBoundingClientRect();
        const heroRect = entry.hero.getBoundingClientRect();
        const specRect = entry.spec.getBoundingClientRect();
        const heroCenterX = heroRect.left - spreadRect.left + heroRect.width * 0.5;
        const heroCenterY = heroRect.top - spreadRect.top + heroRect.height * 0.5;
        const specCenterX = specRect.left - spreadRect.left + specRect.width * 0.5;
        const specCenterY = specRect.top - spreadRect.top + specRect.height * 0.5;
        const cx = heroCenterX + Math.sin(time * 0.18 + entry.seed) * heroRect.width * 0.04;
        const cy = heroCenterY + Math.cos(time * 0.14 + entry.seed * 0.7) * heroRect.height * 0.035;
        const heroLeft = heroRect.left - spreadRect.left;
        const heroTop = heroRect.top - spreadRect.top;
        const heroRight = heroLeft + heroRect.width;
        const heroBottom = heroTop + heroRect.height;
        const specLeft = specRect.left - spreadRect.left;
        const specTop = specRect.top - spreadRect.top;
        const specRight = specLeft + specRect.width;
        const specBottom = specTop + specRect.height;
        const minDimension = Math.min(width, height);
        const maxRadius = Math.max(heroRect.width, heroRect.height) * 0.82;
        const grid = Math.max(5, Math.min(10, minDimension * 0.009));
        const pixelSize = Math.max(3, grid * 0.86);
        const phase = time * 0.72 + entry.seed * 0.029;
        const driftX = Math.sin(time * 0.21 + entry.seed) * 0.24;
        const driftY = Math.cos(time * 0.17 + entry.seed * 0.73) * 0.2;

        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.imageSmoothingEnabled = false;
        ctx.clearRect(0, 0, width, height);
        ctx.lineCap = 'butt';
        ctx.lineJoin = 'miter';

        function hash2(ix, iy) {
            let h = (ix * 374761393) ^ (iy * 668265263) ^ (entry.seed * 362437);
            h = (h ^ (h >>> 13)) >>> 0;
            h = Math.imul(h, 1274126177) >>> 0;
            return ((h ^ (h >>> 16)) >>> 0) / 4294967295;
        }

        function insideRect(x, y, left, top, right, bottom, pad = 0) {
            return x > left - pad && x < right + pad && y > top - pad && y < bottom + pad;
        }

        const creamPixels = [[], [], []];
        const accentPixels = [[], []];
        const bluePixels = [];

        for (let y = -grid; y <= height + grid; y += grid) {
            const ny = (y - cy) / maxRadius;
            for (let x = -grid; x <= width + grid; x += grid) {
                const nx = (x - cx) / maxRadius;
                const sx = (x - specCenterX) / Math.max(specRect.width, 1);
                const sy = (y - specCenterY) / Math.max(specRect.height, 1);
                const ellipse = Math.sqrt((nx * nx) / 1.22 + (ny * ny) / 0.72);
                const angle = Math.atan2(ny * 1.24, nx * 0.82);
                const orbit = ellipse
                    + 0.18 * Math.sin(angle * 6 + phase)
                    + 0.11 * Math.cos(angle * 10 - phase * 0.82);
                const field = Math.sin((nx + driftX) * 16.5 + phase)
                    + Math.cos((ny - driftY) * 18.2 - phase * 0.72)
                    + Math.sin((nx * 0.82 + ny * 1.28) * 26.5 + phase * 1.12)
                    + Math.cos((sx - sy) * 18.0 + phase * 0.62);
                const band = Math.abs(field);
                const ring = Math.abs(orbit - 0.88);
                const diagonal = Math.abs(Math.sin((x + y) / (grid * 5.5) + phase));
                const grain = hash2(Math.round(x / grid), Math.round(y / grid));
                const nearHero = ellipse < 1.85 && ellipse > 0.16;
                const nearSpec = insideRect(x, y, specLeft, specTop, specRight, specBottom, grid * 18);
                const underProduct = insideRect(x, y, heroLeft, heroTop, heroRight, heroBottom, -grid * 2);

                if (underProduct && grain < 0.88) {
                    continue;
                }

                if (!nearHero && !nearSpec && diagonal > 0.2) {
                    continue;
                }

                const jitterX = (grain - 0.5) * grid * 0.18;
                const jitterY = (hash2(Math.round(y / grid), Math.round(x / grid)) - 0.5) * grid * 0.18;
                const px = x + jitterX;
                const py = y + jitterY;
                const size = pixelSize * (0.72 + grain * 0.72);

                if ((ring < 0.055 + grain * 0.02 || band < 0.38) && nearHero) {
                    accentPixels[grain > 0.48 ? 1 : 0].push([px, py, size * 1.1]);
                    continue;
                }

                if (nearSpec && diagonal < 0.13 && grain > 0.34) {
                    bluePixels.push([px, py, size * 0.74]);
                    continue;
                }

                if ((band < 1.18 + grain * 0.36 && nearHero) || (nearSpec && grain > 0.72)) {
                    const bucket = Math.min(2, Math.floor((1 - Math.min(1, band / 2.4)) * 3));
                    creamPixels[bucket].push([px, py, size * (nearSpec ? 0.6 : 0.82)]);
                }
            }
        }

        const creamAlpha = isStatic ? [0.08, 0.13, 0.2] : [0.1, 0.16, 0.24];
        creamPixels.forEach((pixels, bucket) => {
            ctx.fillStyle = `rgba(${ink.r}, ${ink.g}, ${ink.b}, ${creamAlpha[bucket]})`;
            pixels.forEach(([x, y, size]) => {
                ctx.fillRect(x, y, size, size);
            });
        });

        accentPixels.forEach((pixels, bucket) => {
            ctx.fillStyle = `rgba(${accent.r}, ${accent.g}, ${accent.b}, ${isStatic ? 0.36 + bucket * 0.12 : 0.44 + bucket * 0.14})`;
            pixels.forEach(([x, y, size]) => {
                ctx.fillRect(x, y, size, size);
            });
        });

        ctx.fillStyle = `rgba(18, 56, 255, ${isStatic ? 0.18 : 0.26})`;
        bluePixels.forEach(([x, y, size]) => {
            ctx.fillRect(x, y, size, size);
        });

        ctx.strokeStyle = `rgba(${accent.r}, ${accent.g}, ${accent.b}, ${isStatic ? 0.42 : 0.56})`;
        ctx.lineWidth = Math.max(2, grid * 0.35);
        ctx.strokeRect(
            Math.round(heroLeft - grid * 2),
            Math.round(heroTop - grid * 2),
            Math.round(heroRect.width + grid * 4),
            Math.round(heroRect.height + grid * 4)
        );

        ctx.fillStyle = `rgba(${ink.r}, ${ink.g}, ${ink.b}, 0.44)`;
        for (let i = 0; i < 6; i += 1) {
            const x = Math.round((heroCenterX * 0.2 + specCenterX * 0.8) + i * grid * 2.2);
            const y = Math.round(heroTop + grid * (2 + i));
            ctx.fillRect(x, y, grid, grid);
        }
    };

    const shouldAnimateMath = () => (
        !document.hidden
        && !reducedMotionQuery.matches
        && mathEntries.some((entry) => entry.visible)
    );

    const renderMathFrame = (timestamp) => {
        if (!shouldAnimateMath()) {
            mathFrame = null;
            return;
        }

        mathEntries.forEach((entry) => {
            if (entry.visible) {
                drawSwagMath(entry, timestamp, false);
                entry.needsStaticDraw = true;
            }
        });

        mathFrame = window.requestAnimationFrame(renderMathFrame);
    };

    const updateMathState = () => {
        const animate = shouldAnimateMath();

        if (!animate) {
            if (mathFrame !== null) {
                window.cancelAnimationFrame(mathFrame);
                mathFrame = null;
            }

            mathEntries.forEach((entry) => {
                if (entry.needsStaticDraw) {
                    drawSwagMath(entry, 0, true);
                    entry.needsStaticDraw = false;
                }
            });
            return;
        }

        if (mathFrame === null) {
            mathFrame = window.requestAnimationFrame(renderMathFrame);
        }
    };

    const registerMathCanvas = (canvas, hero, spec, spread, item, index) => {
        if (!canvas) {
            return;
        }

        const ctx = canvas.getContext('2d', { alpha: true });
        if (!ctx) {
            return;
        }

        const entry = {
            canvas,
            hero,
            spec,
            spread,
            ctx,
            seed: hashCode(`${item.id || item.name}-${index}`) % 997,
            width: 0,
            height: 0,
            dpr: 1,
            visible: true,
            needsStaticDraw: true
        };
        mathEntries.push(entry);

        if ('IntersectionObserver' in window) {
            if (!mathObserver) {
                mathObserver = new IntersectionObserver((observerEntries) => {
                    observerEntries.forEach((observerEntry) => {
                        const match = mathEntries.find((candidate) => candidate.canvas === observerEntry.target);
                        if (match) {
                            match.visible = observerEntry.isIntersecting;
                            match.needsStaticDraw = true;
                        }
                    });
                    updateMathState();
                }, { threshold: 0.08 });
            }
            entry.visible = false;
            mathObserver.observe(canvas);
        }
    };

    const bindMediaQuery = (query, handler) => {
        if (typeof query.addEventListener === 'function') {
            query.addEventListener('change', handler);
            return;
        }
        if (typeof query.addListener === 'function') {
            query.addListener(handler);
        }
    };

    window.addEventListener('resize', () => {
        mathEntries.forEach((entry) => {
            entry.needsStaticDraw = true;
            resizeMathCanvas(entry);
        });
        updateMathState();
    }, { passive: true });
    document.addEventListener('visibilitychange', updateMathState, { passive: true });
    bindMediaQuery(reducedMotionQuery, updateMathState);

    const renderItem = (item, total, index) => {
        const node = template.content.firstElementChild.cloneNode(true);

        const editionLabel = String(item.edition || index + 1).padStart(2, '0');
        const editionSmallLabel = `Object ${editionLabel} / ${String(total).padStart(2, '0')}`;
        node.querySelector('[data-edition]').textContent = editionLabel;
        node.querySelector('[data-edition-small]').textContent = editionSmallLabel;
        node.querySelector('[data-status-label]').textContent = item.status || 'Sampling';
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
        const specPanel = node.querySelector('.swag-spec');
        const stageMath = node.querySelector('[data-stage-canvas]');
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
                node.style.setProperty('--piece-backdrop', backdrop);
            } else {
                heroFrame.style.removeProperty('--hero-bg');
                node.style.removeProperty('--piece-backdrop');
            }
            if (cw.swatch) {
                node.style.setProperty('--piece-accent', cw.swatch);
            } else {
                node.style.removeProperty('--piece-accent');
            }
            if (stageMath) {
                stageMath.dataset.mathAccent = cw.swatch || backdrop || '#2b2622';
                stageMath.dataset.mathInk = '#f2ead7';
                const matchingEntry = mathEntries.find((entry) => entry.canvas === stageMath);
                if (matchingEntry) {
                    matchingEntry.needsStaticDraw = true;
                    updateMathState();
                }
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
            btn.setAttribute('role', 'option');
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
            status.textContent = item.status ? `${item.status} / Not yet for sale` : 'Not yet for sale';
            cta.appendChild(status);
        }

        sync();
        registerMathCanvas(stageMath, heroFrame, specPanel, node, item, index);
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
            items.forEach((item, index) => collection.appendChild(renderItem(item, items.length, index)));
            window.requestAnimationFrame(updateMathState);
        })
        .catch((err) => {
            console.error('swag.json failed to load', err);
            renderEmpty('Catalog unavailable');
        });
})();
