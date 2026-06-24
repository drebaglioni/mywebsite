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
        const time = isStatic ? entry.seed * 0.01 : timestamp * 0.001;
        const accent = parseHexColor(entry.canvas.dataset.mathAccent);
        let ink = parseHexColor(entry.canvas.dataset.mathInk || '#2b2622');
        const inkLuminance = (ink.r * 0.299) + (ink.g * 0.587) + (ink.b * 0.114);
        if (inkLuminance > 170) {
            ink = { r: 43, g: 38, b: 34 };
        }
        const spreadRect = entry.spread.getBoundingClientRect();
        const heroRect = entry.hero.getBoundingClientRect();
        const specRect = entry.spec.getBoundingClientRect();
        const specIsRightColumn = specRect.left > heroRect.right;
        const clipRight = specIsRightColumn
            ? Math.max(0, Math.min(width, specRect.left - spreadRect.left - 34))
            : width;
        const heroCenterX = heroRect.left - spreadRect.left + heroRect.width * 0.5;
        const heroCenterY = heroRect.top - spreadRect.top + heroRect.height * 0.5;
        const cx = heroCenterX + Math.sin(time * 0.08 + entry.seed) * heroRect.width * 0.028;
        const cy = heroCenterY + Math.cos(time * 0.07 + entry.seed * 0.7) * heroRect.height * 0.024;
        const heroLeft = heroRect.left - spreadRect.left;
        const heroTop = heroRect.top - spreadRect.top;
        const heroRight = heroLeft + heroRect.width;
        const heroBottom = heroTop + heroRect.height;
        const minDimension = Math.min(heroRect.width, heroRect.height);
        const maxRadius = minDimension * 1.18;
        const grid = Math.max(1.55, Math.min(2.25, minDimension * 0.0038));
        const pixelSize = Math.max(1, grid * 0.5);
        const phase = time * 0.36 + entry.seed * 0.021;
        const driftX = Math.sin(time * 0.13 + entry.seed) * 0.16;
        const driftY = Math.cos(time * 0.11 + entry.seed * 0.73) * 0.13;

        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.imageSmoothingEnabled = false;
        ctx.clearRect(0, 0, width, height);
        ctx.lineCap = 'butt';
        ctx.lineJoin = 'miter';
        ctx.save();
        ctx.beginPath();
        ctx.rect(0, 0, clipRight, height);
        ctx.clip();

        function hash2(ix, iy) {
            let h = (ix * 374761393) ^ (iy * 668265263) ^ (entry.seed * 362437);
            h = (h ^ (h >>> 13)) >>> 0;
            h = Math.imul(h, 1274126177) >>> 0;
            return ((h ^ (h >>> 16)) >>> 0) / 4294967295;
        }

        function insideHeroHole(x, y) {
            const padX = -grid * 1.5;
            const padY = -grid * 1.5;
            return x > heroLeft - padX
                && x < heroRight + padX
                && y > heroTop - padY
                && y < heroBottom + padY;
        }

        const startX = Math.max(0, Math.floor((heroLeft - maxRadius * 1.75) / grid) * grid);
        const endX = Math.min(clipRight, Math.ceil((heroRight + maxRadius * 1.58) / grid) * grid);
        const startY = Math.max(0, Math.floor((heroTop - maxRadius * 1.36) / grid) * grid);
        const endY = Math.min(height, Math.ceil((heroBottom + maxRadius * 1.36) / grid) * grid);
        const inkBuckets = [[], [], [], [], []];
        const accentPixels = [];
        const ghostPixels = [];

        for (let y = startY; y <= endY; y += grid) {
            const ny = (y - cy) / maxRadius;
            for (let x = startX; x <= endX; x += grid) {
                if (insideHeroHole(x, y)) {
                    continue;
                }

                const nx = (x - cx) / maxRadius;
                const ellipse = Math.sqrt((nx * nx) / 1.42 + (ny * ny) / 0.86);
                if (ellipse > 1.86 || ellipse < 0.04) {
                    continue;
                }

                const angle = Math.atan2(ny * 1.08, nx * 0.92);
                const orbit = ellipse
                    + 0.16 * Math.sin(angle * 5 + phase)
                    + 0.09 * Math.cos(angle * 9 - phase * 0.7);
                const field = Math.sin((nx + driftX) * 18.5 + phase)
                    * Math.cos((ny - driftY) * 15.2 - phase * 0.86)
                    + Math.sin((nx * 0.74 + ny * 1.1) * 20.5 + phase * 1.18)
                    + Math.cos((orbit * 23.0) - phase * 1.35);
                const band = Math.abs(field);
                const grain = hash2(Math.round(x / grid), Math.round(y / grid));
                const edge = Math.abs(orbit - 1.0);
                const envelope = Math.max(0, 1 - Math.abs(ellipse - 0.98) / 0.78);
                const isSurface = band < 1.02 + grain * 0.22;
                const isRim = edge < 0.09 + grain * 0.024;
                const isGhost = (band < 1.72 && grain > 0.18 && envelope > 0.05)
                    || (grain > 0.46 && envelope > 0.12);

                if (!isSurface && !isRim && !isGhost) {
                    continue;
                }

                const jitterX = (grain - 0.5) * grid * 0.48;
                const jitterY = (hash2(Math.round(y / grid), Math.round(x / grid)) - 0.5) * grid * 0.48;
                const px = x + jitterX;
                const py = y + jitterY;
                const size = pixelSize * (0.45 + grain * 0.78);

                if (isRim && grain > 0.34) {
                    accentPixels.push([px, py, size * 1.12]);
                    continue;
                }

                if (isGhost) {
                    ghostPixels.push([px, py, size * 0.72]);
                    continue;
                }

                const bucket = Math.min(4, Math.floor((1 - Math.min(1, band / 0.86)) * 5));
                inkBuckets[bucket].push([px, py, size]);
            }
        }

        const inkAlpha = isStatic
            ? [0.075, 0.11, 0.155, 0.205, 0.27]
            : [0.084, 0.124, 0.174, 0.23, 0.3];
        inkBuckets.forEach((pixels, bucket) => {
            ctx.fillStyle = `rgba(${ink.r}, ${ink.g}, ${ink.b}, ${inkAlpha[bucket]})`;
            pixels.forEach(([x, y, size]) => {
                ctx.fillRect(x, y, size, size);
            });
        });

        ctx.fillStyle = `rgba(${accent.r}, ${accent.g}, ${accent.b}, ${isStatic ? 0.18 : 0.23})`;
        accentPixels.forEach(([x, y, size]) => {
            ctx.fillRect(x, y, size, size);
        });

        ctx.fillStyle = `rgba(${ink.r}, ${ink.g}, ${ink.b}, ${isStatic ? 0.07 : 0.092})`;
        ghostPixels.forEach(([x, y, size]) => {
            ctx.fillRect(x, y, size, size);
        });
        ctx.restore();
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
            } else {
                heroFrame.style.removeProperty('--hero-bg');
            }
            if (stageMath) {
                stageMath.dataset.mathAccent = cw.swatch || backdrop || '#2b2622';
                stageMath.dataset.mathInk = '#2b2622';
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
