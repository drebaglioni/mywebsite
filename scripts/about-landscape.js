(() => {
    const places = {
        artegna: { title: 'Artegna', src: 'assets/images/about/photos/artegna.webp', alt: 'Castello Savorgnan and the bell tower of Santa Maria Nascente in Artegna, photographed by iw3rua.' },
        merthyr: { title: 'Merthyr Tydfil', src: 'assets/images/about/home-merthyr.jpg', alt: 'Cyfarthfa Castle and its garden in Merthyr Tydfil.' },
        princeton: { title: 'Princeton', src: 'assets/images/about/home-princeton.jpg', alt: 'Nassau Hall at Princeton, seen through trees from the front path.' },
        sf: { title: 'San Francisco', src: 'assets/images/about/home-sanfrancisco.jpg', alt: 'The Golden Gate Bridge with the San Francisco skyline across the bay.' }
    };
    const fonts = { artegna: '700 48px UnifrakturCook', merthyr: '400 48px Anton', princeton: '400 48px Graduate', sf: '400 48px Shrikhand' };
    const readyFonts = Object.fromEntries(Object.entries(fonts).map(([key, font]) => [key, document.fonts.load(font).catch(() => [])]));
    const buttons = [...document.querySelectorAll('.place-selector button')];
    const image = document.getElementById('scene-image');
    const title = document.getElementById('place-title');
    const status = document.getElementById('scene-status');
    const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
    const source = key => places[key].src;
    const loaded = new Map();
    let request = 0;
    let animation;

    function preload(key) {
        if (!loaded.has(key)) {
            const next = new Image();
            const ready = new Promise((resolve, reject) => {
                next.onload = () => resolve(next);
                next.onerror = () => { loaded.delete(key); reject(new Error('Image unavailable')); };
            });
            next.src = source(key);
            loaded.set(key, ready);
        }
        return loaded.get(key);
    }

    async function selectPlace(key) {
        const currentRequest = ++request;
        if (document.body.dataset.place === key) { status.textContent = ''; return; }
        status.textContent = '';
        try {
            await Promise.all([preload(key), readyFonts[key]]);
            if (currentRequest !== request) return;
            animation?.cancel();
            image.src = source(key);
            image.alt = places[key].alt;
            title.textContent = places[key].title;
            document.body.dataset.place = key;
            buttons.forEach(button => button.setAttribute('aria-pressed', String(button.dataset.place === key)));
            if (!reducedMotion.matches) {
                animation = image.animate([
                    { opacity: .2 },
                    { opacity: 1 }
                ], { duration: 250, easing: 'cubic-bezier(.2,.7,.2,1)' });
            }
        } catch {
            if (currentRequest === request) status.textContent = 'Landscape unavailable. Try again.';
        }
    }

    buttons.forEach(button => {
        const warm = () => preload(button.dataset.place).catch(() => {});
        button.addEventListener('pointerenter', warm, { once: true });
        button.addEventListener('focus', warm, { once: true });
        button.addEventListener('click', () => selectPlace(button.dataset.place));
    });
})();
