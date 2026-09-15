(function () {
    "use strict";

    const BOARD_SIZE = 8;
    const files = "abcdefgh";
    const pieceGlyphs = ["♟", "♞", "♝", "♜", "♛", "♙", "♘", "♗", "♖"];
    const boardGrid = document.getElementById("boardGrid");
    const boardStage = document.getElementById("boardStage");
    const trailCanvas = document.getElementById("trailCanvas");
    const trailContext = trailCanvas.getContext("2d");
    const fly = document.getElementById("fly");
    const statusLine = document.getElementById("statusLine");
    const modelState = document.getElementById("modelState");
    const moveCounter = document.getElementById("moveCounter");
    const runButton = document.getElementById("runButton");
    const boardButton = document.getElementById("boardButton");
    const successMetric = document.getElementById("successMetric");
    const journeyLine = document.getElementById("journeyLine");
    const bridgeState = document.getElementById("bridgeState");
    const bridgeSummary = document.getElementById("bridgeSummary");
    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

    const state = {
        policy: null,
        squares: [],
        occupancy: new Uint8Array(64),
        pieces: new Map(),
        agent: { x: 0, y: 7 },
        origin: { x: 0, y: 7 },
        target: { x: 5, y: 1 },
        trail: [{ x: 0, y: 7 }],
        running: false,
        timer: null,
        steps: 0,
        seed: 9317,
    };

    function squareIndex(x, y) {
        return y * BOARD_SIZE + x;
    }

    function insideBoard(x, y) {
        return x >= 0 && x < BOARD_SIZE && y >= 0 && y < BOARD_SIZE;
    }

    function squareName(x, y) {
        return `${files[x]}${BOARD_SIZE - y}`;
    }

    function seededRandom() {
        state.seed = (state.seed * 1664525 + 1013904223) >>> 0;
        return state.seed / 4294967296;
    }

    function randomInteger(minimum, maximum) {
        return minimum + Math.floor(seededRandom() * (maximum - minimum + 1));
    }

    function initializeBoardNodes() {
        for (let y = 0; y < BOARD_SIZE; y += 1) {
            for (let x = 0; x < BOARD_SIZE; x += 1) {
                const square = document.createElement("button");
                square.type = "button";
                square.className = "square";
                square.dataset.x = String(x);
                square.dataset.y = String(y);
                square.setAttribute("role", "gridcell");
                square.setAttribute("aria-label", `Set target to ${squareName(x, y)}`);
                square.innerHTML = `<span class="square-name">${squareName(x, y)}</span>`;
                square.addEventListener("click", () => setTarget(x, y, true));
                boardGrid.appendChild(square);
                state.squares.push(square);
            }
        }

    }

    function setScenarioPieces(entries) {
        state.occupancy.fill(0);
        state.pieces.clear();
        entries.forEach(({ x, y, glyph, white }) => {
            if (x === state.agent.x && y === state.agent.y) return;
            const index = squareIndex(x, y);
            state.occupancy[index] = 1;
            state.pieces.set(index, { glyph, white });
        });
    }

    function defaultScenario() {
        state.agent = { x: 0, y: 7 };
        state.origin = { ...state.agent };
        state.target = { x: 5, y: 1 };
        state.steps = 0;
        state.trail = [{ ...state.agent }];
        setScenarioPieces([
            { x: 1, y: 0, glyph: "♞" },
            { x: 3, y: 0, glyph: "♛" },
            { x: 6, y: 0, glyph: "♚" },
            { x: 5, y: 1, glyph: "♟" },
            { x: 2, y: 2, glyph: "♝" },
            { x: 6, y: 3, glyph: "♟" },
            { x: 3, y: 4, glyph: "♙", white: true },
            { x: 5, y: 5, glyph: "♘", white: true },
            { x: 2, y: 6, glyph: "♙", white: true },
            { x: 7, y: 7, glyph: "♖", white: true },
        ]);
        state.occupancy[squareIndex(state.target.x, state.target.y)] = 0;
    }

    function hasRoute(start, target, occupancy) {
        const queue = [{ ...start }];
        const visited = new Uint8Array(64);
        visited[squareIndex(start.x, start.y)] = 1;
        while (queue.length) {
            const current = queue.shift();
            if (current.x === target.x && current.y === target.y) return true;
            for (const direction of state.policy.directions) {
                const x = current.x + direction.dx;
                const y = current.y + direction.dy;
                if (!insideBoard(x, y)) continue;
                const index = squareIndex(x, y);
                if (occupancy[index] || visited[index]) continue;
                visited[index] = 1;
                queue.push({ x, y });
            }
        }
        return false;
    }

    function newBoard() {
        stopPolicy();
        state.origin = { ...state.agent };
        let occupancy;
        let pieces;
        do {
            occupancy = new Uint8Array(64);
            pieces = new Map();
            const count = randomInteger(8, 15);
            for (let index = 0; index < count; index += 1) {
                const x = randomInteger(0, 7);
                const y = randomInteger(0, 7);
                const square = squareIndex(x, y);
                if ((x === state.agent.x && y === state.agent.y) || (x === state.target.x && y === state.target.y)) continue;
                occupancy[square] = 1;
                const glyph = pieceGlyphs[randomInteger(0, pieceGlyphs.length - 1)];
                pieces.set(square, { glyph, white: "♙♘♗♖".includes(glyph) });
            }
        } while (!hasRoute(state.agent, state.target, occupancy));
        state.occupancy = occupancy;
        state.pieces = pieces;
        state.steps = 0;
        state.trail = [{ ...state.agent }];
        if (state.agent.x === state.target.x && state.agent.y === state.target.y) {
            chooseRandomTarget();
            return;
        }
        statusLine.innerHTML = `New <em>board</em>`;
        journeyLine.textContent = "Choose any open square to set the destination.";
        render();
    }

    function encodeInput() {
        const input = new Float32Array(68);
        input.set(state.occupancy);
        input[squareIndex(state.agent.x, state.agent.y)] = 0;
        input[squareIndex(state.target.x, state.target.y)] = 0;
        input[64] = (state.agent.x / 7) * 2 - 1;
        input[65] = (state.agent.y / 7) * 2 - 1;
        input[66] = (state.target.x / 7) * 2 - 1;
        input[67] = (state.target.y / 7) * 2 - 1;
        return input;
    }

    function softmax(logits) {
        const maximum = Math.max(...logits);
        const values = logits.map((value) => Math.exp(value - maximum));
        const total = values.reduce((sum, value) => sum + value, 0);
        return values.map((value) => value / total);
    }

    function infer() {
        const input = encodeInput();
        const { architecture, weights } = state.policy;
        const hiddenSize = architecture.hiddenLayers[0];
        const hidden = new Float32Array(hiddenSize);
        for (let hiddenIndex = 0; hiddenIndex < hiddenSize; hiddenIndex += 1) {
            let value = weights.b1[hiddenIndex];
            const offset = hiddenIndex * architecture.inputSize;
            for (let inputIndex = 0; inputIndex < architecture.inputSize; inputIndex += 1) {
                value += weights.w1[offset + inputIndex] * input[inputIndex];
            }
            hidden[hiddenIndex] = Math.max(0, value);
        }

        const logits = [];
        for (let outputIndex = 0; outputIndex < architecture.outputSize; outputIndex += 1) {
            let value = weights.b2[outputIndex];
            const offset = outputIndex * hiddenSize;
            for (let hiddenIndex = 0; hiddenIndex < hiddenSize; hiddenIndex += 1) {
                value += weights.w2[offset + hiddenIndex] * hidden[hiddenIndex];
            }
            logits.push(value);
        }
        return softmax(logits);
    }

    function rankedLegalChoice(probabilities) {
        const ranking = probabilities
            .map((probability, index) => ({ probability, index }))
            .sort((left, right) => right.probability - left.probability);
        return ranking.find(({ index }) => {
            const direction = state.policy.directions[index];
            const x = state.agent.x + direction.dx;
            const y = state.agent.y + direction.dy;
            return insideBoard(x, y) && !state.occupancy[squareIndex(x, y)];
        });
    }

    function takePolicyStep() {
        if (!state.policy || !state.running) return;
        if (state.agent.x === state.target.x && state.agent.y === state.target.y) {
            arrive();
            return;
        }

        const probabilities = infer();
        const choice = rankedLegalChoice(probabilities);
        if (!choice) {
            stopPolicy();
            statusLine.innerHTML = `Policy <em>trapped</em>`;
            journeyLine.textContent = "No legal move is available from this square.";
            return;
        }

        const direction = state.policy.directions[choice.index];
        state.agent.x += direction.dx;
        state.agent.y += direction.dy;
        state.steps += 1;
        state.trail.push({ ...state.agent });
        statusLine.innerHTML = `${direction.name} <em>${(choice.probability * 100).toFixed(0)}%</em>`;
        journeyLine.textContent = `Heading to ${squareName(state.target.x, state.target.y)} · step ${state.steps}`;
        render();

        if (state.agent.x === state.target.x && state.agent.y === state.target.y) {
            arrive();
            return;
        }
        if (state.steps >= 32) {
            stopPolicy();
            statusLine.innerHTML = `Route <em>failed</em>`;
            journeyLine.textContent = "The policy did not reach the destination within 32 moves.";
            return;
        }
        state.timer = window.setTimeout(takePolicyStep, reducedMotion.matches ? 80 : 390);
    }

    function arrive() {
        const targetIndex = squareIndex(state.target.x, state.target.y);
        if (state.pieces.has(targetIndex)) {
            state.pieces.delete(targetIndex);
            state.occupancy[targetIndex] = 0;
        }
        stopPolicy();
        runButton.textContent = "Run again";
        const moveLabel = state.steps === 1 ? "move" : "moves";
        statusLine.innerHTML = `Reached <em>${squareName(state.agent.x, state.agent.y)}</em>`;
        journeyLine.textContent = `${state.steps} ${moveLabel} from ${squareName(state.origin.x, state.origin.y)}.`;
        render();
    }

    function runPolicy() {
        if (!state.policy) return;
        if (state.running) {
            stopPolicy();
            statusLine.innerHTML = `Policy <em>paused</em>`;
            journeyLine.textContent = `Paused on the way to ${squareName(state.target.x, state.target.y)}.`;
            return;
        }
        if (state.agent.x === state.target.x && state.agent.y === state.target.y) {
            state.agent = { ...state.origin };
            state.steps = 0;
            state.trail = [{ ...state.agent }];
        }
        state.running = true;
        boardStage.classList.add("is-running");
        runButton.textContent = "Pause policy";
        statusLine.innerHTML = `Policy <em>running</em>`;
        journeyLine.textContent = `Destination ${squareName(state.target.x, state.target.y)}.`;
        takePolicyStep();
    }

    function stopPolicy() {
        state.running = false;
        boardStage.classList.remove("is-running");
        runButton.textContent = "Run policy";
        if (state.timer) window.clearTimeout(state.timer);
        state.timer = null;
    }

    function setTarget(x, y, autoRun) {
        if (!state.policy) return;
        stopPolicy();
        if (x === state.agent.x && y === state.agent.y) {
            statusLine.innerHTML = `Already at <em>${squareName(x, y)}</em>`;
            return;
        }
        const previousTargetIndex = squareIndex(state.target.x, state.target.y);
        if (state.pieces.has(previousTargetIndex)) {
            state.occupancy[previousTargetIndex] = 1;
        }
        state.target = { x, y };
        state.occupancy[squareIndex(x, y)] = 0;
        state.origin = { ...state.agent };
        state.steps = 0;
        state.trail = [{ ...state.agent }];
        statusLine.innerHTML = `Target <em>${squareName(x, y)}</em>`;
        journeyLine.textContent = "Destination selected. Starting the trained policy.";
        runButton.textContent = "Run policy";
        render();
        if (autoRun) runPolicy();
    }

    function chooseRandomTarget() {
        let x;
        let y;
        do {
            x = randomInteger(0, 7);
            y = randomInteger(0, 7);
        } while (
            (x === state.agent.x && y === state.agent.y) ||
            state.occupancy[squareIndex(x, y)] ||
            !hasRoute(state.agent, { x, y }, state.occupancy)
        );
        setTarget(x, y, false);
    }

    function renderSquares() {
        state.squares.forEach((square, index) => {
            const x = index % BOARD_SIZE;
            const y = Math.floor(index / BOARD_SIZE);
            square.classList.toggle("is-target", x === state.target.x && y === state.target.y);
            const oldPiece = square.querySelector(".piece");
            if (oldPiece) oldPiece.remove();
            const piece = state.pieces.get(index);
            if (piece) {
                const span = document.createElement("span");
                span.className = `piece${piece.white ? " is-white" : ""}`;
                span.textContent = piece.glyph;
                square.appendChild(span);
            }
        });
    }

    function renderFly() {
        fly.style.setProperty("--x", state.agent.x);
        fly.style.setProperty("--y", state.agent.y);
        if (state.trail.length > 1) {
            const previous = state.trail[state.trail.length - 2];
            const angle = Math.atan2(state.agent.y - previous.y, state.agent.x - previous.x) * 180 / Math.PI + 90;
            fly.style.setProperty("--angle", `${angle}deg`);
        }
        moveCounter.textContent = `step ${String(state.steps).padStart(2, "0")}`;
    }

    function resizeTrailCanvas() {
        const size = boardStage.clientWidth;
        const dpr = Math.min(window.devicePixelRatio || 1, 2);
        trailCanvas.width = Math.round(size * dpr);
        trailCanvas.height = Math.round(size * dpr);
        trailCanvas.style.width = `${size}px`;
        trailCanvas.style.height = `${size}px`;
        trailContext.setTransform(dpr, 0, 0, dpr, 0, 0);
        drawTrail();
    }

    function drawTrail() {
        const size = boardStage.clientWidth;
        trailContext.clearRect(0, 0, size, size);
        if (state.trail.length < 2) return;
        const cell = size / BOARD_SIZE;
        trailContext.lineJoin = "bevel";
        trailContext.lineCap = "square";
        trailContext.strokeStyle = "rgba(255, 53, 31, 0.72)";
        trailContext.lineWidth = Math.max(2, size * 0.006);
        trailContext.beginPath();
        state.trail.forEach((point, index) => {
            const x = (point.x + 0.5) * cell;
            const y = (point.y + 0.5) * cell;
            if (index === 0) trailContext.moveTo(x, y);
            else trailContext.lineTo(x, y);
        });
        trailContext.stroke();
    }

    function render() {
        renderSquares();
        renderFly();
        drawTrail();
    }

    function enableControls() {
        [runButton, boardButton].forEach((button) => {
            button.disabled = false;
        });
    }

    async function loadPolicy() {
        try {
            const response = await fetch("data/fly-gambit-policy.json");
            if (!response.ok) throw new Error(`Policy request failed: ${response.status}`);
            state.policy = await response.json();
            if (state.policy.format !== "fly-gambit-policy-v1") throw new Error("Unsupported policy format");
            successMetric.textContent = `${(state.policy.metrics.rolloutSuccessRate * 100).toFixed(1)}%`;
            modelState.textContent = "model ready";
            statusLine.innerHTML = `Ready <em>${squareName(state.target.x, state.target.y)}</em>`;
            journeyLine.textContent = "Choose any open square, or run the example route.";
            enableControls();
            render();
        } catch (error) {
            modelState.textContent = "load failed";
            statusLine.innerHTML = `Policy <em>unavailable</em>`;
            journeyLine.textContent = "The saved model could not be loaded.";
            console.error(error);
        }
    }

    async function loadBridgeProof() {
        try {
            const response = await fetch("data/fly-gambit-bridge-proof.json");
            if (!response.ok) throw new Error(`Bridge proof request failed: ${response.status}`);
            const proof = await response.json();
            if (proof.format !== "fly-gambit-flybody-public-proof-v1") {
                throw new Error("Unsupported bridge proof format");
            }
            if (!proof.verified) throw new Error("Physical rollout is not verified");
            bridgeState.textContent = "Physical simulation verified";
            bridgeSummary.textContent = `${proof.arena.obstacleContactSteps} collisions · ${proof.result.finalTargetDistanceCm.toFixed(3)} cm final error`;
        } catch (error) {
            bridgeState.textContent = "Physical simulation unavailable";
            bridgeSummary.textContent = "The verification record could not be loaded.";
            console.error(error);
        }
    }

    runButton.addEventListener("click", runPolicy);
    boardButton.addEventListener("click", newBoard);
    window.addEventListener("resize", resizeTrailCanvas, { passive: true });

    initializeBoardNodes();
    defaultScenario();
    render();
    resizeTrailCanvas();
    loadPolicy();
    loadBridgeProof();
}());
