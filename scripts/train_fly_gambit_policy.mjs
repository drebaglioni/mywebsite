#!/usr/bin/env node

/**
 * Train the high-level Fly's Gambit board-navigation policy.
 *
 * This is deliberately dependency-free so the experiment can be reproduced
 * with the Node runtime already used for the site. The resulting policy maps
 * an 8x8 occupancy grid plus start/goal coordinates to one of eight movement
 * directions. It is the future navigation layer above FlyBody's pretrained
 * low-level locomotion controller; it does not replace that controller.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const BOARD_SIZE = 8;
const INPUT_SIZE = BOARD_SIZE * BOARD_SIZE + 4;
const HIDDEN_SIZE = 48;
const OUTPUT_SIZE = 8;
const DIRECTIONS = [
  { name: "N", dx: 0, dy: -1 },
  { name: "NE", dx: 1, dy: -1 },
  { name: "E", dx: 1, dy: 0 },
  { name: "SE", dx: 1, dy: 1 },
  { name: "S", dx: 0, dy: 1 },
  { name: "SW", dx: -1, dy: 1 },
  { name: "W", dx: -1, dy: 0 },
  { name: "NW", dx: -1, dy: -1 },
];

function parseArgs(argv) {
  const options = {
    seed: 20260911,
    trainSamples: 14000,
    testSamples: 2500,
    epochs: 22,
    batchSize: 64,
    learningRate: 0.035,
    output: null,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const value = argv[index + 1];
    if (argument === "--seed") options.seed = Number(value);
    if (argument === "--train-samples") options.trainSamples = Number(value);
    if (argument === "--test-samples") options.testSamples = Number(value);
    if (argument === "--epochs") options.epochs = Number(value);
    if (argument === "--batch-size") options.batchSize = Number(value);
    if (argument === "--learning-rate") options.learningRate = Number(value);
    if (argument === "--output") options.output = value;
    if (argument.startsWith("--")) index += 1;
  }

  return options;
}

function mulberry32(seed) {
  let state = seed >>> 0;
  return () => {
    state += 0x6d2b79f5;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

function randomInteger(random, minimum, maximum) {
  return minimum + Math.floor(random() * (maximum - minimum + 1));
}

function indexFor(x, y) {
  return y * BOARD_SIZE + x;
}

function insideBoard(x, y) {
  return x >= 0 && x < BOARD_SIZE && y >= 0 && y < BOARD_SIZE;
}

function distancesFromTarget(occupancy, targetX, targetY) {
  const distances = new Int16Array(BOARD_SIZE * BOARD_SIZE);
  distances.fill(-1);
  const queueX = new Int8Array(BOARD_SIZE * BOARD_SIZE);
  const queueY = new Int8Array(BOARD_SIZE * BOARD_SIZE);
  let head = 0;
  let tail = 0;
  queueX[tail] = targetX;
  queueY[tail] = targetY;
  tail += 1;
  distances[indexFor(targetX, targetY)] = 0;

  while (head < tail) {
    const x = queueX[head];
    const y = queueY[head];
    const currentDistance = distances[indexFor(x, y)];
    head += 1;

    for (const direction of DIRECTIONS) {
      const nextX = x + direction.dx;
      const nextY = y + direction.dy;
      if (!insideBoard(nextX, nextY)) continue;
      const nextIndex = indexFor(nextX, nextY);
      if (occupancy[nextIndex] || distances[nextIndex] >= 0) continue;
      distances[nextIndex] = currentDistance + 1;
      queueX[tail] = nextX;
      queueY[tail] = nextY;
      tail += 1;
    }
  }

  return distances;
}

function encodeState(occupancy, agentX, agentY, targetX, targetY) {
  const input = new Float32Array(INPUT_SIZE);
  for (let index = 0; index < occupancy.length; index += 1) {
    input[index] = occupancy[index] ? 1 : 0;
  }
  input[64] = (agentX / 7) * 2 - 1;
  input[65] = (agentY / 7) * 2 - 1;
  input[66] = (targetX / 7) * 2 - 1;
  input[67] = (targetY / 7) * 2 - 1;
  return input;
}

function buildExample(random) {
  for (;;) {
    const occupancy = new Uint8Array(BOARD_SIZE * BOARD_SIZE);
    const obstacleCount = randomInteger(random, 3, 18);
    for (let count = 0; count < obstacleCount; count += 1) {
      occupancy[randomInteger(random, 0, occupancy.length - 1)] = 1;
    }

    const agentX = randomInteger(random, 0, BOARD_SIZE - 1);
    const agentY = randomInteger(random, 0, BOARD_SIZE - 1);
    let targetX = randomInteger(random, 0, BOARD_SIZE - 1);
    let targetY = randomInteger(random, 0, BOARD_SIZE - 1);
    if (agentX === targetX && agentY === targetY) {
      targetX = (targetX + 3) % BOARD_SIZE;
      targetY = (targetY + 5) % BOARD_SIZE;
    }

    occupancy[indexFor(agentX, agentY)] = 0;
    occupancy[indexFor(targetX, targetY)] = 0;
    const distances = distancesFromTarget(occupancy, targetX, targetY);
    const agentDistance = distances[indexFor(agentX, agentY)];
    if (agentDistance <= 0) continue;

    let label = -1;
    let bestDistance = Number.POSITIVE_INFINITY;
    let bestAlignment = Number.NEGATIVE_INFINITY;
    const goalLength = Math.hypot(targetX - agentX, targetY - agentY) || 1;

    DIRECTIONS.forEach((direction, directionIndex) => {
      const nextX = agentX + direction.dx;
      const nextY = agentY + direction.dy;
      if (!insideBoard(nextX, nextY)) return;
      const nextDistance = distances[indexFor(nextX, nextY)];
      if (nextDistance < 0) return;
      const alignment = (
        direction.dx * (targetX - agentX) +
        direction.dy * (targetY - agentY)
      ) / goalLength;
      if (
        nextDistance < bestDistance ||
        (nextDistance === bestDistance && alignment > bestAlignment)
      ) {
        bestDistance = nextDistance;
        bestAlignment = alignment;
        label = directionIndex;
      }
    });

    if (label < 0) continue;
    return {
      input: encodeState(occupancy, agentX, agentY, targetX, targetY),
      label,
      state: { occupancy, agentX, agentY, targetX, targetY },
    };
  }
}

function buildDataset(random, count) {
  return Array.from({ length: count }, () => buildExample(random));
}

function gaussian(random) {
  const first = Math.max(random(), Number.EPSILON);
  const second = random();
  return Math.sqrt(-2 * Math.log(first)) * Math.cos(2 * Math.PI * second);
}

function initializeWeights(random, fanIn, fanOut) {
  const scale = Math.sqrt(2 / fanIn);
  return Float32Array.from(
    { length: fanIn * fanOut },
    () => gaussian(random) * scale,
  );
}

function softmax(logits) {
  let maximum = Number.NEGATIVE_INFINITY;
  for (const value of logits) maximum = Math.max(maximum, value);
  const probabilities = new Float32Array(logits.length);
  let total = 0;
  for (let index = 0; index < logits.length; index += 1) {
    probabilities[index] = Math.exp(logits[index] - maximum);
    total += probabilities[index];
  }
  for (let index = 0; index < probabilities.length; index += 1) {
    probabilities[index] /= total;
  }
  return probabilities;
}

function forward(model, input) {
  const hidden = new Float32Array(HIDDEN_SIZE);
  for (let hiddenIndex = 0; hiddenIndex < HIDDEN_SIZE; hiddenIndex += 1) {
    let activation = model.b1[hiddenIndex];
    const offset = hiddenIndex * INPUT_SIZE;
    for (let inputIndex = 0; inputIndex < INPUT_SIZE; inputIndex += 1) {
      activation += model.w1[offset + inputIndex] * input[inputIndex];
    }
    hidden[hiddenIndex] = Math.max(0, activation);
  }

  const logits = new Float32Array(OUTPUT_SIZE);
  for (let outputIndex = 0; outputIndex < OUTPUT_SIZE; outputIndex += 1) {
    let activation = model.b2[outputIndex];
    const offset = outputIndex * HIDDEN_SIZE;
    for (let hiddenIndex = 0; hiddenIndex < HIDDEN_SIZE; hiddenIndex += 1) {
      activation += model.w2[offset + hiddenIndex] * hidden[hiddenIndex];
    }
    logits[outputIndex] = activation;
  }

  return { hidden, probabilities: softmax(logits) };
}

function argmax(values) {
  let bestIndex = 0;
  for (let index = 1; index < values.length; index += 1) {
    if (values[index] > values[bestIndex]) bestIndex = index;
  }
  return bestIndex;
}

function shuffle(random, items) {
  for (let index = items.length - 1; index > 0; index -= 1) {
    const swapIndex = randomInteger(random, 0, index);
    [items[index], items[swapIndex]] = [items[swapIndex], items[index]];
  }
}

function train(model, dataset, random, options) {
  const gradientW1 = new Float32Array(model.w1.length);
  const gradientB1 = new Float32Array(model.b1.length);
  const gradientW2 = new Float32Array(model.w2.length);
  const gradientB2 = new Float32Array(model.b2.length);
  const velocityW1 = new Float32Array(model.w1.length);
  const velocityB1 = new Float32Array(model.b1.length);
  const velocityW2 = new Float32Array(model.w2.length);
  const velocityB2 = new Float32Array(model.b2.length);
  const momentum = 0.88;

  for (let epoch = 0; epoch < options.epochs; epoch += 1) {
    shuffle(random, dataset);
    const learningRate = options.learningRate * (1 - 0.72 * epoch / options.epochs);

    for (let batchStart = 0; batchStart < dataset.length; batchStart += options.batchSize) {
      gradientW1.fill(0);
      gradientB1.fill(0);
      gradientW2.fill(0);
      gradientB2.fill(0);
      const batchEnd = Math.min(dataset.length, batchStart + options.batchSize);

      for (let sampleIndex = batchStart; sampleIndex < batchEnd; sampleIndex += 1) {
        const sample = dataset[sampleIndex];
        const { hidden, probabilities } = forward(model, sample.input);
        const outputGradient = Float32Array.from(probabilities);
        outputGradient[sample.label] -= 1;

        for (let outputIndex = 0; outputIndex < OUTPUT_SIZE; outputIndex += 1) {
          gradientB2[outputIndex] += outputGradient[outputIndex];
          const offset = outputIndex * HIDDEN_SIZE;
          for (let hiddenIndex = 0; hiddenIndex < HIDDEN_SIZE; hiddenIndex += 1) {
            gradientW2[offset + hiddenIndex] += outputGradient[outputIndex] * hidden[hiddenIndex];
          }
        }

        for (let hiddenIndex = 0; hiddenIndex < HIDDEN_SIZE; hiddenIndex += 1) {
          if (hidden[hiddenIndex] <= 0) continue;
          let hiddenGradient = 0;
          for (let outputIndex = 0; outputIndex < OUTPUT_SIZE; outputIndex += 1) {
            hiddenGradient += (
              model.w2[outputIndex * HIDDEN_SIZE + hiddenIndex] *
              outputGradient[outputIndex]
            );
          }
          gradientB1[hiddenIndex] += hiddenGradient;
          const offset = hiddenIndex * INPUT_SIZE;
          for (let inputIndex = 0; inputIndex < INPUT_SIZE; inputIndex += 1) {
            gradientW1[offset + inputIndex] += hiddenGradient * sample.input[inputIndex];
          }
        }
      }

      const scale = learningRate / (batchEnd - batchStart);
      const update = (weights, gradients, velocities) => {
        for (let index = 0; index < weights.length; index += 1) {
          velocities[index] = momentum * velocities[index] - scale * gradients[index];
          weights[index] += velocities[index];
        }
      };
      update(model.w1, gradientW1, velocityW1);
      update(model.b1, gradientB1, velocityB1);
      update(model.w2, gradientW2, velocityW2);
      update(model.b2, gradientB2, velocityB2);
    }
  }
}

function evaluate(model, dataset) {
  let correct = 0;
  let loss = 0;
  const confusion = Array.from(
    { length: OUTPUT_SIZE },
    () => Array(OUTPUT_SIZE).fill(0),
  );
  for (const sample of dataset) {
    const { probabilities } = forward(model, sample.input);
    const prediction = argmax(probabilities);
    if (prediction === sample.label) correct += 1;
    confusion[sample.label][prediction] += 1;
    loss -= Math.log(Math.max(probabilities[sample.label], 1e-8));
  }
  return {
    accuracy: correct / dataset.length,
    crossEntropy: loss / dataset.length,
    confusion,
  };
}

function evaluateRollouts(model, dataset, maxSteps = 32) {
  let successes = 0;
  let successfulSteps = 0;

  for (const sample of dataset) {
    const { occupancy, targetX, targetY } = sample.state;
    let { agentX, agentY } = sample.state;
    let arrived = false;

    for (let step = 1; step <= maxSteps; step += 1) {
      const { probabilities } = forward(
        model,
        encodeState(occupancy, agentX, agentY, targetX, targetY),
      );
      const rankedDirections = Array.from(
        { length: OUTPUT_SIZE },
        (_, index) => index,
      ).sort((left, right) => probabilities[right] - probabilities[left]);
      let moved = false;

      for (const directionIndex of rankedDirections) {
        const direction = DIRECTIONS[directionIndex];
        const nextX = agentX + direction.dx;
        const nextY = agentY + direction.dy;
        if (!insideBoard(nextX, nextY)) continue;
        if (occupancy[indexFor(nextX, nextY)]) continue;
        agentX = nextX;
        agentY = nextY;
        moved = true;
        break;
      }

      if (!moved) break;
      if (agentX === targetX && agentY === targetY) {
        successes += 1;
        successfulSteps += step;
        arrived = true;
        break;
      }
    }

    if (!arrived) successfulSteps += 0;
  }

  return {
    successRate: successes / dataset.length,
    meanSuccessfulSteps: successes ? successfulSteps / successes : null,
    maxSteps,
  };
}

const options = parseArgs(process.argv.slice(2));
const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const repository = path.resolve(scriptDirectory, "..");
const outputPath = path.resolve(
  options.output || path.join(repository, "data", "fly-gambit-policy.json"),
);
const random = mulberry32(options.seed);
const trainingData = buildDataset(random, options.trainSamples);
const testData = buildDataset(random, options.testSamples);
const model = {
  w1: initializeWeights(random, INPUT_SIZE, HIDDEN_SIZE),
  b1: new Float32Array(HIDDEN_SIZE),
  w2: initializeWeights(random, HIDDEN_SIZE, OUTPUT_SIZE),
  b2: new Float32Array(OUTPUT_SIZE),
};

train(model, trainingData, random, options);
const trainingMetrics = evaluate(model, trainingData);
const testMetrics = evaluate(model, testData);
const rolloutMetrics = evaluateRollouts(model, testData);
const artifact = {
  format: "fly-gambit-policy-v1",
  createdAt: new Date().toISOString(),
  seed: options.seed,
  purpose: "High-level chessboard navigation policy for a future FlyBody locomotion bridge.",
  limitations: "Uses symbolic board occupancy; it is not the FlyBody motor controller and does not choose chess moves.",
  boardSize: BOARD_SIZE,
  inputs: ["occupancy[64]", "agent_x", "agent_y", "target_x", "target_y"],
  directions: DIRECTIONS.map(({ name, dx, dy }) => ({ name, dx, dy })),
  architecture: {
    inputSize: INPUT_SIZE,
    hiddenLayers: [HIDDEN_SIZE],
    activation: "relu",
    outputSize: OUTPUT_SIZE,
    outputActivation: "softmax",
  },
  training: {
    method: "supervised imitation of shortest-path expert",
    trainSamples: options.trainSamples,
    testSamples: options.testSamples,
    epochs: options.epochs,
    batchSize: options.batchSize,
    initialLearningRate: options.learningRate,
    obstacleRange: [3, 18],
  },
  metrics: {
    trainAccuracy: trainingMetrics.accuracy,
    testAccuracy: testMetrics.accuracy,
    testCrossEntropy: testMetrics.crossEntropy,
    rolloutSuccessRate: rolloutMetrics.successRate,
    meanSuccessfulSteps: rolloutMetrics.meanSuccessfulSteps,
    rolloutMaxSteps: rolloutMetrics.maxSteps,
    testConfusion: testMetrics.confusion,
  },
  weights: {
    w1: Array.from(model.w1),
    b1: Array.from(model.b1),
    w2: Array.from(model.w2),
    b2: Array.from(model.b2),
  },
};

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, `${JSON.stringify(artifact)}\n`);
console.log(`Wrote ${outputPath}`);
console.log(`Train accuracy: ${(trainingMetrics.accuracy * 100).toFixed(2)}%`);
console.log(`Test accuracy: ${(testMetrics.accuracy * 100).toFixed(2)}%`);
console.log(`Test cross-entropy: ${testMetrics.crossEntropy.toFixed(4)}`);
console.log(`Rollout success: ${(rolloutMetrics.successRate * 100).toFixed(2)}%`);
