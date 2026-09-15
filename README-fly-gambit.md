# Fly's Gambit

Fly's Gambit is the first executable step toward an embodied chess experiment
using [FlyBody](https://github.com/TuragaLab/flybody).

The current prototype contains a real, dependency-free neural network trained
to navigate an 8x8 board around obstacles. The browser loads the saved weights
from `data/fly-gambit-policy.json` and performs inference for every movement;
the displayed route is not precomputed or scripted.

## Reproduce the policy

```bash
node scripts/train_fly_gambit_policy.mjs
python3 server.py
```

Then open `http://127.0.0.1:5000/fly-gambit.html`.

The training script generates randomized board states, obtains the first action
from a shortest-path expert, and trains a 68 → 48 → 8 ReLU/softmax network by
supervised imitation. It writes the architecture, training configuration,
held-out metrics, confusion matrix, and weights into the policy artifact.

## What is trained now

- Input: 64 occupancy values and normalized agent/target coordinates.
- Output: probabilities for eight movement directions.
- Training target: a shortest-path expert on randomized obstacle boards.
- Runtime behavior: choose the highest-probability legal direction until the
  target is reached or the 32-step limit is exhausted.

## Run the FlyBody bridge

The bridge builds a fly-scale chessboard from 64 visual tile geoms, nine
collision-enabled obstacle pieces, and a target marker. It trains a compact CNN
on RGB cell crops from 96 randomized MuJoCo camera layouts, verifies it on 24
held-out layouts, and uses the inferred occupancy—not the symbolic source—to
plan the reference route. That route is sent to the frozen, pretrained FlyBody
walking controller. The TensorFlow stack stays isolated in Docker and runs
natively on Intel or Apple Silicon hosts.

Download Figshare file `44815195` (`trained-fly-policies.zip`), verify its MD5
is `12934d5a1c60631a710bc2b6d297d3ce`, and extract it so the walking checkpoint
is located at:

```text
.context/flybody-data/trained-fly-policies/walking/saved_model.pb
```

Then run:

```bash
sh experiments/flybody-bridge/run.sh
```

The run writes an MP4, camera frame, cached camera-training set, perception
weights, trajectory archive, and machine-readable proof to
`.context/flybody-output/`. The proof only reports `verified: true` when all of
these hold:

- the camera CNN clears its held-out precision, recall, and accuracy floors;
- its reference-board occupancy is an exact 64-cell match;
- all nine obstacle pieces have collision geometry and record no contact;
- the body finishes within target tolerance and terminates cleanly.

The checked-in `data/fly-gambit-bridge-proof.json` is a compact, path-free
record of the verified reference run. The camera CNN scored 100% on 1,536
held-out cell crops and exactly reconstructed the nine-piece reference board.
The physical run then produced 1,011 distinct poses, zero obstacle contacts,
0.023 cm tracking RMSE, and a 0.052 cm final target error against a 0.25 cm
threshold. The checkpoint, camera-training cache, perception weights, video,
PNGs, and full trajectory remain local in `.context/`; they are reproducible
but are not bundled into the website.

## Architecture

The system now has two trained high-level layers above FlyBody:

1. A camera CNN classifies occupied cells from the overhead MuJoCo render.
2. The 68 → 48 → 8 navigation MLP converts that board state into directions.
3. The directions become a smooth center-of-mass reference.
4. The frozen, pretrained FlyBody network converts the reference into detailed
   joint and adhesion control.
5. MuJoCo simulates contacts and records actual-vs-commanded movement.

Keeping the layers separate lets us train chessboard perception and behavior
without retraining FlyBody's expensive low-level locomotion network. The next
research step is closed-loop replanning from the fly's two body-mounted eye
cameras; the current perception frame is an overhead camera with explicit cell
coordinates.
