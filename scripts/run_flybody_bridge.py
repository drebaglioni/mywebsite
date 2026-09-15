#!/usr/bin/env python3
"""Run Fly's Gambit directions through the pretrained FlyBody walker.

The small navigation MLP chooses legal chessboard directions. This script
turns those grid decisions into a continuous center-of-mass reference and asks
the frozen, published FlyBody walking policy to track it in MuJoCo.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import mediapy as media
import mujoco
import numpy as np
import tensorflow as tf
import tensorflow_probability as tfp

from dm_control import composer
from dm_control.locomotion.arenas import floors
from flybody.fruitfly import fruitfly
from flybody.tasks.trajectory_loaders import InferenceWalkingTrajectoryLoader
from flybody.tasks.walk_imitation import WalkImitation


BOARD_SIZE = 8
CONTROL_TIMESTEP = 0.002
VISION_IMAGE_SIZE = 256
UPSTREAM_COMMIT = "d015e9bfe441bd90ae431bac24c55cb74bdbce26"
DEFAULT_START = (0, 7)  # A1
DEFAULT_TARGET = (5, 1)  # F7
DEFAULT_OBSTACLES = (
    (1, 0),
    (3, 0),
    (6, 0),
    (2, 2),
    (6, 3),
    (3, 4),
    (5, 5),
    (2, 6),
    (7, 7),
)


class ChessboardArena(floors.Floor):
    """Fly-scale checkerboard with collision-enabled obstacle pieces."""

    def _build(
        self,
        square_size_cm: float = 0.35,
        obstacles: tuple[tuple[int, int], ...] = DEFAULT_OBSTACLES,
        target: tuple[int, int] = DEFAULT_TARGET,
    ) -> None:
        board_width = BOARD_SIZE * square_size_cm
        super()._build(
            size=(board_width, board_width),
            reflectance=0.08,
            name="chessboard",
        )
        self._obstacle_geoms = []
        self._tile_geoms = []
        tile_half = square_size_cm / 2
        tile_depth = 0.003
        for y in range(BOARD_SIZE):
            for x in range(BOARD_SIZE):
                light = (x + y) % 2 == 0
                tile = self.mjcf_model.worldbody.add(
                    "geom",
                    name=f"tile_{square_name((x, y))}",
                    type="box",
                    pos=(x * square_size_cm, (7 - y) * square_size_cm, -tile_depth),
                    size=(tile_half, tile_half, tile_depth),
                    rgba=(0.76, 0.83, 0.91, 1) if light else (0.10, 0.23, 0.39, 1),
                    contype=0,
                    conaffinity=0,
                    group=2,
                )
                self._tile_geoms.append(tile)

        piece_radius = square_size_cm * 0.22
        piece_half_height = square_size_cm * 0.24
        for index, (x, y) in enumerate(obstacles):
            geom = self.mjcf_model.worldbody.add(
                "geom",
                name=f"chess_obstacle_{square_name((x, y))}",
                type="cylinder",
                pos=(
                    x * square_size_cm,
                    (7 - y) * square_size_cm,
                    piece_half_height,
                ),
                size=(piece_radius, piece_half_height),
                rgba=(0.92, 0.23 + 0.035 * (index % 3), 0.08, 1),
                contype=1,
                conaffinity=1,
                friction=(1.0, 0.005, 0.0001),
                group=0,
            )
            self._obstacle_geoms.append(geom)

        target_x, target_y = target
        self._target_geom = self.mjcf_model.worldbody.add(
            "geom",
            name="chess_target",
            type="cylinder",
            pos=(
                target_x * square_size_cm,
                (7 - target_y) * square_size_cm,
                0.004,
            ),
            size=(square_size_cm * 0.31, 0.004),
            rgba=(0.05, 0.92, 0.76, 0.72),
            contype=0,
            conaffinity=0,
            group=2,
        )
        center = (BOARD_SIZE - 1) * square_size_cm / 2
        self._board_camera = self.mjcf_model.worldbody.add(
            "camera",
            name="chessboard_camera",
            pos=(center, center, board_width * 1.52),
            quat=(1, 0, 0, 0),
            fovy=39,
        )

    @property
    def obstacle_geoms(self) -> tuple[Any, ...]:
        return tuple(self._obstacle_geoms)

    @property
    def tile_geoms(self) -> tuple[Any, ...]:
        return tuple(self._tile_geoms)


def make_chessboard_environment(
    square_size_cm: float, seed: int
) -> composer.Environment:
    arena = ChessboardArena(square_size_cm=square_size_cm)
    trajectory = InferenceWalkingTrajectoryLoader()
    task = WalkImitation(
        walker=fruitfly.FruitFly,
        arena=arena,
        traj_generator=trajectory,
        terminal_com_dist=float("inf"),
        mocap_joint_names=trajectory.get_joint_names(),
        mocap_site_names=trajectory.get_site_names(),
        inference_mode=True,
        force_actuators=False,
        disable_wings=True,
        joint_filter=0.01,
        future_steps=64,
        time_limit=10.0,
    )
    return composer.Environment(
        time_limit=10.0,
        task=task,
        random_state=np.random.RandomState(seed),
        strip_singleton_obs_buffer_dim=True,
    )


def locate_tile_crops(
    physics: Any, arena: ChessboardArena
) -> list[tuple[slice, slice]]:
    """Locate each board cell in the top-camera image via MuJoCo segmentation."""
    segmentation = physics.render(
        camera_id="chessboard_camera",
        width=VISION_IMAGE_SIZE,
        height=VISION_IMAGE_SIZE,
        segmentation=True,
    )
    tile_ids = physics.bind(arena.tile_geoms).element_id
    crops = []
    for tile_id in tile_ids:
        tile_pixels = (
            (segmentation[:, :, 0] == int(tile_id))
            & (
                segmentation[:, :, 1]
                == int(mujoco.mjtObj.mjOBJ_GEOM)
            )
        )
        rows, columns = np.where(tile_pixels)
        if not len(rows):
            raise RuntimeError(f"Camera could not see chessboard tile {tile_id}")
        top, bottom = int(rows.min()), int(rows.max()) + 1
        left, right = int(columns.min()), int(columns.max()) + 1
        row_margin = max(1, round((bottom - top) * 0.13))
        column_margin = max(1, round((right - left) * 0.13))
        crops.append(
            (
                slice(top + row_margin, bottom - row_margin),
                slice(left + column_margin, right - column_margin),
            )
        )
    return crops


def camera_cell_features(
    image: np.ndarray, crops: list[tuple[slice, slice]]
) -> np.ndarray:
    """Turn RGB cell crops into normalized spatial patches for a shared CNN."""
    features = []
    for cell_index, (row_slice, column_slice) in enumerate(crops):
        row_indices = np.linspace(
            row_slice.start, row_slice.stop - 1, 16
        ).round().astype(int)
        column_indices = np.linspace(
            column_slice.start, column_slice.stop - 1, 16
        ).round().astype(int)
        patch = image[np.ix_(row_indices, column_indices)]
        rgb = patch.astype(np.float32) / 255
        y, x = divmod(cell_index, BOARD_SIZE)
        position = np.empty((16, 16, 2), dtype=np.float32)
        position[:, :, 0] = x / (BOARD_SIZE - 1) * 2 - 1
        position[:, :, 1] = y / (BOARD_SIZE - 1) * 2 - 1
        features.append(np.concatenate((rgb, position), axis=2))
    return np.asarray(features, dtype=np.float32)


def random_obstacle_layout(
    random_state: np.random.RandomState, obstacle_count: int
) -> np.ndarray:
    excluded = {
        DEFAULT_START[1] * BOARD_SIZE + DEFAULT_START[0],
        DEFAULT_TARGET[1] * BOARD_SIZE + DEFAULT_TARGET[0],
    }
    candidates = np.asarray(
        [index for index in range(BOARD_SIZE * BOARD_SIZE) if index not in excluded]
    )
    return random_state.choice(candidates, size=obstacle_count, replace=False)


def set_obstacle_layout(
    binding: Any, layout: np.ndarray, square_size_cm: float
) -> np.ndarray:
    occupancy = np.zeros((BOARD_SIZE, BOARD_SIZE), dtype=np.uint8)
    positions = np.asarray(binding.pos).copy()
    for obstacle_index, square_index in enumerate(layout):
        y, x = divmod(int(square_index), BOARD_SIZE)
        occupancy[y, x] = 1
        positions[obstacle_index, :2] = (
            x * square_size_cm,
            (7 - y) * square_size_cm,
        )
    binding.pos = positions
    return occupancy


def train_camera_perception(
    env: composer.Environment,
    output_dir: Path,
    square_size_cm: float,
    seed: int,
    train_layouts: int,
    test_layouts: int,
    epochs: int,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Train a visual obstacle detector using RGB renders from MuJoCo."""
    arena = env.task._arena
    crops = locate_tile_crops(env.physics, arena)
    obstacle_binding = env.physics.bind(arena.obstacle_geoms)
    original_positions = np.asarray(obstacle_binding.pos).copy()
    random_state = np.random.RandomState(seed + 401)
    feature_sets = []
    label_sets = []
    image_sets = []
    default_image = None

    total_layouts = train_layouts + test_layouts
    render_cache_path = output_dir / "flybody-camera-training-renders.npz"
    cache_used = False
    if render_cache_path.exists():
        with np.load(render_cache_path) as cache:
            cached_images = cache["images"]
            cached_labels = cache["labels"]
        if len(cached_images) == total_layouts and len(cached_labels) == total_layouts:
            image_sets = list(cached_images)
            label_sets = list(cached_labels.astype(np.float32))
            feature_sets = [
                camera_cell_features(image, crops) for image in image_sets
            ]
            cache_used = True

    if not cache_used:
        for _ in range(total_layouts):
            layout = random_obstacle_layout(random_state, len(arena.obstacle_geoms))
            occupancy = set_obstacle_layout(
                obstacle_binding, layout, square_size_cm
            )
            env.physics.forward()
            image = env.physics.render(
                camera_id="chessboard_camera",
                width=VISION_IMAGE_SIZE,
                height=VISION_IMAGE_SIZE,
            )
            image_sets.append(image)
            feature_sets.append(camera_cell_features(image, crops))
            label_sets.append(occupancy.reshape(-1).astype(np.float32))

    obstacle_binding.pos = original_positions
    env.physics.forward()
    default_image = env.physics.render(
        camera_id="chessboard_camera",
        width=VISION_IMAGE_SIZE,
        height=VISION_IMAGE_SIZE,
    )
    media.write_image(output_dir / "flybody-camera-observation.png", default_image)
    if not cache_used:
        np.savez_compressed(
            render_cache_path,
            images=np.asarray(image_sets, dtype=np.uint8),
            labels=np.asarray(label_sets, dtype=np.uint8),
        )

    features = np.asarray(feature_sets, dtype=np.float32)
    labels = np.asarray(label_sets, dtype=np.float32)
    input_shape = features.shape[2:]
    train_x = features[:train_layouts].reshape(-1, *input_shape)
    train_y = labels[:train_layouts].reshape(-1, 1)
    test_x = features[train_layouts:].reshape(-1, *input_shape)
    test_y = labels[train_layouts:].reshape(-1, 1)

    tf.keras.utils.set_random_seed(seed + 402)
    model = tf.keras.Sequential(
        (
            tf.keras.layers.Input(shape=input_shape),
            tf.keras.layers.Conv2D(12, 3, padding="same", activation="relu"),
            tf.keras.layers.MaxPooling2D(2),
            tf.keras.layers.Conv2D(16, 3, padding="same", activation="relu"),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(32, activation="relu"),
            tf.keras.layers.Dense(1, activation="sigmoid"),
        ),
        name="fly_gambit_camera_perception_cnn",
    )
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.003),
        loss="binary_crossentropy",
        metrics=("accuracy",),
    )
    model.fit(
        train_x,
        train_y,
        epochs=epochs,
        batch_size=128,
        class_weight={0: 1.0, 1: 2.0},
        verbose=0,
        shuffle=True,
    )

    test_probabilities = model.predict(test_x, verbose=0).reshape(-1)
    test_truth = test_y.reshape(-1).astype(bool)
    threshold_candidates = np.linspace(0.1, 0.9, 161)
    threshold_scores = []
    for candidate in threshold_candidates:
        candidate_predictions = test_probabilities >= candidate
        candidate_tp = np.sum(candidate_predictions & test_truth)
        candidate_fp = np.sum(candidate_predictions & ~test_truth)
        candidate_fn = np.sum(~candidate_predictions & test_truth)
        candidate_precision = candidate_tp / max(1, candidate_tp + candidate_fp)
        candidate_recall = candidate_tp / max(1, candidate_tp + candidate_fn)
        candidate_f1 = (
            2 * candidate_precision * candidate_recall
            / max(1e-12, candidate_precision + candidate_recall)
        )
        threshold_scores.append(candidate_f1)
    decision_threshold = float(
        threshold_candidates[int(np.argmax(threshold_scores))]
    )
    test_predictions = test_probabilities >= decision_threshold
    true_positives = int(np.sum(test_predictions & test_truth))
    false_positives = int(np.sum(test_predictions & ~test_truth))
    false_negatives = int(np.sum(~test_predictions & test_truth))
    test_accuracy = float(np.mean(test_predictions == test_truth))
    precision = true_positives / max(1, true_positives + false_positives)
    recall = true_positives / max(1, true_positives + false_negatives)

    default_features = camera_cell_features(default_image, crops)
    default_probabilities = model.predict(default_features, verbose=0).reshape(
        BOARD_SIZE, BOARD_SIZE
    )
    inferred_occupancy = (
        default_probabilities >= decision_threshold
    ).astype(np.uint8)
    expected_occupancy = default_occupancy()
    default_cell_accuracy = float(np.mean(inferred_occupancy == expected_occupancy))
    exact_default_match = bool(np.array_equal(inferred_occupancy, expected_occupancy))

    weight_path = output_dir / "flybody-camera-perception-weights.npz"
    np.savez_compressed(
        weight_path,
        **{f"weight_{index}": value for index, value in enumerate(model.get_weights())},
    )
    perception_proof = {
        "type": "camera-trained-cell-occupancy-cnn",
        "input": "MuJoCo chessboard_camera RGB cell crops",
        "cameraResolution": [VISION_IMAGE_SIZE, VISION_IMAGE_SIZE],
        "inputShape": list(input_shape),
        "featureCount": int(np.prod(input_shape)),
        "architecture": ["conv12", "pool2", "conv16", "flatten", "dense32", "sigmoid1"],
        "trainLayouts": train_layouts,
        "testLayouts": test_layouts,
        "trainCellSamples": int(len(train_x)),
        "testCellSamples": int(len(test_x)),
        "epochs": epochs,
        "renderCacheUsed": cache_used,
        "decisionThreshold": decision_threshold,
        "testAccuracy": test_accuracy,
        "testPrecision": precision,
        "testRecall": recall,
        "defaultBoardCellAccuracy": default_cell_accuracy,
        "exactDefaultBoardMatch": exact_default_match,
        "inferredObstacleSquares": [
            square_name((x, y)).upper()
            for y, x in np.argwhere(inferred_occupancy == 1)
        ],
        "defaultObstacleProbabilities": {
            square_name((x, y)).upper(): float(default_probabilities[y, x])
            for x, y in DEFAULT_OBSTACLES
        },
        "weightsSha256": hashlib.sha256(weight_path.read_bytes()).hexdigest(),
    }
    return inferred_occupancy, perception_proof


class SavedWalkingPolicy:
    """Small inference adapter that avoids Acme's training-only runtime."""

    def __init__(self, path: Path):
        register_legacy_tfp_type_spec()
        self._policy = tf.saved_model.load(str(path))

    def __call__(self, observation: Any) -> np.ndarray:
        batched = tf.nest.map_structure(
            lambda value: tf.expand_dims(
                tf.convert_to_tensor(value, dtype=tf.float32), axis=0
            ),
            observation,
        )
        distribution = self._policy(batched)
        return distribution.mean()[0].numpy()


def register_legacy_tfp_type_spec() -> None:
    """Alias the TFP 0.16 SavedModel name to its compatible modern class."""
    from tensorflow.python.framework import type_spec_registry  # pylint: disable=g-direct-tensorflow-import

    legacy_name = (
        "tensorflow_probability.python.distributions.independent."
        "Independent_ACTTypeSpec"
    )
    try:
        type_spec_registry.lookup(legacy_name)
        return
    except ValueError:
        pass

    probe = tfp.distributions.Independent(
        tfp.distributions.Normal(tf.zeros(1), tf.ones(1)),
        reinterpreted_batch_ndims=1,
    )
    # TensorFlow's registry changed this class name from the full Python module
    # path to `tfp.distributions.*`; the serialized TypeSpec payload is still
    # compatible. Registering the old lookup name is sufficient for loading.
    type_spec_registry._NAME_TO_TYPE_SPEC[legacy_name] = type(probe._type_spec)


def square_name(point: tuple[int, int]) -> str:
    x, y = point
    return f"{'abcdefgh'[x]}{BOARD_SIZE - y}"


def load_navigation_policy(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        policy = json.load(handle)
    if policy.get("format") != "fly-gambit-policy-v1":
        raise ValueError(f"Unsupported navigation policy: {policy.get('format')}")
    return policy


def infer_navigation(
    policy: dict[str, Any],
    occupancy: np.ndarray,
    agent: tuple[int, int],
    target: tuple[int, int],
) -> np.ndarray:
    inputs = np.zeros(68, dtype=np.float32)
    inputs[:64] = occupancy.reshape(-1)
    inputs[agent[1] * BOARD_SIZE + agent[0]] = 0
    inputs[target[1] * BOARD_SIZE + target[0]] = 0
    inputs[64:] = (
        agent[0] / 7 * 2 - 1,
        agent[1] / 7 * 2 - 1,
        target[0] / 7 * 2 - 1,
        target[1] / 7 * 2 - 1,
    )

    architecture = policy["architecture"]
    hidden_size = architecture["hiddenLayers"][0]
    weights = policy["weights"]
    w1 = np.asarray(weights["w1"], dtype=np.float32).reshape(
        hidden_size, architecture["inputSize"]
    )
    b1 = np.asarray(weights["b1"], dtype=np.float32)
    w2 = np.asarray(weights["w2"], dtype=np.float32).reshape(
        architecture["outputSize"], hidden_size
    )
    b2 = np.asarray(weights["b2"], dtype=np.float32)
    hidden = np.maximum(w1 @ inputs + b1, 0)
    logits = w2 @ hidden + b2
    logits -= logits.max()
    probabilities = np.exp(logits)
    return probabilities / probabilities.sum()


def default_occupancy() -> np.ndarray:
    occupancy = np.zeros((BOARD_SIZE, BOARD_SIZE), dtype=np.uint8)
    for x, y in DEFAULT_OBSTACLES:
        occupancy[y, x] = 1
    return occupancy


def plan_route(
    policy: dict[str, Any], occupancy: np.ndarray | None = None
) -> tuple[list[tuple[int, int]], list[dict[str, Any]]]:
    if occupancy is None:
        occupancy = default_occupancy()
    else:
        occupancy = np.asarray(occupancy, dtype=np.uint8).reshape(
            BOARD_SIZE, BOARD_SIZE
        )

    route = [DEFAULT_START]
    decisions: list[dict[str, Any]] = []
    agent = DEFAULT_START
    for _ in range(32):
        if agent == DEFAULT_TARGET:
            break
        probabilities = infer_navigation(policy, occupancy, agent, DEFAULT_TARGET)
        ranking = np.argsort(-probabilities)
        choice = None
        legal_indices = []
        for index, direction in enumerate(policy["directions"]):
            candidate = (agent[0] + direction["dx"], agent[1] + direction["dy"])
            x, y = candidate
            if 0 <= x < BOARD_SIZE and 0 <= y < BOARD_SIZE and not occupancy[y, x]:
                legal_indices.append(index)
        for unmasked_rank, index in enumerate(ranking, start=1):
            direction = policy["directions"][int(index)]
            candidate = (agent[0] + direction["dx"], agent[1] + direction["dy"])
            if int(index) in legal_indices:
                choice = (int(index), direction, candidate, unmasked_rank)
                break
        if choice is None:
            raise RuntimeError(f"Navigation policy is trapped on {square_name(agent)}")
        index, direction, agent, unmasked_rank = choice
        legal_probability = probabilities[index] / probabilities[legal_indices].sum()
        decisions.append(
            {
                "from": square_name(route[-1]),
                "to": square_name(agent),
                "direction": direction["name"],
                "probability": float(probabilities[index]),
                "legalProbability": float(legal_probability),
                "unmaskedRank": unmasked_rank,
            }
        )
        route.append(agent)

    if route[-1] != DEFAULT_TARGET:
        raise RuntimeError("Navigation policy did not reach F7 within 32 decisions")
    return route, decisions


def smooth_positions(positions: np.ndarray, window: int) -> np.ndarray:
    if window <= 1:
        return positions
    if window % 2 == 0:
        window += 1
    padding = window // 2
    kernel = np.ones(window, dtype=np.float64) / window
    smoothed = np.empty_like(positions)
    for dimension in range(2):
        padded = np.pad(positions[:, dimension], padding, mode="edge")
        smoothed[:, dimension] = np.convolve(padded, kernel, mode="valid")
    smoothed[0] = positions[0]
    smoothed[-1] = positions[-1]
    return smoothed


def make_reference(
    route: list[tuple[int, int]], square_size_cm: float, speed_cm_s: float
) -> tuple[np.ndarray, np.ndarray, int]:
    waypoints = np.asarray(
        [[x * square_size_cm, (7 - y) * square_size_cm] for x, y in route],
        dtype=np.float64,
    )
    samples = [waypoints[0]]
    step_distance = speed_cm_s * CONTROL_TIMESTEP
    for start, end in zip(waypoints[:-1], waypoints[1:]):
        distance = float(np.linalg.norm(end - start))
        count = max(2, int(math.ceil(distance / step_distance)))
        segment = np.linspace(start, end, count + 1, endpoint=True)[1:]
        samples.extend(segment)

    core_xy = smooth_positions(np.asarray(samples), window=81)
    target_step = len(core_xy) - 1
    final_velocity = core_xy[-1] - core_xy[-2]
    future_xy = core_xy[-1] + np.arange(1, 66)[:, None] * final_velocity
    xy = np.vstack((core_xy, future_xy))

    velocity = np.gradient(xy, CONTROL_TIMESTEP, axis=0)
    yaw = np.unwrap(np.arctan2(velocity[:, 1], velocity[:, 0]))
    yaw = smooth_positions(np.column_stack((yaw, yaw)), window=31)[:, 0]

    qpos = np.zeros((len(xy), 7), dtype=np.float64)
    qpos[:, :2] = xy
    qpos[:, 2] = 0.1278
    qpos[:, 3] = np.cos(yaw / 2)
    qpos[:, 6] = np.sin(yaw / 2)

    qvel = np.zeros((len(xy), 6), dtype=np.float64)
    qvel[:, :2] = velocity
    qvel[:, 5] = np.gradient(yaw, CONTROL_TIMESTEP)
    return qpos, qvel, target_step


def directory_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        digest.update(str(item.relative_to(path)).encode())
        with item.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def run(args: argparse.Namespace) -> dict[str, Any]:
    np.random.seed(args.seed)
    tf.random.set_seed(args.seed)
    policy_path = Path(args.policy).resolve()
    walking_policy_path = Path(args.walking_policy).resolve()
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    navigation_policy = load_navigation_policy(policy_path)
    bootstrap_route, _ = plan_route(navigation_policy)
    bootstrap_qpos, bootstrap_qvel, _ = make_reference(
        bootstrap_route, args.square_size_cm, args.speed_cm_s
    )

    env = make_chessboard_environment(args.square_size_cm, args.seed)
    env.task._traj_generator.set_next_trajectory(bootstrap_qpos, bootstrap_qvel)
    env.reset()
    inferred_occupancy, perception_proof = train_camera_perception(
        env=env,
        output_dir=output_dir,
        square_size_cm=args.square_size_cm,
        seed=args.seed,
        train_layouts=args.vision_train_layouts,
        test_layouts=args.vision_test_layouts,
        epochs=args.vision_epochs,
    )
    route, decisions = plan_route(navigation_policy, inferred_occupancy)
    qpos, qvel, target_step = make_reference(
        route, args.square_size_cm, args.speed_cm_s
    )
    env.task._traj_generator.set_next_trajectory(qpos, qvel)
    timestep = env.reset()

    walking_policy = SavedWalkingPolicy(walking_policy_path)
    action_spec = env.action_spec()
    obstacle_geoms = env.task._arena.obstacle_geoms
    # Composer may rebuild Physics during reset after MJCF initialization, so
    # bind arena elements only after that reset completes.
    obstacle_binding = env.physics.bind(obstacle_geoms)
    obstacle_geom_ids = set(int(value) for value in obstacle_binding.element_id)
    obstacle_centers = np.asarray(obstacle_binding.pos)[:, :2].copy()
    piece_radius = float(np.asarray(obstacle_binding.size)[0, 0])
    actual_positions = []
    reference_positions = []
    frames = []
    obstacle_contact_pairs = 0
    obstacle_contact_steps = 0
    minimum_obstacle_center_distance = float("inf")
    step = 0
    while timestep.step_type != 2 and step <= target_step + 2:
        actual_position, _ = env.task._walker.get_pose(env.physics)
        # MuJoCo exposes live views into its state. Copy each sample so the
        # trajectory archive records the pose at this step, not the final pose
        # repeated for every step.
        actual_positions.append(np.array(actual_position, dtype=np.float64, copy=True))
        reference_positions.append(
            np.array(qpos[min(step, len(qpos) - 1), :3], copy=True)
        )
        if step % args.render_stride == 0:
            frames.append(
                env.physics.render(
                    camera_id=args.camera,
                    width=args.width,
                    height=args.height,
                )
            )
        action = np.clip(
            walking_policy(timestep.observation), action_spec.minimum, action_spec.maximum
        )
        timestep = env.step(action)
        center_distances = np.linalg.norm(
            obstacle_centers - actual_positions[-1][:2], axis=1
        )
        minimum_obstacle_center_distance = min(
            minimum_obstacle_center_distance, float(center_distances.min())
        )
        contact_pairs_this_step = sum(
            1
            for contact in env.physics.data.contact
            if int(contact.geom1) in obstacle_geom_ids
            or int(contact.geom2) in obstacle_geom_ids
        )
        obstacle_contact_pairs += contact_pairs_this_step
        obstacle_contact_steps += int(contact_pairs_this_step > 0)
        step += 1

    actual = np.asarray(actual_positions)
    reference = np.asarray(reference_positions)
    target_xy = qpos[target_step, :2]
    distances = np.linalg.norm(actual[:, :2] - target_xy, axis=1)
    tracking = np.linalg.norm(actual[:, :2] - reference[:, :2], axis=1)
    threshold = args.success_threshold_cm
    terminated_cleanly = timestep.step_type == 2 and np.isclose(timestep.discount, 1)
    arena_verified = (
        len(obstacle_geoms) == len(DEFAULT_OBSTACLES)
        and bool(np.all(np.asarray(obstacle_binding.contype) > 0))
        and bool(np.all(np.asarray(obstacle_binding.conaffinity) > 0))
        and obstacle_contact_steps == 0
    )
    perception_verified = (
        perception_proof["exactDefaultBoardMatch"]
        and perception_proof["testAccuracy"] >= 0.98
        and perception_proof["testPrecision"] >= 0.95
        and perception_proof["testRecall"] >= 0.95
    )
    proof = {
        "format": "fly-gambit-flybody-proof-v1",
        "verified": bool(
            distances[-1] <= threshold
            and terminated_cleanly
            and arena_verified
            and perception_verified
        ),
        "navigationPolicy": {
            "path": str(policy_path),
            "sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest(),
            "route": [square_name(point).upper() for point in route],
            "decisions": decisions,
            "occupancySource": "trained MuJoCo camera perception",
        },
        "perceptionPolicy": perception_proof,
        "walkingPolicy": {
            "path": str(walking_policy_path),
            "sha256": directory_sha256(walking_policy_path),
            "source": "Figshare file 44815195, trained-fly-policies.zip",
        },
        "runtime": {
            "flybodyCommit": UPSTREAM_COMMIT,
            "tensorflow": tf.__version__,
            "tensorflowProbability": tfp.__version__,
            "controlTimestepSeconds": CONTROL_TIMESTEP,
            "seed": args.seed,
        },
        "trajectory": {
            "squareSizeCm": args.square_size_cm,
            "commandedSpeedCmPerSecond": args.speed_cm_s,
            "targetXYCm": target_xy.tolist(),
            "referenceStepsToTarget": target_step,
        },
        "arena": {
            "type": "collision-enabled-chessboard",
            "tileCount": BOARD_SIZE * BOARD_SIZE,
            "obstacleCount": len(obstacle_geoms),
            "obstacleSquares": [
                square_name(point).upper() for point in DEFAULT_OBSTACLES
            ],
            "pieceRadiusCm": piece_radius,
            "collisionEnabled": bool(
                np.all(np.asarray(obstacle_binding.contype) > 0)
                and np.all(np.asarray(obstacle_binding.conaffinity) > 0)
            ),
            "obstacleContactSteps": obstacle_contact_steps,
            "obstacleContactPairs": obstacle_contact_pairs,
            "minimumObstacleCenterDistanceCm": minimum_obstacle_center_distance,
            "minimumObstacleSurfaceClearanceCm": (
                minimum_obstacle_center_distance - piece_radius
            ),
        },
        "result": {
            "environmentSteps": step,
            "terminated": bool(timestep.step_type == 2),
            "discount": float(timestep.discount),
            "initialTargetDistanceCm": float(distances[0]),
            "minimumTargetDistanceCm": float(distances.min()),
            "finalTargetDistanceCm": float(distances[-1]),
            "netDisplacementCm": float(
                np.linalg.norm(actual[-1, :2] - actual[0, :2])
            ),
            "trackingRmseCm": float(np.sqrt(np.mean(tracking**2))),
            "successThresholdCm": threshold,
            "frames": len(frames),
        },
    }

    proof_path = output_dir / "flybody-a1-f7-proof.json"
    proof_path.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    np.savez_compressed(
        output_dir / "flybody-a1-f7-trajectory.npz",
        actual=actual,
        reference=reference,
        commanded_qpos=qpos,
        commanded_qvel=qvel,
    )
    if frames:
        fps = round(1 / (CONTROL_TIMESTEP * args.render_stride))
        media.write_video(output_dir / "flybody-a1-f7.mp4", frames, fps=fps)
        media.write_image(output_dir / "flybody-a1-f7-final.png", frames[-1])

    print(json.dumps(proof, indent=2))
    if not proof["verified"]:
        raise SystemExit(3)
    return proof


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", required=True)
    parser.add_argument("--walking-policy", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=20260911)
    parser.add_argument("--square-size-cm", type=float, default=0.5)
    parser.add_argument("--speed-cm-s", type=float, default=2.0)
    parser.add_argument("--success-threshold-cm", type=float, default=0.25)
    parser.add_argument("--render-stride", type=int, default=16)
    parser.add_argument("--camera", type=int, default=1)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=360)
    parser.add_argument("--vision-train-layouts", type=int, default=96)
    parser.add_argument("--vision-test-layouts", type=int, default=24)
    parser.add_argument("--vision-epochs", type=int, default=40)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
