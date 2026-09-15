FROM python:3.10-slim-bookworm

ARG FLYBODY_COMMIT=d015e9bfe441bd90ae431bac24c55cb74bdbce26

ENV DEBIAN_FRONTEND=noninteractive \
    MUJOCO_GL=osmesa \
    PYOPENGL_PLATFORM=osmesa \
    TF_CPP_MIN_LOG_LEVEL=2 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        ffmpeg \
        git \
        libgl1 \
        libglew2.2 \
        libglfw3 \
        libosmesa6 \
    && rm -rf /var/lib/apt/lists/*

# FlyBody's published walking policy was exported with TensorFlow 2.8. A newer
# SavedModel-compatible runtime lets this image run natively on both Intel and
# Apple Silicon hosts while the website itself stays dependency-free.
RUN python -m pip install --no-cache-dir \
        "numpy==1.26.4" \
        "protobuf==3.20.3" \
        "tensorflow==2.15.1" \
        "tensorflow-probability==0.23.0" \
        "dm-sonnet==2.0.2" \
        "dm-control==1.0.31" \
        "mujoco==3.3.3" \
        "h5py==3.16.0" \
        "mediapy==1.2.4"

RUN git clone https://github.com/TuragaLab/flybody.git /opt/flybody \
    && git -C /opt/flybody checkout "$FLYBODY_COMMIT" \
    && python -m pip install --no-cache-dir --no-deps /opt/flybody

WORKDIR /workspace

ENTRYPOINT ["python", "scripts/run_flybody_bridge.py"]
