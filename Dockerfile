# Base Image: Lightweight Linux with Python 3.10
FROM python:3.10-slim

# Prevent Python from buffering outputs (so you see logs immediately)
ENV PYTHONUNBUFFERED=1

# Install Git (useful for installing research tools later)
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# --- DEPENDENCY INSTALL: single source of truth = pyproject.toml ---
# Previously this installed from requirements.txt, which (a) was a second
# dependency source that could drift from the packaged project. Installing the project with
# `pip install .` resolves the real dependency set from pyproject and makes the
# typo'd requirements.txt unnecessary (it can be deleted from the repo).
#
# pyproject.toml declares readme = "README.md", so README.md MUST be copied
# alongside it or the metadata build fails. The packages it installs
# ([tool.setuptools.packages.find] = src*/strategies*) are copied in first so
# `pip install .` can build them. Runtime code volumes (compose) still overlay
# /app/src and /app/strategies; the baked-in install only fixes the deps.
COPY pyproject.toml README.md ./
COPY src ./src
COPY strategies ./strategies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir .

# Expose the port for JupyterLab
EXPOSE 8888

# The default command: Launch JupyterLab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]