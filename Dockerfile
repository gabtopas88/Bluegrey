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

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Expose the port for JupyterLab
EXPOSE 8888

# The default command: Launch JupyterLab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]