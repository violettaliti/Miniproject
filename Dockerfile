FROM python:3.13.7

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
# These ENV lines in the Dockerfile fine tune how Python behaves inside my container so it’s cleaner, faster, and less annoying:
# PYTHONDONTWRITEBYTECODE=1 tells Python not to write .pyc files (compiled bytecode) to disk. Normally, Python caches compiled files in __pycache__/ directories to speed up future imports. Inside a container, that’s usually not needed - containers are ephemeral, and .pyc files just clutter my image. Setting this to 1 keeps my project folder tidy and slightly reduces image size.
# PYTHONUNBUFFERED=1 forces Python to write directly to stdout and stderr — no buffering --> we see real-time output in docker logs or when debugging my container.
# PIP_NO_CACHE_DIR=1 tells pip not to keep package installation caches inside the container. By default, pip stores downloaded .whl and .tar.gz files in a cache folder. In Docker, we don’t need that cache after pip install finishes — it only bloats my final image. With this variable set, pip deletes its temporary cache automatically.

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

COPY . /app

# default cmd
CMD ["python", "-u", "main.py"]
# -u runs Python in unbuffered mode