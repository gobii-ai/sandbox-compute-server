# syntax=docker/dockerfile:1.7

FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && \
    apt-get install --no-install-recommends -y \
        git \
        procps \
        curl \
        ca-certificates \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        libharfbuzz-subset0 \
        libgdk-pixbuf2.0-0 \
        libcairo2 && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY requirements.txt ./

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

COPY . ./

ARG GIT_COMMIT=unknown
RUN echo "${GIT_COMMIT}" > /app/.git-commit

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

EXPOSE 8080
CMD ["gunicorn", "sandbox_compute_server:application", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "0"]
