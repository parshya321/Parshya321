# One image, two roles: the API (default CMD) and the origin health checker
# (docker-compose overrides the command with `python health_check.py`).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Backend modules only (the Next.js player lives in src/ and is deployed separately).
COPY main.py admin.py database.py redis_cache.py health_check.py origin_security.py ./

# Run as an unprivileged user.
RUN useradd --system --uid 10001 --no-create-home app
USER app

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=4).status == 200 else 1)"

# WEB_CONCURRENCY = number of uvicorn worker processes.
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port 8000 --workers ${WEB_CONCURRENCY:-4} --proxy-headers --forwarded-allow-ips='*'"]