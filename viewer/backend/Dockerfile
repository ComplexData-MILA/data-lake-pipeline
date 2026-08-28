FROM python:3.13-slim

WORKDIR /app

# Install backend deps, then the s3_data_tool package (imported by the backend).
COPY pyproject.toml README.md ./
COPY s3_data_tool ./s3_data_tool
COPY viewer/backend/requirements.txt /tmp/backend-requirements.txt
RUN pip install --no-cache-dir -r /tmp/backend-requirements.txt \
    && pip install --no-cache-dir .

COPY viewer/backend ./backend

ENV PYTHONUNBUFFERED=1

# --proxy-headers: trust X-Forwarded-* set by nginx / the institutional reverse proxy.
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers", "--forwarded-allow-ips=*"]
