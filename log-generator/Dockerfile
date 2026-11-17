FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY loggen ./loggen
RUN pip install --no-cache-dir ".[kinesis]" \
    && useradd --system --uid 10001 loggen
USER loggen

# Lines must reach the sink as they are produced
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["loggen"]
CMD ["--help"]
