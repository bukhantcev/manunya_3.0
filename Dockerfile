FROM python:3.14.7-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && python -m pip check
COPY main.py tickets.py ./

FROM base AS test
COPY tests/test_tickets.py tests/test_tickets.py
RUN --network=none python -m unittest discover -s tests -v

FROM base AS runtime
USER 10001:10001
CMD ["python", "main.py"]
