# One image, three roles: api, worker and the carrier simulator.
FROM python:3.12-slim AS build
WORKDIR /app
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim
ENV PATH="/venv/bin:$PATH" PYTHONUNBUFFERED=1 ROUTES_CONFIG=/app/config/routes.yaml
RUN useradd --create-home --uid 10001 sms
WORKDIR /app
COPY --from=build /venv /venv
COPY smsgw/ smsgw/
USER sms
# default role: the REST API
EXPOSE 8080
CMD ["uvicorn", "smsgw.api:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
