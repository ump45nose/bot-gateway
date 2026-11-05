FROM python:3.13-alpine
WORKDIR /app
COPY pyproject.toml /app/
COPY bot_gateway /app/bot_gateway
RUN pip install --no-cache-dir .
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1001
ENTRYPOINT ["bot-gateway"]
CMD ["--config", "/config/gateway.toml"]
