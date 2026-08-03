FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AXDATA_DATA_DIR=/app/data

WORKDIR /app

COPY pyproject.toml README.md ./
COPY libs ./libs
COPY packages ./packages

RUN pip install -U pip \
    && pip install -e "." \
    && pip install -e libs/axdata_core \
    && pip install -e packages/axdata-source-tdx \
    && pip install -e packages/axdata-source-tdx-ext \
    && pip install -e packages/axdata-source-tencent \
    && pip install -e packages/axdata-source-cninfo \
    && pip install -e packages/axdata-sdk

COPY apps/mcp/requirements.txt ./apps/mcp/requirements.txt
RUN pip install -r apps/mcp/requirements.txt

COPY apps/api ./apps/api
COPY apps/mcp ./apps/mcp
COPY services ./services
COPY scripts ./scripts
COPY plugins ./plugins

EXPOSE 8666

CMD ["python", "-m", "uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8666"]
