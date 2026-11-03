FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pymssql faker
COPY schema_spec.py generate.py ./
CMD ["python", "-u", "generate.py"]
