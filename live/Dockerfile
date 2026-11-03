FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir websockets==17.4
COPY . /app
EXPOSE 8080 9001
CMD ["python", "csms_live.py"]
