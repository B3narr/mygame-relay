# For hosts that run containers (Koyeb, Fly.io, Railway...).
FROM python:3.12-slim
WORKDIR /relay
COPY requirements.txt relay.py ./
RUN pip install --no-cache-dir -r requirements.txt
ENV PORT=8765
EXPOSE 8765
CMD ["python", "relay.py"]
