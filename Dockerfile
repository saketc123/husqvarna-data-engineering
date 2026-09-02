FROM python:3.11-slim

WORKDIR /app

# PySpark requires a Java runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY analysis ./analysis
COPY taxonomy ./taxonomy
COPY notebooks ./notebooks
COPY README.md .
COPY DEPLOYMENT.md .

# Data is supplied at runtime. Generated Bronze/Silver/Gold data
# is intentionally not packaged into the image.
RUN mkdir -p /app/data/raw /app/data/bronze /app/data/silver /app/data/gold

CMD ["sh", "-c", "python -m src.run_pipeline && python -m analysis.run_analysis"]