# Python side of the project: producer, dashboard, validation and analysis tools.
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tools ./tools
COPY producer ./producer
COPY dashboard ./dashboard
COPY .streamlit ./.streamlit
COPY config ./config
COPY answer_key ./answer_key
COPY analysis/test-slice.csv ./analysis/test-slice.csv
ENV PYTHONUNBUFFERED=1
