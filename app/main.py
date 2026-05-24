from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
import joblib
import pandas as pd
import time
import threading
import json
import os
from confluent_kafka import Consumer, Producer, KafkaError
from prometheus_client import Counter, Histogram, generate_latest, REGISTRY
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, ClassificationPreset

app = FastAPI()

model = joblib.load("/app/model/model.pkl")

REQUEST_COUNT = Counter('requests_total', 'Total prediction requests')
LATENCY = Histogram('prediction_latency_seconds', 'Prediction latency')
DRIFT_SCORE = Counter('drift_score', 'Data drift score')

consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'fraud-detector',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['transactions'])

recent_data = []
DRIFT_WINDOW = 100

def monitor_drift():
    global recent_data
    while True:
        time.sleep(120)
        if len(recent_data) >= 50:
            df = pd.DataFrame(recent_data[-DRIFT_WINDOW:])
            reference = pd.read_csv("/app/reference_data.csv")
            report = Report(metrics=[DataDriftPreset(), ClassificationPreset()])
            report.run(reference_data=reference, current_data=df)
            report.save_html("/app/drift_report.html")
            drift_detected = report.as_dict()['metrics'][0]['result']['dataset_drift']
            if drift_detected:
                DRIFT_SCORE.inc()

threading.Thread(target=monitor_drift, daemon=True).start()

@app.on_event("startup")
def start_kafka_consumer():
    def consume_loop():
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            transaction = json.loads(msg.value().decode('utf-8'))
            df = pd.DataFrame([transaction])
            start = time.time()
            proba = model.predict_proba(df)[0][1]
            latency = time.time() - start
            REQUEST_COUNT.inc()
            LATENCY.observe(latency)
            recent_data.append(transaction)
            if len(recent_data) > DRIFT_WINDOW * 2:
                recent_data.pop(0)
            print(f"Kafka tx: {transaction} -> fraud prob {proba:.4f}")

    threading.Thread(target=consume_loop, daemon=True).start()

@app.post("/predict")
async def predict(request: Request):
    transaction = await request.json()
    df = pd.DataFrame([transaction])
    start = time.time()
    proba = model.predict_proba(df)[0][1]
    latency = time.time() - start
    REQUEST_COUNT.inc()
    LATENCY.observe(latency)
    recent_data.append(transaction)
    return {"fraud_probability": proba, "latency_ms": latency * 1000}

@app.get("/metrics")
def metrics():
    return generate_latest(REGISTRY)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/drift-report")
def drift_report():
    if os.path.exists("/app/drift_report.html"):
        return FileResponse("/app/drift_report.html")
    return {"error": "No drift report yet"}