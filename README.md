# Real‑time Fraud Detection with Drift Monitoring

![CI/CD](https://github.com/kallurayaankit/fraud-detection/actions/workflows/ci.yml/badge.svg)

A real‑time streaming fraud detection API with:
- 📊 Synthetic transaction data streamed via Kafka
- 🤖 Pre‑trained RandomForest model served with FastAPI
- 📈 Drift detection using Evidently AI
- 📡 Metrics collected by Prometheus & visualized in Grafana
- 🐳 Containerized with Docker
- ☁️ Live demo on Hugging Face Spaces: [https://kallurayaankit-fraud-detection.hf.space](https://kallurayaankit-fraud-detection.hf.space)

## How to test
\`\`\`bash
curl -X POST "https://kallurayaankit-fraud-detection.hf.space/predict" \
  -H "Content-Type: application/json" \
  -d '{"amount":150.0,"transaction_hour":14,"merchant_category":3,"distance_from_home":25.0}'
\`\`\`
