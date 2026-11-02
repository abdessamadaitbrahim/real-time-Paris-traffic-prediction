# Real-Time Paris Traffic State Prediction

Big Data & Artificial Intelligence project (ENSA).
A streaming pipeline that ingests Paris traffic sensor data, predicts the traffic state with Spark ML, and displays the results in a live Streamlit dashboard.

**Authors:** Ayyoub AMAZAL, Abdessamad Ait Brahim
**Supervisor:** Dr. Imad Sassi

---

## Overview

Urban mobility keeps growing, and optimizing road traffic is a priority for large cities. This project uses open data from [Open Data Paris](https://opendata.paris.fr) (flow `q`, density `k`, geolocation, timestamp) to predict the traffic state in real time, as a tool for decision makers and citizens.

## Architecture

```
data/data.json --> Kafka producer --> Kafka (topic "trafic_paris") --> Spark Structured Streaming
                                                                              |  Spark ML PipelineModel
                                                                              v
                                              Streamlit dashboard <-- MongoDB (traffic.predictions)
```

![Architecture](docs/images/architecture.png)

| Component | Role |
|---|---|
| Kafka | Real-time ingestion of the JSON feed (1 message per second) |
| Spark Structured Streaming | Reads Kafka, parses JSON, cleans data, runs predictions |
| Spark ML | Trains and compares 3 classifiers |
| MongoDB | Stores predictions |
| Streamlit | Interactive dashboard, auto-refreshed every 60 seconds |

## Project structure

```
.
|-- README.md
|-- .gitignore
|-- requirements.txt
|-- docker-compose.yml        # Zookeeper, Kafka, MongoDB (+ optional Spark cluster)
|-- data/
|   `-- data.json             # Traffic dataset
|-- src/
|   |-- kafka_producer.py     # Sends data.json to Kafka
|   |-- spark_consumer.py     # Streaming read + console output (debugging)
|   |-- train_models.py       # Trains, compares and saves the best model
|   |-- spark_predictor.py    # Real-time prediction + MongoDB sink
|   `-- streamlit_app.py      # Dashboard
|-- models/                   # Generated model (git-ignored)
`-- docs/
    |-- presentation_big_data.pptx
    |-- rapport_final_big_data.pdf
    `-- images/               # Screenshots used in this README
```

## Machine learning

- **Features:** `q` (flow) and `k` (density)
- **Target:** `etat_trafic` (traffic state)
- **Models compared:** Logistic Regression, Decision Tree, Random Forest (80/20 split)
- **Metrics:** accuracy, weighted precision, weighted recall, F1
- **Best model:** Logistic Regression, saved as a full `PipelineModel` (indexer + assembler + classifier)

![Model comparison](docs/images/model_comparison.png)

## Prerequisites

- Docker and Docker Compose
- Java 8 or 11 (required by PySpark 3.0.0)
- Python 3.8 or lower (PySpark 3.0.0 does not support Python 3.9+)
- Windows only: `HADOOP_HOME` with `winutils.exe` may be needed for Spark to save models

## Quick start

Run all commands from the project root.

```bash
# 1. Python environment
python -m venv .venv
source .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

# 2. Infrastructure (Kafka, Zookeeper, MongoDB)
docker compose up -d

# 3. Train the model -> creates models/best_model_logisticregression
python src/train_models.py

# 4. Real-time prediction (terminal 1, keep it running)
python src/spark_predictor.py

# 5. Send the data (terminal 2)
python src/kafka_producer.py

# 6. Dashboard (terminal 3)
streamlit run src/streamlit_app.py
```

To debug the Kafka stream alone: `python src/spark_consumer.py`.

![Docker services](docs/images/docker_services.png)

![Kafka producer](docs/images/kafka_producer.png)

![Spark predictions](docs/images/spark_predictions.png)

![MongoDB](docs/images/mongodb_compass.png)

## Dashboard

The Streamlit dashboard shows:

- prediction counts per traffic state
- hourly distribution and evolution over time
- an interactive map of Paris based on geolocation
- a filterable table with CSV export

![Dashboard](docs/images/dashboard.png)

## Future work

- Cloud deployment (AWS, Azure) to handle massive streams and improve fault tolerance
- Integration into a wider Smart City platform (IoT, lighting, parking)
- Real-time external APIs (weather, urban events) to improve accuracy
- Deep learning models (LSTM, GRU) to capture temporal dynamics

## Documents

- [Presentation (PPTX)](docs/presentation_big_data.pptx)
- [Final report (PDF)](docs/rapport_final_big_data.pdf)
