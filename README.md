# TransitPulse

**Real-Time Chicago Transit Analytics Platform**

TransitPulse is an industry-level real-time data engineering project that consumes live CTA (Chicago Transit Authority) bus data, processes it with Apache Flink, and predicts delay risks based on spatial analysis and weather conditions.

## System Architecture

The pipeline consists of the following containerized components:

1.  **Data Source**:
    *   **CTA Producer**: A Python service that polls the CTA Bus Tracker API and enriches vehicle data with real-time weather from OpenWeatherMap.
    *   **Kafka (Redpanda)**: High-performance message broker receiving `raw_bus_locations`.
2.  **Stream Processing**:
    *   **PyFlink Processor**: Consumes Kafka streams, calculates bus velocity (stateful logic), and flags high-traffic delay risks (spatial logic/geofencing).
3.  **Storage layer**:
    *   **PostgreSQL (PostGIS)**: Persistent storage for historical geospatial data.
    *   **Redis**: Real-time caching for live arrival predictions.
4.  **Visualization**:
    *   **Grafana**: Dashboards for checking live bus positions and average route delays.

## Getting Started

### Prerequisites
*   **Docker Desktop** (Required)

### Installation & Running

1.  **Clone the repository**:
    ```bash
    git clone <repo-url>
    cd TransitPulse
    ```

2.  **Configure API Keys**:
    Edit the `.env` file in the root directory:
    ```env
    CTA_API_KEY=your_cta_api_key
    OPENWEATHER_API_KEY=your_openweathermap_api_key
    ```

3.  **Start the Cluster**:
    Build and launch all services using Docker Compose:
    ```bash
    docker-compose up --build -d
    ```

4.  **Verify**:
    Check if containers are running:
    ```bash
    docker ps
    ```

## Dashboards & Interfaces

| Service | FAST URL | Description |
| :--- | :--- | :--- |
| **Redpanda Console** | [http://localhost:8080](http://localhost:8080) | View Kafka topics (`raw_bus_locations`) and messages. |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | Visualization (Login: `admin`/`admin`). Connect to Postgres (`postgres:5432`, db: `transitpulse`, user: `user`, pass: `password`). |
| **Flink Dashboard** | [http://localhost:8081](http://localhost:8081) | Monitor the `TransitPulse Stream Processor` job. |
| **Postgres** | `localhost:5432` | Database access via any SQL client. |

## Project Structure

```
TransitPulse/
├── docker-compose.yml       # Orchestrates Kafka, Flink, Postgres, Redis, Grafana
├── Dockerfile               # Python environment for Producer & Processor
├── requirements.txt         # Python dependencies
├── .env                     # API Secrets
├── producers/
│   └── cta_producer.py      # Polls API -> Kafka
├── processors/
│   └── stream_processor.py  # Kafka -> Flink -> Postgres/Redis
└── sql/
    └── init_db.sql          # PostGIS schema initialization
```

## Troubleshooting

*   **Port 5432 Conflict**: If Postgres fails to start, ensure you don't have a local Postgres instance running. Stop it with `brew services stop postgresql` (Mac) or change the port in `docker-compose.yml`.
*   **No Data**: Ensure your API keys in `.env` are valid. Check producer logs: `docker logs producer`.
