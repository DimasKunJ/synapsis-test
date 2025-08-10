# Project README

## Prerequisites

* Docker & Docker Compose
* A SQL client compatible with ClickHouse (e.g., DBeaver, DataGrip)

---

## Configuration

Clone this repository to your local machine.

---

## How to Run

### Step 1: Launch All Services

This command will build the custom images and start all services (database, warehouse, Superset, etc.) in the background.

```bash
docker compose up -d --build
```

### Step 2: Initialize the Data Warehouse

The ClickHouse data warehouse must be initialized the first time you run the project. This creates the necessary database and tables.

You can do this in one of two ways:

* **Option A (GUI):**
    Connect to the ClickHouse server using a SQL client with the following details:
    * **Host:** `localhost`
    * **Port:** `8123`
    * **User:** `default`

    Open the `/data/clickhouse/init.sql` file from this project, copy its contents, and run the query in your client.

* **Option B (CLI):**
    Or you can install `clickhouse-client` CLI and run this command:
    ```bash
    clickhouse-client -h localhost --multiquery --user default < ./data/clickhouse/init.sql
    ```

### Step 3: Start the ETL Pipeline

Once the warehouse is ready, the ETL container will run automatically but there's a chance the clickhouse hasn't properly started yet. This will begin the process of moving and transforming data manually.

```bash
docker compose start etl-pipeline
```
