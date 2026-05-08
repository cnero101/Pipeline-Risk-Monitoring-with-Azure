# Architecture & Setup Guide

## Azure Resources

All resources are created inside a single resource group (`pipeline-monitoring-rg`) for easy cleanup.

| Resource | Name | Region |
|---|---|---|
| Resource Group | `pipeline-monitoring-rg` | East US 2 |
| Event Hubs Namespace | `pipeline-sensor-ns` | East US 2 |
| Event Hub | `pipeline-sensor-hub` | Inside namespace |
| Storage Account (ADLS) | `pipeline608storage` | East US 2 |
| Container (data) | `sensordata` | Inside storage |
| Container (model) | `modelstore` | Inside storage |
| Function App | `pipeline-processor` | East US 2 |
| Function Storage | `pipeline608funcstore` | East US 2 |
| Logic App | `pipeline-alerts` | East US 2 |

---

## Step-by-Step Azure Setup (Portal)

### 1. Resource Group
- Search **Resource groups** → Create
- Name: `pipeline-monitoring-rg`, Region: `East US 2`

### 2. Event Hubs
- Search **Event Hubs** → Create namespace
- Name: `pipeline-sensor-ns`, Tier: Basic, Region: `East US 2`
- Inside namespace → Create Event Hub: `pipeline-sensor-hub`, Partitions: 1
- Shared access policies → RootManageSharedAccessKey → copy **Primary connection string**

### 3. Storage Account
- Search **Storage accounts** → Create
- Name: `pipeline608storage`, Region: `East US 2`, Redundancy: LRS
- Advanced tab → Enable **hierarchical namespace** (required for ADLS Gen2)
- Access keys → copy **Connection string** (key1)
- Containers → Create `sensordata` and `modelstore`
- Upload `random_forest_model.joblib` into `modelstore`

### 4. Function App
- Search **Function App** → Create → Consumption (Flex)
- Name: `pipeline-processor`, Runtime: Python 3.11, OS: Linux, Region: `East US 2`
- Storage: create new `pipeline608funcstore`
- After creation → Settings → Environment variables → Add:
  - `EVENT_HUB_CONN_STR`
  - `STORAGE_CONN_STR`
  - `LOGIC_APP_URL` (add after step 5)

### 5. Logic App
- Search **Logic Apps** → Create → Consumption (Multi-tenant)
- Name: `pipeline-alerts`, Region: `East US 2`
- Designer → Add trigger: **When an HTTP request is received**
- Use sample payload to generate schema (paste sample JSON)
- Save → copy the **HTTP POST URL**
- Add action: **Send email (Gmail)** → sign in → fill To, Subject, Body
- Set **Is HTML** to Yes in advanced parameters
- Save

---

## Data Flow

```
sensorsim_azure.py
    → generates readings every 1 second for 5 pipes
    → sends batch to Azure Event Hubs

Azure Event Hubs
    → buffers messages
    → triggers Azure Function on new events

Azure Function (pipeline_processor)
    → loads model from Blob Storage (cached after first load)
    → classifies each reading
    → saves Parquet to ADLS Gen2:
       processed_data/pipe_id={id}/year={y}/month={m}/day={d}/{uuid}.parquet
    → if risk_level != Normal AND cooldown passed:
       POST to Logic App HTTP trigger

Azure Logic Apps
    → receives HTTP POST
    → sends HTML email via Gmail

dashboard_azure.py
    → reads last 10 Parquet blobs per pipe from today's partition
    → filters to last 30 minutes
    → renders in Streamlit at localhost:8501
    → auto-refreshes every 30 seconds
```

---

## Parquet Partition Structure

```
sensordata/
└── processed_data/
    └── pipe_id=P-101/
        └── year=2026/
            └── month=05/
                └── day=08/
                    ├── a1b2c3d4-....parquet
                    └── e5f6g7h8-....parquet
```

This Hive-style partitioning is compatible with Azure Synapse, Databricks, and Apache Spark.

---

## Cleanup

Delete all resources in one command:

```bash
az group delete --name pipeline-monitoring-rg
```
