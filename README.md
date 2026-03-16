# GovScape Data Pipeline

A modular data engineering pipeline that ingests, transforms, and analyzes U.S. Congressional legislator data using Python and Pandas.

## 🚀 Project Architecture

The project follows a **Medallion Architecture** to ensure data quality and traceability:

* **Bronze Layer**: Raw data ingested from the Congress API in JSON format.
* **Silver Layer**: Cleaned and filtered data (Democrats only) persisted in Parquet format.
* **Gold Layer**: High-level business metrics and aggregations for reporting.

## 🛠️ Tech Stack

* **Language**: Python 3.x
* **Data Processing**: Pandas, PyArrow
* **API Integration**: Requests
* **Monitoring**: Python Logging module
* **Environment Management**: Dotenv

## 📋 Prerequisites

1.  **API Key**: Obtain a `CONGRESS_API_KEY` from [api.congress.gov](https://api.congress.gov/).
2.  **Environment Variables**: Create a `.env` file in the root directory and add your key:
    ```env
    CONGRESS_API_KEY=your_api_key_here
    ```

## ⚙️ Installation & Setup

1.  **Clone the repository**:
    ```bash
    git clone <your-repo-url>
    cd govscape-pipeline
    ```

2.  **Set up a virtual environment**:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## 🏃 Execution

To run the entire pipeline from ingestion to analysis, execute the main orchestrator:

```bash
python main.py