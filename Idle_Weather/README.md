# 🌤️ Open-Meteo Datalake Project


## 🌍 Overview
This first phase of the project focuses on climate analysis.
This first phase is a complete Data Engineering pipeline focused on extracting, processing, and storing weather data from the Open-Meteo API. It follows modern data lakehouse architecture principles and is structured around three layers: Bronze (raw), Silver (cleaned), and Gold (aggregated).

---

## 📡 Data Source

🌦️ **Open-Meteo API**: A free, no-authentication-required API that provides rich weather data including:

- Geolocation  
- Current weather  
- Forecast (daily and hourly)  
- Historical weather  

---

## 🏗️ Architecture: Bronze, Silver, Gold

### 🥉 Bronze Layer
- Extracts raw weather data from Open-Meteo API endpoints.  
- Saves the data without transformation in Delta Lake format.  
- Maintains source fidelity  
- Monitors the data with metadata_table and logs table  

### 🥈 Silver Layer
- Cleans and standardizes data (column renaming, types, structure).  
- Deduplicates and validates records.  
- Stores clean datasets in Delta format for analytical use.  
- Monitors the data with metadata_table and logs table  

### 🥇 Gold Layer
- Aggregates, filters, and derives KPIs (e.g., average temperatures, daily summaries).  
- Prepares final tables for analytics, dashboards, or machine learning pipelines.  
- Monitors the data with metadata_table and logs table  

---

## 🚀 Usage

### Installing Dependencies
1. Install all required modules by running the following command:

```bash
pip install -r Config/requirements.txt


## 🚀 Running the Pipeline

1. Through Notebooks: `almacenamiento_bronze`, `almacenamiento_silver`, and `almacenamiento_gold` (in the `notebooks/` folder)

2. Through the script `main.py`, which calls the 3 scripts: `bronze_layer.py`, `silver_layer.py`, and `gold_layer.py`.

---

## 📊 Pipeline Workflow

1. **🔄 Data Extraction**  
   The `almacenamiento_bronze.ipynb` notebook (or the script) extracts data from APIs and stores it in `data/bronze`.

2. **⚙️ Data Transformation**  
   `almacenamiento_silver.ipynb` (or the script) cleans and structures the data before storing it in `data/silver`.

3. **💡 Insights Creation**  
   `almacenamiento_gold.ipynb` (or the script) computes new columns, cross tables for further analysis before storing it in `data/gold`.

4. **🔧 Modular Design**  
   The pipeline relies on Python modules from `modules/`, ensuring **modularity** and **reusability**.

---

## 📁 Project Structure

Entrega_Final/
│
├── config/                      # Configuration files (e.g., API URLs, parameters)
│   └── requirements.txt
│
├── data/                        # Data lake folders
│   ├── bronze/                  # Raw Delta tables
│   ├── silver/                  # Cleaned Delta tables
│   ├── gold/                    # Aggregated/analytical Delta tables
│   └── _meta/                   # Central metadata table + Excel exports
│
├── logs/                        # Execution logs and error logs
│   └──
│
├── modules/                     # Custom Python modules
│   ├── openmeteo_api.py         # API calls (current, forecast, historical)
│   ├── DF_functions.py          # DataFrame cleaning utilities
│   ├── metadata_functions.py    # Metadata tracking + Excel export
│                 
│
├── notebooks/                   # Jupyter notebooks (exploration, testing)
│   ├── almacenamiento_bronze.ipynb
│   ├── almacenamiento_silver.ipynb
│   └── almacenamiento_gold.ipynb
│
├── scripts/                     # Main pipeline scripts
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   └── gold_layer.py
│
├── main.py                      # Orchestration script to run all layers
├── README.md                    # Project documentation



---

## 🧠 Features

- 📦 Delta Lake storage at every stage  
- 📄 Centralized metadata table with schema, layer, and history  
- 📊 Export metadata as Excel reports for traceability  
- 🔁 Modular design for maintainability  
- ✅ Robust function design (try/except, docstrings)  
- 🔍 Support for full and incremental extraction  

🚀 **Clean, modular, and production-ready!**  
This weather data pipeline is built to evolve — add endpoints, integrate new layers, or scale for big data.

---

## 🔮 Future Improvements

✅ Add **try/except** to make the code more robust.  
✅ Improve documentation : add business-oriented documentation and technical-oriented documentation. 
✅ Create a cron job to run automatically the script everyday  
✅ Implement a more organized **monitoring, logging, and alerting system**.  
✅ Add new APIs to enrich the data or use other endpoints of the Open-Meteo API:

- **📈 Adzuna**: Job market data.  
- **✈️ Google Flights API**: Flight information.  
- **🌦️ Weatherstack**: Alternative to Open-Meteo.  

