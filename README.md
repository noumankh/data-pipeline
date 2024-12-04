# **Data Pipeline**

A scalable and modular data pipeline that:
- Fetches data from an external API.
- Processes the data in batches.
- Performs transformations and analytics.
- Saves the transformed data for further use.

## **Features**
- **API Ingress**: Fetch data from external APIs.
- **Batch Processing**: Process data in batches to optimize memory usage.
- **Data Transformation**: Clean, mask, and enrich data with custom logic.
- **Analytics**: Generate insights such as Gmail user demographics.

---
## **Table of Contents**
1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
4. [Usage](#usage)
5. [Running Tests](#running-tests)

--- 

### **Project Structure**

```plaintext
├── data_pipeline.py              # Main entry point for the pipeline
├── pyproject.toml                # Poetry configuration
├── poetry.lock                   # Poetry lock file
├── Dockerfile                    # Dockerfile for containerization
├── tests/                        # Unit and integration tests
│   ├── test_api_handler.py
│   ├── test_batch_processor.py
│   ├── test_parquet_io.py
│   ├── test_person_data_transformer.py
├── services/                     # Core pipeline modules
│   ├── io_manager/
│   ├── ingress/
│   ├── transform/
│   ├── egress/
├── validation/                     # api validation module
│   ├── api_validator.py
├── README.md                     # Project documentation
```


---

## **Prerequisites**

Ensure you have the following installed:
- Python 3.10 or higher
- [Docker](https://www.docker.com/)
- [Poetry](https://python-poetry.org/)

---

## **Setup**

### **1. Clone the Repository**
```bash
git clone https://github.com/your-username/data-pipeline.git
cd data-pipeline
```

### **2. Install Dependencies**
Using Poetry:
```bash
poetry install
```

### **3. Set Up the Environment**
Create necessary directories if running locally:
```bash
mkdir -p data/raw data/intermediate data/mart
```

---

## **Usage**

### **Run the Pipeline Locally**
Execute the pipeline with default settings:
```bash
poetry run python data_pipeline.py --root-dir ./data
```

Customize parameters:
```bash
poetry run python data_pipeline.py \\
    --root-dir ./data \\
    --url https://example.com/api \\
    --params "_gender=XXX&_birthday_start=1900-01-01" \\
    --batch-size 100 \\
    --total-records 500
```

### **Run with Docker**

#### **1. Build the Docker Image**
```bash
docker build -t data-pipeline .
```

#### **2. Run the Pipeline**
```bash
docker run -v $(pwd)/data:/app/data data-pipeline
```

#### **3. Run Tests with Docker**
```bash
docker run data-pipeline pytest
```

---

## **Running Tests**

Run the test suite to ensure everything works as expected.

### **Run Tests Locally**
```bash
poetry run pytest
```

### **Run Tests with Docker**
```bash
docker run data-pipeline pytest
```

---

