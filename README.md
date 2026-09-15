# CSV Generator

A Python-based Streamlit application for generating CSV data through a simple web interface. The project uses **Streamlit**, **Elasticsearch**, **pytz**, and **urllib3**.

## Prerequisites

Make sure Python is installed on your system.

Check your Python version:

```bash
python --version
```

Recommended: **Python 3.9+**

## Installation

### 1. Clone the Repository

Clone the repository and navigate to the project directory.

### 2. Install Dependencies

Install the required Python packages:

```bash
pip install streamlit elasticsearch pytz urllib3
```

If the `streamlit` command is not recognized, install Streamlit using Python:

```bash
python -m pip install streamlit
```

## Configuration

Before running the application, update the **Elasticsearch host** and **API key** in `csv-generator.py`.

Look for the Elasticsearch connection configuration and replace the placeholder values with your Elasticsearch details.

Example:

```python
ELASTICSEARCH_HOST = "https://your-elasticsearch-host:9200"
ELASTICSEARCH_API_KEY = "YOUR_API_KEY"
```

Update:

* `ELASTICSEARCH_HOST` → Your Elasticsearch server URL
* `ELASTICSEARCH_API_KEY` → Your Elasticsearch API key

> **Security:** Never commit a real Elasticsearch API key to a public GitHub repository. Use environment variables or a `.env` file for production environments.

## Run the Application

Start the Streamlit application using:

```bash
python -m streamlit run csv-generator.py
```

After starting the application, Streamlit will display a local URL, usually:

```text
Local URL: http://localhost:8501
```

Open the URL in your web browser.

## Required Packages

The application requires the following Python packages:

| Package         | Purpose                    |
| --------------- | -------------------------- |
| `streamlit`     | Web interface              |
| `elasticsearch` | Elasticsearch connectivity |
| `pytz`          | Timezone handling          |
| `urllib3`       | HTTP/URL communication     |

## Quick Start

```bash
python --version
pip install streamlit elasticsearch pytz urllib3
python -m pip install streamlit
python -m streamlit run csv-generator.py
```

Before running the application, make sure the Elasticsearch **host** and **API key** are configured correctly.

## Troubleshooting

### `streamlit` is not recognized

If you see:

```text
streamlit : The term 'streamlit' is not recognized...
```

Use:

```bash
python -m streamlit run csv-generator.py
```

instead of:

```bash
streamlit run csv-generator.py
```

### Verify Streamlit Installation

You can verify that Streamlit is installed correctly:

```bash
python -m streamlit --version
```

Example:

```text
Streamlit, version 1.x.x
```

### Elasticsearch Connection Issues

If the application cannot connect to Elasticsearch, verify:

1. The Elasticsearch host URL is correct.
2. The Elasticsearch server is reachable from your machine.
3. The API key is valid.
4. The API key has the required Elasticsearch permissions.
5. HTTPS/TLS configuration is correct if your Elasticsearch server uses HTTPS.

## Project Structure

```text
csv-generator/
│
├── csv-generator.py
├── README.md
└── ...
```

## Usage

1. Install Python.
2. Install the required dependencies.
3. Configure the Elasticsearch host.
4. Configure the Elasticsearch API key.
5. Run the Streamlit application.
6. Open the provided localhost URL.
7. Use the application interface to generate CSV data.

## Technologies

* Python
* Streamlit
* Elasticsearch
* pytz
* urllib3

## License

This project is available for personal and educational use. Add your preferred license here if the repository will be distributed publicly.
