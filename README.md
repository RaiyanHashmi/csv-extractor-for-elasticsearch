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

### 2. Install Dependencies

Install the required Python packages:

```bash
pip install streamlit elasticsearch pytz urllib3
```

If the `streamlit` command is not recognized, install Streamlit using Python:

```bash
python -m pip install streamlit
```

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

You can install everything with:

```bash
python --version
pip install streamlit elasticsearch pytz urllib3
python -m pip install streamlit
python -m streamlit run csv-generator.py
```

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
3. Run the Streamlit application.
4. Open the provided localhost URL.
5. Use the application interface to generate CSV data.

## Technologies

* Python
* Streamlit
* Elasticsearch
* pytz
* urllib3

## License

This project is available for personal and educational use. Add your preferred license here if the repository will be distributed publicly.
