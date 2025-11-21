# NASDAQ L3 (LOBSTER) Data Processor

This project processes NASDAQ L3 (LOBSTER) data.

It generates agg features for different time intervals.

LOBSTER provides two files:

1. **Order Book (OB) file** contains Level 10 order booksnapshots.
2. **Message file** contains all order events.

For detailed information see https://lobsterdata.com/info/DataStructure.php.

--

## 🚀 Getting Started

Follow these steps to set up and run the project locally.

### 1. Clone the repository

```bash
git clone https://github.com/marjonuzaj/LOBSTER.git AMAZON
cd AMAZON
```

### 2. Create and activate venv

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run code

```bash
python data_processing.py
```
