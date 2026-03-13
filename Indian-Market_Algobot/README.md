# Algo-Trading
Fully automated intraday bullish swing option buying strategy for NIFTY using SmartAPI (Angel One) with Greeks-based stop loss, CE strike selection, and live order execution.

#  Algo Trading – Bullish Swing Option Buying Strategy

This project implements a **fully automated intraday bullish swing trading strategy** using live market data from **Angel One SmartAPI**. It detects swing patterns and places **Call Option (CE)** orders automatically when a breakout is confirmed.

---

## Strategy Overview

The strategy identifies a bullish price structure using swing highs/lows:
- Detects key points: `L1`, `H1`, `A`, `B`, `C`, and `D`
- Executes a **CE Buy** at breakout above `D`
- Sets stop loss below `C`, with a **1:2 risk-reward ratio**
- Selects the most optimal CE option using **Greeks** (Δ, Γ, Θ, Vega, IV)

---

## ⚙ Features

-  Real-time structure detection and entry signal
-  Option chain filtering with Greeks (Delta, Theta, IV, etc.)
-  Auto stop-loss calculation using Greeks + price risk
-  Automatic order placement via SmartAPI
-  Saves all order & options data in CSV logs
-  Rate-limit-safe, retries, and background threads for status checks
-  Local storage of:
  - `order_history/`
  - `options_data/`

---

## 🛠️ Tech Stack

| Component        | Tech/Library             |
|------------------|--------------------------|
| Broker API       | Angel One `SmartAPI`     |
| Auth             | `pyotp` for TOTP         |
| Data Processing  | `pandas`, `numpy`        |
| Visualization    | `matplotlib`, `mplfinance` |
| Logging          | `logzero`                |
| Timezone / Time  | `pytz`, `datetime`       |

---

## 🖥️ Project Structure

```

## 🔧 Environment Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd algo-trading
   ```

2. Create and activate virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   - Copy `.env.example` to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Edit `.env` and fill in your credentials:
     ```ini
     # Angel One API Credentials
     ANGEL_API_KEY=your_api_key
     ANGEL_USERNAME=your_username
     ANGEL_PASSWORD=your_password
     ANGEL_TOTP_KEY=your_totp_key
     
     # Trading Configuration
     SPOT_TOKEN=your_spot_token
     ```
   - Or use the setup script:
     ```bash
     python setup_env.py
     ```

5. For GCP deployment:
   ```bash
   # Follow instructions in GCP_DEPLOYMENT.md
   ```

## 🔒 Security Notes

- Never commit `.env` file - it's in `.gitignore`
- Keep your API credentials secure
- Use environment variables for all sensitive data
- Review `settings.py` before deployment

## 🚀 Running the Application

1. Local development:
   ```bash
   python main.py
   ```

2. On GCP:
   ```bash
    # Deploy using the provided script
    chmod +x deploy.sh
    sudo ./deploy.sh
    ```

```bash
algotradingabhishek/
│
├── brokers/             # Broker connection modules
├── config/              # Config & secrets
├── data/                # Market data handling
├── models/              # ML models or indicators (optional)
├── strategies/          # Trading strategies like bullish3t.py
├── utils/               # Helper functions
├── main.py              # Entry point (can be auto-run on boot)
└── README.md            # You're reading it!
