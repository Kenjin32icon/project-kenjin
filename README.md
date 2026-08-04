# Project Kenjin: Quick-Start Cheat Sheet

Welcome to Project Kenjin. This cheat sheet is designed to get your local environment configured, connected, and trading as quickly as possible.

---

## 1. What the System Does

Project Kenjin is a highly automated, multi-asset algorithmic trading engine. It replaces traditional, rigid trading parameters with dynamic, AI-driven insights. Instead of relying solely on local MetaTrader 5 (MT5) indicators, the system continuously streams market data to a local Python server. This server stores the data, evaluates historical performance, and asks a Large Language Model (Groq) to predict market direction and adjust risk parameters in real-time.

## 2. How it Works (The Architecture Loop)

1. **Data Ingestion:** The MT5 Expert Advisor (EA) watches the markets on your FBS terminal. At the close of every bar, it sends a snapshot of indicators (TEMA, AC, RSI, Volume, etc.) to the local Python Orchestrator.
2. **Storage:** The Orchestrator saves this tick data into your Supabase PostgreSQL database.
3. **Forecasting (Every 30 mins):** A background scheduler grabs the last 30 minutes of data, formats it, and sends it to Groq (an ultra-fast LLM). Groq analyses the trend and returns a bullish/bearish probability alongside optimised Take Profit and Stop Loss multipliers.
4. **Execution:** Before placing a trade, the MT5 EA checks the Orchestrator for the latest Groq forecast and strategy parameters. If conditions match, it executes the trade on FBS MT5.
5. **Gatekeeping (Every 4 hours):** A background job evaluates all closed trades. If a strategy's win rate drops below 55% or its profit factor drops below 1.3, the system automatically revokes its "live approved" status.

---

## 3. Installation & Configuration

### A. Clone the Repository & Install Dependencies

Open your terminal and run the following commands to pull the code and initialise your Python environment.

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/project-kenjin.git
cd project-kenjin

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# 3. Install the required dependencies
pip install fastapi uvicorn asyncpg pydantic apscheduler groq pandas python-dotenv

```

### B. Configure Your `.env` File

Create a file named `.env` in the root directory. You will need a Supabase account (for the database) and a Groq account (for the API key).

| Variable | Description |
| --- | --- |
| `DATABASE_URL` | Your **Transaction pooler** connection string from Supabase (e.g., `postgresql://postgres.[project]:[password]@...:6543/postgres`). |
| `ORCH_API_KEY` | A custom security key you create to lock your server. Generate one using `openssl rand -hex 32` in your terminal. |
| `GROQ_API_KEY` | Your API key from the Groq Cloud Console (`gsk_...`). |
| `GROQ_MODEL` | The LLM to use. Default to `llama3-70b-8192`. |

---

## 4. Running the System

Start the FastAPI Orchestrator using Uvicorn. Keep this terminal open; it acts as the brain of the operation.

```bash
uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000

```

*You should see logs confirming that the DB pool is initialised and the background schedulers (forecast and gatekeeper) have started.*

---

## 5. Connecting to FBS MT5

To allow your MT5 platform to speak to your local Python server, you must whitelist the server address.

1. Open your FBS MT5 terminal.
2. Navigate to **Tools > Options > Expert Advisors**.
3. Tick the box for **"Allow WebRequest for listed URL"**.
4. Click the `+` icon and add `[http://127.0.0.1:8000](http://127.0.0.1:8000)`.
5. Click **OK**.
6. Open your desired asset charts (e.g., EURUSD, GBPUSD).
7. Drag the `MAPSAR_ProfitEngine_v10` EA onto the chart. In the input settings, paste your `ORCH_API_KEY` into the API key field and ensure `InpUseOrchestrator` is set to `true`.

---

## 6. Testing the Endpoints

If you want to test the Orchestrator without waiting for MT5 to generate trades, you can simulate requests using Postman or an Electron test wrapper.

### Postman Testing

You can manually fire payloads to ensure your database and server are responding correctly.

**Test 1: Check Server Health**

* **Method:** GET
* **URL:** `[http://127.0.0.1:8000/health](http://127.0.0.1:8000/health)`
* **Expected Response:** `{"status": "ok", "db": "ok"}`

**Test 2: Simulate a Tick Upload**

* **Method:** POST
* **URL:** `[http://127.0.0.1:8000/ticks](http://127.0.0.1:8000/ticks)`
* **Headers:** Add `X-API-Key` with your custom `ORCH_API_KEY`.
* **Body (raw JSON):**
```json
{
  "asset": "EURUSD",
  "bid": 1.0950,
  "ask": 1.0952,
  "tick_volume": 120,
  "rsi": 55.4,
  "tema": 1.0945
}

```



### Electron Integration (For Custom Testing Dashboards)

If you are building an Electron application to serve as a local GUI or testing harness for Project Kenjin, you can trigger these same endpoints using Electron's main process.

Use the native `net` module in your `main.js` to securely ping the FastAPI backend:

```javascript
const { net } = require('electron');

function sendMockTick() {
  const request = net.request({
    method: 'POST',
    protocol: 'http:',
    hostname: '127.0.0.1',
    port: 8000,
    path: '/ticks'
  });

  request.setHeader('Content-Type', 'application/json');
  request.setHeader('X-API-Key', 'your_orch_api_key_here'); // Match your .env

  request.on('response', (response) => {
    console.log(`STATUS: ${response.statusCode}`);
  });

  const payload = JSON.stringify({
    asset: "GBPUSD",
    bid: 1.2650,
    ask: 1.2652,
    tick_volume: 85
  });

  request.write(payload);
  request.end();
}

```