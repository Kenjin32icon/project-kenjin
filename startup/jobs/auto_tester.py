import os
import subprocess
import xml.etree.ElementTree as ET
import json
import logging
from groq import AsyncGroq
from startup.db import get_pool

log = logging.getLogger("auto_tester")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")

# Adjust this path to where your MT5 is installed inside Wine on Linux Mint
MT5_TERMINAL_PATH = "/home/infoscience/.wine/drive_c/Program Files/MetaTrader 5/terminal64.exe"

async def run_headless_optimization(asset: str) -> str:
    """Generates an INI file and runs MT5 headlessly via Wine."""
    ini_content = f"""[Tester]
Expert=v10_kenjin.ex5
Symbol={asset}
Period=M5
Optimization=1
Model=1
Report={asset}_report.xml
ReplaceReport=1
ShutdownTerminal=1
"""
    ini_path = f"/tmp/{asset}_tester.ini"
    with open(ini_path, "w") as f:
        f.write(ini_content)

    # Call MT5 via Wine
    log.info(f"Starting headless optimization for {asset}...")
    subprocess.run(["wine", MT5_TERMINAL_PATH, f"/config:{ini_path}"], check=False)
    
    # MT5 saves the report in its MQL5/Tester folder. 
    # You must map this to your specific Wine directory structure.
    report_path = f"/home/infoscience/.wine/drive_c/Program Files/MetaTrader 5/Tester/{asset}_report.xml"
    return report_path

async def analyze_missed_trades_with_groq(asset: str, optimization_data: dict) -> None:
    """Uses Groq to find why 5-min trades are failing based on optimization data."""
    client = AsyncGroq(api_key=os.environ["GROQ_API_KEY"])
    
    prompt = f"""
    You are a quantitative analyst. Review this 5-minute timeframe optimization data for {asset}.
    The current system is missing micro-trend executions or executing trades that fail to reach profit.
    Analyze the parameter failures. Respond ONLY in JSON:
    {{"identified_issue": "string", "suggested_logic_tweak": "string"}}
    
    Data: {json.dumps(optimization_data)}
    """
    
    completion = await client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "system", "content": prompt}],
        temperature=0.3,
    )
    
    analysis = json.loads(completion.choices[0].message.content.strip())
    
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO missed_trade_analytics (asset, groq_analysis, suggested_logic_tweak)
            VALUES ($1, $2, $3)
            """,
            asset, analysis["identified_issue"], analysis["suggested_logic_tweak"]
        )
    log.info(f"Groq analysis complete for {asset} 5-min inefficiency.")

async def continuous_tester_cycle() -> None:
    """Main job loop to be added to APScheduler."""
    assets_to_test = ["BTCUSD", "EURUSD"] # Example assets
    
    for asset in assets_to_test:
        try:
            report_path = await run_headless_optimization(asset)
            
            # Basic check if report was generated
            if not os.path.exists(report_path):
                log.error(f"Report not found for {asset}")
                continue
                
            # Here you would use xml.etree to parse the MT5 XML report
            # For brevity, we simulate the extracted data dictionary
            mock_parsed_data = {"total_trades": 120, "win_rate": 41.5, "avg_loss_duration": "4m"}
            
            # Send to AI
            await analyze_missed_trades_with_groq(asset, mock_parsed_data)
            
        except Exception as e:
            log.exception(f"Auto-tester failed for {asset}: {e}")
