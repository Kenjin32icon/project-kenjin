import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
import json
import logging
from datetime import datetime
from groq import AsyncGroq
from startup.db import get_pool

log = logging.getLogger("auto_tester")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")

# Adjust this path to where your MT5 is installed inside Wine on Linux Mint
MT5_TERMINAL_PATH = "env WINEPREFIX=\"/home/infoscience/.wine\" wine-stable C:\\users\\Public\\Desktop\\FBS\ MetaTrader\ 5.lnk"

def archive_and_parse_report(wine_report_path: str, asset: str, local_reports_dir: str = "./reports") -> dict:
    """
    Moves the MT5 report to a local directory and parses the XML to extract real metrics.
    """
    # ---------------------------------------------------------
    # 1. ARCHIVE THE REPORT TO A LOCAL DIRECTORY
    # ---------------------------------------------------------
    
    # Ensure the local reports directory exists
    os.makedirs(local_reports_dir, exist_ok=True)
    
    # Create a unique filename based on the asset and current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_filename = f"{asset}_optimization_{timestamp}.xml"
    local_filepath = os.path.join(local_reports_dir, local_filename)
    
    try:
        # Copy the file from the Wine path to your local Linux directory
        shutil.copy2(wine_report_path, local_filepath)
        log.info(f"Report successfully archived to: {local_filepath}")
    except FileNotFoundError:
        log.error(f"Failed to find MT5 report at Wine path: {wine_report_path}")
        return {}

    # ---------------------------------------------------------
    # 2. PARSE THE XML DATA WITH ELEMENTTREE
    # ---------------------------------------------------------
    
    parsed_metrics = {
        "total_trades": 0,
        "win_rate": 0.0,
        "profit": 0.0,
        "drawdown": 0.0,
        "avg_loss_duration": "Unknown"
    }
    
    try:
        # Parse the XML file saved in your local directory
        tree = ET.parse(local_filepath)
        root = tree.getroot()
        
        # MT5 XML exports often use namespaces depending on the specific output format.
        # This strips namespaces to make finding tags easier.
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        
        # Search for standard metric rows.
        for row in root.findall('.//Row'):
            cells = row.findall('Cell')
            if len(cells) < 2:
                continue
                
            # Extract the text from the first cell (the label) and second cell (the value)
            try:
                label_node = cells[0].find('Data')
                value_node = cells[1].find('Data')
                
                if label_node is not None and value_node is not None:
                    label = label_node.text.strip() if label_node.text else ""
                    value = value_node.text.strip() if value_node.text else "0"
                    
                    if "Total trades" in label:
                        parsed_metrics["total_trades"] = int(value)
                    elif "Profit trades" in label:
                        # Value usually looks like "45 (41.5%)"
                        win_rate_str = value.split('(')[-1].replace('%)', '').strip()
                        parsed_metrics["win_rate"] = float(win_rate_str)
                    elif "Total Net Profit" in label:
                        parsed_metrics["profit"] = float(value)
                    elif "Maximal Drawdown" in label:
                        # Value usually looks like "150.00 (2.5%)"
                        dd_str = value.split(' ')[0].strip()
                        parsed_metrics["drawdown"] = float(dd_str)
            except Exception as e:
                log.warning(f"Skipped parsing a row due to formatting: {e}")
                continue

        log.info(f"Successfully extracted metrics for {asset}: {parsed_metrics}")
        return parsed_metrics

    except ET.ParseError as e:
        log.error(f"Failed to parse XML file {local_filepath}. Error: {e}")
        return {}
    except Exception as e:
        log.error(f"Unexpected error parsing report: {e}")
        return {}


async def run_headless_optimization(asset: str) -> str:
    """Generates an INI file and runs MT5 headlessly via Wine."""
    ini_content = f"""[Tester]
Expert=v10_kenjin.ex5
Symbol={asset}
Period=M5
Optimization=1
Model=2
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
            # 1. Run the headless optimization (Wait for it to finish)
            report_path = await run_headless_optimization(asset)
            
            # Basic check if report was generated
            if not os.path.exists(report_path):
                log.error(f"Report not found for {asset} at {report_path}")
                continue
                
            # 2. Archive to local 'reports' folder and extract real XML data
            real_parsed_data = archive_and_parse_report(
                wine_report_path=report_path, 
                asset=asset, 
                local_reports_dir="/home/infoscience/project-kenjin/reports" 
            )
            
            # 3. Only send to Groq if the parsing was successful and yielded trades
            if real_parsed_data and real_parsed_data.get("total_trades", 0) > 0:
                await analyze_missed_trades_with_groq(asset, real_parsed_data)
            else:
                log.warning(f"Skipping Groq analysis for {asset} due to empty or failed report parsing.")
            
        except Exception as e:
            log.exception(f"Auto-tester failed for {asset}: {e}")