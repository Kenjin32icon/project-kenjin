"""
Headless MT5 Strategy Tester Automation and Feedback Parsing.
"""
import os
import shutil
import subprocess
import asyncio
import xml.etree.ElementTree as ET
import json
import logging
from datetime import datetime
from groq import AsyncGroq
from startup.db import get_pool

log = logging.getLogger("auto_tester")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")


def _mt5_config():
    """Return explicit executable, Wine environment and terminal arguments.

    MT5_TERMINAL_EXE is a Windows executable path visible inside the configured
    Wine prefix (normally terminal64.exe), not a shell command string.
    """
    import shlex
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    wine_bin = os.environ.get("WINE_BIN", "wine")
    wineprefix = os.environ.get("WINEPREFIX_DEMO") or os.environ.get("WINEPREFIX") or os.path.expanduser("~/.wine-kenjin")
    terminal = os.environ.get("MT5_TERMINAL_EXE", r"C:\Program Files\FBS MetaTrader 5\terminal64.exe").strip()
    expert = os.environ.get("MT5_TESTER_EXPERT", "MAPSAR_ProfitEngine_v12_00.ex5").strip()
    reports = os.environ.get("MT5_REPORT_DIR", str(root / "reports"))
    return wine_bin, wineprefix, terminal, expert, reports


WINE_BIN, WINEPREFIX_DEMO, MT5_TERMINAL_EXE, MT5_TESTER_EXPERT, MT5_REPORT_DIR = _mt5_config()


def archive_and_parse_report(wine_report_path: str, asset: str, local_reports_dir: str = "./reports") -> dict:
    """
    Moves the MT5 report to a local directory and parses the XML to extract real metrics.
    """
    os.makedirs(local_reports_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_filename = f"{asset}_optimization_{timestamp}.xml"
    local_filepath = os.path.join(local_reports_dir, local_filename)

    try:
        shutil.copy2(wine_report_path, local_filepath)
        log.info(f"Report successfully archived to: {local_filepath}")
    except FileNotFoundError:
        log.error(f"Failed to find MT5 report at Wine path: {wine_report_path}")
        return {}

    parsed_metrics = {
        "total_trades": 0,
        "win_rate": 0.0,
        "profit": 0.0,
        "drawdown": 0.0,
        "avg_loss_duration": "Unknown"
    }

    try:
        tree = ET.parse(local_filepath)
        root = tree.getroot()

        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]

        for row in root.findall('.//Row'):
            cells = row.findall('Cell')
            if len(cells) < 2:
                continue

            try:
                label_node = cells[0].find('Data')
                value_node = cells[1].find('Data')

                if label_node is not None and value_node is not None:
                    label = label_node.text.strip() if label_node.text else ""
                    value = value_node.text.strip() if value_node.text else "0"

                    if "Total trades" in label:
                        parsed_metrics["total_trades"] = int(value)
                    elif "Profit trades" in label:
                        win_rate_str = value.split('(')[-1].replace('%)', '').strip()
                        parsed_metrics["win_rate"] = float(win_rate_str)
                    elif "Total Net Profit" in label:
                        parsed_metrics["profit"] = float(value)
                    elif "Maximal Drawdown" in label:
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
    """Run MT5 Strategy Tester with a dedicated .set that disables WebRequest.

    Install the compiled v12 EA in the MT5 data directory before enabling this job.
    """
    from pathlib import Path
    import tempfile

    root = Path(__file__).resolve().parents[2]
    reports_dir = Path(MT5_REPORT_DIR)
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_name = f"{asset}_report.xml"
    ini_path = Path(tempfile.gettempdir()) / f"kenjin_{asset}_tester.ini"
    set_path = Path(tempfile.gettempdir()) / f"kenjin_{asset}_tester.set"

    # MT5 .set format: value||value||min||max||step||Y/N.
    set_path.write_text("InpUseOrchestrator=false||false||0||true||N\nInpPostTicks=false||false||0||true||N\nInpUseGuardian=false||false||0||true||N\n", encoding="utf-8")
    ini_path.write_text(
        "[Tester]\n"
        f"Expert={MT5_TESTER_EXPERT}\n"
        f"ExpertParameters={set_path}\n"
        f"Symbol={asset}\n"
        "Period=M5\nOptimization=1\nModel=2\n"
        f"Report={reports_dir / report_name}\n"
        "ReplaceReport=1\nShutdownTerminal=1\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["WINEPREFIX"] = WINEPREFIX_DEMO
    cmd = [WINE_BIN, MT5_TERMINAL_EXE, f"/config:{ini_path}"]
    log.info("Starting MT5 optimization for %s: %s", asset, " ".join(cmd))
    timeout = int(os.environ.get("AUTO_TESTER_TIMEOUT_SECONDS", "14400"))
    proc = await asyncio.create_subprocess_exec(
        *cmd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise TimeoutError(f"MT5 Strategy Tester exceeded {timeout}s for {asset}")
    if proc.returncode != 0:
        log.error("MT5 tester exited %s for %s; stderr=%s", proc.returncode, asset, stderr.decode(errors="replace")[-2000:])

    expected = reports_dir / report_name
    if expected.exists():
        return str(expected)
    # MT5 sometimes writes the report under the active terminal data folder.
    candidates = [Path("/tmp") / report_name, Path(WINEPREFIX_DEMO) / "drive_c/Program Files/FBS MetaTrader 5/Tester" / report_name]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(expected)


async def analyze_missed_trades_with_groq(asset: str, optimization_data: dict) -> None:
    """Uses Groq to analyze 5-min trade failures and output structured RSI limit tweaks."""
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        log.error("GROQ_API_KEY environment variable missing.")
        return

    client = AsyncGroq(api_key=api_key)
    prompt = f"""
    You are a quantitative analyst. Review this 5-minute optimization data for {asset}.
    Identify whether trades were blocked by overly strict RSI veto limits (standard 30/70).
    Respond STRICTLY with a single JSON object containing these exact keys:
    {{
        "identified_issue": "string",
        "suggested_logic_tweak": "string",
        "suggested_rsi_buy_max": float,  # Value between 60.0 and 85.0
        "suggested_rsi_sell_min": float  # Value between 15.0 and 40.0
    }}
    
    Data: {json.dumps(optimization_data)}
    """

    completion = await client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "system", "content": prompt}],
        temperature=0.1,
    )

    raw = completion.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    analysis = json.loads(raw)

    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO missed_trade_analytics 
            (asset, groq_analysis, suggested_logic_tweak)
            VALUES ($1, $2, $3)
            """,
            asset, 
            analysis.get("identified_issue", ""), 
            json.dumps({
                "tweak": analysis.get("suggested_logic_tweak", ""),
                "suggested_rsi_buy_max": analysis.get("suggested_rsi_buy_max", 70.0),
                "suggested_rsi_sell_min": analysis.get("suggested_rsi_sell_min", 30.0)
            })
        )
    log.info(f"Groq structured analysis recorded for {asset}.")


async def continuous_tester_cycle() -> None:
    """Main job loop to be added to APScheduler."""
    assets_to_test = [s.strip().upper() for s in os.environ.get("AUTO_TESTER_ASSETS", "BTCUSD,EURUSD").split(",") if s.strip()]

    for asset in assets_to_test:
        try:
            report_path = await run_headless_optimization(asset)

            if not os.path.exists(report_path):
                log.error(f"Report not found for {asset} at {report_path}")
                continue

            local_reports_dir = MT5_REPORT_DIR
            real_parsed_data = archive_and_parse_report(
                wine_report_path=report_path,
                asset=asset,
                local_reports_dir=local_reports_dir
            )

            if real_parsed_data and real_parsed_data.get("total_trades", 0) > 0:
                await analyze_missed_trades_with_groq(asset, real_parsed_data)
            else:
                log.warning(f"Skipping Groq analysis for {asset} due to empty or failed report parsing.")

        except Exception as e:
            log.exception(f"Auto-tester failed for {asset}: {e}")
