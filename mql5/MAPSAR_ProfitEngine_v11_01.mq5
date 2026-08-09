//+------------------------------------------------------------------+
//|                                 MAPSAR_ProfitEngine_v11.mq5       |
//|        Universal Multi-Asset & Micro/Macro Equity Profit Engine   |
//|  TEMA + AC + SAR + RSI Veto + Volume + MA(10/20/50/100/200)+ADX   |
//|  Local Orchestrator (FastAPI) Integration with Tier-2 ML Signals  |
//|                                                                    |
//|  v11 CHANGES vs v10:                                               |
//|   - Replaced blind fixed 5-min "close no matter what" exit with:   |
//|       (a) intelligent profit-peak giveback exit (locks in gains    |
//|           once an armed winning trade starts handing profit back)  |
//|       (b) a configurable max-hold SAFETY NET (default 60 min, not  |
//|           5) that now correctly filters by Magic Number too        |
//|   - New floating-loss kill switch, independent of SL (slippage/gap |
//|     protection), checked every tick AND every heartbeat tick        |
//|   - Drawdown-halt now force-closes any open position, not just      |
//|     blocking new entries                                            |
//|   - New OnTimer() heartbeat (default 30s) so risk/exit management   |
//|     and the 5-minute evaluation cadence keep running even during    |
//|     quiet tick periods - not fully dependent on ticks arriving      |
//|   - PostTick() moved off the entry-decision critical path and given |
//|     its own short timeout, so a slow orchestrator can never delay   |
//|     an entry                                                        |
//|   - ComputeTodaysRealizedPL() no longer rescans full deal history   |
//|     every tick - maintained incrementally, seeded once at start     |
//|   - InpBlockOversizedMicroRisk / InpMicroRiskTolerance are now      |
//|     actually wired into TryOpenTrade() (were dead inputs in v10)    |
//|   - GetEffectiveThreshold() now has a symmetric win-streak relief,  |
//|     not just a loss-streak penalty                                  |
//|   - Consumes orchestrator-scheduled trading-hour window             |
//|     (scheduled_start_hour/scheduled_end_hour) when available,       |
//|     falling back to the static session inputs otherwise             |
//+------------------------------------------------------------------+
#property copyright "InfoScience"
#property version   "11.01"
// v11.01: fixed PostTelemetry() to actually send tier2_confidence (was captured
// but never transmitted) - required for the new confidence-calibration job.

#include <Trade\Trade.mqh>
#include <Trade\PositionInfo.mqh>

//=====================================================================
// INPUT PARAMETERS
//=====================================================================
input string   InpSection0            = "==== Core Settings ====";
input ulong    InpMagicNumber         = 1955224281;
input bool     InpTradeOnBarCloseOnly = true;

input string   InpSection1            = "==== Local SQLite Fallback (optional, legacy) ====";
input bool     InpUseDatabase         = false;                       // Load parameters from local SQLite (offline fallback only)
input string   InpDatabaseFilename    = "mapsar_strategies.db";

input string   InpSection1b           = "==== Local Orchestrator Settings ====";
input bool     InpUseOrchestrator     = true;                        // Enable orchestrator sync (params + telemetry + tick feed)
input string   InpOrchestratorURL     = "http://127.0.0.1:8000";     // e.g. http://127.0.0.1:8000 locally
input string   InpOrchestratorApiKey  = "";                          // X-API-Key header (fill in on attach - do not hardcode)
input int      InpParamsRefetchMinutes= 15;                          // Re-pull strategy_params this often
input string   InpAssetOverride       = "";                          // Force this asset key (blank = auto-strip broker suffix)
input bool     InpPostTicks           = true;                        // Post one feature snapshot per closed bar to /ticks
input int      InpTickPostTimeoutMs   = 1500;                        // Short timeout - telemetry must never block entries
input bool     InpEnforceLiveApproval = false;                       // Block entries on REAL accounts unless orchestrator says live_approved=true

input string   InpSection2            = "==== Signal Module 1: TEMA (Trend) ====";
input int      InpTemaPeriod          = 14;
input ENUM_APPLIED_PRICE InpTemaPrice = PRICE_CLOSE;
input double   InpTemaWeight          = 1.0;

input string   InpSection3            = "==== Signal Module 2: AC (Momentum) ====";
input double   InpAcWeight            = 0.8;

input string   InpSection4            = "==== Signal Module 3: Parabolic SAR ====";
input double   InpSarStep             = 0.02;
input double   InpSarMax              = 0.2;
input double   InpSarWeight           = 1.0;

input string   InpSection5            = "==== Signal Module 4: Tick Volume ====";
input int      InpVolMaPeriod         = 20;
input double   InpVolWeight           = 0.6;

input string   InpSection5b           = "==== Signal Module 5: RSI (Exhaustion Veto) ====";
input bool     InpUseRsiFilter        = true;
input int      InpRsiPeriod           = 14;
input double   InpRsiOverbought       = 70.0;
input double   InpRsiOversold         = 30.0;

input string   InpSection5c           = "==== Feature Feed Only: Trend Baseline (MA/ADX) ====";
input int      InpAdxPeriod           = 14;

input string   InpSection6            = "==== Entry Decision Thresholds ====";
input double   InpScoreThresholdOpen  = 0.60;
input double   InpWinReliefPerWin     = 0.02;                        // threshold relief per consecutive win
input double   InpWinReliefCap        = 0.10;                        // max total relief from a win streak
input double   InpMinEffectiveThreshold = 0.30;                      // floor - never let relief make entries too easy

input string   InpSection7            = "==== ATR Adaptive Risk Engine (Universal Equity) ====";
input int      InpAtrPeriod           = 14;
input double   InpAtrSLMultiplier     = 1.5;
input double   InpAtrTPMultiplier     = 3.0;
input double   InpRiskPercentPerTrade = 2.0;
input double   InpMinLot              = 0.01;
input double   InpMaxLot              = 5.00;
input double   InpMarginBufferUSD     = 0.50;
input bool     InpBlockOversizedMicroRisk = true;                    // now actually enforced in TryOpenTrade()
input double   InpMicroRiskTolerance  = 1.5;                         // block if lot-step rounding pushes actual risk > intended risk% x this

input string   InpSection8            = "==== Trailing (SAR-driven) ====";
input bool     InpUseSarTrailing      = true;
input double   InpTrailStepPoints     = 50;

input string   InpSection9            = "==== Session / Time Filter ====";
input bool     InpUseSessionFilter    = true;
input int      InpSessionStartHour    = 7;
input int      InpSessionEndHour      = 20;
input bool     InpBlockFridayLateHrs  = true;
input int      InpFridayCutoffHour    = 17;

input string   InpSection10           = "==== Loss-Adaptive Self-Throttling ====";
input bool     InpUseAdaptiveRisk     = true;
input int      InpLossStreakForCooldown = 3;
input double   InpCooldownRiskMultiplier = 0.5;
input int      InpWinsToRestoreRisk   = 2;

input string   InpSection11           = "==== Circuit Breakers ====";
input double   InpMaxDailyLossPercent = 5.0;
input double   InpMaxDrawdownPercent  = 15.0;
input int      InpMaxConsecutiveLosses= 5;

input string   InpSection12           = "==== Floating-Loss Kill Switch (independent of SL/slippage) ====";
input bool     InpUseFloatingLossKillSwitch = true;
input double   InpMaxFloatingLossPercent = 3.0;                      // % of equity - force-close if breached even before SL fills

input string   InpSection13           = "==== Profit-Peak Protection (Trend Give-back Exit) ====";
input bool     InpUseProfitPeakExit   = true;
input double   InpProfitArmPercent    = 0.5;                         // floating profit must reach this % of equity before protection arms
input double   InpProfitGivebackPercent = 30.0;                      // close if profit has receded this % from its peak since arming
input bool     InpRequireMomentumFlipForGiveback = true;             // require AC momentum to also turn against the position (fewer false exits)

input string   InpSection13b          = "==== Optional Early Loss Cut (aggressive - off by default) ====";
input bool     InpEarlyLossCutEnabled = false;                       // OFF by default: cutting before SL is a real win-rate/whipsaw trade-off
input double   InpEarlyLossCutRFraction = 0.6;                       // fraction of SL distance; only fires if momentum also confirms against

input string   InpSection14           = "==== Execution Heartbeat & Max Hold ====";
input int      InpHeartbeatSeconds    = 30;                          // OnTimer cadence - safety net for quiet tick periods
input int      InpMaxHoldMinutes      = 60;                          // hard safety cap only - NOT a profit/loss-blind timer like v10's 5 min

//=====================================================================
// GLOBAL VARIABLES
//=====================================================================
CTrade         trade;
CPositionInfo  posInfo;

int      hTema, hAC, hSAR, hATR, hVol, hRSI, hADX;
int      hMA10, hMA20, hMA50, hMA100, hMA200;

datetime lastBarTime        = 0;
int      todaysTrades       = 0;
datetime todaysDayStamp     = 0;
double   dayStartEquity     = 0.0;
double   dayRealizedPL      = 0.0;      // v11: maintained incrementally, not rescanned every tick
datetime lastParamsFetch    = 0;

double   peakEquity         = 0.0;
int      consecutiveLosses  = 0;
int      consecutiveWins    = 0;
bool     riskCooldownActive = false;
bool     drawdownHalt       = false;
bool     orchestratorHealthy= false;

// Dynamic parameters populated from SQLite or the orchestrator
double   db_opt_threshold      = 0.60;
double   db_opt_sl_mult        = 1.5;
double   db_opt_tp_mult        = 3.0;
bool     db_live_approved      = false;
long     db_latest_forecast_id = -1;
string   db_tier2_action       = "HOLD";
double   db_tier2_confidence   = 0.0;
double   db_tier2_lot_mult     = 1.0;
int      db_scheduled_start_hour = -1;  // v11: -1 = orchestrator hasn't scheduled a window yet -> fall back to static inputs
int      db_scheduled_end_hour   = -1;

//+------------------------------------------------------------------+
//| One struct = one snapshot of every indicator, shared by both the |
//| local entry decision and the tick feed posted to the orchestrator|
//+------------------------------------------------------------------+
struct SignalContext
  {
   double price;
   double tema, ac, sar, adx;
   double ma10, ma20, ma50, ma100, ma200;
   double rsi;
   double curVol, avgVol;
   double score;
  };

//+------------------------------------------------------------------+
//| Entry-context cache: keyed by position ticket, carries everything|
//| known at trade-open time through to the close-time telemetry post|
//| v11 adds peakProfit/armed for the profit-peak giveback exit.     |
//+------------------------------------------------------------------+
struct EntryCtx
  {
   ulong  ticket;
   double rsi;
   double score;
   long   forecastId;
   int    sessionHour;
   double slPrice;
   double tpPrice;
   double tier2Confidence;
   double peakProfit;
   bool   armed;
  };

#define ENTRY_CTX_MAX 200
EntryCtx entryCtxArr[ENTRY_CTX_MAX];
int      entryCtxCount = 0;

void StoreEntryContext(EntryCtx &ctx)
  {
   if(entryCtxCount >= ENTRY_CTX_MAX)
     {
      for(int i = 1; i < ENTRY_CTX_MAX; i++) entryCtxArr[i-1] = entryCtxArr[i];
      entryCtxCount = ENTRY_CTX_MAX - 1;
     }
   entryCtxArr[entryCtxCount] = ctx;
   entryCtxCount++;
  }

// v11: look up an entry context WITHOUT removing it, so profit-peak tracking
// can update it live on every tick while the position is still open.
int FindEntryContextIndex(ulong ticket)
  {
   for(int i = 0; i < entryCtxCount; i++)
      if(entryCtxArr[i].ticket == ticket) return(i);
   return(-1);
  }

bool GetAndRemoveEntryContext(ulong ticket, EntryCtx &outCtx)
  {
   for(int i = 0; i < entryCtxCount; i++)
     {
      if(entryCtxArr[i].ticket == ticket)
        {
         outCtx = entryCtxArr[i];
         for(int j = i; j < entryCtxCount - 1; j++) entryCtxArr[j] = entryCtxArr[j+1];
         entryCtxCount--;
         return(true);
        }
     }
   outCtx.ticket = 0; outCtx.rsi = 0; outCtx.score = 0; outCtx.forecastId = -1;
   outCtx.sessionHour = 0; outCtx.slPrice = 0; outCtx.tpPrice = 0;
   outCtx.tier2Confidence = 0; outCtx.peakProfit = 0; outCtx.armed = false;
   return(false);
  }

//+------------------------------------------------------------------+
bool UsingRemoteParams() { return(InpUseDatabase || InpUseOrchestrator); }

bool IsDemoAccount() { return((ENUM_ACCOUNT_TRADE_MODE)AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO); }

string NormalizeAssetKey()
  {
   if(InpAssetOverride != "") return(InpAssetOverride);
   string s = _Symbol;
   int cutAt = StringLen(s);
   for(int i = 0; i < StringLen(s); i++)
     {
      ushort ch = StringGetCharacter(s, i);
      bool isAlphaNum = (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
      if(!isAlphaNum && i >= 5) { cutAt = i; break; }
     }
   return(StringSubstr(s, 0, cutAt));
  }

//+------------------------------------------------------------------+
//| ORCHESTRATOR REST CLIENT                                          |
//+------------------------------------------------------------------+
string OrchestratorHeaders(string contentType = "")
  {
   string h = StringFormat("X-API-Key: %s\r\nAccept: application/json\r\n", InpOrchestratorApiKey);
   if(contentType != "") h += StringFormat("Content-Type: %s\r\n", contentType);
   return(h);
  }

bool CheckOrchestratorHealth()
  {
   if(!InpUseOrchestrator) return(true);
   string url = InpOrchestratorURL + "/health";
   char data[]; char result[]; string resultHeaders;
   int res = WebRequest("GET", url, OrchestratorHeaders(), 5000, data, result, resultHeaders);
   if(res == 200) { orchestratorHealthy = true; return(true); }

   string body = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   PrintFormat("Orchestrator health check FAILED (HTTP %d): %s | url=%s.", res, body, url);
   orchestratorHealthy = false;
   return(false);
  }

bool JsonGetNumber(string json, string key, double &outVal)
  {
   string needle = "\"" + key + "\":";
   int pos = StringFind(json, needle);
   if(pos < 0) return(false);
   string sub = StringSubstr(json, pos + StringLen(needle));
   StringTrimLeft(sub);
   if(StringSubstr(sub, 0, 4) == "null") return(false);
   outVal = StringToDouble(sub);
   return(true);
  }

bool JsonGetBool(string json, string key, bool &outVal)
  {
   string needle = "\"" + key + "\":";
   int pos = StringFind(json, needle);
   if(pos < 0) return(false);
   string sub = StringSubstr(json, pos + StringLen(needle));
   StringTrimLeft(sub);
   outVal = (StringSubstr(sub, 0, 4) == "true");
   return(true);
  }

bool JsonGetString(string json, string key, string &outVal)
  {
   string needle = "\"" + key + "\":";
   int pos = StringFind(json, needle);
   if(pos < 0) return(false);
   int start = pos + StringLen(needle);
   while(start < StringLen(json) && (StringGetCharacter(json, start) == ' ' || StringGetCharacter(json, start) == '\t'))
      start++;
   if(StringGetCharacter(json, start) == '\"')
     {
      start++;
      int end = StringFind(json, "\"", start);
      if(end < 0) return(false);
      outVal = StringSubstr(json, start, end - start);
      return(true);
     }
   return(false);
  }

// GET /strategy_params?asset=X -> loads parameters including Tier-2 fields + v11 scheduled hours
bool FetchStrategyParams(string asset)
  {
   if(!InpUseOrchestrator) return(false);
   string url = StringFormat("%s/strategy_params?asset=%s", InpOrchestratorURL, asset);
   char data[]; char result[]; string resultHeaders;

   int res = WebRequest("GET", url, OrchestratorHeaders(), 5000, data, result, resultHeaders);
   string body = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);

   if(res != 200)
     {
      PrintFormat("Orchestrator strategy_params FAILED for '%s'. HTTP %d | Body: %s", asset, res, body);
      return(false);
     }

   double v;
   if(JsonGetNumber(body, "opt_threshold", v)) db_opt_threshold = v;
   if(JsonGetNumber(body, "opt_sl_mult", v))    db_opt_sl_mult   = v;
   if(JsonGetNumber(body, "opt_tp_mult", v))    db_opt_tp_mult   = v;
   bool lv;
   if(JsonGetBool(body, "live_approved", lv))   db_live_approved = lv;
   double fid;
   if(JsonGetNumber(body, "forecast_id", fid))  db_latest_forecast_id = (long)fid;
   else                                         db_latest_forecast_id = -1;

   JsonGetString(body, "tier2_action", db_tier2_action);
   if(JsonGetNumber(body, "tier2_confidence", v)) db_tier2_confidence = v;
   if(JsonGetNumber(body, "recommended_lot_multiplier", v)) db_tier2_lot_mult = v;

   // v11: predictive scheduling window, if the orchestrator's hour-scheduler job has populated it
   double sh, eh;
   if(JsonGetNumber(body, "scheduled_start_hour", sh)) db_scheduled_start_hour = (int)sh; else db_scheduled_start_hour = -1;
   if(JsonGetNumber(body, "scheduled_end_hour", eh))   db_scheduled_end_hour   = (int)eh; else db_scheduled_end_hour   = -1;

   PrintFormat("Orchestrator params loaded for '%s' -> Threshold=%.2f SL=%.2f TP=%.2f Tier2Action=%s LotMult=%.2f SchedWindow=%d-%d",
               asset, db_opt_threshold, db_opt_sl_mult, db_opt_tp_mult, db_tier2_action, db_tier2_lot_mult,
               db_scheduled_start_hour, db_scheduled_end_hour);
   return(true);
  }

void PostTick(string asset, SignalContext &ctx)
  {
   if(!InpUseOrchestrator || !InpPostTicks) return;

   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);

   string json = StringFormat(
      "{\"asset\":\"%s\",\"bid\":%.5f,\"ask\":%.5f,\"tick_volume\":%.0f,\"rsi\":%.2f,"
      "\"tema\":%.5f,\"ac\":%.5f,\"sar\":%.5f,\"adx\":%.2f,"
      "\"ma10\":%.5f,\"ma20\":%.5f,\"ma50\":%.5f,\"ma100\":%.5f,\"ma200\":%.5f}",
      asset, bid, ask, ctx.curVol, ctx.rsi, ctx.tema, ctx.ac, ctx.sar, ctx.adx,
      ctx.ma10, ctx.ma20, ctx.ma50, ctx.ma100, ctx.ma200);

   char data[]; char result[]; string resultHeaders;
   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(data, ArraySize(data) - 1);

   // v11: short dedicated timeout - this is telemetry, never allowed to hold up an entry decision.
   WebRequest("POST", InpOrchestratorURL + "/ticks", OrchestratorHeaders("application/json"), InpTickPostTimeoutMs, data, result, resultHeaders);
  }

void PostTelemetry(string asset, string type, double price, double lots, double profit, EntryCtx &ctx)
  {
   if(!InpUseOrchestrator) return;

   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   string accountType = IsDemoAccount() ? "demo" : "live";
   string forecastField = (ctx.forecastId >= 0) ? StringFormat("%d", (int)ctx.forecastId) : "null";

   // v11.1 FIX: tier2_confidence was being captured into EntryCtx at entry time
   // but was never actually placed into this JSON payload, so the orchestrator
   // never received it and it could never be validated against outcomes. Now sent.
   string json = StringFormat(
      "{\"asset\":\"%s\",\"type\":\"%s\",\"price\":%.5f,\"lots\":%.2f,\"profit\":%.2f,"
      "\"rsi\":%.2f,\"entry_score\":%.2f,\"sl_price\":%.5f,\"tp_price\":%.5f,"
      "\"magic_number\":%d,\"account_type\":\"%s\",\"session_hour\":%d,\"forecast_id\":%s,"
      "\"tier2_confidence\":%.3f}",
      asset, type, price, lots, profit, ctx.rsi, ctx.score, ctx.slPrice, ctx.tpPrice,
      (int)InpMagicNumber, accountType, ctx.sessionHour, forecastField, ctx.tier2Confidence);

   char data[]; char result[]; string resultHeaders;
   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(data, ArraySize(data) - 1);

   WebRequest("POST", InpOrchestratorURL + "/telemetry", OrchestratorHeaders("application/json"), 5000, data, result, resultHeaders);
  }

void InitDatabaseStrategy()
  {
   if(!InpUseDatabase) return;
   int db = DatabaseOpen(InpDatabaseFilename, DATABASE_OPEN_READWRITE | DATABASE_OPEN_CREATE | DATABASE_OPEN_COMMON);
   if(db == INVALID_HANDLE) return;
   string createTableSQL = "CREATE TABLE IF NOT EXISTS strategy_db (asset TEXT PRIMARY KEY, win_rate REAL, opt_threshold REAL, opt_sl_mult REAL, opt_tp_mult REAL);";
   DatabaseExecute(db, createTableSQL);
   DatabaseClose(db);
  }

double ComputeTodaysRealizedPL()
  {
   datetime dayStart = todaysDayStamp;
   if(!HistorySelect(dayStart, TimeCurrent())) return(0.0);
   double sum = 0.0;
   int total = HistoryDealsTotal();
   for(int i = 0; i < total; i++)
     {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0) continue;
      if((ulong)HistoryDealGetInteger(ticket, DEAL_MAGIC) != InpMagicNumber) continue;
      if((ENUM_DEAL_ENTRY)HistoryDealGetInteger(ticket, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;
      sum += HistoryDealGetDouble(ticket, DEAL_PROFIT) + HistoryDealGetDouble(ticket, DEAL_SWAP) + HistoryDealGetDouble(ticket, DEAL_COMMISSION);
     }
   return(sum);
  }

int OnInit()
  {
   trade.SetExpertMagicNumber(InpMagicNumber);
   trade.SetTypeFillingBySymbol(_Symbol);

   hTema  = iTEMA(_Symbol, _Period, InpTemaPeriod, 0, InpTemaPrice);
   hAC    = iAC(_Symbol, _Period);
   hSAR   = iSAR(_Symbol, _Period, InpSarStep, InpSarMax);
   hATR   = iATR(_Symbol, _Period, InpAtrPeriod);
   hVol   = iVolumes(_Symbol, _Period, VOLUME_TICK);
   hRSI   = iRSI(_Symbol, _Period, InpRsiPeriod, PRICE_CLOSE);
   hADX   = iADX(_Symbol, _Period, InpAdxPeriod);
   hMA10  = iMA(_Symbol, _Period, 10,  0, MODE_SMA, PRICE_CLOSE);
   hMA20  = iMA(_Symbol, _Period, 20,  0, MODE_SMA, PRICE_CLOSE);
   hMA50  = iMA(_Symbol, _Period, 50,  0, MODE_SMA, PRICE_CLOSE);
   hMA100 = iMA(_Symbol, _Period, 100, 0, MODE_SMA, PRICE_CLOSE);
   hMA200 = iMA(_Symbol, _Period, 200, 0, MODE_SMA, PRICE_CLOSE);

   if(hTema == INVALID_HANDLE || hAC == INVALID_HANDLE || hSAR == INVALID_HANDLE ||
      hATR == INVALID_HANDLE || hVol == INVALID_HANDLE || hRSI == INVALID_HANDLE ||
      hADX == INVALID_HANDLE || hMA10 == INVALID_HANDLE || hMA20 == INVALID_HANDLE ||
      hMA50 == INVALID_HANDLE || hMA100 == INVALID_HANDLE || hMA200 == INVALID_HANDLE)
     {
      Print("OnInit Error: Failed to create one or more indicator handles.");
      return(INIT_FAILED);
     }

   InitDatabaseStrategy();

   if(InpUseOrchestrator)
     {
      CheckOrchestratorHealth();
      string assetKey = NormalizeAssetKey();
      FetchStrategyParams(assetKey);
      lastParamsFetch = TimeCurrent();
     }

   peakEquity = AccountInfoDouble(ACCOUNT_EQUITY);

   // v11: seed the day boundary + realized P/L ONCE via a full history scan;
   // from here on it is maintained incrementally in OnTradeTransaction() so
   // NewEntriesAllowed() never has to rescan the day's full deal history again.
   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   todaysDayStamp = StringToTime(StringFormat("%04d.%02d.%02d", dt.year, dt.mon, dt.day));
   dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   dayRealizedPL  = ComputeTodaysRealizedPL();

   // v11: heartbeat so risk/exit management still runs during quiet tick periods,
   // which matters for keeping a reliable ~5-minute evaluation cadence.
   EventSetTimer(InpHeartbeatSeconds);

   return(INIT_SUCCEEDED);
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
   IndicatorRelease(hTema); IndicatorRelease(hAC); IndicatorRelease(hSAR);
   IndicatorRelease(hATR);  IndicatorRelease(hVol); IndicatorRelease(hRSI);
   IndicatorRelease(hADX);
   IndicatorRelease(hMA10); IndicatorRelease(hMA20); IndicatorRelease(hMA50);
   IndicatorRelease(hMA100); IndicatorRelease(hMA200);
  }

void RefreshParamsIfDue()
  {
   if(!InpUseOrchestrator) return;
   if((TimeCurrent() - lastParamsFetch) < InpParamsRefetchMinutes * 60) return;

   CheckOrchestratorHealth();
   FetchStrategyParams(NormalizeAssetKey());
   lastParamsFetch = TimeCurrent();
  }

bool IsNewBar()
  {
   datetime t = iTime(_Symbol, _Period, 0);
   if(t != lastBarTime) { lastBarTime = t; return(true); }
   return(false);
  }

void RefreshDailyCounter()
  {
   MqlDateTime dt;
   TimeToStruct(TimeCurrent(), dt);
   datetime dayStamp = StringToTime(StringFormat("%04d.%02d.%02d", dt.year, dt.mon, dt.day));
   if(dayStamp != todaysDayStamp)
     {
      todaysDayStamp = dayStamp;
      todaysTrades   = 0;
      dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
      dayRealizedPL  = 0.0;   // v11: fresh day - no rescan needed, just reset the running total
      consecutiveLosses = 0;
     }
  }

void UpdateDrawdownState()
  {
   double eq = AccountInfoDouble(ACCOUNT_EQUITY);
   if(eq > peakEquity) peakEquity = eq;
   double ddPercent = (peakEquity > 0) ? (peakEquity - eq) / peakEquity * 100.0 : 0.0;
   bool wasHalted = drawdownHalt;
   drawdownHalt = (ddPercent >= InpMaxDrawdownPercent);

   // v11: a drawdown halt used to only block NEW entries, leaving any already-open
   // position running on nothing but its original SL. Now it also force-closes.
   if(drawdownHalt && !wasHalted)
     {
      long type;
      if(HasOpenPosition(type))
        {
         Print("DRAWDOWN HALT TRIGGERED: force-closing open position, blocking new entries.");
         trade.PositionClose(_Symbol);
        }
     }
  }

// v11: NEW - protects against slippage/gap risk that can blow straight through
// a normal ATR-based SL (news spikes, thin liquidity, crypto wicks).
void CheckFloatingLossKillSwitch()
  {
   if(!InpUseFloatingLossKillSwitch) return;
   long type;
   if(!HasOpenPosition(type)) return;
   if(!PositionSelect(_Symbol)) return;

   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   if(equity <= 0) return;

   double floatingPL = posInfo.Profit() + posInfo.Swap();
   if(floatingPL >= 0) return;

   double floatingLossPct = (-floatingPL / equity) * 100.0;
   if(floatingLossPct >= InpMaxFloatingLossPercent)
     {
      PrintFormat("KILL SWITCH: floating loss %.2f%% of equity >= cap %.2f%%. Force-closing (SL may not have filled in time).",
                  floatingLossPct, InpMaxFloatingLossPercent);
      trade.PositionClose(_Symbol);
     }
  }

// v11: NEW - "cancel a +profit trade once the trend/profit starts dropping."
// Arms only once a real profit cushion exists (avoids closing on tick noise),
// tracks the floating-profit PEAK since arming, and closes once the position
// has given back InpProfitGivebackPercent of that peak - optionally requiring
// AC momentum to also have turned against the position for confirmation.
void ManageProfitPeakExit()
  {
   if(!InpUseProfitPeakExit) return;
   long type;
   if(!HasOpenPosition(type)) return;
   if(!PositionSelect(_Symbol)) return;

   ulong ticket = posInfo.Ticket();
   int idx = FindEntryContextIndex(ticket);
   if(idx < 0) return;   // no context (e.g. a manually-opened position) - leave it alone

   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double floatingProfit = posInfo.Profit() + posInfo.Swap();
   if(floatingProfit <= 0) return;

   double armLevel = equity * (InpProfitArmPercent / 100.0);

   if(!entryCtxArr[idx].armed)
     {
      if(floatingProfit >= armLevel)
        {
         entryCtxArr[idx].armed = true;
         entryCtxArr[idx].peakProfit = floatingProfit;
        }
      return;
     }

   if(floatingProfit > entryCtxArr[idx].peakProfit)
      entryCtxArr[idx].peakProfit = floatingProfit;

   double peak = entryCtxArr[idx].peakProfit;
   if(peak <= 0) return;

   double giveback    = peak - floatingProfit;
   double givebackPct = (giveback / peak) * 100.0;

   bool momentumAgainst = false;
   if(InpRequireMomentumFlipForGiveback)
     {
      double acBuf[2];
      if(CopyBuffer(hAC, 0, 0, 2, acBuf) >= 2)
        {
         if(type == POSITION_TYPE_BUY  && acBuf[0] < 0 && acBuf[0] < acBuf[1]) momentumAgainst = true;
         if(type == POSITION_TYPE_SELL && acBuf[0] > 0 && acBuf[0] > acBuf[1]) momentumAgainst = true;
        }
     }

   bool givebackTriggered = (givebackPct >= InpProfitGivebackPercent);
   bool shouldClose = InpRequireMomentumFlipForGiveback ? (givebackTriggered && momentumAgainst) : givebackTriggered;

   if(shouldClose)
     {
      PrintFormat("PROFIT-PEAK EXIT: locking in gains. Peak=%.2f Now=%.2f Giveback=%.1f%% (cap %.1f%%) MomentumAgainst=%s",
                  peak, floatingProfit, givebackPct, InpProfitGivebackPercent, momentumAgainst ? "true" : "false");
      trade.PositionClose(_Symbol);
     }
  }

// v11: NEW, OFF by default. An optional, more aggressive early loss cut that
// fires before SL is technically hit, IF momentum has also confirmed the move
// against you. This is a genuine trade-off: it caps worst-case loss per trade
// tighter than the SL alone, but will also cut some trades that would have
// recovered. Test on demo before enabling live.
void ManageEarlyLossCut()
  {
   if(!InpEarlyLossCutEnabled) return;
   long type;
   if(!HasOpenPosition(type)) return;
   if(!PositionSelect(_Symbol)) return;

   int idx = FindEntryContextIndex(posInfo.Ticket());
   if(idx < 0) return;

   double openPrice  = posInfo.PriceOpen();
   double slPrice     = entryCtxArr[idx].slPrice;
   if(slPrice <= 0) return;
   double slDistance = MathAbs(openPrice - slPrice);
   if(slDistance <= 0) return;

   double curPrice = (type == POSITION_TYPE_BUY) ? SymbolInfoDouble(_Symbol, SYMBOL_BID) : SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double adverseMove = (type == POSITION_TYPE_BUY) ? (openPrice - curPrice) : (curPrice - openPrice);
   if(adverseMove <= 0) return;   // currently not in loss

   double adverseFraction = adverseMove / slDistance;
   if(adverseFraction < InpEarlyLossCutRFraction) return;

   double acBuf[2];
   if(CopyBuffer(hAC, 0, 0, 2, acBuf) < 2) return;
   bool momentumAgainst = (type == POSITION_TYPE_BUY  && acBuf[0] < 0 && acBuf[0] < acBuf[1]) ||
                          (type == POSITION_TYPE_SELL && acBuf[0] > 0 && acBuf[0] > acBuf[1]);
   if(!momentumAgainst) return;

   PrintFormat("EARLY LOSS CUT: %.0f%% of SL distance reached with momentum confirming against. Closing early.", adverseFraction * 100.0);
   trade.PositionClose(_Symbol);
  }

double GetEffectiveRiskPercent()
  {
   if(InpUseAdaptiveRisk && riskCooldownActive) return(InpRiskPercentPerTrade * InpCooldownRiskMultiplier);
   return(InpRiskPercentPerTrade);
  }

// v11: added a symmetric win-streak RELIEF, floored so relief can never make
// entries easier than InpMinEffectiveThreshold - previously only loss-streaks
// affected this, which kept the system overly conservative long after a bad
// patch had genuinely ended.
double GetEffectiveThreshold()
  {
   double baseThresh = UsingRemoteParams() ? db_opt_threshold : InpScoreThresholdOpen;
   double lossBoost  = MathMin(consecutiveLosses * 0.05, 0.20);
   double winRelief  = MathMin(consecutiveWins * InpWinReliefPerWin, InpWinReliefCap);
   double effective  = baseThresh + lossBoost - winRelief;
   return(MathMax(InpMinEffectiveThreshold, MathMin(effective, 0.95)));
  }

// v11: prefers the orchestrator's predictively-scheduled hour window
// (strategy_db.scheduled_start_hour/end_hour, populated by hour_scheduler.py)
// when available, falling back to the static inputs otherwise.
bool IsWithinTradingSession()
  {
   if(!InpUseSessionFilter) return(true);
   MqlDateTime dt;
   TimeToStruct(TimeCurrent(), dt);

   int startHour = InpSessionStartHour;
   int endHour   = InpSessionEndHour;
   if(UsingRemoteParams() && db_scheduled_start_hour >= 0 && db_scheduled_end_hour >= 0)
     {
      startHour = db_scheduled_start_hour;
      endHour   = db_scheduled_end_hour;
     }

   bool withinHours = (startHour <= endHour) ?
                      (dt.hour >= startHour && dt.hour < endHour) :
                      (dt.hour >= startHour || dt.hour < endHour);
   if(!withinHours) return(false);
   if(InpBlockFridayLateHrs && dt.day_of_week == 5 && dt.hour >= InpFridayCutoffHour) return(false);
   return(true);
  }

double ComputeSignalScore(SignalContext &ctx)
  {
   double temaBuf[3], acBuf[3], sarBuf[2], adxBuf[2];
   double ma10Buf[2], ma20Buf[2], ma50Buf[2], ma100Buf[2], ma200Buf[2];
   double volBuf[];
   double rsiBuf[1];

   if(CopyBuffer(hTema, 0, 0, 3, temaBuf) < 3) return(0.0);
   if(CopyBuffer(hAC, 0, 0, 3, acBuf)   < 3) return(0.0);
   if(CopyBuffer(hSAR, 0, 0, 2, sarBuf)  < 2) return(0.0);
   if(CopyBuffer(hADX, 0, 1, 1, adxBuf)  < 1) adxBuf[0] = 0.0;
   if(CopyBuffer(hMA10, 0, 1, 1, ma10Buf) < 1) ma10Buf[0] = 0.0;
   if(CopyBuffer(hMA20, 0, 1, 1, ma20Buf) < 1) ma20Buf[0] = 0.0;
   if(CopyBuffer(hMA50, 0, 1, 1, ma50Buf) < 1) ma50Buf[0] = 0.0;
   if(CopyBuffer(hMA100, 0, 1, 1, ma100Buf) < 1) ma100Buf[0] = 0.0;
   if(CopyBuffer(hMA200, 0, 1, 1, ma200Buf) < 1) ma200Buf[0] = 0.0;

   ArraySetAsSeries(volBuf, true);
   if(CopyBuffer(hVol, 0, 0, InpVolMaPeriod, volBuf) < InpVolMaPeriod) return(0.0);

   MqlRates rates[3];
   if(CopyRates(_Symbol, _Period, 0, 3, rates) < 3) return(0.0);

   double price = rates[1].close;
   ctx.price = price;
   ctx.tema  = temaBuf[1];
   ctx.ac    = acBuf[1];
   ctx.sar   = sarBuf[1];
   ctx.adx   = adxBuf[0];
   ctx.ma10  = ma10Buf[0]; ctx.ma20 = ma20Buf[0]; ctx.ma50 = ma50Buf[0];
   ctx.ma100 = ma100Buf[0]; ctx.ma200 = ma200Buf[0];

   double temaVote = (price > temaBuf[1] && temaBuf[1] > temaBuf[2]) ? 1.0 : (price < temaBuf[1] && temaBuf[1] < temaBuf[2]) ? -1.0 : 0.0;
   double acVote = (acBuf[1] > 0 && acBuf[1] > acBuf[2]) ? 1.0 : (acBuf[1] < 0 && acBuf[1] < acBuf[2]) ? -1.0 : 0.0;
   double sarVote = (sarBuf[1] < price) ? 1.0 : -1.0;

   double volSum = 0.0;
   for(int i = 1; i < InpVolMaPeriod; i++) volSum += volBuf[i];
   double avgVol = volSum / (InpVolMaPeriod - 1);
   ctx.curVol = volBuf[0]; ctx.avgVol = avgVol;

   double volMultiplier = 1.0;
   if(avgVol > 0 && InpVolWeight > 0)
     {
      double ratio = ctx.curVol / avgVol;
      if(ratio >= 1.2) volMultiplier = 1.0 + (0.15 * InpVolWeight);
      else if(ratio < 0.7) volMultiplier = 1.0 - (0.20 * InpVolWeight);
     }

   double totalWeight = InpTemaWeight + InpAcWeight + InpSarWeight;
   if(totalWeight <= 0) return(0.0);

   double rawScore = (temaVote * InpTemaWeight + acVote * InpAcWeight + sarVote * InpSarWeight) / totalWeight;
   double finalScore = MathMax(-1.0, MathMin(1.0, rawScore * volMultiplier));

   ctx.rsi = 0.0;
   if(CopyBuffer(hRSI, 0, 1, 1, rsiBuf) >= 1)
     {
      ctx.rsi = rsiBuf[0];
      if(InpUseRsiFilter)
        {
         if(finalScore > 0 && ctx.rsi >= InpRsiOverbought) finalScore = 0.0;
         else if(finalScore < 0 && ctx.rsi <= InpRsiOversold) finalScore = 0.0;
        }
     }

   ctx.score = finalScore;
   return(finalScore);
  }

bool GetAtr(double &atrValue)
  {
   double buf[1];
   if(CopyBuffer(hATR, 0, 1, 1, buf) < 1) return(false);
   atrValue = buf[0];
   return(atrValue > 0);
  }

double CalculateLotSize(double slDistancePrice, double &lots, double &requiredMargin)
  {
   double equity    = AccountInfoDouble(ACCOUNT_EQUITY);
   double riskPct   = GetEffectiveRiskPercent();
   double riskMoney = equity * (riskPct / 100.0);

   double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickValue <= 0 || tickSize <= 0 || slDistancePrice <= 0) { lots = InpMinLot; requiredMargin = 0; return(riskPct); }

   double lossPerLot = (slDistancePrice / tickSize) * tickValue;
   if(lossPerLot <= 0) { lots = InpMinLot; requiredMargin = 0; return(riskPct); }

   // Incorporate Tier-2 Recommended Lot Multiplier from Orchestrator
   double rawLots = (riskMoney / lossPerLot) * db_tier2_lot_mult;
   double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(lotStep > 0) rawLots = MathFloor(rawLots / lotStep) * lotStep;

   lots = MathMax(InpMinLot, MathMin(InpMaxLot, rawLots));

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   requiredMargin = 0.0;
   if(!OrderCalcMargin(ORDER_TYPE_BUY, _Symbol, lots, ask, requiredMargin)) requiredMargin = 0.0;

   return(riskPct);
  }

// v11: NEW - this was a declared-but-dead input pair in v10. Blocks entries where
// lot-step rounding (common on micro/cent accounts with a 0.01 minimum lot) would
// push actual monetary risk meaningfully above the intended risk percentage.
bool IsMicroRiskOversized(double lots, double slDistancePrice)
  {
   if(!InpBlockOversizedMicroRisk) return(false);
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   if(equity <= 0) return(false);
   double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickValue <= 0 || tickSize <= 0) return(false);

   double actualRiskMoney = (slDistancePrice / tickSize) * tickValue * lots;
   double actualRiskPct   = (actualRiskMoney / equity) * 100.0;
   double intendedRiskPct = GetEffectiveRiskPercent();

   return(actualRiskPct > intendedRiskPct * InpMicroRiskTolerance);
  }

bool HasOpenPosition(long &type)
  {
   if(PositionSelect(_Symbol))
      if(posInfo.Magic() == InpMagicNumber) { type = posInfo.PositionType(); return(true); }
   return(false);
  }

bool NewEntriesAllowed()
  {
   if(drawdownHalt) return(false);
   if(consecutiveLosses >= InpMaxConsecutiveLosses) return(false);
   if(InpUseOrchestrator && !orchestratorHealthy) return(false);

   if(InpEnforceLiveApproval && !IsDemoAccount() && !db_live_approved) return(false);

   // v11: dayRealizedPL is now a running total updated in OnTradeTransaction(),
   // not a full HistorySelect/HistoryDealsTotal scan on every single tick.
   if(dayStartEquity > 0)
     {
      double dayLossPct = (-dayRealizedPL / dayStartEquity) * 100.0;
      if(dayRealizedPL < 0 && dayLossPct >= InpMaxDailyLossPercent) return(false);
     }
   return(true);
  }

void TryOpenTrade(double score, SignalContext &ctx)
  {
   double atr;
   if(!GetAtr(atr)) return;
   if(!NewEntriesAllowed()) return;

   // Confirm entry with Tier-2 Action if active
   if(InpUseOrchestrator && db_tier2_action != "HOLD")
     {
      if(db_tier2_action == "BUY" && score < GetEffectiveThreshold()) return;
      if(db_tier2_action == "SELL" && score > -GetEffectiveThreshold()) return;
     }

   double slMult = UsingRemoteParams() ? db_opt_sl_mult : InpAtrSLMultiplier;
   double tpMult = UsingRemoteParams() ? db_opt_tp_mult : InpAtrTPMultiplier;

   double slDist = atr * slMult;
   double tpDist = atr * tpMult;

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);

   double lots, reqMargin;
   CalculateLotSize(slDist, lots, reqMargin);

   // v11: micro-risk guard now actually enforced (was a dead input in v10)
   if(IsMicroRiskOversized(lots, slDist))
     {
      Print("Blocked entry: lot-step rounding pushed actual risk above micro-risk tolerance.");
      return;
     }

   double freeMargin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   if(reqMargin > 0 && (freeMargin - reqMargin) < InpMarginBufferUSD) return;

   double threshold = GetEffectiveThreshold();
   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);

   if(score >= threshold || db_tier2_action == "BUY")
     {
      double sl = NormalizeDouble(ask - slDist, _Digits);
      double tp = NormalizeDouble(ask + tpDist, _Digits);
      if(trade.Buy(lots, _Symbol, ask, sl, tp, "MAPSAR v11 Tier-2 BUY"))
        {
         todaysTrades++;
         if(PositionSelect(_Symbol))
           {
            EntryCtx e; e.ticket = posInfo.Ticket(); e.rsi = ctx.rsi; e.score = score;
            e.forecastId = db_latest_forecast_id; e.sessionHour = dt.hour; e.slPrice = sl; e.tpPrice = tp;
            e.tier2Confidence = db_tier2_confidence; e.peakProfit = 0.0; e.armed = false;
            StoreEntryContext(e);
           }
        }
     }
   else if(score <= -threshold || db_tier2_action == "SELL")
     {
      double sl = NormalizeDouble(bid + slDist, _Digits);
      double tp = NormalizeDouble(bid - tpDist, _Digits);
      if(trade.Sell(lots, _Symbol, bid, sl, tp, "MAPSAR v11 Tier-2 SELL"))
        {
         todaysTrades++;
         if(PositionSelect(_Symbol))
           {
            EntryCtx e; e.ticket = posInfo.Ticket(); e.rsi = ctx.rsi; e.score = score;
            e.forecastId = db_latest_forecast_id; e.sessionHour = dt.hour; e.slPrice = sl; e.tpPrice = tp;
            e.tier2Confidence = db_tier2_confidence; e.peakProfit = 0.0; e.armed = false;
            StoreEntryContext(e);
           }
        }
     }
  }

void ManageTrailing()
  {
   if(!InpUseSarTrailing) return;
   long type;
   if(!HasOpenPosition(type)) return;

   double sarBuf[1];
   if(CopyBuffer(hSAR, 0, 0, 1, sarBuf) < 1) return;
   double sarNow = sarBuf[0];
   double curSL  = posInfo.StopLoss();
   double curTP  = posInfo.TakeProfit();
   double point  = _Point;

   if(type == POSITION_TYPE_BUY)
     {
      if(sarNow < SymbolInfoDouble(_Symbol, SYMBOL_BID) && (curSL == 0 || sarNow > curSL + InpTrailStepPoints * point))
         trade.PositionModify(_Symbol, NormalizeDouble(sarNow, _Digits), curTP);
     }
   else if(type == POSITION_TYPE_SELL)
     {
      if(sarNow > SymbolInfoDouble(_Symbol, SYMBOL_ASK) && (curSL == 0 || sarNow < curSL - InpTrailStepPoints * point))
         trade.PositionModify(_Symbol, NormalizeDouble(sarNow, _Digits), curTP);
     }
  }

void ManageExitOnReversal(double score)
  {
   long type;
   if(!HasOpenPosition(type)) return;
   double threshold = GetEffectiveThreshold();
   if(type == POSITION_TYPE_BUY && score <= -threshold) trade.PositionClose(_Symbol);
   else if(type == POSITION_TYPE_SELL && score >= threshold) trade.PositionClose(_Symbol);
  }

// v11: RENAMED from ManageTimeExits() and fundamentally changed. v10 force-closed
// ANY open position after exactly 300 seconds regardless of whether it was up or
// down - which is almost certainly why "trades every 5 minutes" felt blind: it was
// a flat timer, not a decision. This is now a SAFETY-NET CAP ONLY (default 60 min,
// configurable), with intelligent exits (profit-peak giveback, floating-loss kill
// switch, reversal exit) handling the actual decision of when to leave a trade
// long before this ever fires. Also now correctly filters by Magic Number - v10's
// version filtered by symbol only, so it could have closed non-EA positions on
// the same chart.
void ManageMaxHoldSafetyNet()
  {
   int total = PositionsTotal();
   for(int i = total - 1; i >= 0; i--)
     {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((ulong)PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      datetime openTime = (datetime)PositionGetInteger(POSITION_TIME);
      int elapsedMinutes = (int)((TimeCurrent() - openTime) / 60);
      if(elapsedMinutes >= InpMaxHoldMinutes)
        {
         PrintFormat("MAX HOLD SAFETY NET: closing ticket %d after %d minutes (cap=%d).", ticket, elapsedMinutes, InpMaxHoldMinutes);
         trade.PositionClose(ticket);
        }
     }
  }

void OnTradeTransaction(const MqlTradeTransaction &trans, const MqlTradeRequest &request, const MqlTradeResult &result)
  {
   if(trans.type != TRADE_TRANSACTION_DEAL_ADD) return;
   if(!HistoryDealSelect(trans.deal)) return;
   if((ulong)HistoryDealGetInteger(trans.deal, DEAL_MAGIC) != InpMagicNumber) return;
   if((ENUM_DEAL_ENTRY)HistoryDealGetInteger(trans.deal, DEAL_ENTRY) != DEAL_ENTRY_OUT) return;

   double profit = HistoryDealGetDouble(trans.deal, DEAL_PROFIT) + HistoryDealGetDouble(trans.deal, DEAL_SWAP) + HistoryDealGetDouble(trans.deal, DEAL_COMMISSION);
   ENUM_DEAL_TYPE dealType = (ENUM_DEAL_TYPE)HistoryDealGetInteger(trans.deal, DEAL_TYPE);
   string typeStr = (dealType == DEAL_TYPE_BUY) ? "BUY_CLOSE" : "SELL_CLOSE";
   double price   = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
   double volume  = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);

   EntryCtx ctx;
   GetAndRemoveEntryContext(trans.position, ctx);
   PostTelemetry(NormalizeAssetKey(), typeStr, price, volume, profit, ctx);

   // v11: maintain the running daily P/L incrementally instead of rescanning history.
   dayRealizedPL += profit;

   if(profit < 0)
     {
      consecutiveLosses++; consecutiveWins = 0;
      if(InpUseAdaptiveRisk && consecutiveLosses >= InpLossStreakForCooldown && !riskCooldownActive)
         riskCooldownActive = true;
     }
   else if(profit > 0)
     {
      consecutiveWins++; consecutiveLosses = 0;
      if(riskCooldownActive && consecutiveWins >= InpWinsToRestoreRisk)
         riskCooldownActive = false;
     }
  }

// v11: NEW - runs on a fixed heartbeat (default every 30s) independent of ticks,
// so risk management, exit logic, and the trading-session gate don't silently stop
// working during quiet tick periods. This is what makes the ~5-minute evaluation
// cadence reliable rather than purely a best-effort side-effect of tick arrival.
void OnTimer()
  {
   RefreshDailyCounter();
   UpdateDrawdownState();

   CheckFloatingLossKillSwitch();
   ManageProfitPeakExit();
   ManageEarlyLossCut();
   ManageMaxHoldSafetyNet();

   ManageTrailing();
   RefreshParamsIfDue();
  }

void OnTick()
  {
   RefreshDailyCounter();
   UpdateDrawdownState();

   CheckFloatingLossKillSwitch();
   ManageProfitPeakExit();
   ManageEarlyLossCut();
   ManageMaxHoldSafetyNet();

   ManageTrailing();
   RefreshParamsIfDue();

   if(InpTradeOnBarCloseOnly && !IsNewBar()) return;

   SignalContext ctx;
   double score = ComputeSignalScore(ctx);

   long type;
   if(HasOpenPosition(type))
     {
      ManageExitOnReversal(score);
      PostTick(NormalizeAssetKey(), ctx);
      return;
     }

   if(!IsWithinTradingSession()) return;

   // v11: decision happens first; telemetry posts AFTER, with its own short
   // timeout, so a slow/unreachable orchestrator can never delay an entry.
   TryOpenTrade(score, ctx);
   PostTick(NormalizeAssetKey(), ctx);
  }

double OnTester()
  {
   double profit       = TesterStatistics(STAT_PROFIT);
   double drawdown     = TesterStatistics(STAT_EQUITY_DDREL_PERCENT);
   double profitFactor = TesterStatistics(STAT_PROFIT_FACTOR);
   int    trades       = (int)TesterStatistics(STAT_TRADES);

   if(trades < 20 || profit <= 0.0) return(0.0);
   if(drawdown <= 0.0) drawdown = 0.01;

   return((profitFactor * profit) / drawdown);
  }
//+------------------------------------------------------------------+
