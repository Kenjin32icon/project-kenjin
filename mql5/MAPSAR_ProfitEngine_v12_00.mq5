//+------------------------------------------------------------------+
//|                                 MAPSAR_ProfitEngine_v12_00.mq5   |
//|        Universal Multi-Asset & Micro/Macro Equity Profit Engine |
//|  TEMA + AC + SAR + RSI Veto + Volume + MA(10/20/50/100/200)+ADX |
//|  Local Orchestrator (FastAPI) Integration with Tier-2 ML Signals|
//+------------------------------------------------------------------+
#property copyright "InfoScience"
#property version   "12.00"

#include <Trade\Trade.mqh>
#include <Trade\PositionInfo.mqh>

//=====================================================================
// INPUT PARAMETERS
//=====================================================================
input string   InpSection0            = "==== Core Settings ====";
input ulong    InpMagicNumber         = 1955224281;
input bool     InpTradeOnBarCloseOnly = true;

input string   InpSection1            = "==== Legacy SQLite Schema Bootstrap (no runtime parameter reads) ====";
input bool     InpUseDatabase         = false;
input string   InpDatabaseFilename    = "mapsar_strategies.db";

input string   InpSection1b           = "==== Local Orchestrator Settings ====";
input bool     InpUseOrchestrator     = true;
input string   InpOrchestratorURL     = "http://127.0.0.1:8000";
input string   InpOrchestratorApiKey  = "";
input int      InpParamsRefetchMinutes= 15;
input string   InpAssetOverride       = "";
input bool     InpPostTicks           = true;
input int      InpTickPostTimeoutMs   = 1500;
input bool     InpEnforceLiveApproval = true;

input string   InpSection1c           = "==== Strategic Profit Lifeline (v11.07) ====";
input bool     InpRespectLifelineTermination = true;

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
input double   InpWinReliefPerWin     = 0.02;
input double   InpWinReliefCap        = 0.10;
input double   InpMinEffectiveThreshold = 0.30;

input string   InpSection7            = "==== ATR Adaptive Risk Engine (Universal Equity) ====";
input int      InpAtrPeriod           = 14;
input double   InpAtrSLMultiplier     = 1.5;
input double   InpAtrTPMultiplier     = 3.0;
input double   InpRiskPercentPerTrade = 2.0;
input double   InpMinLot              = 0.01;
input double   InpMaxLot              = 5.00;
input double   InpMarginBufferUSD     = 0.50;
input bool     InpBlockOversizedMicroRisk = true;
input double   InpMicroRiskTolerance  = 1.5;

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

input string   InpSection12           = "==== Floating-Loss Kill Switch ====";
input bool     InpUseFloatingLossKillSwitch = true;
input double   InpMaxFloatingLossPercent = 3.0;

input string   InpSection13           = "==== Profit-Peak Protection (Trend Give-back Exit) ====";
input bool     InpUseProfitPeakExit   = true;
input double   InpProfitArmPercent    = 0.5;
input double   InpProfitGivebackPercent = 30.0;
input bool     InpRequireMomentumFlipForGiveback = true;

input string   InpSection13b          = "==== Optional Early Loss Cut ====";
input bool     InpEarlyLossCutEnabled = false;
input double   InpEarlyLossCutRFraction = 0.6;

input string   InpSection13c          = "==== ML-Informed In-Trade Reversal Exit ====";
input bool     InpUseMLReversalExit   = true;
input int      InpMLReversalConfirmations = 2;
input double   InpMicroTrendMinStrength = 0.4;
input int      InpOpenPositionRefreshSeconds = 60;

input string   InpSection14           = "==== Execution Heartbeat & Max Hold ====";
input int      InpHeartbeatSeconds    = 30; // retained for compatibility; v12 timer uses Guardian cadence
input int      InpMaxHoldMinutes      = 60;

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
double   dayRealizedPL      = 0.0;
datetime lastParamsFetch    = 0;
datetime lastHealthCheck    = 0;
bool     strategyParamsValid = false;

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
bool     db_trading_halted     = false;
double   db_market_bias        = 0.0;
string   db_market_regime      = "unknown";
long     db_latest_forecast_id = -1;
string   db_tier2_action       = "HOLD";
double   db_tier2_confidence   = 0.0;
double   db_tier2_lot_mult     = 1.0;
int      db_scheduled_start_hour = -1;
int      db_scheduled_end_hour   = -1;
string   db_micro_trend          = "NEUTRAL";
double   db_micro_trend_strength = 0.0;
double   db_rsi_buy_max          = 70.0;
double   db_rsi_sell_min         = 30.0;
double   db_calibration_score    = 0.0;
double   db_lifeline_score       = 100.0;
bool     db_lifeline_terminated  = false;
int      mlReversalDisagreeCount = 0;
bool     dailyLossHaltNotified    = false;
bool     consecLossHaltNotified   = false;
bool     lifelineHaltNotified     = false;
string   lastBlockReason          = "";

//=====================================================================
// STRUCTS
//=====================================================================
struct SStrategyParams
  {
   string             symbol;
   double             opt_threshold;
   double             opt_sl_mult;
   double             opt_tp_mult;
   double             rsi_buy_max;
   double             rsi_sell_min;
   int                scheduled_start_hour;
   int                scheduled_end_hour;
   double             calibration_score;
   string             micro_trend;
   double             micro_trend_strength;
   double             lifeline_score;
   bool               lifeline_terminated;
  };

struct SignalContext
  {
   double price;
   double tema, ac, sar, adx;
   double ma10, ma20, ma50, ma100, ma200;
   double rsi;
   double curVol, avgVol;
   double score;
   double rawScoreBeforeVeto;
  };

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
   double guardianPeakR;
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
   outCtx.tier2Confidence = 0; outCtx.peakProfit = 0; outCtx.guardianPeakR = 0; outCtx.armed = false;
   return(false);
  }

//=====================================================================
// POST-QUEUE (non-blocking web posts from OnTimer)
//=====================================================================
#define POST_QUEUE_SIZE 64
string postQueueEndpoint[POST_QUEUE_SIZE];
string postQueuePayload[POST_QUEUE_SIZE];
int    postQueueTimeout[POST_QUEUE_SIZE];
int    postQueueHead = 0;
int    postQueueTail = 0;
int    postQueueCount = 0;

bool EnqueuePost(const string endpoint, const string payload, const int timeoutMs)
  {
   if(!InpUseOrchestrator) return(false);
   if(postQueueCount >= POST_QUEUE_SIZE)
     {
      PrintFormat("[Kenjin EA] Post queue full, dropping %s", endpoint);
      return(false);
     }
   postQueueEndpoint[postQueueTail] = endpoint;
   postQueuePayload[postQueueTail]  = payload;
   postQueueTimeout[postQueueTail]  = timeoutMs;
   postQueueTail = (postQueueTail + 1) % POST_QUEUE_SIZE;
   postQueueCount++;
   return(true);
  }

void FlushPostQueue(int maxToSend)
  {
   int sent = 0;
   while(postQueueCount > 0 && sent < maxToSend)
     {
      string endpoint = postQueueEndpoint[postQueueHead];
      string payload  = postQueuePayload[postQueueHead];
      int timeoutMs   = postQueueTimeout[postQueueHead];

      char data[]; char result[]; string resultHeaders;
      StringToCharArray(payload, data, 0, WHOLE_ARRAY, CP_UTF8);
      ArrayResize(data, ArraySize(data) - 1);

      int http = WebRequest("POST", InpOrchestratorURL + endpoint, OrchestratorHeaders("application/json"), timeoutMs, data, result, resultHeaders);
      if(http == -1)
        {
         int err = GetLastError();
         PrintFormat("[Kenjin EA] WebRequest POST %s failed (error %d). Payload dropped.", endpoint, err);
        }
      else if(http != 200 && http != 201)
        {
         PrintFormat("[Kenjin EA] WebRequest POST %s returned HTTP %d", endpoint, http);
        }

      postQueueHead = (postQueueHead + 1) % POST_QUEUE_SIZE;
      postQueueCount--;
      sent++;
     }
  }

//=====================================================================
// UTILITIES & ORCHESTRATOR CLIENT
//=====================================================================
// v12 reads dynamic parameters from the orchestrator only. The legacy SQLite
// option merely initializes its old table and must not suppress local input defaults.
bool UsingRemoteParams() { return(InpUseOrchestrator); }
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

void GetCurrentStrategyParams(SStrategyParams &params)
  {
   params.symbol                 = NormalizeAssetKey();
   params.opt_threshold          = db_opt_threshold;
   params.opt_sl_mult            = db_opt_sl_mult;
   params.opt_tp_mult            = db_opt_tp_mult;
   params.rsi_buy_max            = db_rsi_buy_max;
   params.rsi_sell_min           = db_rsi_sell_min;
   params.scheduled_start_hour   = db_scheduled_start_hour;
   params.scheduled_end_hour     = db_scheduled_end_hour;
   params.calibration_score      = db_calibration_score;
   params.micro_trend            = db_micro_trend;
   params.micro_trend_strength   = db_micro_trend_strength;
   params.lifeline_score         = db_lifeline_score;
   params.lifeline_terminated    = db_lifeline_terminated;
  }

bool IsWithinScheduledHours(const SStrategyParams &params)
  {
   if(params.scheduled_start_hour < 0 || params.scheduled_end_hour < 0) return(true);

   MqlDateTime dt;
   TimeToStruct(TimeGMT(), dt);

   if(params.scheduled_start_hour <= params.scheduled_end_hour)
     {
      return (dt.hour >= params.scheduled_start_hour && dt.hour <= params.scheduled_end_hour);
     }
   else 
     {
      return (dt.hour >= params.scheduled_start_hour || dt.hour <= params.scheduled_end_hour);
     }
  }

// v12: in-flight reversal protection is handled by KenjinGuardian.mqh.
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

bool FetchStrategyParams(string asset)
  {
   if(!InpUseOrchestrator) return(false);
   string url = StringFormat("%s/strategy_params?asset=%s&account_type=%s", InpOrchestratorURL, asset, IsDemoAccount() ? "demo" : "live");
   char data[]; char result[]; string resultHeaders;

   int res = WebRequest("GET", url, OrchestratorHeaders(), 5000, data, result, resultHeaders);
   string body = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);

   if(res != 200)
     {
      strategyParamsValid = false;
      PrintFormat("Orchestrator strategy_params FAILED for '%s'. HTTP %d | Body: %s", asset, res, body);
      return(false);
     }

   double v;
   if(JsonGetNumber(body, "opt_threshold", v)) db_opt_threshold = v;
   if(JsonGetNumber(body, "opt_sl_mult", v))    db_opt_sl_mult   = v;
   if(JsonGetNumber(body, "opt_tp_mult", v))    db_opt_tp_mult   = v;
   bool lv;
   if(JsonGetBool(body, "live_approved", lv))   db_live_approved = lv;
   bool halted;
   if(JsonGetBool(body, "trading_halted", halted)) db_trading_halted = halted; else db_trading_halted = true;
   if(JsonGetNumber(body, "market_bias", v)) db_market_bias = v; else db_market_bias = 0.0;
   if(!JsonGetString(body, "market_regime", db_market_regime)) db_market_regime = "unknown";
   double fid;
   if(JsonGetNumber(body, "forecast_id", fid))  db_latest_forecast_id = (long)fid;
   else                                         db_latest_forecast_id = -1;

   JsonGetString(body, "tier2_action", db_tier2_action);
   if(JsonGetNumber(body, "tier2_confidence", v)) db_tier2_confidence = v;
   if(JsonGetNumber(body, "recommended_lot_multiplier", v)) db_tier2_lot_mult = v;

   string mt;
   if(JsonGetString(body, "micro_trend", mt)) db_micro_trend = mt; else db_micro_trend = "NEUTRAL";
   if(JsonGetNumber(body, "micro_trend_strength", v)) db_micro_trend_strength = v; else db_micro_trend_strength = 0.0;
   if(JsonGetNumber(body, "rsi_buy_max", v))  db_rsi_buy_max  = v;
   if(JsonGetNumber(body, "rsi_sell_min", v)) db_rsi_sell_min = v;
   if(JsonGetNumber(body, "calibration_score", v)) db_calibration_score = v;

   if(JsonGetNumber(body, "lifeline_score", v)) db_lifeline_score = v; else db_lifeline_score = 100.0;
   bool lt;
   if(JsonGetBool(body, "lifeline_terminated", lt)) db_lifeline_terminated = lt; else db_lifeline_terminated = false;

   double sh, eh;
   if(JsonGetNumber(body, "scheduled_start_hour", sh)) db_scheduled_start_hour = (int)sh; else db_scheduled_start_hour = -1;
   if(JsonGetNumber(body, "scheduled_end_hour", eh))   db_scheduled_end_hour   = (int)eh; else db_scheduled_end_hour   = -1;

   strategyParamsValid = true;
   PrintFormat("Orchestrator params loaded for '%s' -> Threshold=%.2f SL=%.2f TP=%.2f Tier2Action=%s LotMult=%.2f SchedWindow=%d-%d LifelineScore=%.1f LifelineTerminated=%s",
               asset, db_opt_threshold, db_opt_sl_mult, db_opt_tp_mult, db_tier2_action, db_tier2_lot_mult,
               db_scheduled_start_hour, db_scheduled_end_hour, db_lifeline_score, db_lifeline_terminated ? "true" : "false");
   return(true);
  }

//=====================================================================
// TELEMETRY QUEUEING
//=====================================================================
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

   EnqueuePost("/ticks", json, InpTickPostTimeoutMs);
  }

void PostTelemetry(string asset, string type, double price, double lots, double profit, EntryCtx &ctx)
  {
   if(!InpUseOrchestrator) return;

   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   string accountType = IsDemoAccount() ? "demo" : "live";
   string forecastField = (ctx.forecastId >= 0) ? StringFormat("%d", (int)ctx.forecastId) : "null";

   string json = StringFormat(
      "{\"asset\":\"%s\",\"type\":\"%s\",\"price\":%.5f,\"lots\":%.2f,\"profit\":%.2f,"
      "\"rsi\":%.2f,\"entry_score\":%.2f,\"sl_price\":%.5f,\"tp_price\":%.5f,"
      "\"magic_number\":%I64u,\"account_type\":\"%s\",\"session_hour\":%d,\"forecast_id\":%s,"
      "\"tier2_confidence\":%.3f}",
      asset, type, price, lots, profit, ctx.rsi, ctx.score, ctx.slPrice, ctx.tpPrice,
      InpMagicNumber, accountType, ctx.sessionHour, forecastField, ctx.tier2Confidence);

   EnqueuePost("/telemetry", json, 5000);
  }

void PostAccountSnapshot()
  {
   if(!InpUseOrchestrator) return;

   string accountType = IsDemoAccount() ? "demo" : "live";
   double balance      = AccountInfoDouble(ACCOUNT_BALANCE);
   double equity       = AccountInfoDouble(ACCOUNT_EQUITY);
   double margin       = AccountInfoDouble(ACCOUNT_MARGIN);
   double marginLevel  = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   double floatingPL   = equity - balance;
   double ddPct        = (peakEquity > 0) ? (peakEquity - equity) / peakEquity * 100.0 : 0.0;
   double dayLossPct   = (dayStartEquity > 0 && dayRealizedPL < 0) ? (-dayRealizedPL / dayStartEquity) * 100.0 : 0.0;

   string json = StringFormat(
      "{\"account_type\":\"%s\",\"login\":%d,\"asset\":\"%s\",\"balance\":%.2f,\"equity\":%.2f,"
      "\"margin\":%.2f,\"margin_level\":%.2f,\"floating_pl\":%.2f,\"peak_equity\":%.2f,"
      "\"drawdown_pct\":%.2f,\"day_loss_pct\":%.2f,\"day_start_balance\":%.2f,\"consecutive_losses\":%d,\"consecutive_wins\":%d,"
      "\"risk_cooldown_active\":%s,\"drawdown_halt\":%s}",
      accountType, (int)AccountInfoInteger(ACCOUNT_LOGIN), NormalizeAssetKey(), balance, equity,
      margin, marginLevel, floatingPL, peakEquity, ddPct, dayLossPct, dayStartEquity,
      consecutiveLosses, consecutiveWins,
      riskCooldownActive ? "true" : "false", drawdownHalt ? "true" : "false");

   EnqueuePost("/account_snapshot", json, InpTickPostTimeoutMs);
  }

void PostRiskIncident(string reason, string details)
  {
   if(!InpUseOrchestrator) return;

   string accountType = IsDemoAccount() ? "demo" : "live";
   string json = StringFormat(
      "{\"account_type\":\"%s\",\"asset\":\"%s\",\"reason\":\"%s\",\"details\":\"%s\"}",
      accountType, NormalizeAssetKey(), reason, details);

   EnqueuePost("/risk_incident", json, InpTickPostTimeoutMs);
  }

void PostMissedTrade(string asset, string reason, string side, double score, double rawScore,
                      double threshold, SignalContext &ctx, double atr, double slDist, double tpDist)
  {
   if(!InpUseOrchestrator) return;

   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   string accountType = IsDemoAccount() ? "demo" : "live";
   double sidePrice = (side == "BUY") ? SymbolInfoDouble(_Symbol, SYMBOL_ASK) : SymbolInfoDouble(_Symbol, SYMBOL_BID);

   string json = StringFormat(
      "{\"asset\":\"%s\",\"account_type\":\"%s\",\"reason\":\"%s\",\"side\":\"%s\","
      "\"score\":%.4f,\"raw_score\":%.4f,\"threshold\":%.4f,\"rsi\":%.2f,"
      "\"price\":%.5f,\"atr\":%.5f,\"sl_distance\":%.5f,\"tp_distance\":%.5f,"
      "\"tier2_action\":\"%s\",\"tier2_confidence\":%.3f,\"session_hour\":%d,\"magic_number\":%I64u}",
      asset, accountType, reason, side,
      score, rawScore, threshold, ctx.rsi,
      sidePrice, atr, slDist, tpDist,
      db_tier2_action, db_tier2_confidence, dt.hour, InpMagicNumber);

   EnqueuePost("/missed_trade", json, InpTickPostTimeoutMs);
  }

//=====================================================================
// FEATURE / SIGNAL COMPUTATION
//=====================================================================
double ComputeSignalScore(SignalContext &ctx)
  {
   double temaBuf[3], acBuf[3], sarBuf[3], adxBuf[2];
   double ma10Buf[2], ma20Buf[2], ma50Buf[2], ma100Buf[2], ma200Buf[2];
   double volBuf[]; 
   double rsiBuf[1];

   ArraySetAsSeries(temaBuf, true);
   ArraySetAsSeries(acBuf, true);
   ArraySetAsSeries(sarBuf, true);

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
   int volToCopy = MathMax( MathMax(InpVolMaPeriod, 3), 3 );
   if(CopyBuffer(hVol, 0, 0, volToCopy, volBuf) < volToCopy) return(0.0);

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
   ctx.curVol = volBuf[1]; 
   ctx.avgVol = avgVol;

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

   ctx.rawScoreBeforeVeto = finalScore;
   ctx.rsi = 0.0;
   
   if(CopyBuffer(hRSI, 0, 1, 1, rsiBuf) >= 1)
     {
      ctx.rsi = rsiBuf[0];
      if(InpUseRsiFilter)
        {
         if(finalScore > 0 && ctx.rsi >= db_rsi_buy_max) finalScore = 0.0;
         else if(finalScore < 0 && ctx.rsi <= db_rsi_sell_min) finalScore = 0.0;
        }
     }

   ctx.score = finalScore;
   return(finalScore);
  }

//=====================================================================
// EA CORE LOGIC
//=====================================================================

bool GetAtr(double &atrValue)
  {
   double buf[1];
   if(CopyBuffer(hATR, 0, 1, 1, buf) < 1) return(false);
   atrValue = buf[0];
   return(atrValue > 0);
  }

bool CalculateLotSize(double slDistancePrice, bool isBuy, double &lots, double &requiredMargin)
  {
   lots = 0.0;
   requiredMargin = 0.0;
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double riskPct = GetEffectiveRiskPercent();
   double riskMoney = equity * (riskPct / 100.0);
   double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   double volumeMin = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double volumeMax = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double volumeStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(equity <= 0 || riskMoney <= 0 || tickValue <= 0 || tickSize <= 0 || slDistancePrice <= 0 ||
      volumeMin <= 0 || volumeMax < volumeMin || volumeStep <= 0)
      return(false);

   double lossPerLot = (slDistancePrice / tickSize) * tickValue;
   if(lossPerLot <= 0) return(false);
   double rawLots = (riskMoney / lossPerLot) * db_tier2_lot_mult;
   double allowedMin = MathMax(InpMinLot, volumeMin);
   double allowedMax = MathMin(InpMaxLot, volumeMax);
   if(allowedMax < allowedMin) return(false);

   // Round DOWN to the broker step. Never force a broker-minimum lot when it
   // would exceed the configured cash-risk budget.
   lots = MathFloor(rawLots / volumeStep) * volumeStep;
   lots = NormalizeDouble(lots, 8);
   if(lots < allowedMin || lots <= 0) { lots = 0.0; return(false); }
   lots = MathMin(lots, allowedMax);

   double price = isBuy ? SymbolInfoDouble(_Symbol, SYMBOL_ASK) : SymbolInfoDouble(_Symbol, SYMBOL_BID);
   ENUM_ORDER_TYPE orderType = isBuy ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   if(price <= 0 || !OrderCalcMargin(orderType, _Symbol, lots, price, requiredMargin))
     { lots = 0.0; requiredMargin = 0.0; return(false); }
   return(true);
  }

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

bool SelectOurPosition()
  {
   // PositionSelect(symbol) is ambiguous on hedging accounts. Select by ticket
   // after filtering both symbol and magic so this EA never manages another EA's trade.
   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((ulong)PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;
      if(posInfo.SelectByTicket(ticket)) return(true);
     }
   return(false);
  }

bool HasOpenPosition(long &type)
  {
   if(SelectOurPosition())
     { type = posInfo.PositionType(); return(true); }
   return(false);
  }

double GetEffectiveRiskPercent()
  {
   if(InpUseAdaptiveRisk && riskCooldownActive) return(InpRiskPercentPerTrade * InpCooldownRiskMultiplier);
   return(InpRiskPercentPerTrade);
  }

double GetEffectiveThreshold()
  {
   double baseThresh = UsingRemoteParams() ? db_opt_threshold : InpScoreThresholdOpen;
   double lossBoost  = MathMin(consecutiveLosses * 0.05, 0.20);
   double winRelief  = MathMin(consecutiveWins * InpWinReliefPerWin, InpWinReliefCap);
   double effective  = baseThresh + lossBoost - winRelief;
   return(MathMax(InpMinEffectiveThreshold, MathMin(effective, 0.95)));
  }

bool SignalWouldHaveTraded(double rawScore, double threshold, string &sideOut)
  {
   if(rawScore >= threshold || db_tier2_action == "BUY")  { sideOut = "BUY";  return(true); }
   if(rawScore <= -threshold || db_tier2_action == "SELL") { sideOut = "SELL"; return(true); }
   sideOut = "";
   return(false);
  }

bool NewEntriesAllowed()
  {
   if(InpUseOrchestrator && db_trading_halted) { lastBlockReason = "operator_manual_halt"; return(false); }
   if(InpUseOrchestrator && (!orchestratorHealthy || !strategyParamsValid)) { lastBlockReason = "orchestrator_unhealthy_or_params_stale"; return(false); }
   if(drawdownHalt) { lastBlockReason = "drawdown_halt"; return(false); }

   if(InpRespectLifelineTermination && db_lifeline_terminated)
     {
      if(!lifelineHaltNotified)
        {
         PostRiskIncident("lifeline_termination_observed",
                           StringFormat("EA observed lifeline_terminated=true from orchestrator (lifeline_score=%.1f) - "
                                        "blocking new entries locally until an operator calls /control/lifeline_reset.",
                                        db_lifeline_score));
         lifelineHaltNotified = true;
        }
      lastBlockReason = "lifeline_terminated";
      return(false);
     }
   else if(!db_lifeline_terminated)
     {
      lifelineHaltNotified = false; 
     }

   if(consecutiveLosses >= InpMaxConsecutiveLosses)
     {
      if(!consecLossHaltNotified)
        {
         PostRiskIncident("max_consecutive_losses",
                           StringFormat("consecutive_losses=%d cap=%d", consecutiveLosses, InpMaxConsecutiveLosses));
         consecLossHaltNotified = true;
        }
      lastBlockReason = "max_consecutive_losses";
      return(false);
     }

   if(InpEnforceLiveApproval && !IsDemoAccount() && !db_live_approved) { lastBlockReason = "live_approval_required"; return(false); }

   if(dayStartEquity > 0)
     {
      double dayLossPct = (-dayRealizedPL / dayStartEquity) * 100.0;
      if(dayRealizedPL < 0 && dayLossPct >= InpMaxDailyLossPercent)
        {
         if(!dailyLossHaltNotified)
           {
            PostRiskIncident("daily_loss_limit",
                              StringFormat("day_loss_pct=%.2f cap=%.2f", dayLossPct, InpMaxDailyLossPercent));
            dailyLossHaltNotified = true;
           }
         lastBlockReason = "daily_loss_limit";
         return(false);
        }
     }
   lastBlockReason = "";
   return(true);
  }

bool IsWithinTradingSession()
  {
   if(!InpUseSessionFilter) return(true);
   MqlDateTime dt;
   TimeToStruct(TimeGMT(), dt); // session windows in strategy_db are UTC

   SStrategyParams params;
   GetCurrentStrategyParams(params);

   if(UsingRemoteParams() && params.scheduled_start_hour >= 0 && params.scheduled_end_hour >= 0)
     {
      if(!IsWithinScheduledHours(params)) return(false);
     }
   else
     {
      int startHour = InpSessionStartHour;
      int endHour   = InpSessionEndHour;
      bool withinHours = (startHour <= endHour) ?
                         (dt.hour >= startHour && dt.hour < endHour) :
                         (dt.hour >= startHour || dt.hour < endHour);
      if(!withinHours) return(false);
     }

   MqlDateTime brokerDt; TimeToStruct(TimeCurrent(), brokerDt);
   if(InpBlockFridayLateHrs && brokerDt.day_of_week == 5 && brokerDt.hour >= InpFridayCutoffHour) return(false);
   return(true);
  }

void TryOpenTrade(double score, SignalContext &ctx)
  {
   double atr;
   if(!GetAtr(atr)) return; 

   double threshold = GetEffectiveThreshold();
   string wouldBeSide = "";
   bool   signalPresent = SignalWouldHaveTraded(ctx.rawScoreBeforeVeto, threshold, wouldBeSide);

   double slMult = UsingRemoteParams() ? db_opt_sl_mult : InpAtrSLMultiplier;
   double tpMult = UsingRemoteParams() ? db_opt_tp_mult : InpAtrTPMultiplier;
   double slDist = atr * slMult;
   double tpDist = atr * tpMult;

   if(!NewEntriesAllowed())
     {
      if(signalPresent)
         PostMissedTrade(NormalizeAssetKey(), lastBlockReason, wouldBeSide, score, ctx.rawScoreBeforeVeto,
                         threshold, ctx, atr, slDist, tpDist);
      return;
     }

   if(signalPresent && InpUseRsiFilter && score == 0.0 && ctx.rawScoreBeforeVeto != 0.0)
     {
      PostMissedTrade(NormalizeAssetKey(), "rsi_veto", wouldBeSide, score, ctx.rawScoreBeforeVeto,
                      threshold, ctx, atr, slDist, tpDist);
      return;
     }

   if(InpUseOrchestrator && db_tier2_action != "HOLD")
     {
      bool tier2LocalMismatch = (db_tier2_action == "BUY"  && score < threshold) ||
                                (db_tier2_action == "SELL" && score > -threshold);
      if(tier2LocalMismatch)
        {
         PostMissedTrade(NormalizeAssetKey(), "tier2_local_score_mismatch", db_tier2_action, score,
                         ctx.rawScoreBeforeVeto, threshold, ctx, atr, slDist, tpDist);
         return;
        }
     }

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   bool openBuy = (score >= threshold || db_tier2_action == "BUY");
   bool openSell = (score <= -threshold || db_tier2_action == "SELL");
   if(!openBuy && !openSell) return;

   double lots, reqMargin;
   if(!CalculateLotSize(slDist, openBuy, lots, reqMargin) || lots <= 0.0)
     {
      Print("Blocked entry: calculated volume is below broker minimum or fails broker volume/margin rules.");
      if(signalPresent)
         PostMissedTrade(NormalizeAssetKey(), "lot_below_broker_minimum_or_margin_calc", wouldBeSide, score,
                         ctx.rawScoreBeforeVeto, threshold, ctx, atr, slDist, tpDist);
      return;
     }

   if(IsMicroRiskOversized(lots, slDist))
     {
      Print("Blocked entry: broker volume-step rounding pushed actual risk above micro-risk tolerance.");
      if(signalPresent)
         PostMissedTrade(NormalizeAssetKey(), "micro_risk_oversized", wouldBeSide, score, ctx.rawScoreBeforeVeto,
                         threshold, ctx, atr, slDist, tpDist);
      return;
     }

   double freeMargin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   if((freeMargin - reqMargin) < InpMarginBufferUSD)
     {
      if(signalPresent)
         PostMissedTrade(NormalizeAssetKey(), "margin_buffer", wouldBeSide, score, ctx.rawScoreBeforeVeto,
                         threshold, ctx, atr, slDist, tpDist);
      return;
     }

   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);

   if(openBuy)
     {
      double sl = NormalizeDouble(ask - slDist, _Digits);
      double tp = NormalizeDouble(ask + tpDist, _Digits);
      if(trade.Buy(lots, _Symbol, ask, sl, tp, "MAPSAR v12 Tier-2 BUY"))
        {
         todaysTrades++;
         if(SelectOurPosition())
           {
            EntryCtx e; e.ticket = posInfo.Ticket(); e.rsi = ctx.rsi; e.score = score;
            e.forecastId = db_latest_forecast_id; e.sessionHour = dt.hour; e.slPrice = sl; e.tpPrice = tp;
            e.tier2Confidence = db_tier2_confidence; e.peakProfit = 0.0; e.guardianPeakR = 0.0; e.armed = false;
            StoreEntryContext(e);
            SavePositionState(e.ticket, false, 0.0, sl, 0.0);
           }
        }
     }
   else if(openSell)
     {
      double sl = NormalizeDouble(bid + slDist, _Digits);
      double tp = NormalizeDouble(bid - tpDist, _Digits);
      if(trade.Sell(lots, _Symbol, bid, sl, tp, "MAPSAR v12 Tier-2 SELL"))
        {
         todaysTrades++;
         if(SelectOurPosition())
           {
            EntryCtx e; e.ticket = posInfo.Ticket(); e.rsi = ctx.rsi; e.score = score;
            e.forecastId = db_latest_forecast_id; e.sessionHour = dt.hour; e.slPrice = sl; e.tpPrice = tp;
            e.tier2Confidence = db_tier2_confidence; e.peakProfit = 0.0; e.guardianPeakR = 0.0; e.armed = false;
            StoreEntryContext(e);
            SavePositionState(e.ticket, false, 0.0, sl, 0.0);
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
         trade.PositionModify(posInfo.Ticket(), NormalizeDouble(sarNow, _Digits), curTP);
     }
   else if(type == POSITION_TYPE_SELL)
     {
      if(sarNow > SymbolInfoDouble(_Symbol, SYMBOL_ASK) && (curSL == 0 || sarNow < curSL - InpTrailStepPoints * point))
         trade.PositionModify(posInfo.Ticket(), NormalizeDouble(sarNow, _Digits), curTP);
     }
  }

void ManageExitOnReversal(double score)
  {
   long type;
   if(!HasOpenPosition(type)) return;
   double threshold = GetEffectiveThreshold();
   if(type == POSITION_TYPE_BUY && score <= -threshold) trade.PositionClose(posInfo.Ticket());
   else if(type == POSITION_TYPE_SELL && score >= threshold) trade.PositionClose(posInfo.Ticket());
  }

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

void ManageProfitPeakExit()
  {
   if(!InpUseProfitPeakExit) return;
   long type;
   if(!HasOpenPosition(type)) return;
   if(!SelectOurPosition()) return;

   ulong ticket = posInfo.Ticket();
   int idx = FindEntryContextIndex(ticket);
   if(idx < 0) return;

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
         SavePositionState(ticket, true, floatingProfit, entryCtxArr[idx].slPrice, entryCtxArr[idx].guardianPeakR);
        }
      return;
     }

   if(floatingProfit > entryCtxArr[idx].peakProfit)
     {
      entryCtxArr[idx].peakProfit = floatingProfit;
      SavePositionState(ticket, true, floatingProfit, entryCtxArr[idx].slPrice, entryCtxArr[idx].guardianPeakR);
     }

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
      PrintFormat("PROFIT-PEAK EXIT: locking in gains. Peak=%.2f Now=%.2f Giveback=%.1f%%",
                  peak, floatingProfit, givebackPct);
      trade.PositionClose(posInfo.Ticket());
     }
  }

void ManageEarlyLossCut()
  {
   if(!InpEarlyLossCutEnabled) return;
   long type;
   if(!HasOpenPosition(type)) return;
   if(!SelectOurPosition()) return;

   int idx = FindEntryContextIndex(posInfo.Ticket());
   if(idx < 0) return;

   double openPrice  = posInfo.PriceOpen();
   double slPrice     = entryCtxArr[idx].slPrice;
   if(slPrice <= 0) return;
   double slDistance = MathAbs(openPrice - slPrice);
   if(slDistance <= 0) return;

   double curPrice = (type == POSITION_TYPE_BUY) ? SymbolInfoDouble(_Symbol, SYMBOL_BID) : SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double adverseMove = (type == POSITION_TYPE_BUY) ? (openPrice - curPrice) : (curPrice - openPrice);
   if(adverseMove <= 0) return;

   double adverseFraction = adverseMove / slDistance;
   if(adverseFraction < InpEarlyLossCutRFraction) return;

   double acBuf[2];
   if(CopyBuffer(hAC, 0, 0, 2, acBuf) < 2) return;
   bool momentumAgainst = (type == POSITION_TYPE_BUY  && acBuf[0] < 0 && acBuf[0] < acBuf[1]) ||
                          (type == POSITION_TYPE_SELL && acBuf[0] > 0 && acBuf[0] > acBuf[1]);
   if(!momentumAgainst) return;

   PrintFormat("EARLY LOSS CUT: %.0f%% of SL distance reached. Closing early.", adverseFraction * 100.0);
   trade.PositionClose(posInfo.Ticket());
  }

void ManageMLReversalExit()
  {
   if(!InpUseMLReversalExit || !InpUseOrchestrator) return;
   long type;
   if(!HasOpenPosition(type)) { mlReversalDisagreeCount = 0; return; }

   bool tier2Against = (type == POSITION_TYPE_BUY  && db_tier2_action == "SELL") ||
                       (type == POSITION_TYPE_SELL && db_tier2_action == "BUY");
   bool microAgainst = (type == POSITION_TYPE_BUY  && db_micro_trend == "SELL" && db_micro_trend_strength >= InpMicroTrendMinStrength) ||
                       (type == POSITION_TYPE_SELL && db_micro_trend == "BUY"  && db_micro_trend_strength >= InpMicroTrendMinStrength);

   if(tier2Against && microAgainst)
     {
      mlReversalDisagreeCount++;
      PrintFormat("ML REVERSAL WARNING: Tier2=%s MicroTrend=%s (strength %.2f). Confirmation %d/%d.",
                  db_tier2_action, db_micro_trend, db_micro_trend_strength,
                  mlReversalDisagreeCount, InpMLReversalConfirmations);

      if(mlReversalDisagreeCount >= InpMLReversalConfirmations)
        {
         Print("ML REVERSAL EXIT: model AND micro-trend both confirmed against position. Closing.");
         trade.PositionClose(posInfo.Ticket());
         mlReversalDisagreeCount = 0;
        }
     }
   else
     {
      mlReversalDisagreeCount = 0;
     }
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

string EAStatePrefix() { return(StringFormat("MAPSAR_%d_%s_", (int)InpMagicNumber, _Symbol)); }

void SaveEAState()
  {
   string p = EAStatePrefix();
   GlobalVariableSet(p + "peakEquity", peakEquity);
   GlobalVariableSet(p + "consecutiveLosses", (double)consecutiveLosses);
   GlobalVariableSet(p + "consecutiveWins", (double)consecutiveWins);
   GlobalVariableSet(p + "riskCooldownActive", riskCooldownActive ? 1.0 : 0.0);
   GlobalVariableSet(p + "dayRealizedPL", dayRealizedPL);
   GlobalVariableSet(p + "todaysDayStamp", (double)todaysDayStamp);
  }

bool LoadEAState()
  {
   string p = EAStatePrefix();
   if(!GlobalVariableCheck(p + "todaysDayStamp")) return(false);

   datetime savedDayStamp = (datetime)GlobalVariableGet(p + "todaysDayStamp");
   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   datetime todayStamp = StringToTime(StringFormat("%04d.%02d.%02d", dt.year, dt.mon, dt.day));

   if(GlobalVariableCheck(p + "peakEquity"))
      peakEquity = GlobalVariableGet(p + "peakEquity");
   if(GlobalVariableCheck(p + "consecutiveLosses"))
      consecutiveLosses = (int)GlobalVariableGet(p + "consecutiveLosses");
   if(GlobalVariableCheck(p + "consecutiveWins"))
      consecutiveWins = (int)GlobalVariableGet(p + "consecutiveWins");
   if(GlobalVariableCheck(p + "riskCooldownActive"))
      riskCooldownActive = (GlobalVariableGet(p + "riskCooldownActive") > 0.5);

   if(savedDayStamp == todayStamp)
     {
      dayRealizedPL  = GlobalVariableCheck(p + "dayRealizedPL") ? GlobalVariableGet(p + "dayRealizedPL") : 0.0;
      todaysDayStamp = savedDayStamp;
      Print("EA STATE RESTORED: same-day resume - peakEquity=", peakEquity,
            " consecutiveLosses=", consecutiveLosses, " dayRealizedPL=", dayRealizedPL);
      return(true);
     }

   Print("EA STATE RESTORED (cross-day): peakEquity=", peakEquity,
         " consecutiveLosses=", consecutiveLosses, " - day counters will reset fresh for today.");
   return(false);
  }

string PositionStatePrefix(ulong ticket)
  {
   return(StringFormat("MAPSAR_pos_%I64u_", ticket));
  }

void SavePositionState(ulong ticket, bool armed, double peak, double initialSL, double guardianPeakR)
  {
   string p = PositionStatePrefix(ticket);
   GlobalVariableSet(p + "armed", armed ? 1.0 : 0.0);
   GlobalVariableSet(p + "peak", peak);
   GlobalVariableSet(p + "initialSL", initialSL);
   GlobalVariableSet(p + "guardianPeakR", guardianPeakR);
  }

void DeletePositionState(ulong ticket)
  {
   string p = PositionStatePrefix(ticket);
   GlobalVariableDel(p + "armed");
   GlobalVariableDel(p + "peak");
   GlobalVariableDel(p + "initialSL");
   GlobalVariableDel(p + "guardianPeakR");
   GlobalVariableDel(p + "realizedPL");
   GlobalVariableDel(p + "closedVolume");
  }

void ReconcileOpenPositions()
  {
   int total = PositionsTotal();
   for(int i = 0; i < total; i++)
     {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((ulong)PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;
      if(FindEntryContextIndex(ticket) >= 0) continue;

      EntryCtx e;
      e.ticket = ticket;
      e.rsi = 0.0; e.score = 0.0; e.forecastId = -1;
      MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
      e.sessionHour = dt.hour;
      e.slPrice = PositionGetDouble(POSITION_SL);
      e.tpPrice = PositionGetDouble(POSITION_TP);
      e.tier2Confidence = 0.0; e.guardianPeakR = 0.0; e.peakProfit = 0.0; e.armed = false;

      string p = PositionStatePrefix(ticket);
      e.armed      = GlobalVariableCheck(p + "armed") ? (GlobalVariableGet(p + "armed") > 0.5) : false;
      e.peakProfit = GlobalVariableCheck(p + "peak")  ? GlobalVariableGet(p + "peak") : 0.0;
      e.guardianPeakR = GlobalVariableCheck(p + "guardianPeakR") ? GlobalVariableGet(p + "guardianPeakR") : 0.0;
      if(GlobalVariableCheck(p + "initialSL")) e.slPrice = GlobalVariableGet(p + "initialSL");
      SavePositionState(ticket, e.armed, e.peakProfit, e.slPrice, e.guardianPeakR);

      StoreEntryContext(e);
      PrintFormat("RECONCILE: adopted pre-existing position #%d after (re)start - profit-peak armed=%s peak=%.2f",
                  ticket, e.armed ? "true" : "false", e.peakProfit);
     }
  }

void RefreshParamsIfDue()
  {
   if(!InpUseOrchestrator) return;

   long posType;
   bool hasPos = HasOpenPosition(posType);
   int intervalSeconds = strategyParamsValid ? (hasPos ? InpOpenPositionRefreshSeconds : (InpParamsRefetchMinutes * 60)) : 15;
   if((TimeCurrent() - lastParamsFetch) < intervalSeconds) return;

   if((TimeCurrent() - lastHealthCheck) >= 15 || !orchestratorHealthy)
     { CheckOrchestratorHealth(); lastHealthCheck = TimeCurrent(); }
   bool fetched = FetchStrategyParams(NormalizeAssetKey());
   if(!fetched) strategyParamsValid = false;
   lastParamsFetch = TimeCurrent();

   if(fetched) ManageMLReversalExit();
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
      dayRealizedPL  = 0.0;
      consecutiveLosses = 0;
      dailyLossHaltNotified = false;
     }
  }

void UpdateDrawdownState()
  {
   double eq = AccountInfoDouble(ACCOUNT_EQUITY);
   if(eq > peakEquity) peakEquity = eq;
   double ddPercent = (peakEquity > 0) ? (peakEquity - eq) / peakEquity * 100.0 : 0.0;
   bool wasHalted = drawdownHalt;
   drawdownHalt = (ddPercent >= InpMaxDrawdownPercent);

   if(drawdownHalt && !wasHalted)
     {
      PostRiskIncident("drawdown_halt", StringFormat("drawdown_pct=%.2f cap=%.2f peak_equity=%.2f equity=%.2f",
                        ddPercent, InpMaxDrawdownPercent, peakEquity, eq));
      long type;
      if(HasOpenPosition(type))
        {
         Print("DRAWDOWN HALT TRIGGERED: force-closing open position, blocking new entries.");
         trade.PositionClose(posInfo.Ticket());
        }
     }
  }

void CheckFloatingLossKillSwitch()
  {
   if(!InpUseFloatingLossKillSwitch) return;
   long type;
   if(!HasOpenPosition(type)) return;
   if(!SelectOurPosition()) return;

   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   if(equity <= 0) return;

   double floatingPL = posInfo.Profit() + posInfo.Swap();
   if(floatingPL >= 0) return;

   double floatingLossPct = (-floatingPL / equity) * 100.0;
   if(floatingLossPct >= InpMaxFloatingLossPercent)
     {
      PrintFormat("KILL SWITCH: floating loss %.2f%% of equity >= cap %.2f%%. Force-closing.",
                  floatingLossPct, InpMaxFloatingLossPercent);
      PostRiskIncident("floating_loss_kill_switch",
                        StringFormat("floating_loss_pct=%.2f cap=%.2f", floatingLossPct, InpMaxFloatingLossPercent));
      trade.PositionClose(posInfo.Ticket());
     }
  }

void PublishLiveState(double localScore)
  {
   string p = EAStatePrefix();
   GlobalVariableSet(p + "liveScore", localScore);

   double tier2Code = 0.0;
   if(db_tier2_action == "BUY") tier2Code = 1.0;
   else if(db_tier2_action == "SELL") tier2Code = -1.0;
   GlobalVariableSet(p + "liveTier2ActionCode", tier2Code);
   GlobalVariableSet(p + "liveTier2Confidence", db_tier2_confidence);

   double microCode = 0.0;
   if(db_micro_trend == "BUY") microCode = 1.0;
   else if(db_micro_trend == "SELL") microCode = -1.0;
   GlobalVariableSet(p + "liveMicroTrendCode", microCode);
   GlobalVariableSet(p + "liveMicroTrendStrength", db_micro_trend_strength);

   GlobalVariableSet(p + "liveCalibrationScore", db_calibration_score);
   GlobalVariableSet(p + "liveScheduledStartHour", (double)db_scheduled_start_hour);
   GlobalVariableSet(p + "liveScheduledEndHour", (double)db_scheduled_end_hour);
   double eq = AccountInfoDouble(ACCOUNT_EQUITY);
   GlobalVariableSet(p + "liveDrawdownPct", (peakEquity > 0) ? (peakEquity - eq) / peakEquity * 100.0 : 0.0);

   GlobalVariableSet(p + "liveLifelineScore", db_lifeline_score);
   GlobalVariableSet(p + "liveLifelineTerminated", db_lifeline_terminated ? 1.0 : 0.0);
  }

#include "KenjinGuardian.mqh"

//=====================================================================
// EVENT HANDLERS
//=====================================================================
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
      lastHealthCheck = TimeCurrent();
     }

   bool resumedSameDay = LoadEAState();
   ReconcileOpenPositions();

   if(peakEquity <= 0) peakEquity = AccountInfoDouble(ACCOUNT_EQUITY);

   if(!resumedSameDay)
     {
      MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
      todaysDayStamp = StringToTime(StringFormat("%04d.%02d.%02d", dt.year, dt.mon, dt.day));
      dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
      dayRealizedPL  = ComputeTodaysRealizedPL();
     }
   else
     {
      dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY) - dayRealizedPL;
     }

   EventSetTimer(InpUseGuardian ? MathMax(1, InpGuardianSeconds) : MathMax(1, InpHeartbeatSeconds));

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

void OnTradeTransaction(const MqlTradeTransaction &trans, const MqlTradeRequest &request, const MqlTradeResult &result)
  {
   if(trans.type != TRADE_TRANSACTION_DEAL_ADD) return;
   if(!HistoryDealSelect(trans.deal)) return;
   if((ulong)HistoryDealGetInteger(trans.deal, DEAL_MAGIC) != InpMagicNumber) return;
   if((ENUM_DEAL_ENTRY)HistoryDealGetInteger(trans.deal, DEAL_ENTRY) != DEAL_ENTRY_OUT) return;

   double profit = HistoryDealGetDouble(trans.deal, DEAL_PROFIT) + HistoryDealGetDouble(trans.deal, DEAL_SWAP) + HistoryDealGetDouble(trans.deal, DEAL_COMMISSION);
   ENUM_DEAL_TYPE dealType = (ENUM_DEAL_TYPE)HistoryDealGetInteger(trans.deal, DEAL_TYPE);
   // Closing a BUY is a SELL deal; closing a SELL is a BUY deal.
   string typeStr = (dealType == DEAL_TYPE_SELL) ? "BUY_CLOSE" : "SELL_CLOSE";
   double price   = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
   double volume  = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);

   dayRealizedPL += profit;

   string positionPrefix = PositionStatePrefix(trans.position);
   double accumulatedProfit = (GlobalVariableCheck(positionPrefix + "realizedPL") ? GlobalVariableGet(positionPrefix + "realizedPL") : 0.0) + profit;
   double accumulatedVolume = (GlobalVariableCheck(positionPrefix + "closedVolume") ? GlobalVariableGet(positionPrefix + "closedVolume") : 0.0) + volume;
   bool positionStillOpen = PositionSelectByTicket(trans.position);
   if(positionStillOpen)
     {
      // Partial close: persist the realized piece and retain EntryCtx/initial SL.
      GlobalVariableSet(positionPrefix + "realizedPL", accumulatedProfit);
      GlobalVariableSet(positionPrefix + "closedVolume", accumulatedVolume);
      SaveEAState();
      return;
     }

   EntryCtx ctx;
   GetAndRemoveEntryContext(trans.position, ctx);
   PostTelemetry(NormalizeAssetKey(), typeStr, price, accumulatedVolume, accumulatedProfit, ctx);
   DeletePositionState(trans.position);

   if(accumulatedProfit < 0)
     {
      consecutiveLosses++; consecutiveWins = 0;
      if(InpUseAdaptiveRisk && consecutiveLosses >= InpLossStreakForCooldown)
         riskCooldownActive = true;
     }
   else if(accumulatedProfit > 0)
     {
      consecutiveWins++; consecutiveLosses = 0;
      consecLossHaltNotified = false;
      if(riskCooldownActive && consecutiveWins >= InpWinsToRestoreRisk)
         riskCooldownActive = false;
     }

   SaveEAState();
  }

void OnTimer()
  {
   RefreshDailyCounter();
   UpdateDrawdownState();

   // Protective local exits run independently of WebRequest availability.
   CheckFloatingLossKillSwitch();
   ManageProfitPeakExit();
   ManageEarlyLossCut();
   ManageMaxHoldSafetyNet();
   ManageTrailing();

   RefreshParamsIfDue();
   GuardianTick();
   if(SnapshotDue()) PostAccountSnapshot();
   FlushPostQueue(10);
   SaveEAState();
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

   SStrategyParams params;
   GetCurrentStrategyParams(params);


   if(!IsWithinScheduledHours(params))
     {
      return; 
     }

   if(InpTradeOnBarCloseOnly && !IsNewBar()) return;

   SignalContext ctx;
   ZeroMemory(ctx);
   double score = ComputeSignalScore(ctx);
   PublishLiveState(score);

   long type;
   if(HasOpenPosition(type))
     {
      ManageExitOnReversal(score);
      PostTick(NormalizeAssetKey(), ctx);
      return;
     }

   if(!IsWithinTradingSession()) return;

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
