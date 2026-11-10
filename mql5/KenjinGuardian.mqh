//+------------------------------------------------------------------+
//|                                            KenjinGuardian.mqh    |
//|  Trade Guardian client for MAPSAR_ProfitEngine v12               |
//|                                                                  |
//|  Every InpGuardianSeconds the EA reports its open position to    |
//|  POST /guardian/evaluate and executes the answer:               |
//|     CLOSE   -> close the position now (loss trending worse)     |
//|     MOVE_SL -> ratchet the stop to break-even                   |
//|     HOLD    -> nothing                                          |
//|                                                                  |
//|  FAIL-SAFE: if the orchestrator is unreachable the EA applies a  |
//|  purely local rule so a losing trade is NEVER left unmanaged.    |
//|                                                                  |
//|  INCLUDE THIS FILE just above the "EVENT HANDLERS" section of    |
//|  MAPSAR_ProfitEngine (it uses trade, posInfo, entryCtxArr,       |
//|  OrchestratorHeaders, JsonGet*, NormalizeAssetKey, IsDemoAccount).|
//+------------------------------------------------------------------+
input string   InpSectionG           = "==== Trade Guardian (v12) ====";
input bool     InpUseGuardian         = true;
input int      InpGuardianSeconds     = 5;      // report cadence (timer interval)
input int      InpGuardianTimeoutMs   = 1200;
input double   InpLocalHardStopR      = 0.85;   // local fail-safe: close at -0.85R if server unreachable
input double   InpLocalSoftStopR      = 0.45;   // local fail-safe: close if worse than -0.45R AND momentum against
input int      InpGuardianFailsBeforeLocal = 2; // consecutive failed calls before local rules take over
input int      InpSnapshotEverySeconds = 30;    // account snapshot throttle (timer now runs faster)

int      guardianFailCount   = 0;
datetime lastSnapshotPost    = 0;

//--- current R-multiple of the open position (profit / initial stop distance); false if no position
bool Guardian_ReadPosition(ulong &ticket, long &type, double &r, double &peakR, double &slDist,
                           double &openPrice, double &curPrice, double &curSL, double &curTP,
                           double &lots, double &profit, double &openSeconds, bool &momentumAgainst)
  {
   if(!SelectOurPosition()) return(false);

   ticket    = posInfo.Ticket();
   type      = posInfo.PositionType();
   openPrice = posInfo.PriceOpen();
   curSL     = posInfo.StopLoss();
   curTP     = posInfo.TakeProfit();
   lots      = posInfo.Volume();
   profit    = posInfo.Profit() + posInfo.Swap();
   curPrice  = (type == POSITION_TYPE_BUY) ? SymbolInfoDouble(_Symbol, SYMBOL_BID) : SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   openSeconds = (double)(TimeCurrent() - posInfo.Time());

   // Preserve the original stop distance; the current SL may already be trailed.
   slDist = 0.0;
   peakR  = 0.0;
   int idx = FindEntryContextIndex(ticket);
   if(idx >= 0)
     {
      if(entryCtxArr[idx].slPrice > 0) slDist = MathAbs(openPrice - entryCtxArr[idx].slPrice);
      peakR = entryCtxArr[idx].guardianPeakR;
     }
   if(slDist <= 0 && curSL > 0) slDist = MathAbs(openPrice - curSL); // only safe fallback for positions opened before v12
   if(slDist <= 0) return(false);

   // Approximate R from broker-calculated floating P/L divided by initial cash risk.
   double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   double riskMoney = (tickValue > 0 && tickSize > 0) ? (slDist / tickSize) * tickValue * lots : 0.0;
   if(riskMoney > 0.0) r = profit / riskMoney;
   else
     {
      double move = (type == POSITION_TYPE_BUY) ? (curPrice - openPrice) : (openPrice - curPrice);
      r = move / slDist;
     }

   double acBuf[2];
   momentumAgainst = false;
   if(CopyBuffer(hAC, 0, 0, 2, acBuf) >= 2)
     {
      if(type == POSITION_TYPE_BUY  && acBuf[0] < 0 && acBuf[0] < acBuf[1]) momentumAgainst = true;
      if(type == POSITION_TYPE_SELL && acBuf[0] > 0 && acBuf[0] > acBuf[1]) momentumAgainst = true;
     }
   return(true);
  }

//--- fail-safe used when the orchestrator cannot be reached
void Guardian_LocalFailSafe(ulong ticket, double r, bool momentumAgainst, double openSeconds)
  {
   bool close = false;
   string why = "";
   if(r <= -InpLocalHardStopR)                                  { close = true; why = "local_hard_stop"; }
   else if(r <= -InpLocalSoftStopR && momentumAgainst)          { close = true; why = "local_soft_stop_momentum"; }
   else if(openSeconds >= 0.5 * InpMaxHoldMinutes * 60.0 && r <= -0.25 && momentumAgainst)
                                                                { close = true; why = "local_time_underwater"; }
   if(close)
     {
      PrintFormat("[Guardian-LOCAL] closing #%I64u r=%.2f reason=%s (orchestrator unreachable)", ticket, r, why);
      PostRiskIncident("guardian_local_close", StringFormat("ticket=%I64u r=%.2f reason=%s", ticket, r, why));
      trade.PositionClose(ticket);
     }
  }

//--- one guardian cycle; call from OnTimer()
void GuardianTick()
  {
   if(!InpUseGuardian) return;

   ulong ticket; long type; double r, peakR, slDist, openPrice, curPrice, curSL, curTP, lots, profit, openSeconds;
   bool momentumAgainst;
   if(!Guardian_ReadPosition(ticket, type, r, peakR, slDist, openPrice, curPrice, curSL, curTP, lots, profit, openSeconds, momentumAgainst))
     { guardianFailCount = 0; return; }

   if(!InpUseOrchestrator || !orchestratorHealthy)
     { Guardian_LocalFailSafe(ticket, r, momentumAgainst, openSeconds); return; }

   string side = (type == POSITION_TYPE_BUY) ? "BUY" : "SELL";
   string json = StringFormat(
      "{\"asset\":\"%s\",\"ticket\":%I64u,\"side\":\"%s\",\"account_type\":\"%s\",\"open_price\":%.5f,\"price\":%.5f,"
      "\"sl\":%.5f,\"tp\":%.5f,\"lots\":%.2f,\"profit\":%.2f,\"r_multiple\":%.4f,\"peak_r\":%.4f,"
      "\"initial_sl_distance\":%.5f,\"open_seconds\":%.0f,\"max_hold_seconds\":%.0f,\"momentum_against\":%s}",
      NormalizeAssetKey(), ticket, side, IsDemoAccount() ? "demo" : "live", openPrice, curPrice,
      curSL, curTP, lots, profit, r, peakR, slDist, openSeconds, (double)(InpMaxHoldMinutes * 60),
      momentumAgainst ? "true" : "false");

   char data[]; char result[]; string resHeaders;
   StringToCharArray(json, data, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(data, ArraySize(data) - 1);
   int http = WebRequest("POST", InpOrchestratorURL + "/guardian/evaluate", OrchestratorHeaders("application/json"),
                         InpGuardianTimeoutMs, data, result, resHeaders);

   if(http != 200)
     {
      guardianFailCount++;
      if(guardianFailCount >= InpGuardianFailsBeforeLocal)
         Guardian_LocalFailSafe(ticket, r, momentumAgainst, openSeconds);
      return;
     }
   guardianFailCount = 0;

   string body = CharArrayToString(result, 0, WHOLE_ARRAY, CP_UTF8);
   string action = "HOLD", reason = "";
   JsonGetString(body, "action", action);
   JsonGetString(body, "reason", reason);

   double serverPeakR = peakR;
   if(JsonGetNumber(body, "peak_r", serverPeakR))
     {
      int idx = FindEntryContextIndex(ticket);
      if(idx >= 0)
        {
         entryCtxArr[idx].guardianPeakR = MathMax(entryCtxArr[idx].guardianPeakR, serverPeakR);
         SavePositionState(ticket, entryCtxArr[idx].armed, entryCtxArr[idx].peakProfit, entryCtxArr[idx].slPrice, entryCtxArr[idx].guardianPeakR);
        }
     }

   if(action == "CLOSE")
     {
      PrintFormat("[Guardian] CLOSE #%I64u r=%.2f reason=%s", ticket, r, reason);
      trade.PositionClose(ticket);
     }
   else if(action == "MOVE_SL")
     {
      double newSL = 0.0;
      if(JsonGetNumber(body, "new_sl", newSL) && newSL > 0)
        {
         newSL = NormalizeDouble(newSL, _Digits);
         double minDist = (double)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL) * _Point;
         bool valid = (type == POSITION_TYPE_BUY)  ? (curPrice - newSL) > minDist
                                                   : (newSL - curPrice) > minDist;
         if(valid)
           {
            PrintFormat("[Guardian] MOVE_SL #%I64u -> %.5f (%s)", ticket, newSL, reason);
            trade.PositionModify(ticket, newSL, curTP);
           }
        }
     }
  }

//--- throttle for the account snapshot (OnTimer now runs every InpGuardianSeconds)
bool SnapshotDue()
  {
   if(TimeCurrent() - lastSnapshotPost >= InpSnapshotEverySeconds)
     { lastSnapshotPost = TimeCurrent(); return(true); }
   return(false);
  }
