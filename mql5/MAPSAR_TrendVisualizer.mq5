//+------------------------------------------------------------------+
//|                                MAPSAR_TrendVisualizer.mq5         |
//|  Companion chart indicator for MAPSAR_ProfitEngine_v11_05.mq5(EA)|
//|                                                                    |
//|  Attach this to the SAME chart/symbol as the EA. It does not      |
//|  trade, analyze the market, or talk to the orchestrator itself -  |
//|  it purely reads the GlobalVariables the EA already broadcasts    |
//|  every tick (see MAPSAR v11.06's PublishLiveState()) and plots a  |
//|  live composite prediction gauge, blending:                       |
//|    - the EA's raw local TEMA/AC/SAR score                         |
//|    - Tier-2's current action x confidence                         |
//|    - the orchestrator's short-horizon micro-trend x strength      |
//|  into a single -1..+1 value each bar, rendered as a dotted        |
//|  histogram (green = bullish lean, red = bearish lean, GRAY =      |
//|  signal present but currently flagged low-trust - see v1.01).     |
//|                                                                    |
//|  v1.01 CHANGES: reads two more values the EA now broadcasts -     |
//|  calibration_score (Brier score, lower=better, 0.25 is the        |
//|  "no better than a shrug" baseline) and the orchestrator's        |
//|  scheduled_start_hour/end_hour. A bar is now rendered in dim gray |
//|  instead of full-color green/red when EITHER the model hasn't     |
//|  proven well-calibrated over the last 30 days OR the current hour |
//|  is outside the learned optimal session - i.e. exactly the        |
//|  conditions where the EA itself would not act on the signal even  |
//|  if it looks strong. A Comment() line shows the raw numbers.      |
//|                                                                    |
//|  IMPORTANT - a real MQL5 constraint, stated honestly rather than   |
//|  glossed over: native indicator buffer plots in MT5 do NOT support|
//|  true per-pixel alpha-channel transparency. What you get here is  |
//|  a genuinely DOTTED line style (PLOT_LINE_STYLE = STYLE_DOT, which|
//|  MT5 renders natively) using deliberately pale/light colors to    |
//|  READ as translucent against the chart background - not actual    |
//|  alpha blending.                                                   |
//|                                                                    |
//|  Also honest about scope: GlobalVariables only hold the CURRENT   |
//|  live value, not history. Bars before this indicator was attached |
//|  are intentionally left blank rather than fabricated - this is a  |
//|  live gauge, not a backtested reconstruction of past predictions. |
//|  It builds up real history forward from whenever you attach it.   |
//+------------------------------------------------------------------+
#property copyright "InfoScience"
#property version   "1.01"
#property indicator_separate_window
#property indicator_buffers 2
#property indicator_plots   1
#property indicator_label1  "MAPSAR Prediction"
#property indicator_type1   DRAW_COLOR_HISTOGRAM
#property indicator_color1  C'150,220,170', C'230,150,150', C'110,114,122'
#property indicator_style1  STYLE_DOT
#property indicator_width1  3
#property indicator_level1  0.0
#property indicator_minimum -1.0
#property indicator_maximum 1.0

input ulong  InpMagicNumber              = 1955224281;  // MUST match the attached EA's InpMagicNumber exactly
input double InpLocalScoreWeight         = 0.35;        // weight of the EA's raw TEMA/AC/SAR score
input double InpTier2Weight              = 0.40;        // weight of Tier-2 action x confidence
input double InpMicroTrendWeight         = 0.25;        // weight of the short-horizon micro-trend x strength
input double InpCalibrationDimThreshold  = 0.25;        // v1.01: Brier score above this = dim the bar (worse than an uninformed 0.5 guess)
input bool   InpDimOutsideSchedule       = true;         // v1.01: dim the bar when the current hour is outside the orchestrator's scheduled window

double bufValue[];
double bufColor[];

//+------------------------------------------------------------------+
int OnInit()
  {
   SetIndexBuffer(0, bufValue, INDICATOR_DATA);
   SetIndexBuffer(1, bufColor, INDICATOR_COLOR_INDEX);

   PlotIndexSetInteger(0, PLOT_LINE_STYLE, STYLE_DOT);
   PlotIndexSetInteger(0, PLOT_LINE_WIDTH, 3);
   PlotIndexSetString(0, PLOT_LABEL, "MAPSAR v11 Prediction");
   PlotIndexSetDouble(0, PLOT_EMPTY_VALUE, EMPTY_VALUE);

   IndicatorSetString(INDICATOR_SHORTNAME, StringFormat("MAPSAR Trend Visualizer (magic %d)", (int)InpMagicNumber));
   IndicatorSetInteger(INDICATOR_DIGITS, 3);

   double totalWeight = InpLocalScoreWeight + InpTier2Weight + InpMicroTrendWeight;
   if(totalWeight <= 0)
      Print("MAPSAR_TrendVisualizer WARNING: all three weights are zero/negative - the gauge will always read 0.");

   return(INIT_SUCCEEDED);
  }

//+------------------------------------------------------------------+
string GVPrefix() { return(StringFormat("MAPSAR_%d_%s_", (int)InpMagicNumber, _Symbol)); }

//+------------------------------------------------------------------+
//| Reads what the EA last published and blends it into one composite|
//| -1..+1 value. Returns 0 (neutral) if the EA hasn't published      |
//| anything yet - e.g. it isn't attached, is attached to a different |
//| chart, or the InpMagicNumber here doesn't match its InpMagicNumber|
//+------------------------------------------------------------------+
double GetCompositePrediction(bool &eaFound)
  {
   string p = GVPrefix();
   eaFound = GlobalVariableCheck(p + "liveScore");
   if(!eaFound) return(0.0);

   double localScore = GlobalVariableGet(p + "liveScore");

   double tier2Signed = 0.0;
   if(GlobalVariableCheck(p + "liveTier2ActionCode") && GlobalVariableCheck(p + "liveTier2Confidence"))
     {
      double code = GlobalVariableGet(p + "liveTier2ActionCode");     // -1 SELL, 0 HOLD, 1 BUY
      double conf = GlobalVariableGet(p + "liveTier2Confidence");     // 0..1
      tier2Signed = code * conf;
     }

   double microSigned = 0.0;
   if(GlobalVariableCheck(p + "liveMicroTrendCode") && GlobalVariableCheck(p + "liveMicroTrendStrength"))
     {
      double code     = GlobalVariableGet(p + "liveMicroTrendCode");       // -1 SELL, 0 NEUTRAL, 1 BUY
      double strength = GlobalVariableGet(p + "liveMicroTrendStrength");   // 0..1
      microSigned = code * strength;
     }

   double totalWeight = InpLocalScoreWeight + InpTier2Weight + InpMicroTrendWeight;
   if(totalWeight <= 0) return(0.0);

   double composite = (localScore * InpLocalScoreWeight
                        + tier2Signed * InpTier2Weight
                        + microSigned * InpMicroTrendWeight) / totalWeight;

   return(MathMax(-1.0, MathMin(1.0, composite)));
  }

//+------------------------------------------------------------------+
//| v1.01: decides whether the current bar's signal should be flagged|
//| low-trust - either the model hasn't proven well-calibrated over  |
//| the last 30 days (Brier score above InpCalibrationDimThreshold), |
//| or the current hour falls outside the orchestrator's learned     |
//| optimal session, in which case the EA itself won't act on the    |
//| signal even if it looks strong.                                  |
//+------------------------------------------------------------------+
bool IsLowTrustBar(string prefix)
  {
   if(GlobalVariableCheck(prefix + "liveCalibrationScore"))
     {
      double calib = GlobalVariableGet(prefix + "liveCalibrationScore");
      if(calib > InpCalibrationDimThreshold) return(true);
     }

   if(InpDimOutsideSchedule &&
      GlobalVariableCheck(prefix + "liveScheduledStartHour") &&
      GlobalVariableCheck(prefix + "liveScheduledEndHour"))
     {
      int startHr = (int)GlobalVariableGet(prefix + "liveScheduledStartHour");
      int endHr   = (int)GlobalVariableGet(prefix + "liveScheduledEndHour");
      if(startHr >= 0 && endHr >= 0)   // -1 = orchestrator hasn't scheduled a window yet
        {
         MqlDateTime dt;
         TimeToStruct(TimeCurrent(), dt);
         bool withinWindow = (startHr <= endHr) ? (dt.hour >= startHr && dt.hour <= endHr)
                                                 : (dt.hour >= startHr || dt.hour <= endHr);
         if(!withinWindow) return(true);
        }
     }

   return(false);
  }

//+------------------------------------------------------------------+
int OnCalculate(const int rates_total,
                 const int prev_calculated,
                 const datetime &time[],
                 const double &open[],
                 const double &high[],
                 const double &low[],
                 const double &close[],
                 const long &tick_volume[],
                 const long &volume[],
                 const int &spread[])
  {
   if(rates_total < 1) return(0);

   // Only initialize genuinely NEW bars to empty - a bar that already closed
   // keeps whatever live composite value it last had at the moment it closed.
   int start = (prev_calculated <= 1) ? 0 : prev_calculated;
   for(int i = start; i < rates_total; i++)
     {
      bufValue[i] = EMPTY_VALUE;
      bufColor[i] = 0;
     }

   bool eaFound;
   double composite = GetCompositePrediction(eaFound);

   int last = rates_total - 1;
   if(eaFound)
     {
      string prefix = GVPrefix();
      bufValue[last] = composite;

      if(IsLowTrustBar(prefix))
         bufColor[last] = 2;                                  // dim gray - present but not currently trustworthy
      else
         bufColor[last] = (composite >= 0.0) ? 0 : 1;

      double calibShown = GlobalVariableCheck(prefix + "liveCalibrationScore") ? GlobalVariableGet(prefix + "liveCalibrationScore") : -1.0;
      double ddShown     = GlobalVariableCheck(prefix + "liveDrawdownPct") ? GlobalVariableGet(prefix + "liveDrawdownPct") : 0.0;
      int schedStart     = GlobalVariableCheck(prefix + "liveScheduledStartHour") ? (int)GlobalVariableGet(prefix + "liveScheduledStartHour") : -1;
      int schedEnd       = GlobalVariableCheck(prefix + "liveScheduledEndHour") ? (int)GlobalVariableGet(prefix + "liveScheduledEndHour") : -1;

      string calibText = (calibShown >= 0.0) ? StringFormat("%.3f", calibShown) : "n/a (<30 samples)";
      string schedText = (schedStart >= 0 && schedEnd >= 0) ? StringFormat("%02d:00-%02d:00 UTC", schedStart, schedEnd) : "24/7 (not yet scheduled)";

      Comment(StringFormat("MAPSAR composite=%.3f | Brier=%s | Sched=%s | Drawdown=%.1f%%",
              composite, calibText, schedText, ddShown));
     }
   else
     {
      bufValue[last] = EMPTY_VALUE;
      Comment("MAPSAR_TrendVisualizer: no live data from an EA with magic ", (int)InpMagicNumber,
              " on ", _Symbol, " yet. Check the EA is attached to this chart and InpMagicNumber matches.");
     }

   return(rates_total);
  }
//+------------------------------------------------------------------+