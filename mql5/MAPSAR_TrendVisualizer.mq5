//+------------------------------------------------------------------+
//|                                MAPSAR_TrendVisualizer.mq5         |
//|  Companion chart indicator for MAPSAR_ProfitEngine_v11.mq5 (EA)   |
//|                                                                    |
//|  Attach this to the SAME chart/symbol as the EA. It does not      |
//|  trade, analyze the market, or talk to the orchestrator itself -  |
//|  it purely reads the GlobalVariables the EA already broadcasts    |
//|  every tick (see MAPSAR v11.04's PublishLiveState()) and plots a  |
//|  live composite prediction gauge, blending:                       |
//|    - the EA's raw local TEMA/AC/SAR score                         |
//|    - Tier-2's current action x confidence                         |
//|    - the orchestrator's short-horizon micro-trend x strength      |
//|  into a single -1..+1 value each bar, rendered as a dotted        |
//|  histogram (green = bullish lean, red = bearish lean).            |
//|                                                                    |
//|  IMPORTANT - a real MQL5 constraint, stated honestly rather than   |
//|  glossed over: native indicator buffer plots in MT5 do NOT support|
//|  true per-pixel alpha-channel transparency. What you get here is  |
//|  a genuinely DOTTED line style (PLOT_LINE_STYLE = STYLE_DOT, which|
//|  MT5 renders natively) using deliberately pale/light colors to    |
//|  READ as translucent against the chart background - not actual    |
//|  alpha blending. If you want literal ARGB transparency layered    |
//|  over the price chart itself, that requires the MQL5 Standard      |
//|  Library's CCanvas class drawing to an off-screen bitmap resource -|
//|  a meaningfully bigger custom-drawing project. Ask if you want     |
//|  that built as a v2; this version is the standard, robust,        |
//|  broadly-compatible approach and is what most indicators use.     |
//|                                                                    |
//|  Also honest about scope: GlobalVariables only hold the CURRENT   |
//|  live value, not history. Bars before this indicator was attached |
//|  are intentionally left blank rather than fabricated - this is a  |
//|  live gauge, not a backtested reconstruction of past predictions. |
//|  It builds up real history forward from whenever you attach it.   |
//+------------------------------------------------------------------+
#property copyright "InfoScience"
#property version   "1.00"
#property indicator_separate_window
#property indicator_buffers 2
#property indicator_plots   1
#property indicator_label1  "MAPSAR Prediction"
#property indicator_type1   DRAW_COLOR_HISTOGRAM
#property indicator_color1  C'150,220,170', C'230,150,150'   // pale/translucent-reading green, pale red
#property indicator_style1  STYLE_DOT
#property indicator_width1  3
#property indicator_level1  0.0
#property indicator_minimum -1.0
#property indicator_maximum 1.0

input ulong  InpMagicNumber       = 1955224281;   // MUST match the attached EA's InpMagicNumber exactly
input double InpLocalScoreWeight  = 0.35;         // weight of the EA's raw TEMA/AC/SAR score
input double InpTier2Weight       = 0.40;         // weight of Tier-2 action x confidence
input double InpMicroTrendWeight  = 0.25;         // weight of the short-horizon micro-trend x strength

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
   // That's deliberate: each closed bar becomes a snapshot of what the
   // prediction gauge read while that bar was still forming.
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
      bufValue[last] = composite;
      bufColor[last] = (composite >= 0.0) ? 0 : 1;
     }
   else
     {
      bufValue[last] = EMPTY_VALUE;
      Comment("MAPSAR_TrendVisualizer: no live data from an EA with magic ", (int)InpMagicNumber,
              " on ", _Symbol, " yet. Check the EA is attached to this chart and InpMagicNumber matches.");
     }

   if(eaFound) Comment("");   // clear the warning once data starts flowing

   return(rates_total);
  }
//+------------------------------------------------------------------+
