# Energy baseline strategies for community flexibility rewards

**The most effective baseline approach for a Renewable Energy Community (CER) assigning flexibility bonus points combines a "High 4 of 7" averaging method at 15-minute granularity with upward-only same-day adjustments, percentage-weighted hybrid rewards, and multi-layered gaming prevention.** This architecture balances the three competing demands that every baseline system faces — accuracy, simplicity, and manipulation resistance — while remaining implementable with standard smart meter infrastructure. The approach draws on two decades of demand response settlement experience from US ISOs (CAISO, PJM, ERCOT) and emerging European frameworks (ACER, UVAM, UK Demand Flexibility Service), adapted to the specific context of a CER where heterogeneous users receive notifications about renewable surplus and earn points for shifting load.

The fundamental challenge is a counterfactual problem: you can only meter energy that *is* consumed, never what *would have been* consumed without intervention. Every baseline methodology is an imperfect estimate of this unobservable quantity. The choice of method determines who benefits, who can game, and how much trust the system earns.

---

## How the major "X of Y" baseline methods actually work

The industry-standard approach across US ISOs selects **X days from a pool of Y recent eligible days**, averages consumption per time interval across those X days, and calls the result the baseline. The critical design parameter is *which* X days get selected — highest-consumption, most recent, or middle-range — because this choice directly determines bias, accuracy, and gaming vulnerability.

**PJM's "High 4 of 5"** takes the 5 most recent eligible weekdays within a 45-calendar-day lookback window and averages consumption from the 4 highest-load days. It excludes event days, NERC holidays, weekends, and any day where average usage falls below **25% of the 5-day average** (outlier exclusion). PJM applies an **additive same-day adjustment**: actual average load in the 3 hours ending 1 hour before the event, minus the corresponding baseline value, added uniformly to each event interval. This additive approach shifts the entire baseline up or down by a fixed kW amount.

**CAISO's "10 of 10"** averages the same-hour load across the 10 most recent non-event days within a 45-day window, matching business days to business days. CAISO uses a **multiplicative same-day adjustment capped at ±20%** (factor bounded between 0.80 and 1.20). For residential aggregations, CAISO offers a "5 of 10" variant with a wider **±40% adjustment cap**, and a weather-matching method that selects the 4 days with the closest daily maximum temperature from a 90-day window.

**ERCOT's "Middle 8 of 10"** takes a distinctively robust approach: from 10 prior similar days, it excludes both the highest and lowest, averaging the remaining 8. This trims outliers from both tails, producing a more stable estimate.

**NYISO's "High 5 of 10"** selects the 5 highest-usage days from the last 10 eligible like-days, biasing the baseline upward to better approximate consumption on high-demand event days.

| Method | ISO | Selection rule | Lookback | Gaming vulnerability |
|--------|-----|---------------|----------|---------------------|
| 10 of 10 | CAISO | All eligible days | 45 days | Moderate — neutral selection |
| High 4 of 5 | PJM | Top 4 by load | 45 days | Lower — upward bias deters inflation |
| High 5 of 10 | NYISO | Top 5 by load | ~30 days | Lower — same logic as PJM |
| Middle 8 of 10 | ERCOT | Exclude min and max | ~10 days | Higher — robust to outliers |
| 5 of 10 | CAISO (residential) | Last 5 eligible | 45 days | Higher — shorter window, no selection |

LBNL's research across 646 accounts found that **morning adjustment factors reduce baseline error by 25–50%** regardless of which averaging method is used. The adjustment is particularly valuable for weather-sensitive loads where day-to-day variation is high. However, adjustments create a new gaming surface: users can artificially inflate consumption during the adjustment window to push the baseline up.

---

## Regression, machine learning, and the accuracy-transparency tradeoff

Beyond simple averaging, **regression-based models** and **machine learning approaches** offer substantially higher accuracy for variable loads — but at a cost to transparency and auditability that matters enormously in incentive systems.

LBNL's **TOWT (Time-of-Week and Temperature) model** is the most widely validated regression approach. It divides the week into 672 fifteen-minute intervals, creates piecewise-linear temperature response curves across 6 segments (roughly 7°C to 29°C), and fits separate models for occupied and unoccupied periods using weighted least squares with seasonal decay. Evaluated against **537 commercial buildings**, TOWT achieved median percent bias error under 1% for most building types, substantially outperforming averaging methods for weather-sensitive loads. The model is implemented in open-source tools (LBNL's RMV2.0, kW-Labs' nmecr) and underpins California's Normalized Metered Energy Consumption (NMEC) framework.

**Machine learning methods** push accuracy further. LSTM networks achieve **MAPE below 2%** on hourly load forecasting tasks. Attention-based LSTM variants outperform standard LSTM, RNN, DNN, and SVR across all operating modes. XGBoost-ANN ensembles achieve **9.68% MAPE** for residential short-term forecasting, outperforming standalone models. Gradient Boosting Machines (GBMs) allow incorporation of additional variables like solar radiation and production schedules.

However, the critical finding from the literature is that **complexity may not be justified by proportional accuracy gains**. Valentini et al. (2022) in their comprehensive review concluded that "low-complexity approaches are cost-efficient for large consumers with predictable loads." The Elia (2021) Baseline Methodology Assessment confirmed that "regression-based, calculated and control group baseline methodologies are not commonly applied, mainly due to implementation complexity." For a CER with mixed user types, the practical recommendation is to use simple averaging (High X of Y with adjustment) as the default, with regression reserved for users whose coefficient of variation exceeds 50%.

CAISO's **control group method** — using 200–400 non-dispatched customers as a real-time counterfactual — was found to be **more than twice as precise** as any day-matching or weather-matching method. It is also virtually immune to gaming. But it requires withholding a significant population from dispatch, making it impractical for small communities.

---

## Gaming prevention requires layered defenses

The manipulation problem in baseline-based incentive systems is well-documented and theoretically grounded. Chao (2011) demonstrated that administrative baselines create **double-payment incentives** where rational customers over-consume on non-event days to inflate their baseline, then collect rewards for "reducing" to their actual preferred consumption level. Wang and Tang (2020) modeled this as a Markov Decision Process and confirmed that payoff-maximizing customers will systematically engage in underconsumption on DR days and overconsumption on non-DR days.

The UK's Demand Flexibility Service provided a stark real-world example: consumers **artificially boosted demand during baseline establishment periods** to inflate apparent curtailment, with initial trials paying up to £6,000/MWh making gaming highly profitable.

Effective prevention requires multiple simultaneous mechanisms:

**"High X of Y" selection** is the first line of defense. Choosing the highest-consumption days as the baseline means a gamer must inflate consumption on *multiple* days — at least X+1 out of Y — making the electricity cost of gaming exceed the reward. As EnerNOC documented: "a participant would have to control multiple weeks of demand, quickly driving the costs of gaming above and beyond the potential benefits."

**Upward-only same-day adjustments** prevent a second attack vector. With symmetric (bidirectional) adjustments, a user who reduces consumption in the adjustment window inadvertently lowers their own baseline, punishing anticipatory conservation. Restricting adjustments to upward-only corrections, capped at +20%, eliminates this perverse incentive while still correcting for legitimately higher-than-average days.

**Pre-notification timing** for the adjustment window is critical. The adjustment should use consumption from **before the notification is sent**, not before the event starts. If users know a notification is coming (e.g., predictable solar surplus patterns), they could inflate consumption during a post-notification adjustment window. Using the 2 hours ending at notification time closes this gap.

**Randomized validation days** — designating ~15% of potential event periods as unannounced non-events where baselines are calculated but no notifications are sent — provide ground-truth accuracy measurement and make it impossible for users to know which days "count."

Additional mechanisms that real programs employ include minimum performance thresholds (**5% of baseline** before any points are awarded), per-event point caps (**150% of typical earning potential**), streak-based eligibility decay (reset after 3 consecutive non-responses), and statistical anomaly detection using high-frequency data to flag suspicious consumption patterns.

---

## Fairness across residential and industrial users demands hybrid rewards

A CER mixing residential consumers (2–5 kW average) with industrial users (100+ kW) faces an inherent tension: absolute kWh rewards align with actual grid value but allow industrial users to dominate the incentive pool, while percentage-based rewards equalize effort but can let very low-consumption users earn outsized rewards by flipping a single appliance.

The solution is a **hybrid reward formula** that combines both dimensions:

```
Points = kWh_shifted × Base_Rate × Effort_Multiplier × Streak_Bonus × Event_Multiplier
```

The `kWh_shifted` component (absolute) ensures rewards track real grid value. The `Effort_Multiplier` (percentage-based: **0.5× below 10%, 1.0× at 10–25%, 1.5× at 25–50%, 2.0× above 50%**) rewards proportional effort equally across user sizes. Streak bonuses (+3% per consecutive response, max 1.5×) incentivize sustained participation. Event multipliers (1.0× normal, 1.5× high surplus, 2.0× critical) direct behavior when it matters most.

Research strongly supports **individual baselines over community/portfolio baselines** for performance measurement. EnerNOC found that fewer than 10% of customers had their highest demand days aligned with the portfolio's, meaning 90%+ of participants would get inaccurate performance credit under aggregate baselines. Community-level aggregation should be used only for reporting total CER flexibility, never for individual settlement.

For heterogeneous communities, **tiered baseline methods by user class** improve accuracy: residential users (High 4 of 7, rolling 10-day window, ±15% adjustment cap, expected MAE 0.5–1.5 kWh per interval), small commercial (High 5 of 10 with temperature regression, expected MAE 2–5 kWh), and large industrial (regression-based or meter-before/meter-after with declared operating schedules). Academic literature on fairness in energy communities — including Shapley value allocation, Nash Bargaining, and α-fairness frameworks — confirms that contribution-based value sharing achieves Fairness Index values of **0.81–1.0**, far superior to the 0.28 achieved by simple consumption-based allocation.

---

## The Italian CER context creates a unique baseline challenge

Italy's CER framework operates fundamentally differently from US demand response programs. The virtual energy sharing model calculates **shared energy as MIN(total energy injected, total energy withdrawn)** per hour across all community members within the same primary substation perimeter. This uses actual metered data — no counterfactual estimation — and generates two revenue streams: the **tariffa incentivante** (feed-in premium, fixed for 20 years, up to ~100 €/MWh for small plants) applied only to new RES capacity, and the **corrispettivo di valorizzazione** (~8.5 €/MWh) applied to all virtually self-consumed energy.

The critical insight is that **the Italian CER incentive structure already creates an implicit baseline optimization problem** even without a formal baseline. Because shared energy equals MIN(production, consumption) per hour, every additional kWh consumed during solar production hours directly increases incentive payments. A CER that wants to layer a flexibility point system on top of the GSE incentive structure is essentially adding an explicit optimization signal to an already-incentivized behavior.

This creates a specific design consideration: the flexibility baseline should measure **incremental** load shifting beyond what the CER incentive structure already motivates. One approach is to establish the baseline from the user's post-CER-enrollment consumption pattern rather than pre-enrollment, ensuring that points reward additional flexibility beyond the natural alignment that GSE incentives already drive.

Italy's UVAM (Unità Virtuali Abilitate Miste) program provides the closest domestic precedent for formal baseline measurement. Under UVAM, the BSP (Balancing Service Provider) submits a baseline to Terna for each quarter-hour, representing the expected power schedule. Performance is measured against this declarative baseline, with 4-second telemetry required for real-time monitoring and minimum modulable power of **1 MW**. The ACER Framework Guideline on Demand Response (submitted to the European Commission in March 2025) establishes principles — accuracy, anti-gaming, transparency — but deliberately avoids mandating a single EU-wide methodology, leaving implementation to national regulators.

---

## 15-minute granularity is sufficient, with 8-second data as a validation layer

The CER's sensor architecture — 15-minute standard readings with 8-second burst data for deviations ≥300W — maps well to emerging European standards and proven demand response practice. **15-minute granularity is adequate for all baseline calculation, settlement, and reward purposes.** ERCOT measures DR performance at 15-minute intervals, the EU Electricity Directive mandates 15-minute smart meter capability, and both Octopus Energy's Saving Sessions and NESO's Demand Flexibility Service operate successfully at 30-minute resolution.

The 8-second burst data adds significant value as a **validation and anti-gaming layer** rather than a baseline input. Specific applications include detecting rapid load increases just before notification periods (a gaming signature), verifying that claimed load reductions show genuine step-change patterns rather than noise, identifying post-event consumption rebounds that indicate load was merely deferred rather than truly shifted, and enabling non-intrusive load monitoring (NILM) to attribute shifts to specific appliance categories.

The recommended architecture stores all 15-minute data in a time-series database (InfluxDB or TimescaleDB) with a rolling 60-day window for baseline computation. Burst data is processed at the edge, generating anomaly flags that feed into the settlement engine but are not incorporated into the baseline calculation itself — this keeps the baseline transparent and auditable while using high-frequency data for integrity verification.

---

## Recommended implementation for CER flexibility points

For the specific context described — a CER with 15-minute smart meters, heterogeneous users, and notification-triggered renewable surplus events — the following integrated design emerges from the research:

**Baseline method:** High 4 of 7 non-event similar days (weekday/weekend matched), computed per 15-minute interval. This provides a slight upward bias that deters gaming while remaining simple enough for users to understand and verify. Update the baseline daily on a rolling basis.

**Same-day adjustment:** Additive, upward-only, capped at +20%. Use the 2-hour window ending at notification time. Compare actual consumption in this window against the baseline profile for the same intervals, and add the positive difference (if any) to each event interval's baseline.

**Notification and response:** Target 2–6 hours advance notification based on renewable surplus forecasts. Response windows of 1–2 hours with a 30-minute minimum. Allow opt-in per event via app notification. Pro-rate points for partial participation (minimum 15 minutes and 5% reduction).

**Anti-gaming package:** Exclude event days from baseline windows. Randomize ~15% of potential events as silent validation periods. Cap per-event points at 150% of the user's rolling 30-day average. Apply anomaly detection on 8-second burst data. Implement 14-day onboarding with reduced points and no gaming penalties for new users.

**Edge cases:** New users receive cluster-based peer baselines for 14 days. Users with CV > 50% are flagged for regression-based baselines. Vacation days (consecutive readings below 5% of normal) are excluded from baseline windows. Industrial users with shift patterns declare schedules and receive separate per-shift baselines.

## Conclusion

The baseline methodology landscape has converged on a set of proven patterns after two decades of demand response operation. The "High X of Y" averaging framework with same-day adjustment remains the workhorse — not because it is the most accurate method available, but because it achieves the best balance across the four criteria that matter in practice: accuracy, integrity, simplicity, and alignment. For a CER layering flexibility incentives onto Italy's existing virtual energy sharing model, the key insight is that **the baseline system's primary function is trust, not precision**. Users must believe the system is fair, transparent, and resistant to manipulation — otherwise participation collapses regardless of statistical accuracy. This argues for the simplest defensible method (High 4 of 7 with upward-only adjustment) augmented by layered anti-gaming mechanisms, rather than a sophisticated ML model that users cannot verify. The 8-second burst data is most valuable not as a baseline input but as an integrity verification layer that keeps the simple system honest. The hybrid reward formula — combining absolute kWh value with percentage-based effort multipliers — resolves the residential-industrial fairness tension without requiring separate incentive pools, and the Italian regulatory context's hourly MIN(production, consumption) calculation for shared energy provides a natural foundation on which to build the flexibility point system.