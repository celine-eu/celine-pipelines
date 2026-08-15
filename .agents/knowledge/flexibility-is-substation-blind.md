# The flexibility signal is netted across substations, so its allocation is wrong

Verified 2026-07-30.

## There is no join path to `substation_id`

`grep -rn "substation"` across `apps/rec_flexibility/**` returns **zero hits**; so does `rec_id`
across its dbt models. The app never joins `silver_rec_registry`. It scopes by an
`active_devices` list in its config, materialised as a `rec_active_devices` seed and applied in
its silver meters model.

So making the app substation-aware is not a matter of adding a column: **there is no path from
`rec_flexibility` to `substation_id` at all**. It means introducing the registry, or extending
the seed.

## What goes wrong

`rec_flexibility_windows_community.sql` thresholds a forecast `net_exchange_kwh` that is built
upstream by grouping on timestamp and period only — **no substation**. A deficit on one cabina
therefore cancels a surplus on another. `rec_flexibility_windows.sql` then joins windows to
devices on **time only**, so every live device receives every window.

Measured over 180 days of hourly actuals, fleet-scoped (eight devices on one substation, two on
another, none on the third): 1,719 announced hours, signalling 26,755.0 kWh against 26,769.9 kWh
absorbable. **The aggregate is fine; the allocation is not.** Only 3,317.2 kWh — 12.4% — is
reachable on the two-device substation, where a member reaches about 10.4% of the signalled
figure on average (median 9.4%). And **157 of the 1,719 announced hours (9.1%) give zero
marginal benefit**: local net is at or below zero, so extra consumption cannot raise
`least(Σ import, Σ export)`.

**Quote 9.1% / ~10%, not 38.5%.** Screening at a 0.5 kWh per-substation threshold yields 38.5%,
but that is the wrong test — a 0.3 kWh local surplus still helps. The larger number is the one
that will get repeated, so it is worth knowing where it comes from.

## How far it propagates

The flexibility API ranks suggestions by `-community_kwh`, and `rec_settlement_15m.sql` pays
in-window consumption at 10 points per kWh **with no substation check**. So the misallocation
does not stop at the signal; it reaches settlement.

## One operational trap

`rec_flexibility_windows_community` is `full_refresh=false`, accumulate-only, and was restored
from a point-in-time backup on 2026-07-08. **Never full-refresh it.**
