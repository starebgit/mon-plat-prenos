# Prenos performance deep dive: why legacy Delphi is much faster than .NET

This is a deep code-level comparison of the **actual active Delphi path** (`TFTransfer.Getprenos`) versus the .NET worker (`PrenosJob.RunAsync`).

## Executive summary (likely root causes)

The slowdown is most likely not one single thing. It is a compound effect:

1. **The .NET flow does much more semiproduct SAP work than the active Delphi flow.**
   - Delphi deduplicates semiproduct materials with `Preverisam(...)` and then only records combinations (`ZapisSamot`) for repeats.
   - .NET re-queries semiproduct order lists and AFRU deltas per matching component path, without material-level memoization.
2. **.NET calls `ZETA_RFC_READ_AFRU` for Protektor/Sponka/Obroc paths, while active Delphi transfer path does not.**
   - In `transfer.pas`, `ObdelajPolIzd` only writes list entries and does not execute AFRU RFC.
   - In .NET, `ObdelajPolIzdAsync` always executes AFRU-delta processing for returned suborders.
3. **.NET lacks some Delphi narrowing filters on semiproduct suborders** (date/status checks), increasing RFC count on old/closed orders.
4. **.NET SAP adapter is still reflection-heavy in hot loops**, which adds CPU overhead per field/call even when destination/repository are cached.
5. **Everything is largely serialized at order level** in .NET; network RTT accumulates.

If Delphi is ~2 min and .NET ~12 min, these differences are enough to explain a 5–6x gap.

---

## 1) Important baseline: which Delphi code path is actually used

The active UI button and auto-start path call `FTransfer.Getprenos`, not the older thread implementation in `izvedi.pas`:

- `zacetek.pas` calls `FTransfer.Getprenos` on button click and form auto-run.
- That means `legacy-delphi/transfer.pas` is the primary legacy behavior to compare against.

This matters because `transfer.pas` has lighter semiproduct logic than `izvedi.pas`.

---

## 2) Plate-order phase: mostly equivalent, but still expensive in both

### Delphi (`TFTransfer.Getprenos`)
For each plate order:
- `GetProdDetail(order,4,...)` => operations.
- For valid operations, `GetConfList(order, conf, ...)`.
- For each confirmation row, `GetConfDetail(conf, conf_c, ...)` (always detail call).
- Write `plosce` row when `ikolic > 0`.

### .NET (`PrenosJob.RunAsync`)
For each plate order:
- `GetOperationsAsync`.
- `GetConfirmationsAsync`.
- Detail confirmation call only when list yield is 0.
- Writes in-memory `plateDemands` and proceeds.

**Observation:** this part is call-heavy in both systems, but .NET is not obviously doing *more* than Delphi here. The major gap is likely downstream (semiproduct paths).

---

## 3) Biggest behavioral mismatch: semiproduct processing fan-out

## 3.1 Delphi deduplicates semiproduct material processing

Delphi uses `Preverisam(...)` and list caches (`zlist`, `zplist`, etc.) so each semiproduct material is effectively “expensive-processed once”, then subsequent occurrences mostly update aggregate entries.

Pattern:
- first time material appears => expensive logic
- next times => lightweight accumulation (`ZapisSup` / `ZapisSamot`)

This is a strong anti-explosion mechanism.

## 3.2 .NET does not memoize by material

In .NET:
- each matching component can call `ObdelajPolIzdAsync(...)`;
- `ObdelajPolIzdAsync` fetches production orders by material (+ fallback), then AFRU per suborder;
- no cache short-circuit for already-processed material.

With many plate orders sharing same semiproducts, this can multiply RFC count dramatically.

---

## 4) Critical mismatch: AFRU for Protektor/Sponka/Obroc

In active Delphi `transfer.pas`:
- `ObdelajPolIzd(...)` does **not** execute `ZETA_RFC_READ_AFRU`; it mostly writes zero/placeholder semiproduct stock entries and relation rows.

In .NET:
- same family path (`ObdelajPolIzdAsync`) executes `GetAfruYieldDeltaAsync` per returned suborder.

This is a likely major reason for runtime explosion.

In other words: the .NET worker is not only a port; in semiproduct processing it is functionally *more expensive* than the active Delphi transfer implementation.

---

## 5) Missing Delphi narrowing filters on suborders

Delphi semiproduct loops contain limiting behavior like:
- date-based narrowing (`fdate` handling),
- status skipping (`TEHZ` skip).

.NET `ObdelajPolIzdAsync` currently processes all returned suborders directly for AFRU delta, with no equivalent date/status pruning before invoking AFRU.

That increases both RFC count and total payload over time.

---

## 6) Adapter overhead: still relevant but not the whole story

`SapDllSapClient` does cache destination and repository objects, so destination creation is not repeated every call. But the hot path still includes:
- reflection invocation for function execution,
- reflection-based row access,
- repeated string extraction and conversion in large loops.

This overhead is real, but likely secondary to call-count explosion from sections 3–5.

---

## 7) Practical ranking of slowdown drivers

Most likely impact order:

1. **Extra semiproduct RFC workload in .NET** (especially AFRU per polizdelek path).
2. **No semiproduct material dedup/memoization in .NET**.
3. **Missing suborder pruning filters** (date/status) compared to Delphi behavior.
4. **Reflection overhead in SAP adapter**.
5. **Mostly serial orchestration** amplifying RTT.

---

## 8) What to change first (highest ROI)

1. **Replicate active Delphi semiproduct semantics first (before micro-optimizing):**
   - Add material-level memoization (`processedMaterials` per category) equivalent to `Preverisam` behavior.
   - For Protektor/Sponka/Obroc, confirm whether AFRU is truly required; if legacy-equivalence is target, do not call AFRU there.
2. **Apply suborder pruning before AFRU calls:**
   - skip TEHZ/ZAKL where applicable,
   - limit by date window similarly to Delphi.
3. **Only then optimize plumbing:**
   - move away from reflection-heavy access toward typed NCo API or compiled delegates.
4. **Add bounded parallelism only after correctness parity:**
   - parallelize independent SAP calls carefully (respect SAP/session limits).

---

## 9) Why this explains 2 min vs 12 min

A 6x slowdown is very plausible when:
- call counts are inflated by repeated semiproduct processing,
- extra AFRU RFCs are issued for categories not doing AFRU in active Delphi,
- and each call carries moderate per-call managed overhead.

The key is to first match **active** Delphi behavior (`transfer.pas`) exactly, then optimize.

---

## 10) Recommended delivery strategy: step-by-step, not all-at-once

Short answer: **do it step-by-step with hard checkpoints**. A big-bang rewrite will make it hard to prove parity and very hard to debug SAP-side effects.

### Phase 0 — Freeze a parity baseline (1–2 days)

1. Lock one reproducible comparison window (same `orderFrom`, same date, same plant/scheduler).
2. Capture Delphi outputs and key counters as the reference snapshot.
3. Add mandatory run report in .NET with:
   - total orders fetched,
   - RFC call counts per function,
   - unique semiproduct material counts per category,
   - rows written per output category.

**Exit criteria:** same input window is rerunnable and measured in both implementations.

### Phase 1 — Business parity before optimization (highest impact)

Implement only the semantic differences:

1. Add material-level dedup/memoization equivalent to Delphi `Preverisam(...)` behavior.
2. Align Protektor/Sponka/Obroc branch behavior with active Delphi:
   - if parity target is strict transfer behavior, do not run AFRU there.
3. Add Delphi-like suborder pruning before AFRU work:
   - status filter (`TEHZ`/`ZAKL` where relevant),
   - date/window cutoff.

**Exit criteria:**
- output row counts are within agreed tolerance to Delphi (ideally exact for controlled window),
- RFC count drops materially versus current .NET run.

### Phase 2 — Reliability hardening (do before speed tuning)

1. Add retry policy for transient RFC failures (bounded retries + jitter).
2. Add per-function timeout configuration.
3. Add idempotent run markers (run id, deterministic output names, safe rerun behavior).
4. Add structured logging for each business branch decision (why skipped/processed).

**Exit criteria:** repeated runs are stable and outcomes are deterministic.

### Phase 3 — Performance optimization (after parity is proven)

1. Replace reflection hot path with typed NCo APIs (or precompiled delegate accessors).
2. Keep destination/repository/function metadata cached per process lifetime.
3. Introduce bounded concurrency only on independent calls (small SAP-safe degree, e.g., 4–8), behind config flags.
4. Re-profile call counts + p50/p95 per RFC after each optimization.

**Exit criteria:** runtime trend reaches target while preserving parity outputs.

### Phase 4 — Controlled rollout

1. Run shadow mode: Delphi remains source of truth, .NET runs in parallel and compares.
2. Add automatic diff report per run (counts + key business fields).
3. Promote .NET only after X consecutive green comparisons.

**Exit criteria:** operational sign-off to switch production ownership.

### Why this is better than all-at-once

- You avoid mixing **logic changes** and **performance changes** in one release.
- You can attribute wins/losses to a specific phase.
- If a mismatch appears, rollback is surgical and fast.
