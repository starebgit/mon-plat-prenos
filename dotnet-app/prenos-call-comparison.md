# Prenos performance comparison: legacy Delphi vs .NET worker

This document compares the **actual SAP call flow** in the legacy Delphi app and the .NET worker, and pinpoints why .NET can be much slower with the same number of orders.

## 1) Plate-order phase: call-by-call

### Delphi (legacy)
For each plate order:
1. `BAPI_PRODORD_GET_DETAIL` (operations) via `GetProdDetail(order,4,...)`.
2. For each valid operation: `BAPI_PRODORDCONF_GETLIST` via `GetConfList(order, conf, ...)`.
3. For each returned confirmation row: `BAPI_PRODORDCONF_GETDETAIL` via `GetConfDetail(conf, conf_c, yie)`.
4. If missing qty > 0: record plate row, then fetch components once with `BAPI_PRODORD_GET_DETAIL` using `GetProdDetail(dn,5,...)`.

Relevant code:
- Main loop and per-order calls: `GetProdDetail` + `GetConfList` + `GetConfDetail`. (`legacy-delphi/transfer.pas`)
- SAP wrappers: `GetProdList`, `GetProdDetail`, `GetConfList`, `GetConfDetail`.

### .NET worker
For each plate order:
1. `BAPI_PRODORD_GET_DETAIL` (operations) via `GetOperationsAsync`.
2. For each valid operation: `BAPI_PRODORDCONF_GETLIST` via `GetConfirmationsAsync`.
3. For each confirmation row: `BAPI_PRODORDCONF_GETDETAIL` is called **only if list-yield is 0**.
4. If missing qty > 0: fetch components once via `GetComponentsAsync` (`BAPI_PRODORD_GET_DETAIL` with component flag).

Relevant code:
- Orchestration loop in `PrenosJob.RunAsync`.
- SAP wrappers in `SapDllSapClient`.

## 2) Semi-finished phase (Samot/Protektor/Sponka/Obroc/Ulitki)

### Delphi (legacy)
- Uses `GetProdList(2, material, orderFrom, ...)` and fallback without `orderFrom`.
- For each sub-order, calls custom RFC `ZETA_RFC_READ_AFRU` and computes `yi1 - yi2`.
- For last Samot order, reads components with `GetProdDetail(...,5,...)` to detect ULITEK/SPIRALA and recursively process ULITEK path.

### .NET worker
- `ObdelajPolIzdAsync` calls `GetProductionOrdersByMaterialAsync` with fallback.
- For each sub-order, calls `GetAfruYieldDeltaAsync` (`ZETA_RFC_READ_AFRU`) and computes same `yi1 - yi2` logic.
- `ObdelajUliAsync` reads components (`GetComponentsAsync`) and recursively processes ULITEK via `ObdelajPolIzdAsync`.

Conclusion: the **business call graph is very similar**. The big gap is mostly not from missing/extra business RFCs.

## 3) Why .NET is likely ~10x slower

### A) Heavy reflection on every SAP interaction in .NET
`SapDllSapClient` is implemented via runtime reflection (`Assembly.LoadFrom`, `GetMethod`, `Invoke`, dynamic field access) for almost every operation:
- create function,
- invoke function,
- read table rows,
- read each field (`GetString`, `GetValue`, fallbacks).

This is much slower than Delphi's direct COM/OLE access pattern for large row counts and many nested loops.

### B) Destination/repository construction repeated per call in .NET
`CreateFunction()` calls `GetDestination()` each time, and then re-gets repository and reflection metadata each call. That adds overhead proportional to total RFC count.

### C) Strictly sequential awaits in hot loops
`PrenosJob` processes orders one-by-one, operations one-by-one, confirmations one-by-one, and recursive subflows also one-by-one. With network RTT and SAP response times, total wall clock accumulates quickly.

### D) Additional string probing per field
.NET repeatedly tries multiple possible field names and index fallbacks for each row/field (`GetFirstString`, `GetStringByIndex`), increasing per-row CPU cost.

## 4) Side-by-side key call mapping

| Business step | Delphi call | .NET call |
|---|---|---|
| Fetch plate orders | `BAPI_PRODORD_GET_LIST` via `GetProdList(1,...)` | `GetProductionOrdersForPlatesAsync` -> `BAPI_PRODORD_GET_LIST` |
| Read operations | `GetProdDetail(order,4)` -> `BAPI_PRODORD_GET_DETAIL` | `GetOperationsAsync` -> `BAPI_PRODORD_GET_DETAIL` |
| List confirmations | `GetConfList` -> `BAPI_PRODORDCONF_GETLIST` | `GetConfirmationsAsync` -> `BAPI_PRODORDCONF_GETLIST` |
| Confirmation detail | `GetConfDetail` -> always `BAPI_PRODORDCONF_GETDETAIL` | `GetConfirmationsAsync` -> detail only when yield=0 |
| Read components | `GetProdDetail(order,5)` -> `BAPI_PRODORD_GET_DETAIL` | `GetComponentsAsync` -> `BAPI_PRODORD_GET_DETAIL` |
| Sub-orders by material | `GetProdList(2,...)` | `GetProductionOrdersByMaterialAsync` |
| AFRU delta | `ZETA_RFC_READ_AFRU` in `Obdelajsamot/ObdelajPolIzd` | `GetAfruYieldDeltaAsync` -> `ZETA_RFC_READ_AFRU` |

## 5) Most likely bottleneck rank

1. Reflection-heavy SAP adapter in .NET.
2. Rebuilding destination/repository metadata repeatedly.
3. Fully sequential processing (no controlled concurrency in independent calls).
4. Repeated fallback field probing per row.

## 6) What to change first (highest ROI)

1. **Replace reflection SAP access with strongly typed NCo APIs** (or cache delegates/MethodInfo very aggressively).
2. **Cache destination + repository + frequently used method handles** once per run.
3. **Batch/constrain concurrency** for confirmation/detail calls (e.g., `SemaphoreSlim` with small degree like 4–8).
4. Keep current business filters and AFRU logic unchanged while optimizing plumbing.



## 7) Exact code lines: legacy Delphi vs .NET

### A) Plate loop and per-order RFC calls

**Legacy Delphi (`Getprenos`)**
```pascal
GetProdList(1,'',deln,tab,vredu ) ;
...
GetProdDetail(order,4,tabc,vredu) ;
...
GetConfList(order,conf,tabs,vredu) ;
...
GetConfDetail(conf,conf_c,yie) ;
...
GetProdDetail(dn,5,tabc,vredu) ;     // branje komponent
```

**.NET worker (`RunAsync`)**
```csharp
var orders = await _sapClient.GetProductionOrdersForPlatesAsync(...);
...
var operations = await _sapClient.GetOperationsAsync(order.OrderNumber, cancellationToken);
...
var confirmations = await _sapClient.GetConfirmationsAsync(order.OrderNumber, op.Confirmation, cancellationToken);
...
var components = await _sapClient.GetComponentsAsync(order.OrderNumber, cancellationToken);
```

**Difference:** business sequence is almost identical; both are call-heavy per order. Your timing (`GetOperations` + `GetComponents` called 8722 times each) confirms this.

### B) Confirmation detail behavior differs

**Legacy Delphi** always reads confirmation detail for each confirmation row:
```pascal
for m := 1 to mm do
begin
   conf_c := tabs.value(m,2) ;
   GetConfDetail(conf,conf_c,yie) ;
   yield := yield + yie;
end;
```

**.NET** reads detail only when list-yield is zero:
```csharp
var yield = ParseInt(GetFirstString(row, "YIELD", "CONFIRMED_YIELD", "LMNGA", "CONF_QTY"));
if (yield == 0)
{
    var detailFunction = CreateFunction("BAPI_PRODORDCONF_GETDETAIL");
    ...
    yield = ParseInt(GetFirstString(confDetail, "YIELD", "CONFIRMED_YIELD", "LMNGA"));
}
```

**Difference:** .NET already avoids many detail calls, so your slowdown is not caused by *more* confirmation-detail RFCs.

### C) The biggest technical difference: dynamic reflection in .NET SAP adapter

**.NET adapter** creates/invokes SAP calls through reflection repeatedly:
```csharp
_sapAssembly = Assembly.LoadFrom(_sapDllFullPath);
...
var function = CreateFunction("BAPI_PRODORD_GET_DETAIL");
...
return createFunction.Invoke(repository, new object[] { functionName });
...
invokeWithDestination.Invoke(function, new[] { destination });
```

and reads each field with fallback probing + exception swallowing:
```csharp
private static string GetFirstString(object row, params string[] fieldNames)
{
    foreach (var fieldName in fieldNames)
    {
        var value = GetString(row, fieldName);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }
    }
    return string.Empty;
}

private static string GetString(object row, string fieldName)
{
    try
    {
        ... GetString/GetValue via reflection ...
    }
    catch
    {
        return string.Empty;
    }
}
```

**Legacy Delphi** uses direct OCX SAP wrappers (`SAPFunctions1`) without this managed reflection layer.

**Difference:** .NET is paying extra CPU overhead per row/field/call even before network latency is counted.

## 8) Why your timing profile points to RFC latency + per-call overhead

From your report:
- `GetOperations.Invoke`: ~73 ms average, 8722 calls.
- `GetComponents.Invoke`: ~57 ms average, 8722 calls.
- Parse times are near-zero for both methods.

That means most wall time is in SAP call invoke round-trips (plus adapter overhead), not JSON/file parsing and not LINQ filtering.

## 9) Highest-ROI speedups for your .NET project

1. **Use typed NCo calls (no reflection in hot path)**
   - Replace generic reflection-based `CreateFunction`/`Invoke`/`GetString` loops with typed `IRfcFunction`, `IRfcTable`, `IRfcStructure` APIs.
   - This is the single biggest likely gain.

2. **Reduce RFC call count per run**
   - If possible, query only newer orders (move `orderFrom` forward) and/or by narrower start-date window.
   - 8722 × 2 heavy detail calls is expensive even with perfect code.

3. **Use controlled concurrency where SAP allows it**
   - You already have confirmation concurrency.
   - Add optional bounded concurrency for per-order operation/component fetches, but only if SAP backend and connection limits can handle it.

4. **Remove exception-driven field probing from row reads**
   - Today each missing field can incur exception overhead.
   - Precompute field names or use fixed known names for your SAP system.

## 10) Practical target

Given your measured averages, dropping per `GetOperations`/`GetComponents` call by even 15–20 ms would save several minutes across 8722 calls. Reducing call count (date/order window) can save even more immediately.

## 11) Deeper legacy-vs-.NET behavior differences that matter for runtime

This section focuses on details that are easy to miss when only comparing RFC names.

### A) Delphi does one long SAP session with direct OCX objects; .NET does NCo through reflection wrappers

Delphi calls SAP via `SAPFunctions1.add(...)` and direct `funct.call` on COM objects in hot loops (`GetProdList`, `GetProdDetail`, `GetConfList`, `GetConfDetail`). There is no managed reflection layer in between each row/field read.

In .NET, each SAP interaction still goes through reflection-based wrappers:
- create function (`CreateFunction`),
- set/get fields through reflection,
- invoke function via reflection overload detection.

Even with caching, that adds repeated per-row/per-field CPU overhead on top of RFC latency.

### B) The .NET adapter still does exception-capable probing on reads

`GetString` uses accessor probing and `TryInvokeStringAccessor` catches `TargetInvocationException` for unresolved fields. In high-volume loops, exception paths are expensive, and even non-throwing reflection calls remain costly relative to strongly typed NCo access.

### C) .NET only parallelizes confirmation calls, not order-level operations/components

The main order loop is strictly sequential (`for` over all orders). Within each order:
- operations fetch is awaited,
- confirmations can be concurrent,
- then components fetch is awaited,
- recursive semi-finished paths are also awaited.

So the two most expensive repeated calls you measured (`GetOperations`, `GetComponents`) are effectively serialized across all orders.

### D) Legacy code writes intermediate plate rows and reuses that reduced set

Delphi first computes missing plate quantities and writes only positive-missing rows into `plosce`; only then it runs semi-finished processing by iterating that persisted reduced set. The .NET job also filters by missing quantity, but it keeps all orchestration in one in-memory pipeline and keeps producing additional unified tracing rows in the same run, which adds managed allocation and processing overhead.

### E) Your timing profile mathematically fits RTT-dominated cost

With your reported averages and counts:
- `GetOperations`: `8722 * 73 ms ≈ 636,706 ms` (~10.6 min)
- `GetComponents`: `8722 * 57 ms ≈ 497,154 ms` (~8.3 min)

These two families alone represent ~18.9 minutes of cumulative call time if fully serialized. Even with overlap/filters, this is enough to explain a 12-minute wall clock in .NET.

This strongly indicates that latency + per-call adapter overhead dominates, not parsing or business-rule CPU.

## 12) Most probable root-cause stack (confidence-ranked)

1. **RFC round-trip volume on serialized hot calls** (`GetOperations`, `GetComponents`) — very high confidence.
2. **Reflection-mediated NCo adapter overhead** on every call/row/field — high confidence.
3. **Confirmation/detail call fan-out** on orders with many operations — medium/high confidence.
4. **Managed allocation and extra tracing output in .NET path** (unified/semi-finished artifacts) — medium confidence.
5. **Potential SAP destination/session behavior differences under NCo config** — medium confidence.

## 13) Fastest validation experiments (to prove where the 10 minutes goes)

1. **Disable semi-finished recursion for one benchmark run** (Samot/Protektor/Sponka/Obroc/Ulitki path) and compare total time.
   - If runtime barely moves, bottleneck is plate loop RFCs.
2. **Run with order-level bounded concurrency for `GetOperations` + `GetComponents` only** (e.g., 4 workers).
   - If runtime drops sharply, you have serialization bottleneck.
3. **Prototype one typed-NCo implementation of `GetOperations` only** (no reflection read path) and A/B it.
   - If per-call avg drops meaningfully, reflection adapter is confirmed as major overhead.
4. **Log SAP-side response time if available** (STAD/ST05 correlation by RFC/user/time window).
   - Separates network/SAP backend time from client-side marshaling overhead.

## 14) Practical short-term plan to close 12 min -> near Delphi range

1. Keep business logic unchanged.
2. Replace only the hot-path read APIs first:
   - `GetOperationsAsync`
   - `GetComponentsAsync`
   with typed NCo structures/tables.
3. Add bounded order-level concurrency for those two calls (start with 3–4).
4. Keep confirmation concurrency as-is; tune after hot-path improvements.
5. Re-benchmark with the same `orderFrom`, plant, scheduler, and material range.

If your current averages hold, cutting ~20 ms from each of those 17,444 hot calls saves ~5.8 minutes by itself.

## 15) Phased implementation plan (best path)

If you want low-risk execution with measurable gains, implement in this order:

### Phase 0 — Baseline + guardrails (1–2 days)
**Goal:** lock down a reproducible benchmark before touching behavior.

- Add one benchmark mode (fixed `orderFrom`, fixed date window, same plant/scheduler) and export:
  - total runtime,
  - call counts by RFC,
  - avg/p95 per RFC.
- Persist one “golden” output snapshot for functional parity checks.
- Define pass criteria for each next phase (example: `>=20% faster`, `0 output drift` for same input window).

**Implemented now:** the worker supports `Prenos:Benchmark` with snapshot write + optional parity compare against a baseline digest (`Plant` and `OrderFrom` are now config-driven, no longer hard-coded).

**Why first:** prevents “faster but wrong” changes and avoids chasing noisy timings.

### Phase 1 — Kill reflection overhead in hottest two calls (3–5 days)
**Goal:** reduce cost where you already measured biggest totals.

- Implement typed NCo versions (no reflection row access) for:
  1. `GetOperationsAsync`
  2. `GetComponentsAsync`
- Keep old reflection path behind a feature flag (`UseTypedHotPath=true/false`) for rollback.
- A/B benchmark typed vs reflection with identical input.

**Implemented now:** `GetOperationsAsync` and `GetComponentsAsync` support `UseTypedHotPath` with cached compiled delegates for row and field access in hot loops, with automatic rollback to legacy reflection parsing when disabled.

**Expected impact:** highest immediate gain with minimal business-risk because logic stays identical.

### Phase 2 — Order-level bounded concurrency for hot path (2–4 days)
**Goal:** remove global serialization of `GetOperations` and `GetComponents` across orders.

- Introduce bounded parallelism over orders (start conservative: 3–4 workers).
- Keep per-order logic deterministic and preserve output ordering by sorting before write.
- Add safety knobs:
  - `OrderConcurrency`
  - `MaxSapCallsInFlight`
  - automatic fallback to lower concurrency on SAP timeout/error spikes.

**Implemented now:** worker supports `OrderConcurrency` plus global SAP throttle `MaxSapCallsInFlight`, with deterministic result merge by source order index.

**Expected impact:** big wall-clock drop if SAP backend can tolerate the load.

### Phase 3 — Confirmation/detail + field access cleanup (2–3 days)
**Goal:** trim secondary overhead and reduce tail latency.

- Convert `GetConfirmationsAsync` and detail reads to typed access.
- Remove exception-driven field probing in hot loops.
- Keep strict field validation at startup, not repeatedly in hot path.

**Implemented now:** `GetConfirmationsAsync` uses the typed hot-path parser (with rollback via `UseTypedHotPath=false`) and detail-yield reads use fast field access instead of the reflection probing path.

**Expected impact:** moderate but meaningful; improves stability and CPU usage.

### Phase 4 — Call-volume reduction (business-safe filters) (1–3 days)
**Goal:** reduce total RFC count, not just speed per RFC.

- Move `orderFrom` forward by policy (incremental processing watermark).
- Narrow date/material windows where operationally acceptable.
- Skip recursive semi-finished processing in runs where it is not needed (feature-flagged mode).

**Implemented now:** worker supports watermark-driven `orderFrom` progression (`Prenos:Watermark`) and feature-flagged recursion skip (`Prenos:EnableSemiFinishedExpansion=false`) to reduce RFC volume without touching core plate matching rules.

**Expected impact:** strongest long-term scaling lever.

## 16) Recommended “best” sequence in one line

**Best practical order:** **Phase 0 -> Phase 1 -> Phase 2 -> Phase 3 -> Phase 4**.

Reason:
1. You first prove correctness and stable benchmarks.
2. Then you optimize the two proven hotspots with lowest logic risk.
3. Then you parallelize safely for wall-clock wins.
4. Then you clean the remaining overhead.
5. Finally, you cut call volume for structural scale.

## 17) Rollout and risk controls

- Use feature flags per phase so each optimization can be toggled independently.
- Roll out by environment:
  1. local benchmark,
  2. test SAP window,
  3. production at low concurrency,
  4. raise concurrency gradually.
- For each rollout step compare:
  - runtime,
  - RFC error rate,
  - output parity hash vs baseline snapshot.

## 18) Concrete target by phase

- After **Phase 1**: expect noticeable drop (often 20–35% depending on current reflection overhead).
- After **Phase 2**: largest wall-clock reduction if SAP permits parallelism.
- After **Phase 3+4**: incremental gains + better stability and scalability.

Combined, these phases are the most realistic path from ~12 minutes toward the legacy range without rewriting business logic.
