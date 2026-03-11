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

