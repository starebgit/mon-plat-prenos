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
