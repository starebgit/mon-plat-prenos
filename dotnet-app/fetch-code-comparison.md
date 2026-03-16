# Fetch-code comparison: legacy Delphi vs .NET worker

This is a focused comparison of just the **data-fetch paths** (RFC call + row/field reading), excluding business-rule discussion.

## 1) Plate order list fetch (`BAPI_PRODORD_GET_LIST`)

### Delphi
- Calls `GetProdList(1, ..., tab, ...)` and then loops rows by index.
- Reads fields positionally (`tab.value(i,1)`, `tab.value(i,5)`, `tab.value(i,14)`, `tab.value(i,18)`, `tab.value(i,42)`).

### .NET
- Calls `GetProductionOrdersForPlatesAsync` -> `BAPI_PRODORD_GET_LIST`.
- Uses name-based field map (`OrderNumber`, `Material`, `SystemStatus`, `PlannedQuantity`, `StartDate`, `Plant`).
- Now supports a fast typed-hot-path parser for this table (`ParsePlateOrderHeadersFast`) that uses compiled table/row delegates and keeps name-based column lookup.

## 2) Per-order operations fetch (`BAPI_PRODORD_GET_DETAIL` / OPERATION)

### Delphi
- Calls `GetProdDetail(order,4,tabc,...)` and reads operation rows by positional indexes (e.g. confirmation from `tabc.value(j,4)`).

### .NET
- Calls `GetOperationsAsync` -> `BAPI_PRODORD_GET_DETAIL` + `ORDER_OBJECTS` index 4.
- Parses by field names from map (`Confirmation`, `OperationCode`, `StepCode`, `ConfirmableQuantity`, `WorkCenterCode`).
- Has fast parser (`ParseOperationsFast`) using compiled accessors.

## 3) Confirmation list + optional detail (`BAPI_PRODORDCONF_GETLIST` / `GETDETAIL`)

### Delphi
- Calls `GetConfList(order, conf, tabs, ...)`.
- For each confirmation row, calls `GetConfDetail(conf, conf_c, yie)` (detail call for each row) and aggregates yield.

### .NET
- Calls `GetConfirmationsAsync` -> `BAPI_PRODORDCONF_GETLIST`.
- Reads list rows via map (`Confirmation`, `ConfirmationCounter`, `Yield`).
- Calls `BAPI_PRODORDCONF_GETDETAIL` only when list yield is `0`.
- Has fast parser (`ParseConfirmationsFast`) and fast detail read (`LoadConfirmationDetailYieldFast`).

## 4) Component fetch (`BAPI_PRODORD_GET_DETAIL` / COMPONENT)

### Delphi
- Calls `GetProdDetail(order,5,tabc,...)` and reads positional fields for components.

### .NET
- Calls `GetComponentsAsync` -> `BAPI_PRODORD_GET_DETAIL` + `ORDER_OBJECTS` index 3.
- Reads mapped name fields (`Material`, `Description`).
- Has fast parser (`ParseComponentsFast`).

## 5) AFRU delta fetch (`ZETA_RFC_READ_AFRU`)

### Delphi
- Calls custom RFC and reads `IT_AFRU` by positional indexes (`Tabs.value(m,9)`, `Tabs.value(m,38)`, `Tabs.value(m,95)`).

### .NET
- Calls `GetAfruYieldDeltaAsync` -> `ZETA_RFC_READ_AFRU`.
- Reads mapped name fields (`WorkCenterId`, `Yield`, `Reversed`).
- Now supports fast parser (`ParseAfruYieldBucketsFast`) with same bucketing/sign logic.

## 6) Key equivalence + unavoidable difference

- **Equivalent:** RFC sequence and business intent are aligned (list -> detail(s) -> aggregate -> components).
- **Different by design:** Delphi uses positional reads; .NET uses name-based mapped reads.
- **What we changed:** for hot loops, .NET now avoids `MethodInfo.Invoke` and uses compiled delegates while preserving name-based lookup semantics.

## 7) Answer to “does .NET require name-based lookup?”

- NCo supports both name-based and index-based access in different APIs.
- In this project, name-based mapped lookup is the safer compatibility choice (schema readability + field-map configurability).
- To get Delphi-like speed without losing this safety, fast cached delegates + reduced per-row framework work is the right compromise.
