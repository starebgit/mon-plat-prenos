# RFC field lockdown plan (no guessing)

This plan removes runtime field guessing from the worker and replaces it with exact SAP field names that you provide.

## Goal
- Use **one exact field name per business value**.
- Remove `GetFirstString(...many names...)` from hot paths.
- Fail fast if a configured field is wrong.

## Change list
1. Add field-map config to `Prenos:Sap:FieldMap` (one field per value).
2. Replace fallback reads in `SapDllSapClient` with exact reads from `FieldMap`.
3. Add `Prenos:Sap:StrictFieldValidation` (default `true` in production).
4. Validate configured fields once per RFC output table/structure and cache success.
5. Keep a temporary compatibility mode (fallback aliases) only behind `Prenos:Sap:AllowFallbackFieldAliases=false`.

## Required field names from your SAP system

Provide exact names for these RFC outputs.

### 1) `BAPI_PRODORD_GET_LIST` -> `ORDER_HEADER`
- OrderNumber
- Material
- SystemStatus
- PlannedQuantity
- StartDate
- SchedulerCode
- Plant

### 2) `BAPI_PRODORD_GET_DETAIL` -> `OPERATION`
- Confirmation
- OperationCode
- StepCode
- ConfirmableQuantity
- WorkCenterCode

### 3) `BAPI_PRODORD_GET_DETAIL` -> `COMPONENT`
- Material
- Description

### 4) `BAPI_PRODORDCONF_GETLIST` -> `CONFIRMATIONS`
- Confirmation
- ConfirmationCounter
- Yield

### 5) `BAPI_PRODORDCONF_GETDETAIL` -> `CONF_DETAIL`
- Yield

### 6) `ZETA_RFC_READ_AFRU` output
- Yield1 (first total)
- Yield2 (second total)

## How to collect the names (simple)

### Option A (best): SAP GUI -> SE37
For each RFC above:
1. Open transaction `SE37`.
2. Enter function module name.
3. Click **Display**.
4. Open **Tables** and **Export/Import** parameters.
5. Open each relevant structure/table type and copy exact technical field names.

### Option B: send one sample payload per RFC
Run each RFC once and export one sample result row for each table/structure above.
We can map exact field names from sample output.

## Fill this template and send back

```text
BAPI_PRODORD_GET_LIST.ORDER_HEADER
- OrderNumber = 
- Material = 
- SystemStatus = 
- PlannedQuantity = 
- StartDate = 
- SchedulerCode = 
- Plant = 

BAPI_PRODORD_GET_DETAIL.OPERATION
- Confirmation = 
- OperationCode = 
- StepCode = 
- ConfirmableQuantity = 
- WorkCenterCode = 

BAPI_PRODORD_GET_DETAIL.COMPONENT
- Material = 
- Description = 

BAPI_PRODORDCONF_GETLIST.CONFIRMATIONS
- Confirmation = 
- ConfirmationCounter = 
- Yield = 

BAPI_PRODORDCONF_GETDETAIL.CONF_DETAIL
- Yield = 

ZETA_RFC_READ_AFRU
- Yield1 = 
- Yield2 = 
```

## Definition of done
- Worker runs with `StrictFieldValidation=true` and `AllowFallbackFieldAliases=false`.
- No `GetFirstString` alias lists in hot loops.
- Timing report shows lower parse overhead and fewer exception/fallback probes.
