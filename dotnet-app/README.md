# MonPlatPrenos .NET Worker

This is a .NET 8 worker app that mirrors the current Delphi behavior and is ready for extension with extra term rules.

## What is already implemented

- Daily scheduling at **07:30** (`Prenos:DailyRunTime`).
- Current core filtering logic ported conceptually:
  - plate order list fetch with scheduler `200` and material range,
  - operation filtering (`PP04`, `PP14`, `PP02`, `PP10`, step `0010`, confirmable > 0),
  - yield subtraction to compute remaining quantity,
  - term-based component matching for legacy terms.
- Configurable **extra terms** (`Prenos:ExtraTerms`) that go into a unified output.
- JSON output files in `output/`:
  - `plates-*.json`
  - `unified-*.json`
  - `semi-finished-*.json` (debug trace for AFRU/semi-finished flow)

## Run

Runtime alignment: `net8.0` worker + `sap.dll` / `sa_utils.dll` paths from `appsettings.json`.

```bash
cd dotnet-app/MonPlatPrenos.Worker
# one-time run for testing:
dotnet run -- --run-once

# replay one specific day (uses order StartDate filter):
dotnet run -- --run-once --from-date 2026-03-09

# replay date range day-by-day (inclusive):
dotnet run -- --run-once --from-date 2026-03-01 --to-date 2026-03-09

# scheduler mode (runs every day at DailyRunTime):
dotnet run
```

`--from-date`/`--to-date` are test/replay helpers. In replay mode, one job run is executed per day and logs per-step counters (orders fetched, filtered, operations, confirmations, component matches, and outputs).

During `--run-once`, the console now also prints a command-line progress bar while iterating orders (Delphi-like loop visibility).

## Configure new terms

`Prenos:EnableDebugJson` controls whether semi-finished debug traces are written to JSON.
`Prenos:EnableDebugTextDump` controls writing a text dump (`prenos-debug-*.txt`) for manual verification.


Edit `appsettings.json`:

```json
"ExtraTerms": [
  { "Name": "MyNewTerm", "Contains": "YOUR_TERM_HERE" },
  { "Name": "Another", "Contains": "ABC", "ExcludeContains": "XYZ", "MaxLength": 20 }
]
```

## How operation filtering works

Operation filtering is used to decide **which SAP operations count toward produced quantity (yield)**.

Current logic in `PrenosJob`:

1. Load operations for an order.
2. Keep only operations where all conditions are true:
   - `OperationCode` is in `Prenos:OperationCodes` (default: `PP04`, `PP14`, `PP02`, `PP10`),
   - `ConfirmableQty > 0`,
   - `StepCode == "0010"`.
3. For those valid operations, fetch confirmations and sum `Yield`.
4. Compute missing quantity:

```text
missingQty = PlannedQuantity - Sum(Yield on valid operations)
```

5. If `missingQty <= 0`, that order is skipped.

Why this exists: it prevents unrelated operations from affecting plate demand calculation.

Example:

- PlannedQuantity = 100
- Operations:
  - `PP04`, `0010`, confirmable 10 ✅ (included)
  - `PP99`, `0010`, confirmable 5 ❌ (excluded: not in configured list)
- Confirmations for included operation sum to 30
- Result: `missingQty = 100 - 30 = 70`

So "filtering by operations" means "use only selected operation types as the source of progress/yield".

## Connect to real SAP

Right now `MockSapClient` provides local test data.

### Using `sap.dll` and `sa_utils.dll`

The project is now prepared for these two DLLs:

1. Put files here:

```text
dotnet-app/MonPlatPrenos.Worker/lib/sap.dll
dotnet-app/MonPlatPrenos.Worker/lib/sa_utils.dll
```

2. The `.csproj` already auto-references them if present (`<Reference>` + `<HintPath>`), and copies them to output on build.
3. In `appsettings.json`, set:

```json
"Sap": {
  "UseMock": false,
  "SapDllPath": "lib/sap.dll",
  "SaUtilsDllPath": "lib/sa_utils.dll"
}
```

4. Run app. It will load both assemblies via `SapDllSapClient`.

Path resolution note: if a configured DLL path is relative, the worker now tries multiple runtime locations (`current directory`, `AppContext.BaseDirectory`, and common `dotnet run` output-parent path) before failing.

### Important: what is ready vs not ready

- ✅ **Ready now**:
  - DLL location convention,
  - project references,
  - runtime assembly load check,
  - config switch between mock and DLL mode.
- ⛏️ **Still to implement**:
  - actual SAP calls and response mapping inside `SapDllSapClient` methods.

`SapDllSapClient` currently throws `NotImplementedException` after successful DLL load, by design, until exact API calls are wired.

### SAP credentials/logon context in .NET worker

The .NET app does not directly send username/password in appsettings. Authentication is delegated to `sap.dll` / SAP runtime context on the machine (same principle as existing runtime integration). The worker logs this explicitly at startup and will also print optional `SAP_USER`, `SAP_CLIENT`, `SAP_SYSTEM` environment hints if present.

### Where to implement real calls

- File: `MonPlatPrenos.Worker/Services/SapDllSapClient.cs`
- Implement methods:
  - `GetProductionOrdersForPlatesAsync`
  - `GetOperationsAsync`
  - `GetConfirmationsAsync`
  - `GetComponentsAsync`

Map results into records in `Models/SapModels.cs`.


## Debug button runner (no DB writes)

A Windows Forms debug app is included: `MonPlatPrenos.DebugRunner`.

- Open solution `MonPlatPrenos.sln`
- Set startup project to `MonPlatPrenos.DebugRunner`
- Click **Run Prenos** button
- It executes the same `PrenosJob` and writes:
  - `plates-*.json`
  - `unified-*.json`
  - `semi-finished-*.json`
  - `prenos-debug-*.txt`

The form displays the generated debug text dump, so you can validate parity before any DB writing stage.

## Legacy Delphi DB tables to inspect daily transfer counts

The Delphi app writes transfer data into Access/ADO tables defined through `Montaz_pl.udl`. Key tables updated in the transfer flow:

- `plosce` (plate transfer rows),
- `samoti`,
- `protekt`,
- `sponke`,
- `ulitki`,
- `obroci`,
- `Zadprenos` (run timestamp marker).

Useful daily checks in the legacy DB are based on `plosce.danstart` (orders per day) and the latest record in `Zadprenos` (last transfer execution).
