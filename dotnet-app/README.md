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

Runtime alignment: `net8.0` worker + `sapnco.dll` / `sapnco_utils.dll` paths from `appsettings.json`.

```bash
cd dotnet-app/MonPlatPrenos.Worker
# one-time run: prints SAP login check and executes prenos for one day (today by default):
dotnet run -- --run-once
# if you already built once and want to avoid build/roslyn noise:
dotnet run --no-build -- --run-once
# run-once for a specific day:
dotnet run -- --run-once --from-date 2026-03-11


# scheduler mode (runs every day at DailyRunTime):
dotnet run
```

When using `--run-once`, `--from-date` is optional and sets the one-day run date (`DateTime.Today` when omitted). `--to-date` is ignored.


## Visual Studio (F5) run-once

The worker now includes `Properties/launchSettings.json` with a default debug profile:

- **MonPlatPrenos.Worker (run-once)** → starts with `--run-once` (one-shot execution when pressing F5).
- **MonPlatPrenos.Worker (scheduler)** → starts without args (normal scheduled service mode).

In Visual Studio, select the desired profile from the debug target dropdown before pressing F5.

## VS Code debug/run-once workflow

For easier debugging in VS Code, the repository now includes:

- `.vscode/launch.json` with **MonPlatPrenos: Debug run-once** (F5 launch in integrated terminal),
- `.vscode/tasks.json` with **run-once-with-log** task for one-shot execution and captured console output.

The task writes terminal output to:

- `output/run-once-YYYYMMDD-HHMMSS.log`

The worker also writes fetched order code rows to:

- `output/fetched-codes-YYYYMMDD-HHMMSS.txt`

You can change the fetched-code filename pattern with:

```json
"Prenos": {
  "FetchedCodeLogFilePattern": "fetched-codes-{timestamp}.txt"
}
```

## Phase 0 benchmark mode (baseline + guardrails)

You can now enable a reproducible benchmark snapshot in `Prenos:Benchmark`:

```json
"Prenos": {
  "Plant": "1061",
  "OrderFrom": "000005223286",
  "Benchmark": {
    "Enabled": true,
    "SnapshotPath": "output/baseline-snapshot.json",
    "CompareSnapshotPath": "output/baseline-snapshot.json"
  }
}
```

Behavior when enabled:
- writes `output/benchmark-*.json` with runtime, per-step timings, SAP timing report, and deterministic output digests,
- optionally writes a baseline snapshot to `SnapshotPath`,
- optionally compares current output digests to `CompareSnapshotPath` and fails fast on drift.

## Phase 1 hot-path mode

`Prenos:UseTypedHotPath` controls the optimized parser path for hot SAP table parsing calls:
- `GetOperationsAsync`
- `GetComponentsAsync`
- `GetConfirmationsAsync` (including detail-yield read when list yield is 0)

When `true` (default), the worker uses cached compiled delegates for SAP table row + field access in those methods to avoid repeated `MethodInfo.Invoke` in row loops.
Set it to `false` for immediate rollback to the older reflection row parser.

## Phase 2 bounded order concurrency

Use these knobs to safely parallelize order processing:
- `Prenos:OrderConcurrency` (default `1`) controls how many orders are processed concurrently.
- `Prenos:MaxSapCallsInFlight` (default `8`) sets a global SAP-call semaphore across all order workers.

Start with conservative values (for example `OrderConcurrency=3`, `MaxSapCallsInFlight=6`) and use benchmark snapshots to tune.

## Phase 3 confirmation/detail cleanup

Phase 3 extends the typed hot-path to `GetConfirmationsAsync`, including typed/fast field access when fetching `BAPI_PRODORDCONF_GETDETAIL` for zero-yield rows.

## Phase 4 call-volume reduction

Phase 4 adds two business-safe volume controls:
- `Prenos:EnableSemiFinishedExpansion` (default `true`): when `false`, skips recursive semi-finished SAP expansion (`Samot/Protektor/Sponka/Obroc/Ulitki`), keeping only direct plate-component matching.
- `Prenos:Watermark`:
  - `Enabled`: when `true`, uses and updates a persisted order watermark to move `OrderFrom` forward between runs,
  - `FilePath`: watermark file path (default `output/orderfrom.watermark.txt`).

The effective `orderFrom` is `max(Prenos:OrderFrom, watermarkValue)` when watermarking is enabled.

## Parity mode (fixed fromDate + fixed orderFrom)

Use parity mode when you need deterministic reruns against the exact same inputs.

When `Prenos:ParityMode:Enabled` is `true`, the worker:
- always uses `Prenos:ParityMode:FixedFromDate` as `fromDate`,
- always uses `Prenos:ParityMode:FixedOrderFrom` as `orderFrom`,
- skips watermark read/write entirely,
- fails fast at startup if either fixed value is missing.

Sample config:

```json
"Prenos": {
  "ParityMode": {
    "Enabled": true,
    "FixedFromDate": "2026-03-11",
    "FixedOrderFrom": "000005223286"
  }
}
```

Run command:

```bash
dotnet run -- --run-once
```

With parity mode enabled, `--from-date` is ignored and the fixed date is always used.

`--run-once` does two steps:

1. login/config check (loads SAP settings from direct `Prenos:Sap` values or DB lookup and prints `RUN-ONCE LOGIN CHECK`)
2. executes a real one-day prenos (`RUN-ONCE PRENOS DAY`) and exits.

It prints `LoginSource` and `LoginMessage` so you can see why values are empty (for example missing connection string, DB query returned no rows, or DB exception).

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

The worker uses `SapDllSapClient` for SAP access.

### Using `sapnco.dll` and `sapnco_utils.dll`

The project is now prepared for these two DLLs:

1. Put files here:

```text
dotnet-app/MonPlatPrenos.Worker/lib/sapnco.dll
dotnet-app/MonPlatPrenos.Worker/lib/sapnco_utils.dll
```

2. In `appsettings.json`, set:

```json
"Sap": {
  "SapDllPath": "lib/sapnco.dll",
  "SaUtilsDllPath": "lib/sapnco_utils.dll"
}
```

3. Run app. It will load both assemblies directly from the configured paths via `SapDllSapClient`.

4. Use **x64** SAP NCo binaries and run the worker as **x64** (the project sets `PlatformTarget` to `x64`).
   If you still get `The specified module could not be found`, that usually means a missing native dependency (commonly Visual C++ runtime) or mismatched `sapnco`/`sapnco_utils` versions.

#### Interpreting `sap-debug-*.txt` (common failure)

If your log shows:

- `Loaded SAP libraries: ...sapnco.dll and ...sapnco_utils.dll`
- followed by: `Could not load file or assembly '...sapnco_utils.dll'. The specified module could not be found.`

then the file itself is usually present, but one of its **native dependencies** is missing (for example SAP NW RFC SDK binaries on `PATH`, or required Visual C++ runtime).

This is **not** the classic `.NET Framework 4.7 vs 4.8` issue in this worker:

- the project targets `net8.0`,
- runs on `.NET` (`Microsoft.NETCore.App`),
- and uses SAP NCo assemblies from local DLLs.

In other words, fix native SAP/runtime dependencies first, not .NET Framework 4.x.

Practical checks on the machine where the worker runs:

1. Verify architecture is consistent everywhere (worker x64, sapnco x64, sapnco_utils x64, SAP native SDK x64).
2. Confirm the SAP native RFC SDK DLLs required by `sapnco_utils.dll` are available in process search path.
3. Install/update Microsoft Visual C++ Redistributable x64 (2015-2022) if missing.
4. Keep `sapnco.dll` and `sapnco_utils.dll` from the exact same SAP NCo package/version.
5. Re-run with host tracing and check for first `FileNotFoundException`/`The specified module could not be found` entry.

5. Quick DLL/version check in PowerShell:

```powershell
Get-Item .\lib\sapnco.dll, .\lib\sapnco_utils.dll |
  Select-Object Name, Length, @{n="ProductVersion";e={$_.VersionInfo.ProductVersion}}, @{n="FileVersion";e={$_.VersionInfo.FileVersion}}
```


### SAP destination behavior (Delphi-aligned)

If logs show:

- `RfcInvalidStateException: Cannot get destination <name> -- no destination configuration registered`

then SAP NCo can load, but destination parameters were not registered.

The worker now uses Delphi-style direct connection fields from `Prenos:Sap` (or DB `prijava` fallback):

```json
"Sap": {
  "DestinationName": "",
  "AppServerHost": "your-sap-host",
  "SystemNumber": "00",
  "Client": "100",
  "User": "YOUR_USER",
  "Password": "YOUR_PASSWORD",
  "Language": "EN",
  "Router": ""
}
```

When these fields are present, the worker calls `RfcDestinationManager.GetDestination(RfcConfigParameters)` directly (no global named-destination registration).

If these SAP fields are empty, the worker can load Delphi-style login from SQL table `prijava` using a configured connection string:

```json
"Sap": {
  "SapLoginConnectionString": "Provider=SQLOLEDB.1;Password=akplat;Persist Security Info=True;User ID=akplat;Initial Catalog=SAPkontrola;Data Source=172.20.1.14",
  "SapLoginIdent": null
}
```

- `SapLoginConnectionString`: direct DB connection string for table `prijava`.
- `SapLoginIdent`: optional specific `prijava.ident`; when null, uses default row `glavni = 'X'`.

Mapped columns: `uporab -> User`, `client -> Client`, `streznik -> AppServerHost`, `sysnnum -> SystemNumber`, `pass -> Password`, `jezik -> Language`.

### Important: what is ready vs not ready

- ✅ **Ready now**:
  - DLL location convention,
  - loading SAP DLLs directly from configured paths,
  - runtime assembly load check,
  - config switch between mock and DLL mode.
- ⛏️ **Still to implement**:
  - actual SAP calls and response mapping inside `SapDllSapClient` methods.

`SapDllSapClient` currently throws `NotImplementedException` after successful DLL load, by design, until exact API calls are wired.

### Where to implement real calls

- File: `MonPlatPrenos.Worker/Services/SapDllSapClient.cs`
- Implement methods:
  - `GetProductionOrdersForPlatesAsync`
  - `GetOperationsAsync`
  - `GetConfirmationsAsync`
  - `GetComponentsAsync`

Map results into records in `Models/SapModels.cs`.

## Migration plan: .NET 8 worker -> .NET Framework 4.8 (for legacy SAP NCo)

If your SAP NCo binaries only support classic .NET Framework, use this plan to migrate safely.

### 1) Confirm SAP/NCo compatibility first

Before changing code, verify exactly which SAP NCo package you have and which runtime it supports:

- if it is a .NET Framework-only build, target `net48` in this solution,
- keep all SAP binaries and native dependencies strictly x64.

### 2) Project retargeting

- Change project SDK from worker-style modern hosting assumptions to a plain console app targeting .NET Framework:
  - replace `TargetFramework` `net8.0` with `net48`,
  - keep `PlatformTarget` = `x64`, `Prefer32Bit` = `false`.
- Replace `PackageReference` set with versions that support .NET Framework 4.8 (or move to `packages.config` if needed).

### 3) Hosting/bootstrap changes in `Program.cs`

Current startup uses modern generic host APIs (`Host.CreateApplicationBuilder`) from .NET 8.
For `net48`, migrate to one of:

- `HostBuilder` + `Microsoft.Extensions.Hosting` version compatible with net48, or
- manual dependency wiring if host package compatibility is insufficient.

Keep behavior parity:

- `--run-once`,
- date range replay,
- scheduled mode.

### 4) Assembly loading changes in `SapDllSapClient`

`AssemblyLoadContext` is a .NET Core/.NET 5+ API and must be replaced on .NET Framework.
Use .NET Framework-compatible loading:

- `Assembly.LoadFrom(...)` for managed SAP assemblies,
- `AppDomain.CurrentDomain.AssemblyResolve` only if absolutely required,
- keep explicit checks and clear diagnostics for x64 mismatch and missing dependencies.

### 5) SAP native dependency verification (on target machine)

For the machine that runs the worker:

- install required SAP NW RFC SDK/native DLL set expected by your NCo build,
- install required VC++ redistributable version (x64),
- confirm PATH includes required native dependency locations,
- verify `sapnco.dll` + `sapnco_utils.dll` come from the exact same package/version.

### 6) Build/test matrix for migration acceptance

Run these checks as migration gates:

1. `Debug|x64` and `Release|x64` build pass.
2. `--run-once` reaches destination creation without loader exceptions.
3. One-day replay (`--from-date`) produces expected output files.
4. Scheduled mode starts and logs next-run behavior correctly.

### 7) Rollout strategy

- Keep a branch/tag with current `net8.0` state.
- Introduce migration in small commits:
  1) retarget project,
  2) startup/hosting refactor,
  3) SAP loader refactor,
  4) environment validation scripts/docs.
- Validate on a staging machine that matches production SAP runtime prerequisites before production rollout.

### 8) Main risks to watch

- host package version incompatibility on net48,
- hidden transitive native DLL dependency gaps,
- architecture drift (x86 vs x64),
- behavior drift in scheduling/replay mode after host/bootstrap refactor.




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


If you still see very verbose host lines (Roslyn/corehost), that output usually comes from environment variables like `COREHOST_TRACE` set in your shell, not from app logging.

## RFC field mapping input (required for no-guess mode)

Use `rfc-field-lockdown-plan.md` to provide exact SAP technical field names for every RFC output we read.
