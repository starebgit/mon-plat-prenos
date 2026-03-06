# MonPlatPrenos .NET Worker

This is a new .NET worker app that mirrors the current Delphi behavior and is ready for extension with extra term rules.

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

## Run

```bash
cd dotnet-app/MonPlatPrenos.Worker
# one-time run for testing:
dotnet run -- --run-once

# scheduler mode (runs every day at DailyRunTime):
dotnet run
```

## Configure new terms

Edit `appsettings.json`:

```json
"ExtraTerms": [
  { "Name": "MyNewTerm", "Contains": "YOUR_TERM_HERE" },
  { "Name": "Another", "Contains": "ABC", "ExcludeContains": "XYZ", "MaxLength": 20 }
]
```

## Connect to real SAP

Right now `MockSapClient` provides local test data.

To connect real SAP:
1. Add SAP .NET Connector dependency in your environment.
2. Create `SapNcoClient : ISapClient`.
3. Replace DI registration in `Program.cs` from `MockSapClient` to `SapNcoClient`.
4. Map BAPI responses to the model records in `Models/SapModels.cs`.

