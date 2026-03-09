# PDP Runner (No Python Required)

Use the OS-specific standalone binary:

- macOS/Linux: `pdp-local-runner`
- Windows: `pdp-local-runner.exe`

## Required folder structure (same folder as binary)

- `pdp-input` (crawl output file)
- `pdp-crawl-input` (crawl input file)
- `pdp-masters` (master file)
- `pdp-run-output` (result goes here)

The runner picks the newest file from each input folder and writes an output `.xlsx` in `pdp-run-output`.

## Run

macOS/Linux:

```bash
./pdp-local-runner
```

Windows (PowerShell):

```powershell
.\pdp-local-runner.exe
```

## Build binaries (for distributors)

Local build on current OS:

```bash
./build_pdp_executable.sh
```

Cross-platform builds (Windows/macOS/Linux):

- Run GitHub Action: `Build PDP Binaries` (`.github/workflows/build-pdp-binaries.yml`)
- Download artifacts for each OS and share the matching one.
