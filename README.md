# TCIA Data Retriever

> A cross-platform desktop GUI (and CLI) for downloading medical imaging data from the Cancer Imaging Archive

Please note that this is a beta release version.  Full feature implementation is not complete.  If you encounter any bugs please create an issue on the project.

[![Go Version](https://img.shields.io/badge/Go-1.24.4-blue.svg)](https://golang.org)

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [GUI Usage](#gui-usage)
- [CLI Usage](#cli-usage)
- [Advanced Options](#advanced-options)
- [Directory Structure](#directory-structure)
- [Architecture](#architecture)
- [Building from Source](#building-from-source)
- [Troubleshooting](#troubleshooting)

---

## Overview

TCIA Data Retriever is a desktop application for downloading datasets from the [Cancer Imaging Archive (TCIA)](https://www.cancerimagingarchive.net/). It accepts TCIA manifest files (`.tcia`), s5cmd manifests (`.s5cmd`), and spreadsheets (`.csv`, `.tsv`, `.xlsx`) and downloads the associated data with parallel workers, retry logic, and real-time progress tracking.

---

## Features

- **Desktop GUI** built with [Wails](https://wails.io/) (Go backend + Angular frontend) — native look on Windows, macOS, and Linux
- **Dual-mode** — launch with `--cli` to use the same binary as a command-line tool
- **Multiple manifest formats** — `.tcia`, `.s5cmd`, `.csv`, `.tsv`, `.xlsx`
- **Parallel downloads** with configurable worker count and per-host connection limits
- **Real-time progress** — per-series status (queued → downloading → complete), bytes transferred 
- **Pause & resume** — pause an in-progress run and resume from where it left off using skip-existing logic
- **Multiple concurrent runs** — queue several manifests simultaneously, each shown in its own card
- **CRDC / Gen3 authentication** — load a JSON credentials file for restricted datasets, with an in-app prompt if credentials are needed mid-run
- **Directory modes** — `classic` or `descriptive` naming
- **Automatic retry** with configurable backoff
- **Dark/light mode** with system preference detection

---

## Quick Start

### GUI

1. Download and launch the app for your platform (see [Installation](#installation)).
2. Click **+** to open the Manifest & Destination dialog.
3. Select your manifest file and a download directory.
4. Click **Fetch Files** — progress card will appear immediately.

### CLI

```bash
# Basic download
./TCIA_Data_Retriever --cli -i manifest.tcia -o ./output

# With authentication
./TCIA_Data_Retriever --cli -i manifest.tcia -o ./output --auth /path/to/credentials.json

# Resume an interrupted download
./TCIA_Data_Retriever --cli -i manifest.tcia -o ./output --skip-existing
```

---

## Installation

### Requirements

- **GUI**: no runtime dependencies — the frontend is embedded in the binary
- **CLI**: no runtime dependencies
- **Building from source**: Go 1.24.4+, Node.js 18+, [Wails v2 CLI](https://wails.io/docs/gettingstarted/installation)

### Option 1: Download Pre-built Binary

```bash

# Download the .zip for your platform from the Releases page
Apple App Store and Microsoft Store versions coming soon.

```

### Option 2: Build from Source

See [Building from Source](#building-from-source).

---

## GUI Usage

### Adding a Download

Click the **+** button in the header to open the Manifest & Destination dialog:

- **Manifest file** — path to your `.tcia`, `.s5cmd`, `.csv`, `.tsv`, or `.xlsx` file. You can also user 'Open With' to select the TCIA Data Retriever and open a manifest directly. 
- **Download directory** — where the downloaded files will be saved. The app pre-fills this with your system Downloads folder. Note - Mac version requires the Download Directory to be specified with the Browse Files icon.

Click **Fetch Files** to start. The dialog closes and a new download card appears.

### Download Cards

Each manifest gets its own card showing:
- Overall progress bar and series count
- Per-series rows with status icons (queued, downloading, decompressing, complete, failed, skipped)
- Bytes downloaded and elapsed time
- Pause / Resume and Cancel buttons
- An **Open Folder** button when the run finishes

Multiple manifests can run simultaneously.

### Advanced Settings

Click the **⚙** button to open Advanced Settings:

| Setting | Default | Description |
|---|---|---|
| Max Connections | 8 | Concurrent HTTP connections per host |
| Max Retries | 3 | Retry attempts per series |
| Simultaneous Downloads | 8 | Parallel download workers |
| Skip Existing | on | Skip series that are already fully downloaded |
| Download in Parallel | on | Enable multi-worker downloads |
| CRDC Auth Path | — | JSON credentials file for restricted datasets |
| Directory Mode | Classic | `classic` or `descriptive` output folder naming |

### Authentication

For restricted TCIA datasets that require CRDC / Gen3 credentials:

1. Open **Advanced Settings → Authentication File** and select your downloaded JSON key file before starting a run, **or**
2. If auth is required mid-run, the app will show an **Authentication Required** dialog automatically — select your credentials file and click **Confirm** to resume. Click **Cancel** to continue without providing authentication.

---

## CLI Usage

Launch with `--cli` (or `-cli`) to skip the GUI entirely:

```bash
./TCIA_Data_Retriever --cli [OPTIONS]
```

### Options

| Option | Short | Default | Description |
|---|---|---|---|
| `--input` | `-i` | *required* | Path to manifest file |
| `--output` | `-o` | `./` | Output directory |
| `--processes` | `-p` | `2` | Parallel download workers |
| `--max-connections` | | `8` | Max connections per host |
| `--max-retries` | | `3` | Retry attempts per series |
| `--skip-existing` | | off | Skip already-downloaded series |
| `--force` | `-f` | off | Re-download even if files exist |
| `--auth` | | — | Path to Gen3/CRDC JSON credentials |
| `--directory-mode` | | — | `classic` or `descriptive` |
| `--meta` | `-m` | off | Fetch metadata only (no images) |
| `--no-md5` | | off | Disable MD5 validation |
| `--no-decompress` | | off | Keep downloads as ZIP archives |
| `--refresh-metadata` | | off | Force re-fetch all metadata |
| `--metadata-workers` | | `20` | Parallel metadata fetch workers |
| `--server-friendly` | | off | Conservative settings to avoid rate-limiting |
| `--proxy` | `-x` | — | HTTP or SOCKS5 proxy URL |
| `--prompt` | `-w` | off | Prompt for password interactively |
| `--save-log` | | off | Write progress log to file |
| `--accept-data-policy` | | off | Accept the TCIA data usage policy without interactive prompt |
| `--debug` | | off | Verbose debug output |
| `--version` | `-v` | | Print version info |
| `--help` | `-h` | | Show help |

### Examples

```bash
# Public dataset
./TCIA_Data_Retriever --cli -i manifest.tcia -o /data/dicom

# Restricted dataset with credentials
./TCIA_Data_Retriever --cli -i manifest.tcia -o /data/dicom --auth ~/credentials.json

# Large dataset — maximize throughput
./TCIA_Data_Retriever --cli -i manifest.tcia -o /data/dicom -p 20 --max-connections 25

# Slow or unreliable connection
./TCIA_Data_Retriever --cli -i manifest.tcia -o /data/dicom --server-friendly --skip-existing --save-log

# Keep ZIP archives instead of extracting
./TCIA_Data_Retriever --cli -i manifest.tcia -o /data/zips --no-md5 --no-decompress
```

---

## Advanced Options

### Server-Friendly Mode

Pass `--server-friendly` (CLI) or lower the worker/connection counts in Advanced Settings (GUI) if you see HTTP 429 errors or connection resets. Server-friendly mode sets:

- 1 worker
- 2 max connections
- 30s retry delay
- 2s inter-request delay
- 5 metadata workers


### Directory Modes

- **classic** —  The Classic Directory Name organizes the files in a child folder under the destination folder as follows: Collection Name > Patient ID > Study Instance UID > Series Instance UID
- **descriptive** —  The Descriptive Directory Name organizes the files in a child folder under the destination folder as follows: Collection Name > Patient ID > Study Date + Study ID + Study Description (54 char max) + last 5 digits of Study Instance UID > Series Number + Series Description (54 char max) + last 5 digits of Series Instance UID

---

## Building from Source

### Prerequisites

```bash
# Install Wails CLI
go install github.com/wailsapp/wails/v2/cmd/wails@latest

# Install frontend dependencies
cd frontend && npm install && cd ..
```

### GUI Binary

```bash
wails build
# Output: build/bin/TCIA_Data_Retriever (or .exe / .app)
```

### Cross-compilation

```bash
# Windows from Linux/macOS
GOOS=windows GOARCH=amd64 wails build -platform windows/amd64

# macOS universal binary
wails build -platform darwin/universal
```


---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Auth modal keeps appearing | Credentials expired or rejected | Download a fresh key file from CRDC and confirm again |
| HTTP 429 errors | Server rate limiting | Use server-friendly mode / reduce workers |
| Series stuck at 0% | Network timeout | Increase max retries, check proxy settings |
| MD5 validation failed | Corrupted download | Delete the series folder and re-run |
| App won't open `.tcia` files | File association not set | Right-click the file → Open With → select the app |
| `parquet init failed` on startup | First launch, parquet index download failed | Check network; the app still works, IDC downloads may not resolve |

### Debug Logging (CLI)

```bash
./TCIA_Data_Retriever --cli -i manifest.tcia --debug --save-log
tail -f progress.log
```

---

## License

This project is licensed under the [Apache License 2.0](LICENSE).

You may use, reproduce, modify, and distribute this software under the terms of the Apache 2.0 License. It is provided "as is" without warranties or conditions of any kind. See the [LICENSE](LICENSE) file for the full license text, or visit [apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Testing

The CLI now includes automated tests focused on command-line option behavior.

### What is tested

- **Option parsing and normalization** in `core/app/options_test.go`
	- Validates defaults, aliases, and all supported CLI flags.
	- Verifies option interactions (for example, `--no-decompress` incompatibility with default MD5 validation).
	- Confirms special mode overrides (for example, `--server-friendly` values).
	- Covers prompt-based password input without interactive terminal dependency.

- **CLI process behavior** in `main_cli_test.go`
	- Verifies expected exit behavior for `--help`, `--version`, invalid flags, and invalid option combinations.
	- Uses a helper-process pattern so tests execute the real `main()` flow safely.

### Test design notes

- Tests are deterministic and do not require external network access.
- Option parsing uses a test seam (`ParseOptions(args, promptReader)`) so tests can validate behavior without mutating global process arguments.
- Existing CLI runtime behavior is preserved; `InitOptions()` still handles user-facing exit paths.

### Run tests locally

- Run option parser tests:

	```bash
	go test ./core/app -v
	```

- Run all Go tests (including CLI subprocess tests):

	```bash
	go test ./...
	```

## Acknowledgments

- [Grigory Evko](https://github.com/GrigoryEvko) for the original Go CLI NBIA retriever implementation 
- [Wails](https://wails.io/) for the Go + web desktop framework
- [Angular](https://angular.io/) for the frontend framework
- Portions of this codebase were developed with the assistance of AI tools (Claude by Anthropic, ChatGPT by OpenAI, etc).
