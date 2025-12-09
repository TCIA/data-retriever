# Universal Data Retriever

> A robust replacement for NBIA Data Retriever with enhanced features and reliability

[![Go Version](https://img.shields.io/badge/Go-1.24.4-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [How It Works](#how-it-works)
- [Command Reference](#command-reference)
- [Usage Guide](#usage-guide)
- [Directory Structure](#directory-structure)
- [Performance & Optimization](#performance--optimization)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Developer Guide](#developer-guide)

## Features

### Core Features
- **Thread-safe parallel downloads** with configurable workers
- **Automatic retry** with exponential backoff for reliability
- **Server-friendly mode** to avoid rate limiting
- **Progress tracking** with real-time ETA calculation
- **Metadata caching** for faster subsequent runs
- **MD5 validation** enabled by default for data integrity
- **Flexible storage** - extracted DICOM files or compressed ZIP archives
- **Smart organization** - files organized by Patient ID / Study UID / Series UID
- **Connection pooling** for efficient HTTP/1.1 connections
- **OAuth token management** with automatic refresh

## Quick Start

### 1. Install the tool
```bash
# Download latest release (recommended)
wget https://github.com/GrigoryEvko/NBIA_data_retriever_CLI/releases/latest/download/nbia-data-retriever-cli-linux-amd64
chmod +x nbia-data-retriever-cli-linux-amd64
mv nbia-data-retriever-cli-linux-amd64 nbia-data-retriever-cli

# Or build from source
git clone https://github.com/GrigoryEvko/NBIA_data_retriever_CLI.git
cd NBIA_data_retriever_CLI
go build -o nbia-data-retriever-cli .
```

### 2. Download your first dataset
```bash
./nbia-data-retriever-cli -i manifest.tcia
```

### What to Expect

You'll see progress output like this:

```
Found 18321 series to fetch metadata for
[18321/18321] 100.0% | Fetched: 0 | Cached: 18321 | Failed: 0 | Current: 1.3.6.1.4.1.14519.5.2.1.7310.5...

Downloading 18321 series with 20 workers...

[18321/18321] 100.0% | Downloaded: 10181 | Skipped: 8140 | Failed: 0 | Current: 1.3.6.1.4.1.14519.5.2.1.7311.5...
```

After completion:
```
=== Download Summary ===
Total files: 18321
Downloaded: 10181
Skipped: 8140
Failed: 0
Total time: 8m34s
Average rate: 35.7 files/second
```

## Installation

### Requirements
- Go 1.24.4 or later (for building from source)
- Network connection to TCIA servers
- Valid TCIA manifest file (.tcia)
- Sufficient disk space

### Option 1: Download Pre-built Binary
```bash
# Linux AMD64
wget https://github.com/GrigoryEvko/NBIA_data_retriever_CLI/releases/latest/download/nbia-data-retriever-cli-linux-amd64

# macOS AMD64
wget https://github.com/GrigoryEvko/NBIA_data_retriever_CLI/releases/latest/download/nbia-data-retriever-cli-darwin-amd64

# Windows AMD64
wget https://github.com/GrigoryEvko/NBIA_data_retriever_CLI/releases/latest/download/nbia-data-retriever-cli-windows-amd64.exe

chmod +x nbia-data-retriever-cli-*
```

### Option 2: Build from Source
```bash
git clone https://github.com/GrigoryEvko/NBIA_data_retriever_CLI.git
cd NBIA_data_retriever_CLI
go mod tidy
go build -o nbia-data-retriever-cli .

# Or use the Python build script for cross-compilation
python build.py --platform linux --arch amd64
```

### Option 3: Docker
```bash
# Build image
git clone https://github.com/GrigoryEvko/NBIA_data_retriever_CLI.git
cd NBIA_data_retriever_CLI
docker build -t nbia .

# Run with docker
docker run --rm -v $PWD:/data -w /data nbia -i manifest.tcia -o /data/output
```

### Verify Installation
```bash
./nbia-data-retriever-cli --version
```

## How It Works

### Download Workflow

```
1. Read TCIA Manifest
   ↓
2. Fetch Metadata (Parallel)
   ├─→ Check local cache
   └─→ Query NBIA API for missing
   ↓
3. Download Series (Parallel Workers)
   ├─→ Check if already exists
   ├─→ Download with retry logic
   ├─→ Validate MD5 (if enabled)
   └─→ Extract to final location
   ↓
4. Organize Files
   └─→ PatientID/StudyUID/SeriesUID/
```

### Token Management

The tool automatically handles OAuth authentication:
- Stores tokens in `{output_dir}/{username}.json`
- Auto-refreshes before expiration
- Secure permissions (0600)

### Metadata Caching

To speed up subsequent runs, metadata is cached locally:
- Stored in `{output_dir}/metadata/`
- One JSON file per series
- Automatically used unless `--refresh-metadata` is specified

## Command Reference

### Synopsis
```bash
nbia-data-retriever-cli [OPTIONS]
```

### Complete Options Table

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--input` | `-i` | *required* | Path to TCIA manifest file |
| `--output` | `-o` | `./` | Output directory for downloaded files |
| `--processes` | `-p` | `2` | Number of parallel download workers |
| `--user` | `-u` | `nbia_guest` | Username for authentication |
| `--passwd` | | | Password (use --prompt for security) |
| `--prompt` | `-w` | | Prompt for password interactively |
| `--max-connections` | | `8` | Maximum connections per host |
| `--max-retries` | | `3` | Maximum retry attempts per file |
| `--server-friendly` | | | Use conservative settings |
| `--force` | `-f` | | Force re-download existing files |
| `--skip-existing` | | | Skip files that already exist |
| `--proxy` | `-x` | | Proxy URL (http/socks5) |
| `--meta` | `-m` | | Download metadata only |
| `--save-log` | | | Save debug log to progress.log |
| `--no-md5` | | | Disable MD5 validation |
| `--no-decompress` | | | Keep files as ZIP archives |
| `--refresh-metadata` | | | Force refresh all metadata |
| `--metadata-workers` | | `20` | Parallel metadata fetch workers |
| `--token-url` | | *NBIA default* | Custom OAuth endpoint |
| `--meta-url` | | *NBIA default* | Custom metadata endpoint |
| `--image-url` | | *NBIA default* | Custom image endpoint |
| `--debug` | | | Show debug information |
| `--version` | `-v` | | Show version information |
| `--help` | `-h` | | Show help message |

## Usage Guide

### Basic Usage

#### Public Dataset (No Authentication)
```bash
./nbia-data-retriever-cli -i manifest.tcia
```

#### Authenticated Download
```bash
# With password in command (less secure)
./nbia-data-retriever-cli -i manifest.tcia -u myusername --passwd mypassword

# With password prompt (recommended)
./nbia-data-retriever-cli -i manifest.tcia -u myusername --prompt
```

#### Specify Output Directory
```bash
./nbia-data-retriever-cli -i manifest.tcia -o /data/dicom/prostate
```

### Common Scenarios

#### Resume Interrupted Download
```bash
# The tool automatically skips completed files
./nbia-data-retriever-cli -i manifest.tcia --skip-existing
```

#### Large Dataset Download
```bash
# Start conservatively
./nbia-data-retriever-cli -i large_dataset.tcia \
  --server-friendly \
  --skip-existing \
  --save-log

# If stable, increase parallelism
./nbia-data-retriever-cli -i large_dataset.tcia \
  -p 10 \
  --max-connections 20 \
  --metadata-workers 30 \
  --skip-existing
```

#### Unreliable Network
```bash
./nbia-data-retriever-cli -i manifest.tcia \
  -p 2 \
  --max-retries 5 \
  --skip-existing \
  --save-log
```

#### Metadata Only
```bash
# Useful for dataset exploration
./nbia-data-retriever-cli -i manifest.tcia --meta -o metadata.json
```

#### Keep ZIP Archives
```bash
# For archival or transfer (requires --no-md5)
./nbia-data-retriever-cli -i manifest.tcia --no-md5 --no-decompress
```

### Advanced Usage

#### Custom API Endpoints
```bash
# For private NBIA instances
./nbia-data-retriever-cli -i manifest.tcia \
  --token-url https://private-nbia.org/oauth/token \
  --meta-url https://private-nbia.org/api/v2/getSeriesMetaData \
  --image-url https://private-nbia.org/api/v2/getImageWithMD5Hash
```

#### Proxy Configuration
```bash
# HTTP proxy
./nbia-data-retriever-cli -i manifest.tcia --proxy http://proxy.company.com:8080

# SOCKS5 proxy with auth
./nbia-data-retriever-cli -i manifest.tcia --proxy socks5://user:pass@proxy.company.com:1080
```

#### Automation Script
```bash
#!/bin/bash
# automated_download.sh

MANIFEST="/path/to/manifest.tcia"
OUTPUT="/data/dicom/$(date +%Y%m%d)"
LOG="$OUTPUT/download.log"

# Create output directory
mkdir -p "$OUTPUT"

# Download with notifications
if ./nbia-data-retriever-cli -i "$MANIFEST" -o "$OUTPUT" \
    --skip-existing --save-log > "$LOG" 2>&1; then
    echo "Success: Downloaded to $OUTPUT" | mail -s "NBIA Download Complete" admin@example.com
    # Optional: trigger downstream processing
    /path/to/process_dicom.sh "$OUTPUT"
else
    echo "Failed: Check $LOG" | mail -s "NBIA Download Failed" admin@example.com
    exit 1
fi
```

## Directory Structure

### Output Organization

After download, files are organized hierarchically:

```
output_directory/
├── metadata/                          # Cached metadata
│   ├── 1.3.6.1.4.1.14519.5.2.1.7311.5101.158323547117540061132729905711.json
│   ├── 1.3.6.1.4.1.14519.5.2.1.7311.5101.160028252338004527274326500702.json
│   └── ...
├── username.json                      # OAuth token (auto-managed)
├── progress.log                       # Debug log (if --save-log used)
│
└── PatientID/                         # Patient level
    └── StudyInstanceUID/              # Study level
        └── SeriesInstanceUID/         # Series level
            ├── 1-001.dcm
            ├── 1-002.dcm
            └── ...
```

### Example Structure
```
/data/prostate_study/
├── metadata/
│   └── *.json files
├── nbia_guest.json
├── ProstateX-0001/
│   ├── 1.2.840.113619.2.55.3.604688119.838.1521652564.123/
│   │   ├── 1.3.6.1.4.1.14519.5.2.1.7311.5101.158323547117540061132729905711/
│   │   │   ├── 1-001.dcm
│   │   │   ├── 1-002.dcm
│   │   │   └── ... (28 files)
│   │   └── 1.3.6.1.4.1.14519.5.2.1.7311.5101.160028252338004527274326500702/
│   │       └── ... (36 files)
│   └── 1.2.840.113619.2.55.3.604688119.838.1521652564.456/
│       └── ...
└── ProstateX-0002/
    └── ...
```

## Performance & Optimization


### Performance Tuning

#### For Fast Networks
```bash
# Maximize throughput
./nbia-data-retriever-cli -i manifest.tcia \
  -p 20 \
  --max-connections 25
```

#### For Rate-Limited Servers
```bash
# Avoid 429 errors
./nbia-data-retriever-cli -i manifest.tcia \
  -p 1 \
  --server-friendly
```

#### Balanced Performance
```bash
# Good default for most cases
./nbia-data-retriever-cli -i manifest.tcia \
  -p 5 \
  --max-connections 10 \
  --max-retries 3
```

### Server-Friendly Mode

When enabled with `--server-friendly`, the tool uses:
- Single worker (no parallel downloads)
- 2 max connections (reduced from 8)
- 30s retry delay (increased from 10s)
- 2s delay between requests
- 5 metadata workers (reduced from 20)

Use this mode if you encounter:
- HTTP 429 (Too Many Requests) errors
- Truncated downloads
- Connection resets

## Advanced Features

### MD5 Validation

MD5 validation is **enabled by default** for data integrity:
- Uses NBIA's MD5 API endpoint
- Validates each file during extraction
- Ensures complete, uncorrupted downloads

To disable (faster but less secure):
```bash
./nbia-data-retriever-cli -i manifest.tcia --no-md5
```

### Storage Modes

#### Extracted Mode (Default)
- Downloads as ZIP, extracts DICOM files
- Validates MD5 checksums
- Removes ZIP after successful extraction
- Organized in series directories

#### Compressed Mode
- Keeps original ZIP files
- Saves disk space (~40-60% smaller)
- Faster (no extraction time)
- Requires `--no-md5` flag

```bash
# Keep as ZIP archives
./nbia-data-retriever-cli -i manifest.tcia --no-md5 --no-decompress
```

### Metadata Caching

The tool automatically caches metadata to speed up subsequent runs:

```bash
# First run: fetches all metadata
./nbia-data-retriever-cli -i manifest.tcia
# [346/346] 100.0% | Fetched: 346 | Cached: 0 | Failed: 0

# Second run: uses cache
./nbia-data-retriever-cli -i updated_manifest.tcia
# [380/380] 100.0% | Fetched: 34 | Cached: 346 | Failed: 0
```

To force refresh all metadata:
```bash
./nbia-data-retriever-cli -i manifest.tcia --refresh-metadata
```

### Custom Endpoints

For private NBIA instances or testing:
```bash
./nbia-data-retriever-cli -i manifest.tcia \
  --token-url https://private-nbia.org/oauth/token \
  --meta-url https://private-nbia.org/api/v2/getSeriesMetaData \
  --image-url https://private-nbia.org/api/v2/getImageWithMD5Hash
```

## Troubleshooting

### Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| **"Token request failed"** | Invalid credentials | Check username/password |
| **"429 Too Many Requests"** | Rate limiting | Use `--server-friendly` |
| **"EOF" or "connection reset"** | Network interruption | Retry with `--skip-existing` |
| **"MD5 validation failed"** | Corrupted download | Delete series folder and retry |
| **"Permission denied"** | Output directory permissions | Check write permissions |
| **"No such file"** | Invalid manifest path | Check file path |
| **Incomplete downloads** | Server timeout | Use `--server-friendly` mode |

### Debug Mode

For detailed troubleshooting:
```bash
# Enable debug output
./nbia-data-retriever-cli -i manifest.tcia --debug

# Save debug log
./nbia-data-retriever-cli -i manifest.tcia --debug --save-log

# View log
tail -f progress.log
```

### Network Issues

#### Behind Corporate Proxy
```bash
# Set environment variables
export HTTP_PROXY=http://proxy.company.com:8080
export HTTPS_PROXY=http://proxy.company.com:8080

# Or use --proxy flag
./nbia-data-retriever-cli -i manifest.tcia --proxy http://proxy.company.com:8080
```

#### Slow/Unstable Connection
```bash
# Increase timeouts and retries
./nbia-data-retriever-cli -i manifest.tcia \
  -p 1 \
  --max-retries 5 \
  --skip-existing
```

### Verification

#### Check Download Completeness
```bash
# Re-run with skip-existing to verify
./nbia-data-retriever-cli -i manifest.tcia --skip-existing --debug

# Check for "already exists with correct size/checksum" messages
```

#### Manual MD5 Verification
```bash
# For a specific series
find /path/to/PatientID/StudyUID/SeriesUID -name "*.dcm" -exec md5sum {} \;
```

## Testing

The project includes comprehensive test suites:

```bash
cd tests

# Run all tests
./run_all_tests.sh

# Individual test suites
./test_smoke.sh              # Basic functionality
./test_parallel.sh           # Parallel download tests
./test_integration.sh        # Edge cases and error handling
./test_md5_nodecompress.sh   # MD5 validation and storage options
./test_metadata_caching.sh   # Metadata cache functionality
./test_performance.sh        # Performance benchmarks
./test_advanced_features.sh  # Advanced features

# Test with specific manifest
./test_smoke.sh /path/to/test.tcia
```

## Developer Guide

### Architecture Overview

```
main.go           - Entry point, worker orchestration
download.go       - Core download logic, MD5 validation
options.go        - CLI argument parsing
token.go          - OAuth token management
client.go         - HTTP client configuration
http_utils.go     - Request handling with v2→v1 fallback
logger.go         - Logging configuration
progress.go       - Progress bar utilities
utils.go          - Helper functions
```

### Building

```bash
# Standard build
go build -o nbia-data-retriever-cli .

# Cross-compilation
GOOS=windows GOARCH=amd64 go build -o nbia-data-retriever-cli.exe .
GOOS=darwin GOARCH=amd64 go build -o nbia-data-retriever-cli-mac .

# With version info
go build -ldflags "-X main.version=v1.2.3" -o nbia-data-retriever-cli .
```

### Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Run tests (`cd tests && ./run_all_tests.sh`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

### Code Style

- Follow standard Go conventions
- Use `gofmt` for formatting
- Add tests for new features
- Update documentation

## Known Limitations

- TCIA servers do not support HTTP Range requests (no partial downloads)
- Some servers may truncate large files (use `--server-friendly` mode)
- Rate limiting varies by server configuration
- No support for selective series download (must use manifest as-is)

## License

This project maintains the same license as the original NBIA Data Retriever.

## Acknowledgments

- Original NBIA team for the data retriever concept
- Go community for excellent libraries
- Contributors and testers

---

For issues, feature requests, or contributions, please visit: https://github.com/GrigoryEvko/NBIA_data_retriever_CLI
