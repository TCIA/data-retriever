# TCIA Data Retriever

> A robust replacement for NBIA Data Retriever with enhanced features and reliability

This project is a work in progress.  Full features and instructions to be listed at a later date.  Any releases listed on the project are only for demonstration/testing purposes.

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
