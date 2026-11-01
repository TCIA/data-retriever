# CLI progress and credential lifetime

These changes follow the published `0.1.0rc1` candidate. RC1 remains unchanged;
install a later build containing this change to use the new behavior.

## Credentials

| Entry point | Credential behavior |
| --- | --- |
| Desktop GUI | Remembers a selected key using the existing private saved-auth file and GUI authentication flow. |
| Headless CLI, desktop executable with `--cli`, and Python wrapper | Reads the supplied key file for the current invocation. Does not create a saved-auth copy, automatically reuse a saved GUI key, or open a GUI authentication prompt. |

Supply the path on each controlled-access run:

```bash
tcia-data-retriever --input controlled.csv --output images --accept-data-policy --auth /path/to/key.json
```

```python
from tcia_data_retriever import download

report = download(
    "controlled.csv", "images", accept_data_policy=True,
    credential_file="/path/to/key.json",
)
```

Keys and access tokens remain in memory as needed for this run's authorization
and retries. No key-file contents are placed in command arguments or completion
reports. Omitting the key still allows anonymous routes supported by the service;
restricted files fail through the existing error/report mechanism. An explicitly
supplied unreadable or malformed key fails rather than silently proceeding with
another identity. Existing saved GUI keys are not deleted or migrated.

The persistent `NewGen3AuthManager` constructor remains compatible with GUI
callers. CLI execution chooses a transient constructor. The GUI's explicit
`SaveAuthFile`, saved-key lookup and authentication dialog remain in place.

## Visible progress in terminals and notebooks

Progress remains enabled. `--progress auto` chooses an updating line for an
interactive terminal and plain newline snapshots for redirected output. A
`TERM=dumb` terminal uses plain mode. Override the rendering format with
`--progress plain` or `--progress terminal`; there is no quiet-mode addition.

Python exposes the same choice as `download(..., progress="auto")`. The wrapper
detects its Python output stream, reads both child pipes concurrently, and relays
output to `sys.stdout` and `sys.stderr`. This allows Jupyter/Colab output capture
to show progress instead of depending on the kernel's underlying file descriptors.
Output is streamed in bounded chunks rather than saved as a growing memory log.

Plain output is globally paced across workers, normally one progress snapshot
per five seconds, with a final snapshot regardless of the throttle interval.
Terminal updates are paced at most five times per second. Informational messages,
warnings and the normal final summary still appear; those are distinct from the
periodic progress snapshots.

Overall counts mean completed **work items**, which may be files or series
depending on the manifest. The active-file view separately shows transferred
bytes and a percentage when the byte total is known, or marks the total pending
when it is not known. This makes progress visible during a large file without
claiming that its entire work item has completed. Active-transfer percentages
are kept below 100 until the transfer and validation finish. Decompression is
labeled separately. With several active transfers the display shows an active
count and one representative file, rather than a line for every worker.

Final failures and cancellation remain visible, retain nonzero CLI status and
Python exceptions, and do not become successful 100-percent downloads. JSON
completion-report schema and transfer routing are unchanged. GUI structured
progress events keep their original cadence; this console view is independent.

Tests use synthetic keys, temporary storage and local HTTP/TLS fixtures. They
do not establish real controlled-data eligibility or service performance.
