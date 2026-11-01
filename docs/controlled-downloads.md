# Controlled HTTP downloads

TDR uses Go's HTTP client directly for the TCIA CRDC/Gen3 integration. It does
not install or invoke the Gen3 Python package or `gen3-client`, and does not
implement a general DRS resolver.

Only `drs://nci-crdc.datacommons.io/<object-id>` identifiers are recognized.
Unrecognized hosts, explicit ports, user information, query strings, fragments,
and invalid object paths are rejected before credentials are sent. Legacy
spreadsheet GUIDs are still expanded to this recognized host. Anonymous DRS
requests remain supported when no key file is configured.

An approved JSON key file with a nonempty `api_key` is exchanged for an access
token at `/user/credentials/api/access_token`. The token is sent only to the
recognized service's `/user/data/download/<object-id>` endpoint. Neither service
endpoint follows redirects. A service 401 triggers at most one token refresh
and retry; a 403 is returned without refreshing. JWT expiry is a cache hint
only, not local authorization or signature verification. Opaque tokens use the
401 refresh path. Service requests have a 30-second context deadline and honor
cancellation.

The returned storage URL must use HTTPS without user information. Storage
redirects must also use HTTPS; bearer credentials are never attached to storage
requests. TLS certificate and hostname verification are enabled in the shared
HTTP client. Sites using a private certificate authority must configure a
trusted CA through their system or supported Go certificate environment, rather
than disabling verification.

Temporary storage URLs stay on a transfer-local copy and are not saved into
DRS metadata or completion records. Request headers, credentials, and service
response bodies are not logged. Transport errors exposed by these requests omit
URLs while preserving internal retry and cancellation classification.

## Direct-file integrity

CSV, TSV, and XLSX direct/DRS manifests may supply `FileSize` (also `File Size`,
`file_size_bytes`, `size_bytes`, `CTDC Size`, or `size`) as an integer byte count.
They may supply `md5`/`md5sum`/`md5_checksum` and
`sha256`/`sha256sum`/`sha256_checksum` as hexadecimal hashes. Header comparison
ignores case, spaces, and punctuation. Empty values mean unavailable; malformed
supplied values fail the transfer. Both hashes are checked if both are supplied.

TDR checks the available size and checksums before renaming a temporary payload
into its final location. A failed validation removes the temporary file and
preserves any previous final file. Existing direct files are checked against
these fields before skipping; without size or checksums the existing behavior
remains an existence check. HTTP Content-Length is also checked where meaningful.
These direct-file sizes refer to transferred payload bytes. NBIA archive sizes
retain their separate uncompressed-series meaning and verification path.

Transfers still restart after interruption. HTTP range resume, other DRS hosts,
and other controlled-access providers require separate implementation and tests.
Tests use local HTTP/TLS servers and synthetic keys; no live controlled-data
authorization or download is implied by the test suite.
