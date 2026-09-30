# TCIA Croissant example fixtures

These JSON-LD files are exact copies of the route-specific examples in
[`kirbyju/tcia-croissant-odrl`](https://github.com/kirbyju/tcia-croissant-odrl/tree/main/examples/recordset-manifests)
at commit `698f163`.

The tests keep the JSON-LD intact and replace external CSV responses with small,
local fixtures. This verifies the Data Retriever ingestion contract without
depending on network availability or downloading dataset payloads during CI.

The covered examples exercise:

- public DICOM series (`SeriesInstanceUID`);
- controlled DRS files (`drs_uri`);
- direct PathDB images (`imageUrl`); and
- external inventories that are intentionally unsupported as payload routes
  (for example, Aspera handoffs and archive-member metadata).
