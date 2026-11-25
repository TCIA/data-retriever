# Bounded Croissant smoke manifests

These runnable derivatives replace the full-dataset Croissant benchmark copies. The exact source documents remain in `core/app/testdata` for offline parser tests. Dataset identifiers, licenses and citations identify the original datasets; these examples are explicitly incomplete inventories.

| Manifest | Bounded payload selection | Access |
| --- | --- | --- |
| `4d-lung.open-manifest.croissant.jsonld` | First public DICOM series (about 26 MB source estimate) | Public |
| `breast-cancer-screening-dbt.noncommercial.croissant.jsonld` | First training-manifest series (about 52 MB source estimate) | Public, noncommercial license |
| `cmb-aml.radiology-pathology-external-clinical.croissant.jsonld` | First public DICOM series and first CTDC DRS object (about 2.7 MB and 12.9 MB source estimates) | Public + controlled |
| `hnscc.mixed-access.croissant.jsonld` | One General Commons DRS object with the smallest positive source series-size estimate | Controlled |
| `hnscc-mif-mihc-comparison.croissant.jsonld` | First two PathDB image URLs | Public |
| `saros.analysis-result-nifti-derived.croissant.jsonld` | One small CSV containing the first two published data records | Public |

The SAROS multi-gigabyte NIfTI ZIP is omitted: selecting archive-member rows cannot limit the bytes in the enclosing ZIP. Full-cohort nested manifests, optional source downloads, Aspera packages and extra clinical workbooks are omitted too. Their original parsing/selection behavior remains covered by internal regression fixtures. HNSCC is a controlled-route smoke example; the CMB-AML example covers mixed public/controlled access.

`records/` contains all bounded inventories and the SAROS table. Croissant FileObjects point to these files at immutable Data Retriever commit `09828f916cf868c357968aa2bbf4cf36727178ce`, so they can run from a local JSON-LD document without a local HTTP server. SHA-256, byte size and row-count metadata describe the bounded files, not the original full inventories. `record-provenance.json` records each source and selection.

From the repository root:

```bash
tdr --cli --input test-manifests/croissant/4d-lung.open-manifest.croissant.jsonld \
  --croissant-list

tdr --cli --input test-manifests/croissant/4d-lung.open-manifest.croissant.jsonld \
  --meta --accept-data-policy --output ./croissant-smoke-output/4d-lung
```

Review TCIA's usage policy before accepting it. Run one JSON-LD at a time. `--croissant-list` previews components; `--meta` expands inventories and generates metadata/references without imaging transfers. Omit `--meta` for actual payloads. CTDC/General Commons transfers require your own authorization and `--auth /path/to/credentials.json`; no credentials are included here.

One UID is still a whole DICOM series and one DRS object may be a ZIP of all slices in a series. Entry counts bound the cohort, not the byte count. Source series-size estimates are not compressed ZIP sizes and are deliberately not supplied as payload `file_size_bytes` values. The `records/*.csv` route inventories are internal children of these JSON-LD manifests; `saros-data.csv` is a direct table payload rather than a standalone TDR routing manifest.
