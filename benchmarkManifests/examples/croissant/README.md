# Croissant JSON-LD examples

These five files are exact copies of the curated examples in
[`kirbyju/tcia-croissant-odrl`](https://github.com/kirbyju/tcia-croissant-odrl/tree/698f1638816489194fec9373df7bec44eb29d81a/examples),
pinned to source commit `698f1638816489194fec9373df7bec44eb29d81a`.
They exercise the Croissant input support currently implemented by TCIA Data
Retriever: each JSON-LD document embeds TCIA access rows and points to a nested
Data Retriever manifest or a directly downloadable data file.

| Example | Coverage |
| --- | --- |
| `4d-lung.open-manifest.croissant.jsonld` | Open DICOM manifest routed through Data Retriever, with IDC identified as the downstream system. |
| `breast-cancer-screening-dbt.noncommercial.croissant.jsonld` | Multiple DICOM manifests and direct files under a noncommercial data license. |
| `cmb-aml.radiology-pathology-external-clinical.croissant.jsonld` | Open IDC DICOM, controlled CTDC DRS, and an unsupported Aspera transfer-package row that should be skipped. |
| `hnscc.mixed-access.croissant.jsonld` | Controlled General Commons manifests plus directly downloadable clinical files. |
| `saros.analysis-result-nifti-derived.croissant.jsonld` | Analysis Result with direct NIfTI/ZIP and CSV artifacts and source-dataset provenance. |

Run one file at a time and use a fresh output directory. These are realistic
dataset examples, not small smoke-test payloads; a Croissant document may
expand a nested manifest into a large download.

```bash
scripts/bench_download.sh \
  benchmarkManifests/examples/croissant/saros.analysis-result-nifti-derived.croissant.jsonld \
  --output-dir /path/to/local-scratch/bench-results
```

The controlled CMB-AML and HNSCC routes require the tester's own authorization
and credential file. No credentials are included here. The retriever currently
skips transfer-package rows such as Aspera rather than treating their URLs as
ordinary data files.

## External-CSV RecordSet pilot

The source repository also contains a newer
[`recordset-manifests`](https://github.com/kirbyju/tcia-croissant-odrl/tree/698f1638816489194fec9373df7bec44eb29d81a/examples/recordset-manifests)
pilot. Those files are intentionally not copied here yet. Their Croissant
`RecordSet` fields refer to external inventory CSV `FileObject`s; the current
Data Retriever parser does not expand those CSV rows and would instead treat
the inventory CSV itself as a downloadable payload. Add them after that
recordset-selection and row-dispatch contract is implemented.

## Provenance

The source repository reports successful validation of all five examples with
`mlcroissant` 1.1.0. The copies in this directory retain the source bytes
unchanged. Their SHA-256 values are:

```text
7353043079b18eaba87af947e19129d74a9a3411a820172a490f1333434f6e11  4d-lung.open-manifest.croissant.jsonld
53282b6f59d7ffb9d6ca390897dca5c18b1561460d4c958c99b92daafb71de3f  breast-cancer-screening-dbt.noncommercial.croissant.jsonld
4e917f38a3bfb2e0df76d0b4e01358487252d09c97932712ddc33f9e9ea1141c  cmb-aml.radiology-pathology-external-clinical.croissant.jsonld
cf94ee1d9e04a5e6cff74877e065741cf29d6a38251c10e2c7d9abbcc4b6ac45  hnscc.mixed-access.croissant.jsonld
55945631397a69815d3db84bec65601a9023160f2d5e2d544d523f33f0a33c7d  saros.analysis-result-nifti-derived.croissant.jsonld
```
