# TCIA Data Retriever benchmark manifests

These manifests exercise each current Data Retriever route without requiring a
single very large disk. Run one manifest at a time and use a fresh output
directory for every trial.

| Manifest | Route and payload shape | Approximate transfer |
| --- | --- | ---: |
| `idc-rider-lung-pet-ct-pt.csv` | IDC/S3: 509 PT series, 115,076 DICOM instances | 4.73 GiB |
| `nbia-a091105-annotations.csv` | NBIA v4: 2,445 one-object annotation series | 264.91 MB |
| `general-commons-lgg-1p19qdeletion.csv` | General Commons DRS: 637 ZIP objects, including source imaging and SEG | 2.61 GiB |
| `ctdc-cmb-pca.csv` | CTDC DRS: 73 CT/MR/PT ZIP objects | 2.96 GiB |
| `pathdb-many-small-{prod,tst}.csv` | PathDB: the same 8,192 small TIFF files on production and staging | about 0.5 GB |
| `pathdb-large-svs-{prod,tst}.csv` | PathDB: the same three large SVS files on production and staging | 4.9997 GiB |

The General Commons and CTDC manifests are controlled-access metadata. A
manifest does not grant access. Testers need authorization and their own TCIA
Data Retriever JSON API key as described in the
[TCIA NIH Controlled Data Access Policy](https://www.cancerimagingarchive.net/nih-controlled-data-access-policy/).
Do not commit credentials or pass key values on the command line; pass only the
credential file path with `--auth`.

## Why these cohorts

- IDC stresses high object counts and local filesystem creation while keeping
  the transfer near 5 GiB. The selection is PET series from
  `rider_lung_pet_ct`, IDC data version v24, licensed CC BY 3.0. Every series is
  present in the Data Retriever's embedded IDC index.
- A091105 is a current public TCIA annotation manifest licensed CC BY 4.0. Its
  2,445 Series Instance UIDs are absent from both embedded IDC indexes, so the
  Data Retriever routes them through NBIA rather than silently turning this
  into a second IDC test.
- The General Commons cohort combines 478 source-image ZIPs and 159 SEG ZIPs,
  exercising both file populations in the official TCIA manifests for
  `phs004225`.
- The CTDC cohort uses the official CMB-PCA DRS manifest for `phs002192` and
  includes CT, MR, and PT series.
- The paired PathDB manifests isolate host/protocol differences because each
  production/staging pair contains identical paths in identical order. The
  small-file cohort measures multiplexing and request overhead; the SVS cohort
  validates long-running streamed downloads.

## Suggested benchmark procedure

1. Build a baseline binary from the production release and a candidate binary
   from the PR. Record the exact commit SHA, OS, CPU, available memory, network,
   Data Retriever options, and manifest SHA-256.
2. Use local non-synchronized storage (for example, `/private/tmp`) and verify
   free space before each trial. Use a new output directory; do not benchmark a
   resume or `--skip-existing` path unless that is the stated test.
3. Run a short preflight first. Confirm that the expected route appears in the
   logs (`IDC`, `nbia`, `drs`, or direct spreadsheet URL) and that credentials
   work before starting a controlled-access trial.
4. Compare identical settings. Alternate baseline and candidate runs to reduce
   time-of-day bias, and collect at least three successful repetitions per arm.
   Report the median and range rather than the single fastest run.
5. For transfer-only NBIA/DRS tests, consider `--no-decompress --no-md5` so ZIP
   expansion does not dominate timing or disk use. Apply that choice to every
   compared arm. IDC and PathDB payloads are not ZIP-expanded by this option.
6. Record elapsed time, transferred bytes, completed/failed objects, retries,
   HTTP status failures, negotiated protocol, and final on-disk counts. A
   faster run with missing or corrupt output is a failed trial.
7. Use the paired PathDB manifests for the HTTP/1.1-versus-HTTP/2 question.
   Treat IDC, NBIA, General Commons, and CTDC as route/regression benchmarks;
   their servers and transfer stacks differ, so cross-system speed comparisons
   do not isolate HTTP protocol effects.

Example public run:

```bash
TCIA_Data_Retriever --cli \
  --input benchmark/manifests/idc-rider-lung-pet-ct-pt.csv \
  --output /private/tmp/tcia-benchmark/idc-trial-1 \
  --processes 8 --max-connections 8 --verbose
```

Example authorized controlled run:

```bash
TCIA_Data_Retriever --cli \
  --input benchmark/manifests/ctdc-cmb-pca.csv \
  --output /private/tmp/tcia-benchmark/ctdc-trial-1 \
  --auth /path/to/the-testers-own-key.json \
  --processes 8 --max-connections 8 --no-decompress --no-md5 --verbose
```

## Provenance snapshot

The manifests were assembled on 2026-09-30.

- IDC: `idc-index` 0.12.5, IDC data version v24, DOI
  `10.7937/k9/tcia.2015.ofip7tvm`.
- NBIA: TCIA download id 46387,
  `A091105_Tumor-Annotations-manifest_2026-02-13.tcia`, converted to the current
  one-column CSV contract without changing the UID set.
- General Commons:
  `GC_manifest_LGG-1p19qDeletion_20260326.csv` and
  `GC_manifest_LGG-1p19qDeletion_20260326_SEGonly.csv`.
- CTDC: `CMB-PCA_drs_metadata_manifest.csv`.
- Controlled/public non-DICOM metadata: TCIA query-skill V2 release fingerprint
  `bc6d6c58ab0f5b522503f05089b6219cc58f911e7bdc7fa0916bdc898d6e6fea`.
- PathDB large-SVS expected bytes: 5,368,392,639. The 8,192-file TIFF subset is
  a deterministic prefix of the previously hash-ordered 81,211-file cohort.
