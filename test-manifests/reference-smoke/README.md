# Reference smoke tests

One entry per supported source: IDC, NBIA, CTDC, General Commons and PathDB. These are small manual integration inputs for the citation pipeline, separate from the multi-gigabyte throughput benchmarks.

The manifests intentionally omit DOIs, collection names and supplied citations. IDC/NBIA identify their datasets through provider metadata; CTDC/General Commons DRS locators and the PathDB image URL exercise the snapshot DOI resolver. Successful runs write `metadata/references/*/references.txt`, `.bib` and `.json`. Expected DOIs (provider display names can differ from citation titles):

| Source | Dataset | DOI |
| --- | --- | --- |
| IDC | RIDER-Lung-PET-CT | `10.7937/K9/TCIA.2015.OFIP7TVM` |
| NBIA | A091105-Tumor-Annotations | `10.7937/t8rn-j447` |
| CTDC | CMB-PCA | `10.7937/25T7-6Y12` |
| General Commons | LGG-1p19qDeletion | `10.7937/K9/TCIA.2017.DWEHTZ9V` |
| PathDB | AML-Cytomorphology_MLL_Helmholtz | `10.7937/6PPE-4020` |

From the repository root, inspect references without imaging transfers or credentials:

```bash
for source in idc nbia ctdc general-commons pathdb; do
  tdr --cli --input "test-manifests/reference-smoke/$source.csv" \
    --meta --accept-data-policy --output "./reference-smoke-output/$source"
done
```

Review TCIA's data usage policy before accepting it. Use a fresh output directory. Check that each reference has APA, BibTeX and the expected DOI; DRS/PathDB JSON should show `Exact file-locator match` and `resolver_release_fingerprint`. Known-DOI bypass is covered by the automated reference tests.

For actual transfers, omit `--meta`; CTDC and General Commons require your own authorization and `--auth /path/to/credentials.json`. No credentials are included. A DICOM entry is a **whole series**, including every instance; a DRS entry can be a ZIP containing a whole series. A one-row manifest does not impose a byte limit. The UID/DRS samples retain the first record of their corresponding benchmark CSVs; the PathDB TIFF is the first AML file used in the reference validation.

Run these files separately. A CSV with a `SeriesInstanceUID` header is routed as a UID inventory; mixing UID and DRS/image rows in that CSV would not exercise all sources.
