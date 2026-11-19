# Croissant regression fixtures

These public TCIA examples are pinned to
[`kirbyju/tcia-croissant-odrl@d5f4db7`](https://github.com/kirbyju/tcia-croissant-odrl/tree/d5f4db7b6d694da984f9d94cf7ac6d49c2bd4c07/examples).
No live WordPress, imaging payload, or controlled-access requests are needed to run the tests.

The five files in `croissant-summaries/` are the published inline download summaries.
Their `key` identifies a download row, not the retrieval key in a nested manifest.

`croissant-examples/hnscc-mif-mihc-comparison.croissant.jsonld` and
`croissant-examples/pathdb-images.csv` are the published PathDB JSON-LD/inventory pair.
The CSV contains 3,212 unique URLs, including mIF/mIHC images with identical filename stems.
Tests serve this inventory locally and verify all jobs and destinations without fetching images.

SHA-256 checksums of the copied fixtures:

```text
4c46769efd3ae5b23a718ec6a1c8e3ffda83479be8a5776b013b19d9893a1bda  croissant-summaries/4d-lung.open-manifest.croissant.jsonld
3fcbd4e6684ffc6027ae129f564bc6cc96c6fbb9ab476e9b7996a37bf5fa84cc  croissant-summaries/breast-cancer-screening-dbt.noncommercial.croissant.jsonld
23a7e5241dcc3e0cc94f344c1659c344691a9664319d46b6055bff0a34f1bc86  croissant-summaries/cmb-aml.radiology-pathology-external-clinical.croissant.jsonld
18186172bd2fbfea32768bc1008f1b852cfd6b34565aa444dcb5e905df6326b3  croissant-summaries/hnscc.mixed-access.croissant.jsonld
555dd8b1ce69fc69aa190807e8df4935b686797e6f1760b6a8f6f89c4969c87e  croissant-summaries/saros.analysis-result-nifti-derived.croissant.jsonld
716f28aea10e323fbd332e4090a091189f79dff3e88a6e555f6e864e9bad41db  croissant-examples/hnscc-mif-mihc-comparison.croissant.jsonld
e424bf74370020a34d88adc2b1f632dc10a4416d646383305e8a97d7bb916396  croissant-examples/pathdb-images.csv
```

Other files in `croissant-examples/` retain their existing source versions.
