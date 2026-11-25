# Small manual test manifests

Use fresh output directories and start with `--meta` to inspect the planned metadata and references without downloading imaging payloads.

- [`croissant/`](croissant/README.md): bounded JSON-LD examples covering public DICOM, noncommercial terms, CTDC/General Commons, direct PathDB images and a small SAROS table.
- `manifest-1790098311121.tcia`: the existing two-series UID example. A UID represents an entire DICOM series, not one slice.

Large throughput cohorts remain under [`benchmarkManifests/examples/`](../benchmarkManifests/examples/README.md). Full published Croissant regression fixtures and placeholder XML wrappers are internal test inputs under `core/app/testdata`; they are not runnable smoke manifests.
