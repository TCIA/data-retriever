# Small manual test manifests

Use fresh output directories and start with `--meta` to inspect the planned metadata and references without downloading imaging payloads.

- [`croissant/`](croissant/README.md): bounded JSON-LD examples covering public DICOM, noncommercial terms, CTDC/General Commons, direct PathDB images and a small SAROS table.
- `manifest-1790098311121.tcia`: the existing two-series UID example. A UID represents an entire DICOM series, not one slice.

Large throughput cohorts remain under [`benchmarkManifests/examples/`](../benchmarkManifests/examples/README.md). Full published Croissant regression fixtures are internal test inputs under `core/app/testdata`. The existing `manifest-v2-template.tcia` and nested legacy fixture remain unchanged here for their regression test; the XML template contains a placeholder URL and is not a runnable smoke manifest.
