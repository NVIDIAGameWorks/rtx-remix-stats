# RTX Remix Stats

Private GitHub repository for generated repository statistics reports.

The workflow in `.github/workflows/repository-stats.yml` collects stats for:

- `NVIDIAGameWorks/rtx-remix`
- `NVIDIAGameWorks/dxvk-remix`
- `NVIDIAGameWorks/toolkit-remix`

Reports are committed to the `github-repo-stats` branch under:

```text
NVIDIAGameWorks/rtx-remix/latest-report/report.html
NVIDIAGameWorks/dxvk-remix/latest-report/report.html
NVIDIAGameWorks/toolkit-remix/latest-report/report.html
```

## Required Secret

Add a repository secret named `GH_STATS_TOKEN`.

The token needs access to these repositories:

- `NVIDIAGameWorks/rtx-remix`
- `NVIDIAGameWorks/dxvk-remix`
- `NVIDIAGameWorks/toolkit-remix`
- `NVIDIAGameWorks/rtx-remix-stats`

Required fine-grained token permissions:

- `Administration: Read-only` on the three source repositories for traffic stats
- `Contents: Read-only` on the three source repositories for release data
- `Contents: Read and write` on `NVIDIAGameWorks/rtx-remix-stats`

## Optional Private Pages

If private GitHub Pages is available for the org, publish from:

- branch: `github-repo-stats`
- folder: `/`

After GitHub shows the private Pages URL, add a repository variable named
`GH_STATS_PAGES_URL` with that URL.
