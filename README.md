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

## Required Secrets

Add these repository secrets:

- `GH_STATS_READ_TOKEN`
- `GH_STATS_WRITE_TOKEN`

`GH_STATS_READ_TOKEN` needs access to:

- `NVIDIAGameWorks/rtx-remix`
- `NVIDIAGameWorks/dxvk-remix`
- `NVIDIAGameWorks/toolkit-remix`

Required fine-grained token permissions:

- `Administration: Read-only` for traffic stats
- `Contents: Read-only` for release data
- `Metadata: Read-only`, which GitHub grants automatically

`GH_STATS_WRITE_TOKEN` needs access to:

- `NVIDIAGameWorks/rtx-remix-stats`

Required fine-grained token permissions:

- `Contents: Read and write` on `NVIDIAGameWorks/rtx-remix-stats`
- `Metadata: Read-only`, which GitHub grants automatically

## Optional Private Pages

If private GitHub Pages is available for the org, publish from:

- branch: `github-repo-stats`
- folder: `/`

After GitHub shows the private Pages URL, add a repository variable named
`GH_STATS_PAGES_URL` with that URL.
