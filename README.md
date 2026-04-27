# RTX Remix Stats

Private GitHub repository for generated repository statistics reports.

The workflow in `.github/workflows/repository-stats.yml` collects stats for:

- `NVIDIAGameWorks/rtx-remix`
- `NVIDIAGameWorks/dxvk-remix`
- `NVIDIAGameWorks/toolkit-remix`

Reports stay private in this repository. Each workflow run commits the generated
HTML reports to the `github-repo-stats` branch under:

```text
NVIDIAGameWorks/rtx-remix/latest-report/report.html
NVIDIAGameWorks/dxvk-remix/latest-report/report.html
NVIDIAGameWorks/toolkit-remix/latest-report/report.html
```

The workflow also uploads each latest report as an Actions artifact named:

```text
stats-rtx-remix
stats-dxvk-remix
stats-toolkit-remix
```

The generated data branch also includes a root `index.html` that links to the
latest report for each repository. If GitHub Pages is enabled from the
`github-repo-stats` branch, open the Pages root after the next workflow run.

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

## Viewing Reports

This setup does not require GitHub Pages.

To view a report:

- Open the latest `repository-stats` workflow run.
- Download the matching `stats-*` artifact.
- Open `report.html` locally.

The historical snapshots and latest reports are also available on the private
`github-repo-stats` branch for anyone with repository read access.
