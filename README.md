# RTX Remix Stats

GitHub repository for generated repository statistics reports.

The workflow in `.github/workflows/repository-stats.yml` collects stats for:

- `NVIDIAGameWorks/rtx-remix`
- `NVIDIAGameWorks/dxvk-remix`
- `NVIDIAGameWorks/toolkit-remix`

Each workflow run commits the generated
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

Add this repository secret:

- `GH_STATS_WRITE_TOKEN`

The workflow passes it as both `ghtoken` and `write-token`, so one token
covers reads and writes. It needs access to all four repositories:

- `NVIDIAGameWorks/rtx-remix`
- `NVIDIAGameWorks/dxvk-remix`
- `NVIDIAGameWorks/toolkit-remix`
- `NVIDIAGameWorks/rtx-remix-stats` (snapshot and report storage)

Required fine-grained token permissions:

- `Administration: Read-only` for traffic stats
- `Contents: Read and write` for release data, stargazer timestamps, and
  pushing to the data branch
- `Metadata: Read-only`, which GitHub grants automatically

`Contents` must be write rather than read because GitHub
[restricted the stargazer listing](https://github.blog/changelog/2026-06-30-upcoming-access-restrictions-to-public-api-endpoints-and-ui-views/)
to a repository's own admins and collaborators on 2026-06-30. If that listing
is unavailable the run logs a warning and continues; the star count still
comes from repository metadata, so only the per-user star timeline stalls.

## Viewing Reports

This setup does not require GitHub Pages.

To view a report:

- Open the latest `repository-stats` workflow run.
- Download the matching `stats-*` artifact.
- Open `report.html` locally.

