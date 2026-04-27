#!/usr/bin/env python3
"""Collect GitHub repository stats and generate a static HTML report."""

from __future__ import annotations

import contextlib
import dataclasses
import fnmatch
import html
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
DEFAULT_USER_AGENT = "github-stats-action-nv/1"
DATA_REMOTE_NAME = "github-stats-data"
INIT_BRANCH_PREFIX = "github-stats-data-init-"


class ConfigError(RuntimeError):
    """Raised when action inputs are invalid."""


class ApiError(RuntimeError):
    """Raised when the GitHub API returns an error response."""


@dataclasses.dataclass(frozen=True)
class Config:
    token: str
    repository: str
    data_repository: str
    data_branch: str
    ghpages_prefix: str
    api_base_url: str
    github_server_url: str
    api_version: str
    include_traffic: bool
    include_stargazers: bool
    include_forks: bool
    include_releases: bool
    release_asset_patterns: tuple[str, ...]
    max_pages: int
    output_directory: str
    push: bool
    commit_user_name: str
    commit_user_email: str
    workspace: Path

    @classmethod
    def from_env(cls) -> "Config":
        token = required_env("GHRS_TOKEN")
        repository = required_env("GHRS_REPOSITORY")
        data_repository = env_value("GHRS_DATA_REPOSITORY", repository)
        data_branch = env_value("GHRS_DATA_BRANCH", "github-repo-stats")
        api_base_url = env_value("GHRS_API_BASE_URL", "https://api.github.com")
        github_server_url = env_value("GHRS_GITHUB_SERVER_URL", "https://github.com")
        max_pages_raw = env_value("GHRS_MAX_PAGES", "1000")
        workspace = Path(env_value("GITHUB_WORKSPACE", os.getcwd())).resolve()

        if not REPOSITORY_RE.match(repository):
            raise ConfigError(f"GHRS_REPOSITORY must be in owner/repo form: {repository!r}")
        if not REPOSITORY_RE.match(data_repository):
            raise ConfigError(
                f"GHRS_DATA_REPOSITORY must be in owner/repo form: {data_repository!r}"
            )
        try:
            max_pages = int(max_pages_raw)
        except ValueError as exc:
            raise ConfigError(f"GHRS_MAX_PAGES must be an integer: {max_pages_raw!r}") from exc
        if max_pages < 1:
            raise ConfigError("GHRS_MAX_PAGES must be at least 1")

        return cls(
            token=token,
            repository=repository,
            data_repository=data_repository,
            data_branch=data_branch,
            ghpages_prefix=env_value("GHRS_GHPAGES_PREFIX", ""),
            api_base_url=api_base_url.rstrip("/"),
            github_server_url=github_server_url.rstrip("/"),
            api_version=env_value("GHRS_API_VERSION", "2022-11-28"),
            include_traffic=parse_bool(env_value("GHRS_INCLUDE_TRAFFIC", "true")),
            include_stargazers=parse_bool(env_value("GHRS_INCLUDE_STARGAZERS", "true")),
            include_forks=parse_bool(env_value("GHRS_INCLUDE_FORKS", "true")),
            include_releases=parse_bool(env_value("GHRS_INCLUDE_RELEASES", "true")),
            release_asset_patterns=parse_patterns(env_value("GHRS_RELEASE_ASSET_PATTERNS", "*")),
            max_pages=max_pages,
            output_directory=env_value("GHRS_OUTPUT_DIRECTORY", ""),
            push=parse_bool(env_value("GHRS_PUSH", "true")),
            commit_user_name=env_value("GHRS_COMMIT_USER_NAME", "github-actions[bot]"),
            commit_user_email=env_value(
                "GHRS_COMMIT_USER_EMAIL",
                "41898282+github-actions[bot]@users.noreply.github.com",
            ),
            workspace=workspace,
        )


def env_value(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Missing required environment variable {name}")
    return value


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigError(f"Expected a boolean value, got {value!r}")


def parse_patterns(value: str) -> tuple[str, ...]:
    patterns = tuple(part.strip() for part in value.split(",") if part.strip())
    return patterns or ("*",)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def snapshot_filename(generated_at: str) -> str:
    return generated_at.replace("-", "").replace(":", "").replace("Z", "Z") + ".json"


class GitHubClient:
    def __init__(self, config: Config):
        self.config = config

    def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        accept: str = "application/vnd.github+json",
    ) -> Any:
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params)
        url = f"{self.config.api_base_url}{path}{query}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": accept,
                "Authorization": f"Bearer {self.config.token}",
                "User-Agent": DEFAULT_USER_AGENT,
                "X-GitHub-Api-Version": self.config.api_version,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise self._api_error(exc, body, path) from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"GitHub API request failed for {path}: {exc}") from exc

    def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        accept: str = "application/vnd.github+json",
    ) -> list[Any]:
        page = 1
        per_page = 100
        items: list[Any] = []
        params = dict(params or {})

        while page <= self.config.max_pages:
            page_params = {**params, "per_page": per_page, "page": page}
            page_items = self.get_json(path, page_params, accept)
            if not isinstance(page_items, list):
                raise ApiError(f"Expected list response for {path}, got {type(page_items).__name__}")
            if not page_items:
                break
            items.extend(page_items)
            if len(page_items) < per_page:
                break
            page += 1
        else:
            raise ApiError(
                f"Stopped paginating {path} after {self.config.max_pages} pages; "
                "increase max-pages if this repository has more history."
            )

        return items

    @staticmethod
    def _api_error(exc: urllib.error.HTTPError, body: str, path: str) -> ApiError:
        message = ""
        try:
            decoded = json.loads(body)
            message = decoded.get("message", "")
        except json.JSONDecodeError:
            message = body.strip()
        rate_remaining = exc.headers.get("x-ratelimit-remaining")
        rate_reset = exc.headers.get("x-ratelimit-reset")
        detail = f"GitHub API returned HTTP {exc.code} for {path}"
        if message:
            detail += f": {message}"
        if exc.code == 403 and rate_remaining == "0" and rate_reset:
            detail += f" (rate limit reset epoch: {rate_reset})"
        return ApiError(detail)


def fetch_snapshot(config: Config) -> dict[str, Any]:
    owner, repo = config.repository.split("/", 1)
    client = GitHubClient(config)
    generated_at = utc_now()

    print(f"Fetching repository metadata for {config.repository}")
    metadata = client.get_json(f"/repos/{owner}/{repo}")
    aggregate_counts = aggregate_counts_from_metadata(metadata)

    traffic: dict[str, Any] = {}
    if config.include_traffic:
        print("Fetching traffic views and clones")
        traffic["views"] = client.get_json(
            f"/repos/{owner}/{repo}/traffic/views", {"per": "day"}
        )
        traffic["clones"] = client.get_json(
            f"/repos/{owner}/{repo}/traffic/clones", {"per": "day"}
        )
        print("Fetching popular referrers and paths")
        traffic["popular_referrers"] = client.get_json(
            f"/repos/{owner}/{repo}/traffic/popular/referrers"
        )
        traffic["popular_paths"] = client.get_json(
            f"/repos/{owner}/{repo}/traffic/popular/paths"
        )

    stargazers: list[dict[str, Any]] = []
    if config.include_stargazers:
        print("Fetching stargazer timestamps")
        raw_stargazers = client.get_paginated(
            f"/repos/{owner}/{repo}/stargazers",
            accept="application/vnd.github.star+json",
        )
        stargazers = normalize_stargazers(raw_stargazers)

    forks: list[dict[str, Any]] = []
    if config.include_forks:
        print("Fetching forks")
        raw_forks = client.get_paginated(
            f"/repos/{owner}/{repo}/forks",
            params={"sort": "oldest"},
        )
        forks = normalize_forks(raw_forks)

    releases: list[dict[str, Any]] = []
    if config.include_releases:
        print("Fetching releases and release assets")
        raw_releases = client.get_paginated(f"/repos/{owner}/{repo}/releases")
        releases = fetch_release_assets(
            client,
            owner,
            repo,
            raw_releases,
            config.release_asset_patterns,
        )

    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "repository": config.repository,
        "api_base_url": config.api_base_url,
        "repository_metadata": select_repo_metadata(metadata),
        "aggregate_counts": aggregate_counts,
        "traffic": traffic,
        "stargazers": stargazers,
        "forks": forks,
        "releases": releases,
    }


def select_repo_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "id",
        "name",
        "full_name",
        "html_url",
        "description",
        "private",
        "fork",
        "created_at",
        "updated_at",
        "pushed_at",
        "default_branch",
        "stargazers_count",
        "forks_count",
        "watchers_count",
        "subscribers_count",
        "open_issues_count",
        "visibility",
    ]
    return {key: metadata.get(key) for key in keys if key in metadata}


def aggregate_counts_from_metadata(metadata: dict[str, Any]) -> dict[str, int]:
    return {
        "stargazers": int_or_zero(metadata.get("stargazers_count")),
        "forks": int_or_zero(metadata.get("forks_count")),
        "watchers": int_or_zero(metadata.get("watchers_count")),
        "subscribers": int_or_zero(metadata.get("subscribers_count")),
        "open_issues": int_or_zero(metadata.get("open_issues_count")),
    }


def normalize_stargazers(raw_stargazers: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in raw_stargazers:
        if not isinstance(item, dict):
            continue
        user = item.get("user") if "user" in item else item
        if not isinstance(user, dict):
            user = {}
        normalized.append(
            {
                "starred_at": item.get("starred_at"),
                "login": user.get("login"),
                "id": user.get("id"),
                "html_url": user.get("html_url"),
            }
        )
    normalized.sort(key=lambda row: (row.get("starred_at") or "", row.get("login") or ""))
    return normalized


def normalize_forks(raw_forks: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in raw_forks:
        if not isinstance(item, dict):
            continue
        owner = item.get("owner") if isinstance(item.get("owner"), dict) else {}
        normalized.append(
            {
                "created_at": item.get("created_at"),
                "full_name": item.get("full_name"),
                "html_url": item.get("html_url"),
                "owner_login": owner.get("login"),
                "id": item.get("id"),
            }
        )
    normalized.sort(key=lambda row: (row.get("created_at") or "", row.get("full_name") or ""))
    return normalized


def fetch_release_assets(
    client: GitHubClient,
    owner: str,
    repo: str,
    raw_releases: list[Any],
    asset_patterns: tuple[str, ...],
) -> list[dict[str, Any]]:
    releases: list[dict[str, Any]] = []
    for release in raw_releases:
        if not isinstance(release, dict):
            continue
        release_id = int_or_zero(release.get("id"))
        raw_assets: list[Any] = []
        if release_id:
            raw_assets = client.get_paginated(
                f"/repos/{owner}/{repo}/releases/{release_id}/assets"
            )
        releases.append(normalize_release(release, raw_assets, asset_patterns))
    releases.sort(key=lambda row: (row.get("published_at") or row.get("created_at") or ""), reverse=True)
    return releases


def normalize_release(
    release: dict[str, Any],
    raw_assets: list[Any],
    asset_patterns: tuple[str, ...],
) -> dict[str, Any]:
    assets = [
        normalize_release_asset(asset)
        for asset in raw_assets
        if isinstance(asset, dict) and release_asset_matches(asset, asset_patterns)
    ]
    assets.sort(key=lambda row: (row.get("name") or "", row.get("id") or 0))
    return {
        "id": release.get("id"),
        "tag_name": release.get("tag_name"),
        "name": release.get("name"),
        "html_url": release.get("html_url"),
        "draft": bool(release.get("draft")),
        "prerelease": bool(release.get("prerelease")),
        "created_at": release.get("created_at"),
        "published_at": release.get("published_at"),
        "zipball_url": release.get("zipball_url"),
        "tarball_url": release.get("tarball_url"),
        "source_download_counts_available": False,
        "assets": assets,
    }


def normalize_release_asset(asset: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": asset.get("id"),
        "name": asset.get("name"),
        "label": asset.get("label"),
        "state": asset.get("state"),
        "content_type": asset.get("content_type"),
        "size": int_or_zero(asset.get("size")),
        "download_count": int_or_zero(asset.get("download_count")),
        "browser_download_url": asset.get("browser_download_url"),
        "created_at": asset.get("created_at"),
        "updated_at": asset.get("updated_at"),
    }


def release_asset_matches(asset: dict[str, Any], patterns: tuple[str, ...]) -> bool:
    name = str(asset.get("name") or "")
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)


def load_snapshots(snapshot_dir: Path) -> list[dict[str, Any]]:
    if not snapshot_dir.exists():
        return []
    snapshots: list[dict[str, Any]] = []
    for path in sorted(snapshot_dir.glob("*.json")):
        with path.open("r", encoding="utf-8") as file:
            snapshot = json.load(file)
        if isinstance(snapshot, dict):
            snapshots.append(snapshot)
    snapshots.sort(key=lambda row: row.get("generated_at", ""))
    return snapshots


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        file.write(content)


def resolve_workspace_path(workspace: Path, raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = workspace / candidate
    resolved = candidate.resolve()
    workspace_resolved = workspace.resolve()
    if resolved != workspace_resolved and workspace_resolved not in resolved.parents:
        raise ConfigError(
            f"Output directory must be inside the workspace: {raw_path!r}"
        )
    return resolved


def copy_generated_output(
    worktree: Path,
    rel_root: Path,
    report_rel: Path,
    summary_rel: Path,
    snapshot_rel: Path,
    config: Config,
) -> dict[str, str]:
    if not config.output_directory:
        return {}
    output_root = resolve_workspace_path(config.workspace, config.output_directory)
    source_root = worktree / rel_root
    destination_root = output_root / rel_root
    destination_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_root, destination_root, dirs_exist_ok=True)
    print(f"Copied generated report output to {destination_root}")
    return {
        "local-output-directory": relative_to_workspace(output_root, config.workspace),
        "local-report-path": relative_to_workspace(output_root / report_rel, config.workspace),
        "local-summary-path": relative_to_workspace(output_root / summary_rel, config.workspace),
        "local-snapshot-path": relative_to_workspace(output_root / snapshot_rel, config.workspace),
    }


def relative_to_workspace(path: Path, workspace: Path) -> str:
    try:
        return path.resolve().relative_to(workspace.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def merge_daily_metric(
    snapshots: Iterable[dict[str, Any]], metric_name: str, points_key: str
) -> list[dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = {}
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        generated_at = snapshot.get("generated_at", "")
        metric = snapshot.get("traffic", {}).get(metric_name, {})
        points = metric.get(points_key, []) if isinstance(metric, dict) else []
        if not isinstance(points, list):
            continue
        for point in points:
            if not isinstance(point, dict):
                continue
            day = date_part(point.get("timestamp"))
            if not day:
                continue
            by_day[day] = {
                "date": day,
                "count": int_or_zero(point.get("count")),
                "uniques": int_or_zero(point.get("uniques")),
                "observed_at": generated_at,
            }
    return [by_day[day] for day in sorted(by_day)]


def latest_non_empty_list(
    snapshots: Iterable[dict[str, Any]], key: str
) -> list[dict[str, Any]]:
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", ""), reverse=True):
        value = snapshot.get(key)
        if isinstance(value, list) and value:
            return [row for row in value if isinstance(row, dict)]
    return []


def cumulative_timeline(events: Iterable[dict[str, Any]], date_key: str) -> list[dict[str, Any]]:
    counts = Counter()
    for event in events:
        day = date_part(event.get(date_key))
        if day:
            counts[day] += 1

    total = 0
    timeline: list[dict[str, Any]] = []
    for day in sorted(counts):
        total += counts[day]
        timeline.append({"date": day, "count": total, "delta": counts[day]})
    return timeline


def latest_snapshot(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    if not snapshots:
        return {}
    return max(snapshots, key=lambda row: row.get("generated_at", ""))


AGGREGATE_COUNTER_SPECS: tuple[tuple[str, str, str], ...] = (
    ("stargazers", "Stargazers", "#67e8f9"),
    ("forks", "Forks", "#fb7185"),
    ("subscribers", "Subscribers", "#81e2b2"),
    ("open_issues", "Open issues", "#f4c76d"),
)


def aggregate_counts_from_snapshot(snapshot: dict[str, Any]) -> dict[str, int]:
    counts = snapshot.get("aggregate_counts")
    if isinstance(counts, dict):
        return {key: int_or_zero(value) for key, value in counts.items()}
    metadata = snapshot.get("repository_metadata", {})
    if isinstance(metadata, dict):
        return aggregate_counts_from_metadata(metadata)
    return {}


def aggregate_counter_timeline(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        counts = aggregate_counts_from_snapshot(snapshot)
        row: dict[str, Any] = {"date": day}
        for key, _, _ in AGGREGATE_COUNTER_SPECS:
            row[key] = int_or_zero(counts.get(key))
        rows.append(row)
    return rows


def aggregate_counter_observations(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        counts = aggregate_counts_from_snapshot(snapshot)
        for key, label, _ in AGGREGATE_COUNTER_SPECS:
            observations.append(
                {
                    "date": day,
                    "key": key,
                    "label": label,
                    "count": int_or_zero(counts.get(key)),
                }
            )
    return observations


def release_assets_from_snapshot(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    releases = snapshot.get("releases", [])
    if not isinstance(releases, list):
        return rows
    for release in releases:
        if not isinstance(release, dict):
            continue
        tag_name = str(release.get("tag_name") or "")
        release_name = str(release.get("name") or tag_name)
        release_url = release.get("html_url")
        assets = release.get("assets", [])
        if not isinstance(assets, list):
            continue
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            asset_name = str(asset.get("name") or "")
            asset_id = asset.get("id")
            key = f"id:{asset_id}" if asset_id is not None else f"name:{tag_name}/{asset_name}"
            rows.append(
                {
                    "key": key,
                    "release": tag_name,
                    "release_name": release_name,
                    "release_url": release_url,
                    "asset": asset_name,
                    "asset_url": asset.get("browser_download_url"),
                    "download_count": int_or_zero(asset.get("download_count")),
                    "size": int_or_zero(asset.get("size")),
                    "content_type": asset.get("content_type"),
                    "updated_at": asset.get("updated_at"),
                    "label": f"{tag_name} / {asset_name}" if tag_name else asset_name,
                }
            )
    rows.sort(key=lambda row: (row["download_count"], row["release"], row["asset"]), reverse=True)
    return rows


def release_asset_observations(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        for asset in release_assets_from_snapshot(snapshot):
            observations.append(
                {
                    "date": day,
                    "key": asset["key"],
                    "label": asset["label"],
                    "count": asset["download_count"],
                    "release": asset["release"],
                    "asset": asset["asset"],
                    "url": asset["asset_url"],
                }
            )
    return observations


def release_total_download_timeline(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        assets = release_assets_from_snapshot(snapshot)
        rows.append(
            {
                "date": day,
                "count": sum(asset["download_count"] for asset in assets),
                "assets": len(assets),
            }
        )
    return rows


def monthly_counter_deltas(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    previous: dict[str, int] = {}
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for observation in sorted(observations, key=lambda row: (row.get("date", ""), row.get("key", ""))):
        day = date_part(observation.get("date"))
        if not day:
            continue
        key = str(observation.get("key") or "")
        current = int_or_zero(observation.get("count"))
        previous_count = previous.get(key)
        delta = 0 if previous_count is None else max(0, current - previous_count)
        previous[key] = current
        month = day[:7]
        bucket_key = (month, key)
        bucket = buckets.setdefault(
            bucket_key,
            {
                "month": month,
                "key": key,
                "label": observation.get("label") or key,
                "delta": 0,
                "latest_count": current,
                "url": observation.get("url"),
                "release": observation.get("release"),
                "asset": observation.get("asset"),
            },
        )
        bucket["delta"] += delta
        bucket["latest_count"] = current
        if observation.get("url"):
            bucket["url"] = observation.get("url")
    return sorted(buckets.values(), key=lambda row: (row["month"], row["label"]))


def build_summary(
    repository: str,
    snapshots: list[dict[str, Any]],
    views: list[dict[str, Any]],
    clones: list[dict[str, Any]],
    stargazers: list[dict[str, Any]],
    forks: list[dict[str, Any]],
    release_assets: list[dict[str, Any]],
    monthly_release_downloads: list[dict[str, Any]],
    report_path: str,
    report_url: str,
) -> dict[str, Any]:
    generated = latest_snapshot(snapshots).get("generated_at")
    latest_counts = aggregate_counts_from_snapshot(latest_snapshot(snapshots))
    return {
        "repository": repository,
        "generated_at": generated,
        "snapshot_count": len(snapshots),
        "report_path": report_path,
        "report_url": report_url,
        "days": {
            "views": len(views),
            "clones": len(clones),
        },
        "totals": {
            "views": sum(row["count"] for row in views),
            "unique_visitors": sum(row["uniques"] for row in views),
            "clones": sum(row["count"] for row in clones),
            "unique_cloners": sum(row["uniques"] for row in clones),
            "stargazers": len(stargazers),
            "forks": len(forks),
            "release_asset_downloads": sum(asset["download_count"] for asset in release_assets),
            "release_assets": len(release_assets),
        },
        "aggregate_counts": latest_counts,
        "monthly_release_asset_downloads": current_month_delta(monthly_release_downloads),
    }


def render_report(
    repository: str,
    snapshots: list[dict[str, Any]],
    config: Config | None,
    report_url: str = "",
) -> str:
    latest = latest_snapshot(snapshots)
    metadata = latest.get("repository_metadata", {})
    views = merge_daily_metric(snapshots, "views", "views")
    clones = merge_daily_metric(snapshots, "clones", "clones")
    stargazers = latest_non_empty_list(snapshots, "stargazers")
    forks = latest_non_empty_list(snapshots, "forks")
    stargazer_timeline = cumulative_timeline(stargazers, "starred_at")
    fork_timeline = cumulative_timeline(forks, "created_at")
    aggregate_timeline = aggregate_counter_timeline(snapshots)
    monthly_aggregate_deltas = monthly_counter_deltas(aggregate_counter_observations(snapshots))
    release_assets = release_assets_from_snapshot(latest)
    release_total_timeline = release_total_download_timeline(snapshots)
    monthly_release_downloads = monthly_counter_deltas(release_asset_observations(snapshots))
    referrer_items = latest.get("traffic", {}).get("popular_referrers", [])
    path_items = latest.get("traffic", {}).get("popular_paths", [])
    server_url = config.github_server_url if config else "https://github.com"
    generated_at = latest.get("generated_at", "")
    repo_url = metadata.get("html_url") or f"{server_url}/{repository}"
    description = metadata.get("description") or ""
    latest_counts = aggregate_counts_from_snapshot(latest)
    total_views = sum(row["count"] for row in views)
    total_unique_visitors = sum(row["uniques"] for row in views)
    total_clones = sum(row["count"] for row in clones)
    total_release_downloads = sum(asset["download_count"] for asset in release_assets)

    views_svg = render_line_chart(
        "Daily repository views",
        views,
        [
            ("Total views", "count", "#7db7ff"),
            ("Unique visitors", "uniques", "#81e2b2"),
        ],
    )
    clones_svg = render_line_chart(
        "Daily repository clones",
        clones,
        [
            ("Total clones", "count", "#b69cff"),
            ("Unique cloners", "uniques", "#f4c76d"),
        ],
    )
    stars_svg = render_line_chart(
        "Stargazers over time",
        stargazer_timeline,
        [("Stargazers", "count", "#67e8f9")],
    )
    forks_svg = render_line_chart(
        "Forks over time",
        fork_timeline,
        [("Forks", "count", "#fb7185")],
    )
    aggregate_svg = render_line_chart(
        "Repository counters over time",
        aggregate_timeline,
        [
            (label, key, color)
            for key, label, color in AGGREGATE_COUNTER_SPECS
        ],
    )
    release_downloads_svg = render_line_chart(
        "Tracked release asset downloads over time",
        release_total_timeline,
        [("Downloads", "count", "#7db7ff")],
    )
    referrer_history_svg = render_popularity_chart(
        snapshots,
        "popular_referrers",
        "referrer",
        "Top referrers: rolling 14-day unique visitors",
    )
    path_history_svg = render_popularity_chart(
        snapshots,
        "popular_paths",
        "path",
        "Top paths: rolling 14-day unique visitors",
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Statistics for {esc(repository)}</title>
  <style>
{REPORT_CSS}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="brand">
      <span class="brand-mark" aria-hidden="true">GS</span>
      <div>
        <p class="eyebrow">Service intelligence</p>
        <h1><a href="{attr(repo_url)}">{esc(repository)}</a></h1>
      </div>
    </div>
    <div class="run-meta">
      <span>{esc(generated_at)} UTC</span>
      <span>{len(snapshots)} snapshot(s)</span>
    </div>
  </header>
  <nav class="section-nav" aria-label="Report sections">
      <a href="#views">Views</a>
      <a href="#clones">Clones</a>
      <a href="#stargazers">Stars</a>
      <a href="#forks">Forks</a>
      <a href="#repository-counters">Counters</a>
      <a href="#releases">Releases</a>
      <a href="#popular">Acquisition</a>
  </nav>
  <main class="report-shell">
    <section class="intro-panel" aria-label="Report summary">
      <div>
        <p class="eyebrow">Latest report</p>
        <h2>{esc(repository)}</h2>
        {f'<p class="description">{esc(description)}</p>' if description else ''}
        {f'<p class="meta">Published report: <a href="{attr(report_url)}">{esc(report_url)}</a></p>' if report_url else ''}
      </div>
      {render_overview_metrics([
          ("Views", total_views, "daily total sum", "blue"),
          ("Visitors", total_unique_visitors, "daily unique sum", "green"),
          ("Stars", int_or_zero(latest_counts.get("stargazers")), "latest counter", "cyan"),
          ("Release downloads", total_release_downloads, "tracked assets", "amber"),
      ])}
    </section>
    <section id="views" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Traffic</p>
        <h2>Views</h2>
      </div>
      {render_metric_cards([
          ("Unique visitors", total_unique_visitors, "sum of daily uniques"),
          ("Total views", total_views, "sum of daily views"),
          ("Days covered", len(views), "daily points"),
      ])}
      <div class="content-grid">
        {views_svg}
        <div class="table-panel">{render_daily_table("Recent view data", views, ("Uniques", "uniques"), ("Views", "count"))}</div>
      </div>
    </section>
    <section id="clones" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Distribution</p>
        <h2>Clones</h2>
      </div>
      {render_metric_cards([
          ("Unique cloners", sum(row["uniques"] for row in clones), "sum of daily uniques"),
          ("Total clones", total_clones, "sum of daily clones"),
          ("Days covered", len(clones), "daily points"),
      ])}
      <div class="content-grid">
        {clones_svg}
        <div class="table-panel">{render_daily_table("Recent clone data", clones, ("Uniques", "uniques"), ("Clones", "count"))}</div>
      </div>
    </section>
    <section id="stargazers" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Community</p>
        <h2>Stargazers</h2>
      </div>
      {render_metric_cards([
          ("Stargazers", len(stargazers), "current API listing"),
          ("Days with stars", len(stargazer_timeline), "event days"),
      ])}
      <div class="content-grid">
        {stars_svg}
        <div class="table-panel">{render_event_table("Recent stargazer events", stargazers, "starred_at", "login", "html_url")}</div>
      </div>
    </section>
    <section id="forks" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Community</p>
        <h2>Forks</h2>
      </div>
      {render_metric_cards([
          ("Forks", len(forks), "current API listing"),
          ("Days with forks", len(fork_timeline), "event days"),
      ])}
      <div class="content-grid">
        {forks_svg}
        <div class="table-panel">{render_event_table("Recent fork events", forks, "created_at", "full_name", "html_url")}</div>
      </div>
    </section>
    <section id="repository-counters" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Health</p>
        <h2>Repository counters</h2>
        <p class="note">Cumulative GitHub counters sampled on each run. Monthly values are observed deltas between snapshots.</p>
      </div>
      {render_metric_cards(repository_counter_cards(latest))}
      <div class="content-grid">
        {aggregate_svg}
        <div class="table-panel">{render_monthly_delta_table("Monthly observed repository counter changes", monthly_aggregate_deltas, 40)}</div>
      </div>
    </section>
    <section id="releases" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Delivery</p>
        <h2>Release asset downloads</h2>
        <p class="note">GitHub exposes download counts for uploaded release assets. Generated source ZIP and TAR archives do not expose download counters.</p>
      </div>
      {render_metric_cards([
          ("Tracked assets", len(release_assets), "uploaded assets"),
          ("Total downloads", total_release_downloads, "latest cumulative count"),
          ("Observed this month", current_month_delta(monthly_release_downloads), "delta between snapshots"),
      ])}
      <div class="content-grid">
        {release_downloads_svg}
        <div class="table-panel">{render_release_asset_table("Top release assets", release_assets)}</div>
      </div>
      <div class="table-panel wide-table">{render_monthly_delta_table("Monthly observed release asset downloads", monthly_release_downloads, 60)}</div>
    </section>
    <section id="popular" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Acquisition</p>
        <h2>Top referrers and paths</h2>
        <p class="note">GitHub reports referrers and paths as rolling 14-day windows. These snapshots are sampled window values, not additive totals.</p>
      </div>
      <div class="split">
        <div class="table-panel">
          <h3>Top referrers</h3>
          {render_rank_table(referrer_items, "referrer")}
        </div>
        <div class="table-panel">
          <h3>Top paths</h3>
          {render_rank_table(path_items, "path", server_url)}
        </div>
      </div>
      <div class="chart-grid">
        {referrer_history_svg}
        {path_history_svg}
      </div>
    </section>
  </main>
  <footer>
    Generated by github-stats-action-nv.
  </footer>
</body>
</html>
"""


REPORT_CSS = """    :root {
      color-scheme: dark;
      --bg: #0b1110;
      --panel: #111c17;
      --panel-soft: #14231d;
      --ink: #edf7f1;
      --muted: #9caea5;
      --line: #263831;
      --line-strong: #3b5449;
      --blue: #7db7ff;
      --green: #81e2b2;
      --cyan: #67e8f9;
      --amber: #f4c76d;
      --rose: #fb7185;
      --chart-axis: #52685d;
      --chart-grid: #22342d;
      --chart-label: #a5b6ae;
      --shadow: 0 18px 44px rgba(0, 0, 0, 0.34);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      background:
        linear-gradient(180deg, #101d18 0, #0d1714 260px, var(--bg) 100%);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.38;
    }
    a {
      color: #9bc9ff;
      text-decoration-thickness: 0.08em;
      text-underline-offset: 0.16em;
    }
    .topbar, .section-nav, .report-shell, footer {
      max-width: 1240px;
      margin: 0 auto;
      padding-left: 22px;
      padding-right: 22px;
    }
    .topbar {
      align-items: center;
      display: flex;
      gap: 18px;
      justify-content: space-between;
      padding-top: 20px;
      padding-bottom: 12px;
    }
    .brand {
      align-items: center;
      display: flex;
      gap: 12px;
      min-width: 0;
    }
    .brand a {
      color: var(--ink);
      text-decoration: none;
    }
    .brand a:hover {
      color: var(--green);
    }
    .brand-mark {
      align-items: center;
      background: #0f1915;
      border: 1px solid #315144;
      border-radius: 8px;
      color: #f1fff7;
      display: inline-flex;
      flex: 0 0 auto;
      font-size: 0.72rem;
      font-weight: 800;
      height: 34px;
      justify-content: center;
      letter-spacing: 0;
      width: 34px;
    }
    .eyebrow {
      color: var(--green);
      font-size: 0.72rem;
      font-weight: 800;
      letter-spacing: 0;
      margin: 0 0 4px;
      text-transform: uppercase;
    }
    h1, h2, h3 { letter-spacing: 0; line-height: 1.12; }
    h1 {
      font-size: 1.45rem;
      margin: 0;
      overflow-wrap: anywhere;
    }
    h2 { font-size: 1.22rem; margin: 0; }
    h3 { font-size: 0.98rem; margin: 0 0 10px; }
    .run-meta {
      align-items: center;
      color: var(--muted);
      display: flex;
      flex-wrap: wrap;
      font-size: 0.82rem;
      gap: 8px;
      justify-content: flex-end;
    }
    .run-meta span {
      background: rgba(17, 28, 23, 0.86);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 5px 9px;
    }
    .section-nav {
      backdrop-filter: blur(12px);
      background: rgba(11, 17, 16, 0.84);
      border-bottom: 1px solid rgba(59, 84, 73, 0.62);
      border-top: 1px solid rgba(38, 56, 49, 0.82);
      display: flex;
      gap: 6px;
      overflow-x: auto;
      padding-bottom: 8px;
      padding-top: 8px;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    .section-nav a {
      background: #121d18;
      border: 1px solid var(--line);
      border-radius: 8px;
      color: #dcebe4;
      flex: 0 0 auto;
      font-size: 0.83rem;
      font-weight: 650;
      padding: 6px 10px;
      text-decoration: none;
    }
    .section-nav a:hover {
      border-color: var(--line-strong);
      box-shadow: 0 6px 18px rgba(0, 0, 0, 0.26);
    }
    .report-shell {
      display: grid;
      gap: 16px;
      padding-bottom: 24px;
      padding-top: 16px;
    }
    .intro-panel {
      align-items: center;
      background:
        linear-gradient(135deg, rgba(15, 25, 21, 0.99), rgba(18, 58, 52, 0.96));
      border: 1px solid rgba(129, 226, 178, 0.14);
      border-radius: 8px;
      box-shadow: var(--shadow);
      color: #f9fbf8;
      display: grid;
      gap: 18px;
      grid-template-columns: minmax(0, 0.72fr) minmax(420px, 1fr);
      padding: 18px;
    }
    .intro-panel .eyebrow { color: #8de1b5; }
    .intro-panel h2 {
      font-size: 1.8rem;
      margin: 0 0 7px;
      overflow-wrap: anywhere;
    }
    .description {
      color: #d7e2dc;
      font-size: 0.95rem;
      margin: 0;
      max-width: 760px;
    }
    .meta, .note {
      color: var(--muted);
      font-size: 0.86rem;
      margin: 7px 0 0;
    }
    .intro-panel .meta { color: #c8d6d0; }
    .intro-panel a { color: #b7d9ff; }
    .overview {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .overview-item {
      background: rgba(255, 255, 255, 0.075);
      border: 1px solid rgba(255, 255, 255, 0.13);
      border-radius: 8px;
      min-width: 0;
      padding: 12px;
    }
    .overview-item span,
    .metric span {
      color: inherit;
      display: block;
      font-size: 0.76rem;
      font-weight: 700;
      margin-bottom: 6px;
      opacity: 0.74;
      text-transform: uppercase;
    }
    .overview-item strong,
    .metric strong {
      display: block;
      font-size: 1.55rem;
      line-height: 1;
      overflow-wrap: anywhere;
    }
    .overview-item small,
    .metric small {
      display: block;
      font-size: 0.78rem;
      margin-top: 7px;
      opacity: 0.72;
    }
    .overview-item.blue { border-top: 3px solid #7fb4ff; }
    .overview-item.green { border-top: 3px solid #8de1b5; }
    .overview-item.cyan { border-top: 3px solid #67e8f9; }
    .overview-item.amber { border-top: 3px solid #f6c76b; }
    .report-section {
      background: rgba(17, 28, 23, 0.88);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 14px 34px rgba(0, 0, 0, 0.22);
      padding: 16px;
    }
    .section-heading {
      align-items: end;
      display: grid;
      gap: 4px;
      grid-template-columns: minmax(0, 1fr);
      margin-bottom: 12px;
    }
    .metrics {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
      margin: 0 0 12px;
    }
    .metric {
      background: var(--panel-soft);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
      padding: 10px 12px;
    }
    .metric span { color: var(--muted); opacity: 1; }
    .metric strong { color: var(--ink); font-size: 1.38rem; }
    .metric small { color: var(--muted); opacity: 1; }
    .content-grid {
      align-items: start;
      display: grid;
      gap: 12px;
      grid-template-columns: minmax(0, 1.42fr) minmax(320px, 0.82fr);
    }
    .chart-grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-top: 12px;
    }
    .chart {
      background: #0f1814;
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
      overflow-x: auto;
      padding: 10px;
    }
    .chart svg {
      display: block;
      height: auto;
      min-width: 560px;
      width: 100%;
    }
    .legend {
      color: var(--muted);
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      font-size: 0.78rem;
      margin-top: 8px;
    }
    .legend i {
      border-radius: 999px;
      display: inline-block;
      height: 8px;
      margin-right: 5px;
      width: 8px;
    }
    .table-panel {
      background: #0f1814;
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
      overflow-x: auto;
      padding: 10px;
    }
    .wide-table { margin-top: 12px; }
    table {
      border-collapse: collapse;
      font-size: 0.82rem;
      min-width: 420px;
      width: 100%;
    }
    caption {
      color: #d9e8e0;
      font-size: 0.82rem;
      font-weight: 750;
      margin: 0 0 7px;
      text-align: left;
    }
    th, td {
      border-bottom: 1px solid #1e2c26;
      padding: 7px 6px;
      text-align: left;
      vertical-align: middle;
    }
    th {
      color: var(--muted);
      font-size: 0.7rem;
      font-weight: 800;
      text-transform: uppercase;
    }
    tr:last-child td { border-bottom: 0; }
    td.num, th.num { text-align: right; }
    .split {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .bar-cell {
      min-width: 90px;
      width: 24%;
    }
    .bar {
      background: linear-gradient(90deg, #6eb6ff, #8de1b5);
      border-radius: 999px;
      display: block;
      height: 7px;
    }
    footer {
      color: var(--muted);
      font-size: 0.82rem;
      padding-bottom: 32px;
      padding-top: 4px;
    }
    @media (max-width: 980px) {
      .intro-panel,
      .content-grid,
      .chart-grid,
      .split {
        grid-template-columns: 1fr;
      }
      .overview { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 620px) {
      .topbar, .section-nav, .report-shell, footer {
        padding-left: 12px;
        padding-right: 12px;
      }
      .topbar {
        align-items: flex-start;
        flex-direction: column;
      }
      .run-meta { justify-content: flex-start; }
      .intro-panel, .report-section { padding: 12px; }
      .overview { grid-template-columns: 1fr; }
      h1 { font-size: 1.22rem; }
      .intro-panel h2 { font-size: 1.42rem; }
      .chart svg { min-width: 520px; }
    }"""


def render_overview_metrics(cards: list[tuple[str, int, str, str]]) -> str:
    content = []
    for label, value, detail, tone in cards:
        content.append(
            f'<div class="overview-item {attr(tone)}"><span>{esc(label)}</span>'
            f"<strong>{format_int(value)}</strong><small>{esc(detail)}</small></div>"
        )
    return '<div class="overview">' + "".join(content) + "</div>"


def render_metric_cards(cards: list[tuple[str, int, str]]) -> str:
    content = []
    for label, value, detail in cards:
        content.append(
            f'<div class="metric"><span>{esc(label)}</span><strong>{format_int(value)}</strong>'
            f"<small>{esc(detail)}</small></div>"
        )
    return '<div class="metrics">' + "".join(content) + "</div>"


def repository_counter_cards(snapshot: dict[str, Any]) -> list[tuple[str, int, str]]:
    counts = aggregate_counts_from_snapshot(snapshot)
    return [
        (label, int_or_zero(counts.get(key)), "latest snapshot")
        for key, label, _ in AGGREGATE_COUNTER_SPECS
    ]


def current_month_delta(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    month = max(str(row.get("month") or "") for row in rows)
    return sum(int_or_zero(row.get("delta")) for row in rows if row.get("month") == month)


def render_release_asset_table(title: str, rows: list[dict[str, Any]], limit: int = 30) -> str:
    if not rows:
        return f'<p class="note">No {esc(title.lower())}.</p>'
    body = []
    for row in rows[:limit]:
        asset = str(row.get("asset") or "")
        asset_url = row.get("asset_url")
        release = str(row.get("release") or "")
        release_url = row.get("release_url")
        rendered_asset = f'<a href="{attr(asset_url)}">{esc(asset)}</a>' if asset_url else esc(asset)
        rendered_release = (
            f'<a href="{attr(release_url)}">{esc(release)}</a>' if release_url else esc(release)
        )
        body.append(
            "<tr>"
            f"<td>{rendered_release}</td>"
            f"<td>{rendered_asset}</td>"
            f'<td class="num">{format_int(int_or_zero(row.get("download_count")))}</td>'
            f'<td class="num">{format_bytes(int_or_zero(row.get("size")))}</td>'
            "</tr>"
        )
    return (
        f"<table><caption>{esc(title)}</caption><thead><tr><th>Release</th>"
        '<th>Asset</th><th class="num">Downloads</th><th class="num">Size</th>'
        f"</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def render_monthly_delta_table(
    title: str,
    rows: list[dict[str, Any]],
    limit: int,
) -> str:
    nonzero = [row for row in rows if int_or_zero(row.get("delta")) > 0]
    visible = list(reversed(nonzero[-limit:]))
    if not visible:
        return f'<p class="note">No {esc(title.lower())} yet. At least two snapshots are needed for a non-zero observed delta.</p>'
    body = []
    for row in visible:
        label = str(row.get("label") or row.get("key") or "")
        url = row.get("url")
        rendered_label = f'<a href="{attr(url)}">{esc(label)}</a>' if url else esc(label)
        body.append(
            "<tr>"
            f"<td>{esc(row.get('month') or '')}</td>"
            f"<td>{rendered_label}</td>"
            f'<td class="num">{format_int(int_or_zero(row.get("delta")))}</td>'
            f'<td class="num">{format_int(int_or_zero(row.get("latest_count")))}</td>'
            "</tr>"
        )
    return (
        f"<table><caption>{esc(title)}</caption><thead><tr><th>Month</th>"
        '<th>Counter</th><th class="num">Observed delta</th>'
        f'<th class="num">Latest total</th></tr></thead><tbody>{"".join(body)}</tbody></table>'
    )


def render_line_chart(
    title: str,
    rows: list[dict[str, Any]],
    series: list[tuple[str, str, str]],
    height: int = 260,
    width: int = 880,
) -> str:
    if not rows:
        return f'<p class="note">No data available for {esc(title.lower())}.</p>'

    padding_left = 56
    padding_right = 18
    padding_top = 18
    padding_bottom = 42
    chart_width = width - padding_left - padding_right
    chart_height = height - padding_top - padding_bottom
    dates = [row["date"] for row in rows]
    max_value = max(float(row.get(field, 0) or 0) for row in rows for _, field, _ in series)
    max_value = max(1.0, max_value)
    y_top = nice_ceiling(max_value)

    def x_for(index: int) -> float:
        if len(rows) == 1:
            return padding_left + chart_width / 2
        return padding_left + index * chart_width / (len(rows) - 1)

    def y_for(value: float) -> float:
        return padding_top + chart_height - (value / y_top) * chart_height

    grid_parts = [
        f'<line x1="{padding_left}" y1="{padding_top}" x2="{padding_left}" y2="{padding_top + chart_height}" class="axis" />',
        f'<line x1="{padding_left}" y1="{padding_top + chart_height}" x2="{padding_left + chart_width}" y2="{padding_top + chart_height}" class="axis" />',
    ]
    for fraction in (0.25, 0.5, 0.75, 1.0):
        y = padding_top + chart_height - fraction * chart_height
        value = y_top * fraction
        grid_parts.append(
            f'<line x1="{padding_left}" y1="{y:.1f}" x2="{padding_left + chart_width}" y2="{y:.1f}" class="grid" />'
        )
        grid_parts.append(
            f'<text x="{padding_left - 8}" y="{y + 4:.1f}" text-anchor="end" class="label">{format_axis(value)}</text>'
        )

    label_indexes = sorted({0, len(rows) // 2, len(rows) - 1})
    for index in label_indexes:
        x = x_for(index)
        grid_parts.append(
            f'<text x="{x:.1f}" y="{height - 12}" text-anchor="middle" class="label">{esc(short_date(dates[index]))}</text>'
        )

    path_parts = []
    for label, field, color in series:
        points = [
            (x_for(index), y_for(float(row.get(field, 0) or 0)))
            for index, row in enumerate(rows)
        ]
        if len(points) == 1:
            x, y = points[0]
            path_parts.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{attr(color)}"><title>{esc(label)}</title></circle>'
            )
        else:
            path = " ".join(
                f"{'M' if index == 0 else 'L'} {x:.1f} {y:.1f}"
                for index, (x, y) in enumerate(points)
            )
            path_parts.append(
                f'<path d="{path}" fill="none" stroke="{attr(color)}" stroke-width="2.4">'
                f"<title>{esc(label)}</title></path>"
            )

    legend = "".join(
        f'<span><i style="background:{attr(color)}"></i>{esc(label)}</span>'
        for label, _, color in series
    )
    return f"""<div class="chart" role="img" aria-label="{attr(title)}">
  <svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
    <style>
      .axis {{ stroke: var(--chart-axis, #52685d); stroke-width: 1; }}
      .grid {{ stroke: var(--chart-grid, #22342d); stroke-width: 1; }}
      .label {{ fill: var(--chart-label, #a5b6ae); font: 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    </style>
    <text x="{padding_left}" y="13" class="label">{esc(title)}</text>
    {''.join(grid_parts)}
    {''.join(path_parts)}
  </svg>
  <div class="legend">{legend}</div>
</div>"""


def render_popularity_chart(
    snapshots: list[dict[str, Any]],
    list_key: str,
    name_key: str,
    title: str,
) -> str:
    samples_by_day: dict[str, dict[str, float]] = {}
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        rows = snapshot.get("traffic", {}).get(list_key, [])
        if not isinstance(rows, list):
            continue
        samples_by_day[day] = {
            str(row.get(name_key)): float(int_or_zero(row.get("uniques"))) / 14.0
            for row in rows
            if isinstance(row, dict) and row.get(name_key)
        }

    if not samples_by_day:
        return f'<p class="note">No historical data available for {esc(title.lower())}.</p>'

    totals = Counter()
    for sample in samples_by_day.values():
        for name, value in sample.items():
            totals[name] += value
    names = [name for name, _ in totals.most_common(5)]
    if not names:
        return f'<p class="note">No historical data available for {esc(title.lower())}.</p>'

    rows = []
    for day in sorted(samples_by_day):
        row: dict[str, Any] = {"date": day}
        for index, name in enumerate(names):
            row[f"s{index}"] = samples_by_day[day].get(name, 0.0)
        rows.append(row)

    colors = ["#7db7ff", "#81e2b2", "#b69cff", "#f4c76d", "#67e8f9"]
    series = [
        (truncate_middle(name, 42), f"s{index}", colors[index % len(colors)])
        for index, name in enumerate(names)
    ]
    return render_line_chart(title, rows, series)


def render_daily_table(
    title: str,
    rows: list[dict[str, Any]],
    first_metric: tuple[str, str],
    second_metric: tuple[str, str],
) -> str:
    recent = list(reversed(rows[-30:]))
    if not recent:
        return f'<p class="note">No {esc(title.lower())}.</p>'
    body = []
    for row in recent:
        body.append(
            "<tr>"
            f"<td>{esc(row['date'])}</td>"
            f"<td class=\"num\">{format_int(row[first_metric[1]])}</td>"
            f"<td class=\"num\">{format_int(row[second_metric[1]])}</td>"
            "</tr>"
        )
    return (
        f"<table><caption>{esc(title)}</caption><thead><tr><th>Date</th>"
        f'<th class="num">{esc(first_metric[0])}</th>'
        f'<th class="num">{esc(second_metric[0])}</th>'
        f"</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def render_event_table(
    title: str,
    rows: list[dict[str, Any]],
    date_key: str,
    label_key: str,
    url_key: str,
) -> str:
    recent = list(reversed(rows[-20:]))
    if not recent:
        return f'<p class="note">No {esc(title.lower())}.</p>'
    body = []
    for row in recent:
        label = str(row.get(label_key) or "")
        url = row.get(url_key)
        rendered_label = f'<a href="{attr(url)}">{esc(label)}</a>' if url else esc(label)
        body.append(
            f"<tr><td>{esc(date_part(row.get(date_key)) or '')}</td><td>{rendered_label}</td></tr>"
        )
    return f"<table><caption>{esc(title)}</caption><thead><tr><th>Date</th><th>Item</th></tr></thead><tbody>{''.join(body)}</tbody></table>"


def render_rank_table(
    rows: Any,
    label_key: str,
    server_url: str | None = None,
) -> str:
    if not isinstance(rows, list) or not rows:
        return '<p class="note">No data available.</p>'
    typed_rows = [row for row in rows if isinstance(row, dict)]
    max_count = max((int_or_zero(row.get("count")) for row in typed_rows), default=1)
    body = []
    for index, row in enumerate(typed_rows[:15], start=1):
        label = str(row.get(label_key) or "")
        width = 100 * int_or_zero(row.get("count")) / max(1, max_count)
        if server_url and label_key == "path" and label:
            url = f"{server_url.rstrip('/')}/{label.lstrip('/')}"
            rendered_label = f'<a href="{attr(url)}">{esc(label)}</a>'
        else:
            rendered_label = esc(label)
        body.append(
            "<tr>"
            f'<td class="num">{index}</td>'
            f"<td>{rendered_label}</td>"
            f'<td class="num">{format_int(int_or_zero(row.get("uniques")))}</td>'
            f'<td class="num">{format_int(int_or_zero(row.get("count")))}</td>'
            f'<td class="bar-cell"><span class="bar" style="width:{width:.1f}%"></span></td>'
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        '<th class="num">Rank</th><th>Name</th><th class="num">Uniques</th>'
        '<th class="num">Count</th><th>Share</th>'
        f"</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def date_part(value: Any) -> str:
    if not isinstance(value, str) or len(value) < 10:
        return ""
    return value[:10]


def short_date(value: str) -> str:
    if len(value) >= 10:
        return value[5:10]
    return value


def nice_ceiling(value: float) -> float:
    if value <= 1:
        return 1
    exponent = math.floor(math.log10(value))
    fraction = value / (10**exponent)
    if fraction <= 2:
        nice = 2
    elif fraction <= 5:
        nice = 5
    else:
        nice = 10
    return nice * (10**exponent)


def format_axis(value: float) -> str:
    if value >= 10 or value.is_integer():
        return format_int(int(round(value)))
    return f"{value:.1f}"


def format_int(value: int) -> str:
    return f"{value:,}"


def format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(max(0, value))
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(amount)} {unit}"
            return f"{amount:.1f} {unit}"
        amount /= 1024


def truncate_middle(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    left = max(1, (limit - 3) // 2)
    right = max(1, limit - 3 - left)
    return value[:left] + "..." + value[-right:]


def esc(value: Any) -> str:
    return html.escape(str(value), quote=False)


def attr(value: Any) -> str:
    return html.escape(str(value), quote=True)


def report_url(prefix: str, report_path: str) -> str:
    if not prefix or prefix.strip().lower() == "none":
        return ""
    return prefix.rstrip("/") + "/" + report_path.lstrip("/")


def run_command(
    args: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {shlex_join(args)}\n{result.stdout}"
        )
    return result


def shlex_join(args: list[str]) -> str:
    return " ".join(quote_arg(arg) for arg in args)


def quote_arg(value: str) -> str:
    if re.match(r"^[A-Za-z0-9_./:=@%+-]+$", value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


@contextlib.contextmanager
def git_auth_env(token: str) -> Iterable[dict[str, str]]:
    with tempfile.TemporaryDirectory(prefix="ghrs-askpass-") as tmp:
        askpass = Path(tmp) / "askpass.sh"
        askpass.write_text(
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            "  *Username*) printf '%s\\n' x-access-token ;;\n"
            "  *) printf '%s\\n' \"$GHRS_GIT_TOKEN\" ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        askpass.chmod(askpass.stat().st_mode | stat.S_IXUSR)
        env = os.environ.copy()
        env["GIT_ASKPASS"] = str(askpass)
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GHRS_GIT_TOKEN"] = token
        yield env


def configure_data_remote(workspace: Path, config: Config, env: dict[str, str]) -> str:
    remote_url = f"{config.github_server_url}/{config.data_repository}.git"
    existing = run_command(
        ["git", "remote", "get-url", DATA_REMOTE_NAME],
        workspace,
        check=False,
    )
    if existing.returncode == 0:
        run_command(["git", "remote", "set-url", DATA_REMOTE_NAME, remote_url], workspace, env)
    else:
        run_command(["git", "remote", "add", DATA_REMOTE_NAME, remote_url], workspace, env)
    return DATA_REMOTE_NAME


def prepare_data_worktree(config: Config, env: dict[str, str]) -> Path:
    workspace = config.workspace
    run_command(["git", "rev-parse", "--show-toplevel"], workspace)
    run_command(["git", "check-ref-format", "--branch", config.data_branch], workspace)
    data_remote = configure_data_remote(workspace, config, env)

    fetch = run_command(
        [
            "git",
            "fetch",
            "--no-tags",
            data_remote,
            f"+refs/heads/{config.data_branch}:refs/remotes/{data_remote}/{config.data_branch}",
        ],
        workspace,
        env,
        check=False,
    )
    branch_exists = fetch.returncode == 0
    worktree = Path(tempfile.mkdtemp(prefix="ghrs-data-")).resolve()

    if branch_exists:
        run_command(
            [
                "git",
                "worktree",
                "add",
                "--force",
                "-B",
                config.data_branch,
                str(worktree),
                f"{data_remote}/{config.data_branch}",
            ],
            workspace,
            env,
        )
    else:
        print(f"Data branch {config.data_branch!r} does not exist; creating it")
        init_branch = INIT_BRANCH_PREFIX + uuid.uuid4().hex
        run_command(["git", "worktree", "add", "--detach", str(worktree), "HEAD"], workspace, env)
        run_command(["git", "checkout", "--orphan", init_branch], worktree, env)
        run_command(["git", "rm", "-rf", "."], worktree, env, check=False)
        for child in worktree.iterdir():
            if child.name != ".git":
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()

    return worktree


def commit_and_push(worktree: Path, config: Config, env: dict[str, str]) -> bool:
    run_command(["git", "config", "user.name", config.commit_user_name], worktree, env)
    run_command(["git", "config", "user.email", config.commit_user_email], worktree, env)
    run_command(["git", "config", "commit.gpgsign", "false"], worktree, env)
    run_command(["git", "add", "."], worktree, env)
    diff = run_command(["git", "diff", "--cached", "--quiet"], worktree, env, check=False)
    if diff.returncode == 0:
        print("No changes to commit")
        return False
    message = f"Update GitHub stats for {config.repository}"
    run_command(["git", "commit", "-m", message], worktree, env)
    if config.push:
        run_command(
            ["git", "push", DATA_REMOTE_NAME, f"HEAD:refs/heads/{config.data_branch}"],
            worktree,
            env,
        )
    else:
        print("Skipping push because GHRS_PUSH is false")
    cleanup_transient_init_branch(worktree, env)
    return True


def cleanup_transient_init_branch(worktree: Path, env: dict[str, str]) -> None:
    current = run_command(["git", "branch", "--show-current"], worktree, env, check=False)
    branch = current.stdout.strip()
    if not branch.startswith(INIT_BRANCH_PREFIX):
        return
    detach = run_command(["git", "checkout", "--detach", "HEAD"], worktree, env, check=False)
    delete = run_command(["git", "branch", "-D", branch], worktree, env, check=False)
    if detach.returncode != 0 or delete.returncode != 0:
        print(f"warning: could not clean up transient local branch {branch!r}")


def write_outputs(values: dict[str, str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as file:
            for key, value in values.items():
                file.write(f"{key}={value}\n")
    for key, value in values.items():
        print(f"{key}: {value}")


def write_step_summary(summary: dict[str, Any]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    totals = summary.get("totals", {})
    aggregate_counts = summary.get("aggregate_counts", {})
    lines = [
        "## GitHub repository stats",
        "",
        f"- Repository: `{summary.get('repository', '')}`",
        f"- Snapshots: `{summary.get('snapshot_count', 0)}`",
        f"- Views: `{format_int(int_or_zero(totals.get('views')))} total`, `{format_int(int_or_zero(totals.get('unique_visitors')))} unique daily sum`",
        f"- Clones: `{format_int(int_or_zero(totals.get('clones')))} total`, `{format_int(int_or_zero(totals.get('unique_cloners')))} unique daily sum`",
        f"- Stargazers: `{format_int(int_or_zero(aggregate_counts.get('stargazers', totals.get('stargazers'))))}`",
        f"- Forks: `{format_int(int_or_zero(aggregate_counts.get('forks', totals.get('forks'))))}`",
        f"- Release asset downloads: `{format_int(int_or_zero(totals.get('release_asset_downloads')))}` across `{format_int(int_or_zero(totals.get('release_assets')))}` tracked assets",
        f"- Observed release downloads this month: `{format_int(int_or_zero(summary.get('monthly_release_asset_downloads')))}`",
    ]
    if summary.get("report_url"):
        lines.append(f"- Report: {summary['report_url']}")
    else:
        lines.append(f"- Report path: `{summary.get('report_path', '')}`")
    Path(summary_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def remove_worktree(workspace: Path, worktree: Path) -> None:
    run_command(["git", "worktree", "remove", "--force", str(worktree)], workspace, check=False)
    if worktree.exists():
        shutil.rmtree(worktree, ignore_errors=True)


def run_action(config: Config) -> dict[str, str]:
    owner, repo = config.repository.split("/", 1)
    rel_root = Path(owner) / repo
    snapshots_rel = rel_root / "snapshots"
    report_rel = rel_root / "latest-report" / "report.html"
    summary_rel = rel_root / "latest-report" / "summary.json"
    report_url_value = report_url(config.ghpages_prefix, report_rel.as_posix())

    snapshot = fetch_snapshot(config)
    snapshot_rel = snapshots_rel / snapshot_filename(snapshot["generated_at"])
    local_outputs: dict[str, str] = {}

    with git_auth_env(config.token) as env:
        worktree = prepare_data_worktree(config, env)
        try:
            write_json(worktree / snapshot_rel, snapshot)
            snapshots = load_snapshots(worktree / snapshots_rel)
            views = merge_daily_metric(snapshots, "views", "views")
            clones = merge_daily_metric(snapshots, "clones", "clones")
            stargazers = latest_non_empty_list(snapshots, "stargazers")
            forks = latest_non_empty_list(snapshots, "forks")
            latest = latest_snapshot(snapshots)
            release_assets = release_assets_from_snapshot(latest)
            monthly_release_downloads = monthly_counter_deltas(
                release_asset_observations(snapshots)
            )
            html_report = render_report(config.repository, snapshots, config, report_url_value)
            write_text(worktree / report_rel, html_report)
            summary = build_summary(
                config.repository,
                snapshots,
                views,
                clones,
                stargazers,
                forks,
                release_assets,
                monthly_release_downloads,
                report_rel.as_posix(),
                report_url_value,
            )
            write_json(worktree / summary_rel, summary)
            local_outputs = copy_generated_output(
                worktree,
                rel_root,
                report_rel,
                summary_rel,
                snapshot_rel,
                config,
            )
            committed = commit_and_push(worktree, config, env)
            write_step_summary(summary)
        finally:
            remove_worktree(config.workspace, worktree)

    return {
        "report-path": report_rel.as_posix(),
        "report-url": report_url_value,
        "snapshot-path": snapshot_rel.as_posix(),
        "committed": "true" if committed else "false",
        **local_outputs,
    }


def main() -> int:
    try:
        config = Config.from_env()
        outputs = run_action(config)
        write_outputs(outputs)
        return 0
    except (ApiError, ConfigError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
