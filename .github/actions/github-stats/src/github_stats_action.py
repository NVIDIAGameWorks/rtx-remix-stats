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
from datetime import date, datetime, timezone
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
    write_token: str
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
        write_token = env_value("GHRS_WRITE_TOKEN", token)
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
            write_token=write_token,
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
        published_at = release.get("published_at") or release.get("created_at")
        assets = release.get("assets", [])
        if not isinstance(assets, list):
            continue
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            asset_name = str(asset.get("name") or "")
            asset_id = asset.get("id")
            key = f"id:{asset_id}" if asset_id is not None else f"name:{tag_name}/{asset_name}"
            family_key = asset_family(asset_name)
            rows.append(
                {
                    "key": key,
                    "release": tag_name,
                    "release_name": release_name,
                    "release_url": release_url,
                    "release_published_at": published_at,
                    "asset": asset_name,
                    "asset_url": asset.get("browser_download_url"),
                    "download_count": int_or_zero(asset.get("download_count")),
                    "size": int_or_zero(asset.get("size")),
                    "content_type": asset.get("content_type"),
                    "updated_at": asset.get("updated_at"),
                    "label": f"{tag_name} / {asset_name}" if tag_name else asset_name,
                    "family_key": family_key,
                    "family_label": asset_family_label(family_key),
                }
            )
    rows.sort(key=lambda row: (row["download_count"], row["release"], row["asset"]), reverse=True)
    return rows


VERSION_SEGMENT_RE = re.compile(r"^\d+(?:\.\d+)*$")


def asset_family(name: str) -> str:
    """Derive a canonical 'family' key for an asset by stripping version segments.

    Examples:
        ``remix-1.4.2-release.zip`` -> ``remix-release.zip``
        ``remix-1.4.2-debug-symbols.zip`` -> ``remix-debug-symbols.zip``
        ``remix-toolkit-installer-1.4.2.1.zip`` -> ``remix-toolkit-installer.zip``
        ``remix-0.2.0.zip`` -> ``remix.zip``
    """
    if not name:
        return ""
    base, dot, ext = name.rpartition(".")
    if not dot:
        base, ext = name, ""
    parts = re.split(r"[-_]+", base)
    kept = [part for part in parts if part and not VERSION_SEGMENT_RE.match(part)]
    cleaned = "-".join(kept) if kept else base
    return f"{cleaned}.{ext}" if ext else cleaned


_FAMILY_LABEL_OVERRIDES: dict[str, str] = {
    "": "Source archive",
}


def asset_family_label(family_key: str) -> str:
    """Human-friendly label for an asset family key."""
    if family_key in _FAMILY_LABEL_OVERRIDES:
        return _FAMILY_LABEL_OVERRIDES[family_key]
    return family_key or "Other"


def release_collection_totals(
    snapshot: dict[str, Any], group_by: str
) -> list[dict[str, Any]]:
    """Aggregate a snapshot's release assets by collection.

    ``group_by`` may be ``"release"`` (release tag) or ``"family"`` (asset family).
    Returns rows with ``key``, ``label``, ``download_count``, ``asset_count``,
    plus optional ``url`` and ``published_at`` for the release variant.
    """
    if group_by not in {"release", "family"}:
        raise ValueError(f"unsupported group_by: {group_by!r}")
    assets = release_assets_from_snapshot(snapshot)
    buckets: dict[str, dict[str, Any]] = {}
    for asset in assets:
        if group_by == "release":
            key = str(asset.get("release") or "")
            label = str(asset.get("release_name") or key or "Unknown release")
            url = asset.get("release_url")
            published_at = asset.get("release_published_at")
        else:
            key = str(asset.get("family_key") or "")
            label = str(asset.get("family_label") or "Other")
            url = None
            published_at = None
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "label": label,
                "download_count": 0,
                "asset_count": 0,
                "url": url,
                "published_at": published_at,
                "release_count": 0,
                "_releases": set(),
            },
        )
        bucket["download_count"] += int_or_zero(asset.get("download_count"))
        bucket["asset_count"] += 1
        if url and not bucket.get("url"):
            bucket["url"] = url
        if published_at and not bucket.get("published_at"):
            bucket["published_at"] = published_at
        release_tag = str(asset.get("release") or "")
        if release_tag:
            bucket["_releases"].add(release_tag)
    rows: list[dict[str, Any]] = []
    for bucket in buckets.values():
        bucket["release_count"] = len(bucket.pop("_releases"))
        rows.append(bucket)
    rows.sort(
        key=lambda row: (row["download_count"], row["asset_count"]),
        reverse=True,
    )
    return rows


def release_collection_observations(
    snapshots: list[dict[str, Any]], group_by: str
) -> list[dict[str, Any]]:
    """Time-ordered observations of cumulative downloads grouped by collection."""
    observations: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        for row in release_collection_totals(snapshot, group_by):
            observations.append(
                {
                    "date": day,
                    "key": row["key"],
                    "label": row["label"],
                    "count": row["download_count"],
                    "url": row.get("url"),
                }
            )
    return observations


def collection_timeline_rows(
    snapshots: list[dict[str, Any]],
    group_by: str,
    keys: list[str],
) -> list[dict[str, Any]]:
    """Build line-chart rows: per-day cumulative downloads for a fixed key set."""
    rows: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        totals = {
            entry["key"]: entry["download_count"]
            for entry in release_collection_totals(snapshot, group_by)
        }
        row: dict[str, Any] = {"date": day}
        for key in keys:
            row[key] = totals.get(key, 0)
        rows.append(row)
    return rows


def release_asset_timeline_rows(
    snapshots: list[dict[str, Any]], keys: list[str]
) -> list[dict[str, Any]]:
    """Build line-chart rows: per-day cumulative downloads for individual assets."""
    rows: list[dict[str, Any]] = []
    for snapshot in sorted(snapshots, key=lambda row: row.get("generated_at", "")):
        day = date_part(snapshot.get("generated_at"))
        if not day:
            continue
        totals = {
            asset["key"]: asset["download_count"]
            for asset in release_assets_from_snapshot(snapshot)
        }
        row: dict[str, Any] = {"date": day}
        for key in keys:
            row[key] = totals.get(key, 0)
        rows.append(row)
    return rows


def build_timeline_series(
    items: list[dict[str, Any]],
    keys: list[str],
    label_field: str = "label",
) -> list[tuple[str, str, str]]:
    """Build (label, field, color) tuples for ``render_line_chart``."""
    by_key = {item.get("key"): item for item in items}
    series: list[tuple[str, str, str]] = []
    for index, key in enumerate(keys):
        item = by_key.get(key, {})
        label = str(item.get(label_field) or item.get("label") or key)
        color = COLLECTION_LINE_COLORS[index % len(COLLECTION_LINE_COLORS)]
        series.append((truncate_middle(label, 48), key, color))
    return series


COLLECTION_LINE_COLORS: tuple[str, ...] = (
    "#7db7ff",
    "#81e2b2",
    "#f4c76d",
    "#fb7185",
    "#67e8f9",
    "#b69cff",
    "#ffa36b",
    "#9bd35a",
)


def compute_share_rows(
    rows: list[dict[str, Any]], value_key: str
) -> list[dict[str, Any]]:
    """Annotate rows with ``share`` (0-100 percent) and ``share_label`` strings."""
    total = sum(int_or_zero(row.get(value_key)) for row in rows)
    annotated: list[dict[str, Any]] = []
    for row in rows:
        value = int_or_zero(row.get(value_key))
        share = 100 * value / total if total else 0.0
        annotated_row = dict(row)
        annotated_row["share"] = share
        annotated_row["share_label"] = f"{share:.1f}%"
        annotated.append(annotated_row)
    return annotated


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


SUPPORTED_DELTA_BUCKETS: tuple[str, ...] = ("day", "week", "month")
BUCKET_ADJECTIVE: dict[str, str] = {"day": "daily", "week": "weekly", "month": "monthly"}
BUCKET_NOUN_LABEL: dict[str, str] = {"day": "Daily", "week": "Weekly", "month": "Monthly"}


def bucket_label(date_str: str, bucket: str) -> str:
    """Map a YYYY-MM-DD date to a bucket label.

    Buckets:
        ``day``   -> ``2026-04-29``
        ``week``  -> ``2026-W17`` (ISO calendar week)
        ``month`` -> ``2026-04``
    """
    if not date_str:
        return ""
    if bucket == "day":
        return date_str[:10] if len(date_str) >= 10 else ""
    if bucket == "month":
        return date_str[:7] if len(date_str) >= 7 else ""
    if bucket == "week":
        if len(date_str) < 10:
            return ""
        try:
            year, week, _ = date.fromisoformat(date_str[:10]).isocalendar()
        except ValueError:
            return ""
        return f"{year}-W{week:02d}"
    raise ValueError(f"unsupported bucket: {bucket!r}")


def bucketed_counter_deltas(
    observations: list[dict[str, Any]], bucket: str = "week"
) -> list[dict[str, Any]]:
    """Aggregate cumulative-counter ``observations`` into per-bucket per-key deltas.

    Each observation should look like::

        {"date": "2026-04-15", "key": "asset-1", "label": "v1 / app.zip",
         "count": 16, "url": "...", "release": "v1", "asset": "app.zip"}

    Returns rows sorted by ``(bucket, label)`` containing the summed delta for
    each ``(bucket, key)`` and the latest cumulative count seen for that key
    inside the bucket.
    """
    if bucket not in SUPPORTED_DELTA_BUCKETS:
        raise ValueError(f"unsupported bucket: {bucket!r}")

    previous: dict[str, int] = {}
    accumulator: dict[tuple[str, str], dict[str, Any]] = {}
    for observation in sorted(
        observations, key=lambda row: (row.get("date", ""), row.get("key", ""))
    ):
        day = date_part(observation.get("date"))
        if not day:
            continue
        key = str(observation.get("key") or "")
        current = int_or_zero(observation.get("count"))
        previous_count = previous.get(key)
        delta = 0 if previous_count is None else max(0, current - previous_count)
        previous[key] = current
        bucket_key = bucket_label(day, bucket)
        if not bucket_key:
            continue
        composite = (bucket_key, key)
        entry = accumulator.setdefault(
            composite,
            {
                "bucket": bucket_key,
                "bucket_kind": bucket,
                "key": key,
                "label": observation.get("label") or key,
                "delta": 0,
                "latest_count": current,
                "url": observation.get("url"),
                "release": observation.get("release"),
                "asset": observation.get("asset"),
            },
        )
        entry["delta"] += delta
        entry["latest_count"] = current
        if observation.get("url"):
            entry["url"] = observation.get("url")
    return sorted(accumulator.values(), key=lambda row: (row["bucket"], row["label"]))


def bucketed_timeline_rows(
    observations: list[dict[str, Any]],
    bucket: str,
    keys: list[str],
) -> list[dict[str, Any]]:
    """Pivot bucketed deltas into chart rows: ``[{"date": bucket, key: delta, ...}]``.

    Buckets with no data for any of ``keys`` are omitted. A bucket is included
    if any of the listed keys had a non-zero delta in that bucket.
    """
    bucket_data: dict[str, dict[str, int]] = {}
    for row in bucketed_counter_deltas(observations, bucket):
        bucket_data.setdefault(row["bucket"], {})[row["key"]] = int_or_zero(
            row.get("delta")
        )
    rows: list[dict[str, Any]] = []
    for bucket_key in sorted(bucket_data.keys()):
        record: dict[str, Any] = {"date": bucket_key}
        bucket_for_keys = bucket_data[bucket_key]
        for key in keys:
            record[key] = bucket_for_keys.get(key, 0)
        if any(record[key] > 0 for key in keys):
            rows.append(record)
    return rows


def bucket_daily_metric_rows(
    rows: list[dict[str, Any]],
    bucket: str,
    value_keys: tuple[str, ...] = ("count", "uniques"),
) -> list[dict[str, Any]]:
    """Aggregate per-day metric rows (such as ``views`` or ``clones``) into buckets.

    Inputs are rows with a ``date`` (``YYYY-MM-DD``) key plus one or more numeric
    fields. Returns a list of rows ``{"date": bucket_label, value_key: sum, ...}``
    sorted ascending by bucket label. ``day`` returns a shallow copy of the
    inputs (with non-numeric fields preserved).
    """
    if bucket not in SUPPORTED_DELTA_BUCKETS:
        raise ValueError(f"unsupported bucket: {bucket!r}")
    if bucket == "day":
        return [
            {
                "date": row.get("date"),
                **{key: int_or_zero(row.get(key)) for key in value_keys},
            }
            for row in rows
        ]
    accumulator: dict[str, dict[str, Any]] = {}
    for row in rows:
        day = date_part(row.get("date") or "")
        if not day:
            continue
        bk = bucket_label(day, bucket)
        if not bk:
            continue
        entry = accumulator.setdefault(
            bk, {"date": bk, **{key: 0 for key in value_keys}}
        )
        for key in value_keys:
            entry[key] += int_or_zero(row.get(key))
    return [accumulator[bk] for bk in sorted(accumulator)]


def bucket_event_counts(
    events: list[dict[str, Any]],
    date_key: str,
    bucket: str,
) -> list[dict[str, Any]]:
    """Count events per bucket. Each event must carry a ``date_key`` string.

    Returns ``[{"date": bucket_label, "count": events_in_bucket}]``.
    """
    if bucket not in SUPPORTED_DELTA_BUCKETS:
        raise ValueError(f"unsupported bucket: {bucket!r}")
    counts: dict[str, int] = {}
    for event in events:
        day = date_part(event.get(date_key))
        if not day:
            continue
        bk = bucket_label(day, bucket)
        if not bk:
            continue
        counts[bk] = counts.get(bk, 0) + 1
    return [{"date": bk, "count": counts[bk]} for bk in sorted(counts)]


def bucketed_total_deltas(
    observations: list[dict[str, Any]], bucket: str
) -> list[dict[str, Any]]:
    """Sum delta across all keys per bucket. Returns ``[{date, count}]`` rows."""
    bucket_data: dict[str, int] = {}
    for row in bucketed_counter_deltas(observations, bucket):
        bucket_data[row["bucket"]] = bucket_data.get(row["bucket"], 0) + int_or_zero(
            row.get("delta")
        )
    return [
        {"date": bucket_key, "count": delta}
        for bucket_key, delta in sorted(bucket_data.items())
    ]


def latest_bucket_delta(rows: list[dict[str, Any]]) -> int:
    """Sum of ``delta`` for the most recent bucket present in ``rows``."""
    if not rows:
        return 0
    bucket_field = "bucket" if any("bucket" in row for row in rows) else "month"
    latest = max(str(row.get(bucket_field) or "") for row in rows)
    if not latest:
        return 0
    return sum(
        int_or_zero(row.get("delta"))
        for row in rows
        if str(row.get(bucket_field) or "") == latest
    )


def monthly_counter_deltas(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Backward-compatible wrapper around :func:`bucketed_counter_deltas`.

    Returned rows expose both ``bucket`` and the legacy ``month`` alias.
    """
    rows = bucketed_counter_deltas(observations, "month")
    for row in rows:
        row["month"] = row["bucket"]
    return rows


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


def load_report_summaries(worktree: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(worktree.glob("*/*/latest-report/summary.json")):
        try:
            with path.open("r", encoding="utf-8") as file:
                summary = json.load(file)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(summary, dict):
            continue
        if not summary.get("report_path"):
            summary["report_path"] = (
                path.with_name("report.html").relative_to(worktree).as_posix()
            )
        summaries.append(summary)
    summaries.sort(key=lambda row: str(row.get("repository") or row.get("report_path") or ""))
    return summaries


def render_index(summaries: list[dict[str, Any]], config: Config | None) -> str:
    generated_at = max(
        (str(summary.get("generated_at") or "") for summary in summaries),
        default="",
    )
    data_repository = config.data_repository if config else ""
    total_views = sum(
        int_or_zero(summary_child(summary, "totals").get("views"))
        for summary in summaries
    )
    total_clones = sum(
        int_or_zero(summary_child(summary, "totals").get("clones"))
        for summary in summaries
    )
    total_stars = sum(
        int_or_zero(summary_child(summary, "aggregate_counts").get("stargazers"))
        for summary in summaries
    )
    total_release_downloads = sum(
        int_or_zero(summary_child(summary, "totals").get("release_asset_downloads"))
        for summary in summaries
    )
    report_count = len(summaries)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Repository Statistics</title>
  <style>
{REPORT_CSS}
{INDEX_CSS}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="brand">
      <span class="brand-mark" aria-hidden="true">GS</span>
      <div>
        <p class="eyebrow">Service intelligence</p>
        <h1>Repository stats</h1>
      </div>
    </div>
    <div class="run-meta">
      {f"<span>{esc(generated_at)} UTC</span>" if generated_at else ""}
      <span>{report_count} report(s)</span>
    </div>
  </header>
  <main class="report-shell index-shell">
    <section class="intro-panel" aria-label="Report summary">
      <div>
        <p class="eyebrow">Latest reports</p>
        <h2>{esc(data_repository or "GitHub repository statistics")}</h2>
        <p class="description">Generated repository traffic, community, counter, and release asset download reports.</p>
      </div>
      {render_overview_metrics([
          ("Reports", report_count, "repositories tracked", "blue"),
          ("Views", total_views, "daily total sum", "green"),
          ("Stars", total_stars, "latest counters", "cyan"),
          ("Release downloads", total_release_downloads, "tracked assets", "amber"),
      ])}
    </section>
    <section class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Repositories</p>
        <h2>Available reports</h2>
      </div>
      {render_index_cards(summaries)}
    </section>
  </main>
  <footer>
    Generated by github-stats-action-nv.
  </footer>
</body>
</html>
"""


def summary_child(summary: dict[str, Any], key: str) -> dict[str, Any]:
    value = summary.get(key)
    return value if isinstance(value, dict) else {}


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
    asset_observations = release_asset_observations(snapshots)
    release_observations = release_collection_observations(snapshots, "release")
    family_observations = release_collection_observations(snapshots, "family")
    monthly_release_downloads = monthly_counter_deltas(asset_observations)
    weekly_release_downloads = bucketed_counter_deltas(asset_observations, "week")
    daily_release_downloads = bucketed_counter_deltas(asset_observations, "day")
    monthly_release_collection_deltas = monthly_counter_deltas(release_observations)
    monthly_family_collection_deltas = monthly_counter_deltas(family_observations)
    release_collections = release_collection_totals(latest, "release")
    family_collections = release_collection_totals(latest, "family")
    top_asset_keys = [row["key"] for row in release_assets[:8]]
    top_release_keys = [row["key"] for row in release_collections[:6]]
    top_family_keys = [row["key"] for row in family_collections[:6]]
    asset_timeline_series = build_timeline_series(
        release_assets[:8], top_asset_keys, "label"
    )
    release_timeline_series = build_timeline_series(
        release_collections[:6], top_release_keys, "label"
    )
    family_timeline_series = build_timeline_series(
        family_collections[:6], top_family_keys, "label"
    )
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

    def _traffic_panel(
        rows: list[dict[str, Any]],
        bucket: str,
        title_prefix: str,
        total_label: str,
        unique_label: str,
        total_color: str,
        unique_color: str,
    ) -> str:
        bucketed = bucket_daily_metric_rows(rows, bucket, ("count", "uniques"))
        return render_line_chart(
            f"{title_prefix} per {bucket}",
            bucketed,
            [
                (total_label, "count", total_color),
                (unique_label, "uniques", unique_color),
            ],
        )

    def _event_panel(
        events: list[dict[str, Any]],
        date_field: str,
        bucket: str,
        title: str,
        series_label: str,
        color: str,
    ) -> str:
        return render_stacked_bar_chart(
            f"{title} per {bucket}",
            bucket_event_counts(events, date_field, bucket),
            [(series_label, "count", color)],
            height=220,
            y_axis_label=f"{series_label.lower()} / {bucket}",
        )

    views_tabs = render_bucket_tabs(
        "traffic-views",
        {
            bucket: _traffic_panel(
                views, bucket, "Repository views",
                "Total views", "Unique visitors", "#7db7ff", "#81e2b2",
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    clones_tabs = render_bucket_tabs(
        "traffic-clones",
        {
            bucket: _traffic_panel(
                clones, bucket, "Repository clones",
                "Total clones", "Unique cloners", "#b69cff", "#f4c76d",
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    stars_tabs = render_bucket_tabs(
        "stargazers",
        {
            bucket: _event_panel(
                stargazers, "starred_at", bucket,
                "New stargazers", "New stars", "#67e8f9",
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    forks_tabs = render_bucket_tabs(
        "forks",
        {
            bucket: _event_panel(
                forks, "created_at", bucket,
                "New forks", "New forks", "#fb7185",
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    aggregate_observations = aggregate_counter_observations(snapshots)
    counter_chart_keys = [key for key, _, _ in AGGREGATE_COUNTER_SPECS]
    counter_chart_series = [
        (label, key, color) for key, label, color in AGGREGATE_COUNTER_SPECS
    ]
    aggregate_tabs = render_bucket_tabs(
        "repository-counters-deltas",
        {
            bucket: render_stacked_bar_chart(
                f"Repository counter changes per {bucket}",
                bucketed_timeline_rows(
                    aggregate_observations, bucket, counter_chart_keys
                ),
                counter_chart_series,
                y_axis_label=f"changes / {bucket}",
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    release_downloads_svg = render_line_chart(
        "Tracked release asset downloads over time (cumulative)",
        release_total_timeline,
        [("Cumulative", "count", "#7db7ff")],
    )

    def _total_panel(bucket: str) -> str:
        return render_stacked_bar_chart(
            f"Downloads per {bucket} (delta between snapshots)",
            bucketed_total_deltas(asset_observations, bucket),
            [("Downloads added", "count", "#81e2b2")],
            height=220,
            y_axis_label=f"downloads / {bucket}",
        )

    def _series_panel(
        observations: list[dict[str, Any]],
        keys: list[str],
        series: list[tuple[str, str, str]],
        title_prefix: str,
        bucket: str,
    ) -> str:
        return render_stacked_bar_chart(
            f"{title_prefix} {BUCKET_ADJECTIVE[bucket]} downloads (delta)",
            bucketed_timeline_rows(observations, bucket, keys),
            series,
            y_axis_label=f"downloads / {bucket}",
        )

    release_total_tabs = render_bucket_tabs(
        "release-total-deltas",
        {bucket: _total_panel(bucket) for bucket in SUPPORTED_DELTA_BUCKETS},
        default_bucket="day",
    )
    release_assets_tabs = render_bucket_tabs(
        "releases-by-asset-deltas",
        {
            bucket: _series_panel(
                asset_observations, top_asset_keys, asset_timeline_series,
                "Top assets", bucket,
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    release_by_release_tabs = render_bucket_tabs(
        "releases-by-release-deltas",
        {
            bucket: _series_panel(
                release_observations, top_release_keys, release_timeline_series,
                "Top releases", bucket,
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    release_by_family_tabs = render_bucket_tabs(
        "releases-by-family-deltas",
        {
            bucket: _series_panel(
                family_observations, top_family_keys, family_timeline_series,
                "Asset families", bucket,
            )
            for bucket in SUPPORTED_DELTA_BUCKETS
        },
        default_bucket="day",
    )
    release_release_count = len({row["release"] for row in release_assets if row.get("release")})
    release_family_count = len({row["family_key"] for row in release_assets if row.get("family_key")})
    asset_share_rows = compute_share_rows(release_assets, "download_count")
    release_collection_share_rows = compute_share_rows(release_collections, "download_count")
    family_collection_share_rows = compute_share_rows(family_collections, "download_count")
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
        {views_tabs}
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
        {clones_tabs}
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
        {stars_tabs}
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
        {forks_tabs}
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
        {aggregate_tabs}
        <div class="table-panel">{render_monthly_delta_table("Monthly observed repository counter changes", monthly_aggregate_deltas, 40)}</div>
      </div>
    </section>
    <section id="releases" class="report-section">
      <div class="section-heading">
        <p class="eyebrow">Delivery</p>
        <h2>Release asset downloads</h2>
        <p class="note">GitHub exposes download counts for uploaded release assets. Generated source ZIP and TAR archives do not expose download counters. Each asset is also grouped into a release (collection by tag) and an asset family (collection by canonical name).</p>
      </div>
      {render_metric_cards([
          ("Tracked assets", len(release_assets), "uploaded assets"),
          ("Releases", release_release_count, "tags with assets"),
          ("Asset families", release_family_count, "canonical names"),
          ("Total downloads", total_release_downloads, "latest cumulative count"),
          ("Today", latest_bucket_delta(daily_release_downloads), "latest day-over-day delta"),
          ("This week", latest_bucket_delta(weekly_release_downloads), "delta across all assets"),
          ("This month", current_month_delta(monthly_release_downloads), "delta across all assets"),
      ])}
      {release_total_tabs}
      <div class="content-grid">
        {release_downloads_svg}
        <div class="table-panel">{render_monthly_delta_table("Monthly observed release asset downloads", monthly_release_downloads, 40)}</div>
      </div>
      <div class="release-subsections">
        <div class="release-subsection" id="releases-by-asset">
          <h3>By individual asset</h3>
          <p class="note">Top {min(8, len(release_assets))} of {len(release_assets)} tracked assets, ranked by total downloads. The chart on the left shows download deltas between snapshots (use the granularity toggle to switch between daily, weekly and monthly buckets); the bars on the right rank assets by their lifetime total.</p>
          <div class="content-grid">
            {release_assets_tabs}
            {render_horizontal_bar_chart(
                "Top assets by downloads",
                asset_share_rows,
                "label",
                "download_count",
                color="#7db7ff",
                limit=10,
                secondary_key="share_label",
                secondary_label="Share of total",
                label_url_key="asset_url",
            )}
          </div>
          <div class="table-panel wide-table">{render_release_asset_table("Top assets", asset_share_rows, limit=30)}</div>
        </div>
        <div class="release-subsection" id="releases-by-release">
          <h3>By release (collection of assets per tag)</h3>
          <p class="note">{release_release_count} release tag(s) carry uploaded assets. Cumulative downloads roll up every asset attached to a tag. The bar chart shows download deltas per tag at the selected granularity.</p>
          <div class="content-grid">
            {release_by_release_tabs}
            {render_horizontal_bar_chart(
                "Top releases by downloads",
                release_collection_share_rows,
                "label",
                "download_count",
                color="#81e2b2",
                limit=12,
                secondary_key="share_label",
                secondary_label="Share of total",
                label_url_key="url",
            )}
          </div>
          <div class="table-panel wide-table">{render_release_collection_table(
              "Releases ranked by total downloads",
              release_collection_share_rows,
              limit=40,
          )}</div>
          <div class="table-panel wide-table">{render_monthly_delta_table(
              "Monthly observed downloads by release",
              monthly_release_collection_deltas,
              40,
          )}</div>
        </div>
        <div class="release-subsection" id="releases-by-family">
          <h3>By asset family (collection of versions per canonical name)</h3>
          <p class="note">Family keys are derived by stripping version segments from each asset name. They group the same artifact across releases, e.g. <code>{esc("remix-1.4.2-release.zip")}</code> &rarr; <code>{esc("remix-release.zip")}</code>. The bar chart shows download deltas summed across every version that belongs to each family, at the selected granularity.</p>
          <div class="content-grid">
            {release_by_family_tabs}
            {render_horizontal_bar_chart(
                "Top asset families by downloads",
                family_collection_share_rows,
                "label",
                "download_count",
                color="#f4c76d",
                limit=12,
                secondary_key="share_label",
                secondary_label="Share of total",
            )}
          </div>
          <div class="table-panel wide-table">{render_family_collection_table(
              "Asset families ranked by total downloads",
              family_collection_share_rows,
              limit=40,
          )}</div>
          <div class="table-panel wide-table">{render_monthly_delta_table(
              "Monthly observed downloads by asset family",
              monthly_family_collection_deltas,
              40,
          )}</div>
        </div>
      </div>
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
      grid-template-columns: minmax(0, 0.72fr) minmax(0, 1fr);
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
      grid-template-columns: minmax(0, 1.42fr) minmax(0, 0.82fr);
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
      max-width: 100%;
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
      min-width: 56px;
      width: 18%;
    }
    .bar {
      background: linear-gradient(90deg, #6eb6ff, #8de1b5);
      border-radius: 999px;
      display: block;
      height: 7px;
    }
    .bucket-tabs {
      display: grid;
      gap: 10px;
      min-width: 0;
      position: relative;
    }
    .bucket-tabs > .bucket-radio {
      height: 1px;
      margin: -1px;
      opacity: 0;
      overflow: hidden;
      pointer-events: none;
      position: absolute;
      width: 1px;
    }
    .bucket-tab-strip {
      align-items: center;
      color: var(--muted);
      display: flex;
      flex-wrap: wrap;
      font-size: 0.74rem;
      gap: 6px;
      letter-spacing: 0;
    }
    .bucket-tab-strip::before {
      content: "Granularity:";
      font-weight: 700;
      letter-spacing: 0;
      margin-right: 4px;
      text-transform: uppercase;
    }
    .bucket-tab-label {
      background: var(--panel-soft);
      border: 1px solid var(--line);
      border-radius: 999px;
      color: #dcebe4;
      cursor: pointer;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0;
      padding: 5px 13px;
      transition: background 120ms ease, color 120ms ease, border-color 120ms ease;
      user-select: none;
    }
    .bucket-tab-label:hover {
      border-color: var(--line-strong);
      color: var(--green);
    }
    .bucket-tabs > .bucket-radio:focus-visible + .bucket-tab-strip > .bucket-tab-label,
    .bucket-tabs > .bucket-radio:focus-visible ~ .bucket-tab-strip > .bucket-tab-label {
      outline: 2px solid transparent;
    }
    .bucket-tabs:has(> .bucket-radio[id$="-day"]:checked) .bucket-tab-strip > label[data-bucket="day"],
    .bucket-tabs:has(> .bucket-radio[id$="-week"]:checked) .bucket-tab-strip > label[data-bucket="week"],
    .bucket-tabs:has(> .bucket-radio[id$="-month"]:checked) .bucket-tab-strip > label[data-bucket="month"] {
      background: var(--green);
      border-color: var(--green);
      color: #0d1714;
    }
    .bucket-tabs > .bucket-panel { display: none; min-width: 0; }
    .bucket-tabs > .bucket-radio[id$="-day"]:checked ~ .bucket-panel[data-bucket="day"],
    .bucket-tabs > .bucket-radio[id$="-week"]:checked ~ .bucket-panel[data-bucket="week"],
    .bucket-tabs > .bucket-radio[id$="-month"]:checked ~ .bucket-panel[data-bucket="month"] {
      display: block;
    }
    .release-subsections {
      display: grid;
      gap: 14px;
      margin-top: 14px;
    }
    .release-subsection {
      background: #0f1814;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
    }
    .release-subsection h3 {
      margin: 0 0 4px;
    }
    .release-subsection .note {
      margin: 0 0 12px;
    }
    .release-subsection .content-grid {
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
    }
    .hbar-chart {
      background: #0f1814;
      border: 1px solid var(--line);
      border-radius: 8px;
      display: grid;
      gap: 6px;
      min-width: 0;
      padding: 12px;
    }
    .hbar-chart .chart-title {
      color: #d9e8e0;
      font-size: 0.82rem;
      font-weight: 750;
      letter-spacing: 0;
      margin: 0 0 4px;
    }
    .hbar-row {
      align-items: center;
      column-gap: 10px;
      display: grid;
      grid-template-columns: minmax(0, 0.95fr) minmax(0, 1.6fr) auto auto;
      min-width: 0;
      row-gap: 0;
    }
    .hbar-label {
      color: #dcebe4;
      font-size: 0.82rem;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .hbar-label a {
      color: inherit;
      text-decoration: none;
    }
    .hbar-label a:hover {
      color: var(--green);
      text-decoration: underline;
    }
    .hbar-track {
      background: rgba(125, 183, 255, 0.12);
      border-radius: 999px;
      height: 8px;
      min-width: 0;
      overflow: hidden;
      position: relative;
    }
    .hbar-fill {
      border-radius: 999px;
      display: block;
      height: 100%;
    }
    .hbar-value {
      color: var(--ink);
      font-size: 0.82rem;
      font-variant-numeric: tabular-nums;
      font-weight: 700;
      text-align: right;
    }
    .hbar-secondary {
      color: var(--muted);
      font-size: 0.74rem;
      font-variant-numeric: tabular-nums;
      text-align: right;
    }
    @media (max-width: 720px) {
      .hbar-row {
        grid-template-columns: minmax(0, 1.2fr) auto;
        row-gap: 4px;
      }
      .hbar-track {
        grid-column: 1 / -1;
      }
      .hbar-value {
        grid-column: auto;
      }
      .hbar-secondary {
        grid-column: auto;
      }
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
      .split,
      .release-subsection .content-grid {
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
    }"""


INDEX_CSS = """    .index-shell {
      padding-top: 16px;
    }
    .repo-list {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }
    .repo-card {
      background: #0f1814;
      border: 1px solid var(--line);
      border-radius: 8px;
      display: grid;
      gap: 12px;
      min-width: 0;
      padding: 14px;
    }
    .repo-card h3 {
      font-size: 1.05rem;
      margin: 0;
      overflow-wrap: anywhere;
    }
    .repo-card h3 a {
      color: var(--ink);
      text-decoration: none;
    }
    .repo-card h3 a:hover {
      color: var(--green);
    }
    .repo-card .meta {
      margin: 0;
    }
    .repo-metrics {
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .repo-metrics div {
      background: var(--panel-soft);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px;
    }
    .repo-metrics span {
      color: var(--muted);
      display: block;
      font-size: 0.7rem;
      font-weight: 800;
      text-transform: uppercase;
    }
    .repo-metrics strong {
      display: block;
      font-size: 1.12rem;
      margin-top: 4px;
    }"""


def render_index_cards(summaries: list[dict[str, Any]]) -> str:
    if not summaries:
        return '<p class="note">No reports have been generated yet.</p>'

    cards = []
    for summary in summaries:
        repository = str(summary.get("repository") or "Unknown repository")
        report_path = str(summary.get("report_path") or "")
        generated_at = str(summary.get("generated_at") or "")
        totals = summary.get("totals") if isinstance(summary.get("totals"), dict) else {}
        aggregate_counts = (
            summary.get("aggregate_counts")
            if isinstance(summary.get("aggregate_counts"), dict)
            else {}
        )
        views = format_int(int_or_zero(totals.get("views")))
        clones = format_int(int_or_zero(totals.get("clones")))
        stars = format_int(int_or_zero(aggregate_counts.get("stargazers")))
        downloads = format_int(int_or_zero(totals.get("release_asset_downloads")))
        href = report_path or "#"
        cards.append(
            '<article class="repo-card">'
            f'<h3><a href="{attr(href)}">{esc(repository)}</a></h3>'
            f'<p class="meta">Updated {esc(generated_at or "unknown")}</p>'
            '<div class="repo-metrics">'
            f"<div><span>Views</span><strong>{views}</strong></div>"
            f"<div><span>Clones</span><strong>{clones}</strong></div>"
            f"<div><span>Stars</span><strong>{stars}</strong></div>"
            f"<div><span>Downloads</span><strong>{downloads}</strong></div>"
            "</div>"
            "</article>"
        )
    return '<div class="repo-list">' + "".join(cards) + "</div>"


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
    visible = list(rows[:limit])
    max_downloads = max(
        (int_or_zero(row.get("download_count")) for row in visible), default=1
    ) or 1
    body = []
    for row in visible:
        asset = str(row.get("asset") or "")
        asset_url = row.get("asset_url")
        release = str(row.get("release") or "")
        release_url = row.get("release_url")
        family_label = str(row.get("family_label") or "")
        rendered_asset = f'<a href="{attr(asset_url)}">{esc(asset)}</a>' if asset_url else esc(asset)
        rendered_release = (
            f'<a href="{attr(release_url)}">{esc(release)}</a>' if release_url else esc(release)
        )
        download_count = int_or_zero(row.get("download_count"))
        share_pct = 100 * download_count / max(1, max_downloads)
        body.append(
            "<tr>"
            f"<td>{rendered_release}</td>"
            f"<td>{rendered_asset}</td>"
            f"<td>{esc(family_label)}</td>"
            f'<td class="num">{format_int(download_count)}</td>'
            f'<td class="num">{format_bytes(int_or_zero(row.get("size")))}</td>'
            f'<td class="bar-cell"><span class="bar" style="width:{share_pct:.1f}%"></span></td>'
            "</tr>"
        )
    return (
        f"<table><caption>{esc(title)}</caption><thead><tr><th>Release</th>"
        '<th>Asset</th><th>Family</th><th class="num">Downloads</th><th class="num">Size</th>'
        '<th>Share</th>'
        f"</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def render_release_collection_table(
    title: str, rows: list[dict[str, Any]], limit: int = 40
) -> str:
    if not rows:
        return f'<p class="note">No {esc(title.lower())}.</p>'
    visible = list(rows[:limit])
    max_downloads = max(
        (int_or_zero(row.get("download_count")) for row in visible), default=1
    ) or 1
    body = []
    for row in visible:
        label = str(row.get("label") or row.get("key") or "")
        url = row.get("url")
        rendered_label = (
            f'<a href="{attr(url)}">{esc(label)}</a>' if url else esc(label)
        )
        downloads = int_or_zero(row.get("download_count"))
        share_pct = 100 * downloads / max(1, max_downloads)
        published = row.get("published_at") or ""
        body.append(
            "<tr>"
            f"<td>{rendered_label}</td>"
            f"<td>{esc(date_part(published) or '')}</td>"
            f'<td class="num">{format_int(int_or_zero(row.get("asset_count")))}</td>'
            f'<td class="num">{format_int(downloads)}</td>'
            f'<td class="num">{esc(str(row.get("share_label") or ""))}</td>'
            f'<td class="bar-cell"><span class="bar" style="width:{share_pct:.1f}%"></span></td>'
            "</tr>"
        )
    return (
        f"<table><caption>{esc(title)}</caption><thead><tr><th>Release</th>"
        '<th>Published</th><th class="num">Assets</th>'
        '<th class="num">Downloads</th><th class="num">Share</th><th>Distribution</th>'
        f"</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


def render_family_collection_table(
    title: str, rows: list[dict[str, Any]], limit: int = 40
) -> str:
    if not rows:
        return f'<p class="note">No {esc(title.lower())}.</p>'
    visible = list(rows[:limit])
    max_downloads = max(
        (int_or_zero(row.get("download_count")) for row in visible), default=1
    ) or 1
    body = []
    for row in visible:
        label = str(row.get("label") or row.get("key") or "")
        downloads = int_or_zero(row.get("download_count"))
        share_pct = 100 * downloads / max(1, max_downloads)
        body.append(
            "<tr>"
            f"<td>{esc(label)}</td>"
            f'<td class="num">{format_int(int_or_zero(row.get("asset_count")))}</td>'
            f'<td class="num">{format_int(int_or_zero(row.get("release_count")))}</td>'
            f'<td class="num">{format_int(downloads)}</td>'
            f'<td class="num">{esc(str(row.get("share_label") or ""))}</td>'
            f'<td class="bar-cell"><span class="bar" style="width:{share_pct:.1f}%"></span></td>'
            "</tr>"
        )
    return (
        f"<table><caption>{esc(title)}</caption><thead><tr><th>Family</th>"
        '<th class="num">Assets</th><th class="num">Releases</th>'
        '<th class="num">Downloads</th><th class="num">Share</th>'
        '<th>Distribution</th>'
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


def render_bucket_tabs(
    group_id: str,
    panels_by_bucket: dict[str, str],
    default_bucket: str = "day",
) -> str:
    """Render a CSS-only tab control that switches between bucket-specific HTML panels.

    Uses radio inputs + sibling selectors for panel visibility and ``:has()`` for
    the active-tab style. Falls back gracefully on browsers without ``:has()``
    (panels still toggle, but the active tab pill won't update visually).
    """
    available = [
        (bucket, BUCKET_NOUN_LABEL[bucket])
        for bucket in SUPPORTED_DELTA_BUCKETS
        if panels_by_bucket.get(bucket)
    ]
    if not available:
        return ""
    if len(available) == 1:
        return panels_by_bucket[available[0][0]]
    if default_bucket not in {bucket for bucket, _ in available}:
        default_bucket = available[0][0]

    inputs_html: list[str] = []
    labels_html: list[str] = []
    panels_html: list[str] = []
    for bucket, label in available:
        input_id = f"bt-{group_id}-{bucket}"
        checked = " checked" if bucket == default_bucket else ""
        inputs_html.append(
            f'<input type="radio" name="bt-{attr(group_id)}" id="{attr(input_id)}"'
            f'{checked} class="bucket-radio" />'
        )
        labels_html.append(
            f'<label for="{attr(input_id)}" data-bucket="{attr(bucket)}" '
            f'class="bucket-tab-label">{esc(label)}</label>'
        )
        panels_html.append(
            f'<div class="bucket-panel" data-bucket="{attr(bucket)}">{panels_by_bucket[bucket]}</div>'
        )
    return (
        '<div class="bucket-tabs">'
        + "".join(inputs_html)
        + '<div class="bucket-tab-strip" role="tablist">'
        + "".join(labels_html)
        + "</div>"
        + "".join(panels_html)
        + "</div>"
    )


def render_stacked_bar_chart(
    title: str,
    rows: list[dict[str, Any]],
    series: list[tuple[str, str, str]],
    height: int = 260,
    width: int = 880,
    y_axis_label: str = "",
) -> str:
    """Vertical stacked bars: one bar per row, stack components per series.

    Each ``row`` provides a ``date`` (the bucket label) and a value per
    ``series`` field. Rows with all-zero series are still rendered to keep the
    x-axis continuous.
    """
    if not rows or not series:
        return f'<p class="note">No data available for {esc(title.lower())}.</p>'

    padding_left = 56
    padding_right = 18
    padding_top = 22
    padding_bottom = 42
    chart_width = width - padding_left - padding_right
    chart_height = height - padding_top - padding_bottom

    max_total = max(
        (
            sum(int_or_zero(row.get(field)) for _, field, _ in series)
            for row in rows
        ),
        default=0,
    )
    max_total = max(1, max_total)
    y_top = nice_ceiling(max_total)

    bar_count = len(rows)
    slot_width = chart_width / max(bar_count, 1)
    bar_pad = min(slot_width * 0.22, 16)
    bar_width = max(slot_width - bar_pad, 1.0)

    parts: list[str] = [
        f'<line x1="{padding_left}" y1="{padding_top}" x2="{padding_left}" y2="{padding_top + chart_height}" class="axis" />',
        f'<line x1="{padding_left}" y1="{padding_top + chart_height}" x2="{padding_left + chart_width}" y2="{padding_top + chart_height}" class="axis" />',
    ]
    for fraction in (0.25, 0.5, 0.75, 1.0):
        y = padding_top + chart_height - fraction * chart_height
        v = y_top * fraction
        parts.append(
            f'<line x1="{padding_left}" y1="{y:.1f}" x2="{padding_left + chart_width}" y2="{y:.1f}" class="grid" />'
        )
        parts.append(
            f'<text x="{padding_left - 8}" y="{y + 4:.1f}" text-anchor="end" class="label">{format_axis(v)}</text>'
        )

    if bar_count <= 12:
        label_indexes = list(range(bar_count))
    else:
        step = max(1, bar_count // 8)
        label_indexes = sorted({0, bar_count - 1, *range(0, bar_count, step)})

    for index, row in enumerate(rows):
        slot_left = padding_left + index * slot_width
        x = slot_left + (slot_width - bar_width) / 2
        cumulative_value = 0
        for label, field, color in series:
            value = int_or_zero(row.get(field))
            if value <= 0:
                continue
            seg_height = (value / y_top) * chart_height
            cumulative_value += value
            bar_total_height = (cumulative_value / y_top) * chart_height
            seg_y = padding_top + chart_height - bar_total_height
            parts.append(
                f'<rect x="{x:.1f}" y="{seg_y:.1f}" width="{bar_width:.1f}" '
                f'height="{seg_height:.1f}" fill="{attr(color)}" rx="2" ry="2">'
                f'<title>{esc(label)} ({esc(row.get("date", ""))}): {format_int(value)}</title>'
                "</rect>"
            )
        if index in label_indexes:
            cx = slot_left + slot_width / 2
            parts.append(
                f'<text x="{cx:.1f}" y="{height - 12}" text-anchor="middle" class="label">'
                f'{esc(short_date(row.get("date", "")))}</text>'
            )

    legend = "".join(
        f'<span><i style="background:{attr(color)}"></i>{esc(label)}</span>'
        for label, _, color in series
    )

    y_axis_html = (
        f'<text x="14" y="{padding_top - 6}" class="label">{esc(y_axis_label)}</text>'
        if y_axis_label
        else ""
    )

    return f"""<div class="chart" role="img" aria-label="{attr(title)}">
  <svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
    <style>
      .axis {{ stroke: var(--chart-axis, #52685d); stroke-width: 1; }}
      .grid {{ stroke: var(--chart-grid, #22342d); stroke-width: 1; }}
      .label {{ fill: var(--chart-label, #a5b6ae); font: 12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    </style>
    <text x="{padding_left}" y="13" class="label">{esc(title)}</text>
    {y_axis_html}
    {''.join(parts)}
  </svg>
  <div class="legend">{legend}</div>
</div>"""


def render_horizontal_bar_chart(
    title: str,
    rows: list[dict[str, Any]],
    label_key: str,
    value_key: str,
    color: str = "#7db7ff",
    limit: int = 12,
    secondary_key: str | None = None,
    value_formatter=None,
    secondary_formatter=None,
    secondary_label: str = "",
    label_url_key: str | None = None,
) -> str:
    if not rows:
        return f'<p class="note">No data available for {esc(title.lower())}.</p>'
    visible = list(rows[:limit])
    max_value = max((int_or_zero(row.get(value_key)) for row in visible), default=1) or 1
    formatter = value_formatter or format_int
    items: list[str] = []
    for row in visible:
        label = str(row.get(label_key) or "")
        value = int_or_zero(row.get(value_key))
        width_pct = 100 * value / max(1, max_value)
        url = row.get(label_url_key) if label_url_key else None
        rendered_label = (
            f'<a href="{attr(url)}">{esc(label)}</a>' if url else esc(label)
        )
        secondary_html = ""
        if secondary_key:
            secondary_value = row.get(secondary_key)
            secondary_text = (
                secondary_formatter(secondary_value)
                if secondary_formatter
                else (str(secondary_value) if secondary_value is not None else "")
            )
            if secondary_text:
                secondary_html = (
                    f'<span class="hbar-secondary"'
                    f' title="{attr(secondary_label)}">{esc(secondary_text)}</span>'
                )
        items.append(
            '<div class="hbar-row">'
            f'<span class="hbar-label">{rendered_label}</span>'
            '<span class="hbar-track" aria-hidden="true">'
            f'<span class="hbar-fill" style="width:{width_pct:.1f}%; background:{attr(color)}"></span>'
            "</span>"
            f'<span class="hbar-value">{esc(formatter(value))}</span>'
            f"{secondary_html}"
            "</div>"
        )
    return (
        '<div class="hbar-chart" role="img" aria-label="' + attr(title) + '">'
        f'<p class="chart-title">{esc(title)}</p>'
        + "".join(items)
        + "</div>"
    )


def render_line_chart(
    title: str,
    rows: list[dict[str, Any]],
    series: list[tuple[str, str, str]],
    height: int = 260,
    width: int = 880,
) -> str:
    if not rows or not series:
        return f'<p class="note">No data available for {esc(title.lower())}.</p>'

    padding_left = 56
    padding_right = 18
    padding_top = 18
    padding_bottom = 42
    chart_width = width - padding_left - padding_right
    chart_height = height - padding_top - padding_bottom
    dates = [row["date"] for row in rows]
    max_value = max(
        (float(row.get(field, 0) or 0) for row in rows for _, field, _ in series),
        default=0.0,
    )
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

    with git_auth_env(config.write_token) as env:
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
            write_text(
                worktree / "index.html",
                render_index(load_report_summaries(worktree), config),
            )
            write_text(worktree / ".nojekyll", "")
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
