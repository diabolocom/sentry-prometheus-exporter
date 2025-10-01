# pylint: disable=consider-using-f-string,logging-format-interpolation

import logging
import asyncio
from datetime import datetime, timedelta
from os import getenv

from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeHistogramMetricFamily,
    GaugeMetricFamily,
)

from helpers.utils import get_cached, write_cache
from helpers.filecache import FileCachePickleSingleFlight

# constants for caching file
JSON_CACHE_FILE = "/tmp/sentry-prometheus-exporter-cache.json"
DEFAULT_CACHE_EXPIRE_TIMESTAMP = int(
    datetime.timestamp(datetime.now() + timedelta(minutes=2))
)
CONCURRENCY = int(getenv("SENTRY_CONCURRENCY", "64"))


CACHE_DIR = getenv("SENTRY_EXPORTER_CACHE_DIR", "/tmp/sentry_exporter_cache")
CACHE_TTL = int(getenv("SENTRY_EXPORTER_CACHE_TTL", "30"))  # seconds
CACHE_STALE = getenv("SENTRY_EXPORTER_CACHE_STALE_ON_ERROR", "True") == "True"

log = logging.getLogger(__name__)


def clean_registry():
    # Loop with try except to remove all default collectors
    for _, collector in list(REGISTRY._names_to_collectors.items()):
        try:
            REGISTRY.unregister(collector)
        except KeyError:
            pass


class SentryCollector:
    """A simple :class:`SentryCollector <SentryCollector>` returns a list of Metric objects.

    Proxy metrics from sentry building consistent with the Prometheus exposition formats:

    GaugeHistogramMetricFamily: projects issues split into 3 issues age buckets: 1h, 24h, and 14d
    GaugeMetricFamily:          count the open issues adding several labels to help aggregation
    CounterMetricFamily:        total projects events count

    Typical usage example:

      >>> from helpers.prometheus import SentryCollector
      >>> REGISTRY.register(SentryCollector(sentry, org_slug, projects_slug))
    """

    def __init__(
        self,
        sentry_api,
        sentry_org_slug,
        metric_scraping_config,
        sentry_projects_slug=None,
    ):
        """Inits SentryCollector with a SentryAPI object"""
        self.__sentry_api = sentry_api
        self.sentry_org_slug = sentry_org_slug
        self.sentry_projects_slug = sentry_projects_slug
        self.issue_metrics = metric_scraping_config[0]
        self.events_metrics = metric_scraping_config[1]
        self.rate_limit_metrics = metric_scraping_config[2]
        self.get_1h_metrics = metric_scraping_config[3]
        self.get_24h_metrics = metric_scraping_config[4]
        self.get_14d_metrics = metric_scraping_config[5]
        self.logger = log

        key = "sentry_snapshot"
        self._cache = FileCachePickleSingleFlight(
            CACHE_DIR, key, CACHE_TTL, allow_stale_on_error=CACHE_STALE
        )

    def __build_sentry_data_from_api(self):
        """Build a local data structure from sentry API calls.

        Returns:
            A dict mapping keys to the corresponding sentry authenticated session.

            The metadata key will store organization and projects metadata info
            (i.e.: slug names, ids, status, etc...) and projects_data key will store
            project's issues data, each key is a corrensponding environment
            which contains 3 different ages: 1h, 24h and 14d lists of issues

            Example:
                data = {
                    "metadata": {
                        "orgs": org,
                        "projects": projects,
                        "orgs_slug": orgs_slug,
                        "projects_slug": projects_slug,
                        "projects_envs": projects_envs,
                    },
                    "projects_data" : {
                        "project_slug": {
                            "production": {
                                "1h": [],
                                "24h": [],
                                "14d": []
                            },
                            "staging": {
                                "1h": [],
                                "24h": [],
                                "14d": []
                            }
                        }
                    }
                }
        """

        projects_slug = []
        projects_envs = {}
        projects = []
        self.org = self.__sentry_api.get_org(self.sentry_org_slug)
        log.info(
            "metadata: sentry organization: {org}".format(org=self.org.get("slug"))
        )

        if self.sentry_projects_slug:
            log.info(
                "metadata: projects specified: {num_proj}".format(
                    num_proj=len(self.sentry_projects_slug.split(","))
                )
            )
            for project_slug in self.sentry_projects_slug.split(","):
                log.debug(
                    "metadata: getting {proj} project data from API".format(
                        proj=project_slug
                    )
                )
                project = self.__sentry_api.get_project(
                    self.org.get("slug"), project_slug
                )
                projects.append(project)
                projects_slug.append(project_slug)
                if self.issue_metrics == "True":
                    envs = self.__sentry_api.environments(self.org.get("slug"), project)
                    projects_envs[project.get("slug")] = envs
            log.info(
                "metadata: projects loaded from API: {num_proj}".format(
                    num_proj=len(projects)
                )
            )
        else:
            log.info("metadata: no projects specified, loading from API")
            for project in self.__sentry_api.projects(self.sentry_org_slug):
                projects.append(project)
                projects_slug.append(project.get("slug"))
                if self.issue_metrics == "True":
                    envs = self.__sentry_api.environments(self.org.get("slug"), project)
                    projects_envs[project.get("slug")] = envs
            log.info(
                "metadata: projects loaded from API: {num_proj}".format(
                    num_proj=len(projects)
                )
            )

        log.debug("metadata: building projects metadata structure")
        data = {
            "metadata": {
                "org": self.org,
                "projects": projects,
                "projects_slug": projects_slug,
                "projects_envs": projects_envs,
            }
        }
        if self.issue_metrics == "True":
            __metadata = data.get("metadata")

            projects_issue_data = {}

            for project in __metadata.get("projects"):
                projects_issue_data[project.get("slug")] = {}
                envs = __metadata.get("projects_envs").get(project.get("slug"))
                for env in envs:
                    project_issues_1h = project_issues_24h = project_issues_14d = {}
                    if self.get_1h_metrics == "True":
                        log.debug(
                            "metadata: getting issues from api - project: {proj} env: {env} age: 1h".format(
                                proj=project.get("slug"), env=env
                            )
                        )
                        project_issues_1h = self.__sentry_api.issues(
                            self.org.get("slug"), project, env, age="1h"
                        )
                    if self.get_24h_metrics == "True":
                        log.debug(
                            "metadata: getting issues from api - project: {proj} env: {env} age: 24h".format(
                                proj=project.get("slug"), env=env
                            )
                        )
                        project_issues_24h = self.__sentry_api.issues(
                            self.org.get("slug"), project, env, age="24h"
                        )
                    if self.get_14d_metrics == "True":
                        log.debug(
                            "metadata: getting issues from api - project: {proj} env: {env} age: 14d".format(
                                proj=project.get("slug"), env=env
                            )
                        )
                        project_issues_14d = self.__sentry_api.issues(
                            self.org.get("slug"), project, env, age="14d"
                        )

                    log.debug("data structure: building projects issues data")
                    for k, v in project_issues_1h.items():
                        projects_issue_data[project.get("slug")][k] = {"1h": v}

                    for k, v in project_issues_24h.items():
                        projects_issue_data[project.get("slug")][k].update({"24h": v})

                    for k, v in project_issues_14d.items():
                        projects_issue_data[project.get("slug")][k].update({"14d": v})

            data["projects_data"] = projects_issue_data

        write_cache(JSON_CACHE_FILE, data, DEFAULT_CACHE_EXPIRE_TIMESTAMP)
        log.debug(
            "cache: writing data structure to file: {cache}".format(
                cache=JSON_CACHE_FILE
            )
        )
        return data

    def __build_sentry_data(self):

        data = get_cached(JSON_CACHE_FILE)

        if data is False:
            log.debug("cache: {cache} not found.".format(cache=JSON_CACHE_FILE))
            log.debug("cache: rebuilding from API...")
            api_data = self.__build_sentry_data_from_api()
            return api_data

        log.debug(
            "cache: reading data structure from file: {cache}".format(
                cache=JSON_CACHE_FILE
            )
        )
        return data

    def collect(self):
        """Yields metrics from the collectors in the registry."""
        try:
            metric_families = self._cache.get_or_compute(
                lambda: list(self._compute_metrics_once())
            )
        except Exception:
            log.exception("snapshot compute failed; no cache to serve")
            return  # yield nothing this scrape

        yield from metric_families

    def _compute_metrics_once(self):
        __data = self.__build_sentry_data()
        __metadata = __data.get("metadata")
        __projects_data = __data.get("projects_data")

        self.org = __metadata.get("org")
        self.projects_data = {}

        if self.issue_metrics == "True":
            issues_histogram_metrics = GaugeHistogramMetricFamily(
                "sentry_issues",
                "Number of open issues (aka is:unresolved) per project",
                buckets=None,
                gsum_value=None,
                labels=[
                    "project_slug",
                    "environment",
                ],
                unit="",
            )

            log.info("collector: loading projects issues")
            for project in __metadata.get("projects"):
                envs = __metadata.get("projects_envs").get(project.get("slug"))
                project_issues = __projects_data.get(project.get("slug"))
                for env in envs:
                    log.debug(
                        "collector: loading issues - project: {proj} env: {env}".format(
                            proj=project.get("slug"), env=env
                        )
                    )

                    project_issues_1h = project_issues.get(env).get("1h")
                    project_issues_24h = project_issues.get(env).get("24h")
                    project_issues_14d = project_issues.get(env).get("14d")

                    events_1h = 0
                    events_24h = 0
                    events_14d = 0

                    if project_issues_1h:
                        for issue in project_issues_1h:
                            events_1h += int(issue.get("count") or 0)

                    if project_issues_24h:
                        for issue in project_issues_24h:
                            events_24h += int(issue.get("count") or 0)

                    if project_issues_14d:
                        for issue in project_issues_14d:
                            events_14d += int(issue.get("count") or 0)

                    sum_events = events_1h + events_24h + events_14d
                    histo_buckets = []
                    if self.get_1h_metrics == "True":
                        histo_buckets.append(("1h", float(events_1h)))
                    if self.get_24h_metrics == "True":
                        histo_buckets.append(("24h", float(events_24h)))
                    if self.get_14d_metrics == "True":
                        histo_buckets.append(("+Inf", float(events_14d)))
                    issues_histogram_metrics.add_metric(
                        labels=[
                            str(project.get("slug")),
                            str(env),
                        ],
                        buckets=histo_buckets,
                        gsum_value=int(sum_events),
                    )

            yield issues_histogram_metrics

            issues_metrics = GaugeMetricFamily(
                "sentry_open_issue_events",
                "Number of open issues (aka is:unresolved) per project",
                labels=[
                    "issue_id",
                    "logger",
                    "level",
                    "status",
                    "platform",
                    "project_slug",
                    "environment",
                    "release",
                    "isUnhandled",
                    "firstSeen",
                    "lastSeen",
                ],
            )

            for project in __metadata.get("projects"):
                envs = __metadata.get("projects_envs").get(project.get("slug"))
                project_issues = __projects_data.get(project.get("slug"))
                for env in envs:
                    project_issues_1h = project_issues.get(env).get("1h")
                    for issue in project_issues_1h:
                        release = self.__sentry_api.issue_release(issue.get("id"), env)
                        issues_metrics.add_metric(
                            [
                                str(issue.get("id")),
                                str(issue.get("logger")) or "None",
                                str(issue.get("level")),
                                str(issue.get("status")),
                                str(issue.get("platform")),
                                str(issue.get("project").get("slug")),
                                str(env),
                                str(release),
                                str(issue.get("isUnhandled")),
                                str(
                                    datetime.strftime(
                                        datetime.strptime(
                                            str(
                                                issue.get("firstSeen")
                                                # if the issue age is recent, firstSeen returns None
                                                # and we'll return datetime.now() as default
                                                or datetime.strftime(
                                                    datetime.now(), "%Y-%m-%dT%H:%M:%SZ"
                                                )
                                            ),
                                            "%Y-%m-%dT%H:%M:%SZ",
                                        ),
                                        "%Y-%m-%d",
                                    )
                                ),
                                str(
                                    datetime.strftime(
                                        datetime.strptime(
                                            str(
                                                issue.get("lastSeen")
                                                # if the issue age is recent, lastSeen returns None
                                                # and we'll return datetime.now() as default
                                                or datetime.strftime(
                                                    datetime.now(), "%Y-%m-%dT%H:%M:%SZ"
                                                )
                                            ),
                                            "%Y-%m-%dT%H:%M:%SZ",
                                        ),
                                        "%Y-%m-%d",
                                    )
                                ),
                            ],
                            int(issue.get("count")),
                        )
            yield issues_metrics
        for builder in (
            self._build_sentry_org_events_last_hour_by_outcome_metrics,
            self._build_sentry_org_events_last_hour_by_reason_metrics,
        ):
            try:
                metrics = asyncio.run(builder(__metadata))
                yield from (metrics or [])
            except Exception as e:
                if hasattr(self, "logger"):
                    self.logger.warning("org-level stats builder failed: %r", e)

        if self.events_metrics == "True":

            def _sync_build():
                return asyncio.run(self._build_sentry_events_metrics(__metadata))

            project_events_metrics = _sync_build()
            if project_events_metrics is not None:
                yield project_events_metrics

        if self.rate_limit_metrics == "True":
            project_rate_metrics = GaugeMetricFamily(
                "sentry_rate_limit_events_sec",
                "Rate limit events per second for a project",
                labels=["project_slug"],
            )

            for project in __metadata.get("projects"):
                rate_limit_second = self.__sentry_api.rate_limit(
                    self.org.get("slug"), project.get("slug")
                )
                project_rate_metrics.add_metric(
                    [str(project.get("slug"))], round(rate_limit_second, 6)
                )

            yield project_rate_metrics

    async def _build_sentry_events_metrics(self, __metadata):
        """
        Async worker that concurrently calls Sentry for each project,
        then assembles the CounterMetricFamily exactly as before.
        Expects self.__sentry_api to be an instance of SentryAsyncAPI.
        """
        projects = __metadata.get("projects", [])

        # If there are no projects, short-circuit
        if not projects:
            return None

        limiter = asyncio.Semaphore(CONCURRENCY)
        metric = CounterMetricFamily(
            "sentry_events_last_hour",
            "Total events counts per project",
            labels=["project_slug", "stat"],
        )

        async def per_project(project: dict):
            slug = project.get("slug")
            events = await self.__sentry_api.project_stats(
                self.org.get("slug"), slug, limiter=limiter
            )
            return slug, events

        results = await asyncio.gather(
            *(per_project(p) for p in projects), return_exceptions=True
        )

        for res in results:
            if isinstance(res, Exception):
                self.logger.warning("Sentry project_stats failed: %r", res)
                continue
            slug, events = res
            for stat, value in (events or {}).items():
                metric.add_metric([str(slug), str(stat)], int(value))

        return metric

    async def _build_sentry_org_events_last_hour_by_outcome_metrics(self, __metadata):
        org_slug = (self.org or {}).get("slug") or (__metadata.get("org") or {}).get(
            "slug"
        )
        if not org_slug:
            return []

        limiter = asyncio.Semaphore(CONCURRENCY)
        rows = await self.__sentry_api.organization_event_counts_last_hour_by_outcome(
            org_slug, limiter=limiter
        )

        m_qty = CounterMetricFamily(
            "sentry_org_events_last_hour_quantity_by_outcome",
            "Org sum(quantity) over last hour grouped by category,outcome "
            "(bytes for attachments; event counts for most categories).",
            labels=["category", "outcome"],
        )
        m_seen = CounterMetricFamily(
            "sentry_org_events_last_hour_times_seen_by_outcome",
            "Org sum(times_seen) over last hour grouped by category,outcome "
            "(unique sessions for sessions; attachment count for attachments).",
            labels=["category", "outcome"],
        )

        for r in rows:
            cat = str(r.get("category") or "null")
            out = str(r.get("outcome") or "null")
            m_qty.add_metric([cat, out], int(r.get("sum_quantity", 0)))
            m_seen.add_metric([cat, out], int(r.get("sum_times_seen", 0)))

        return [m_qty, m_seen]

    async def _build_sentry_org_events_last_hour_by_reason_metrics(self, __metadata):
        org_slug = (self.org or {}).get("slug") or (__metadata.get("org") or {}).get(
            "slug"
        )
        if not org_slug:
            return []

        limiter = asyncio.Semaphore(CONCURRENCY)
        rows = await self.__sentry_api.organization_event_counts_last_hour_by_reason(
            org_slug, limiter=limiter
        )

        m_qty = CounterMetricFamily(
            "sentry_org_events_last_hour_quantity_by_reason",
            "Org sum(quantity) over last hour grouped by category,reason "
            "(bytes for attachments; event counts for most categories).",
            labels=["category", "reason"],
        )
        m_seen = CounterMetricFamily(
            "sentry_org_events_last_hour_times_seen_by_reason",
            "Org sum(times_seen) over last hour grouped by category,reason "
            "(unique sessions for sessions; attachment count for attachments).",
            labels=["category", "reason"],
        )

        for r in rows:
            cat = str(r.get("category") or "null")
            rsn = str(r.get("reason") or "null")
            m_qty.add_metric([cat, rsn], int(r.get("sum_quantity", 0)))
            m_seen.add_metric([cat, rsn], int(r.get("sum_times_seen", 0)))

        return [m_qty, m_seen]
