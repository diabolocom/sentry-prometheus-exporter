# pylint: disable=consider-using-f-string

from datetime import datetime, timezone
from os import getenv
import traceback
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlencode

from retry import retry
import requests
import httpx

from libs.httpx_accesslog import on_request, on_response

retry_settings = {
    "tries": int(getenv("SENTRY_RETRY_TRIES", "5")),
    "delay": float(getenv("SENTRY_RETRY_DELAY", "0.2")),
    "max_delay": float(getenv("SENTRY_RETRY_MAX_DELAY", "10")),
    "backoff": float(getenv("SENTRY_RETRY_BACKOFF", "2")),
    "jitter": float(getenv("SENTRY_RETRY_JITTER", "0.5")),
}


TRIES = int(getenv("SENTRY_RETRY_TRIES", "5"))
DELAY = float(getenv("SENTRY_RETRY_DELAY", "0.2"))
MAX_DELAY = float(getenv("SENTRY_RETRY_MAX_DELAY", "10"))
BACKOFF = float(getenv("SENTRY_RETRY_BACKOFF", "2"))
JITTER = float(getenv("SENTRY_RETRY_JITTER", "0.5"))

CONCURRENCY = int(getenv("SENTRY_CONCURRENCY", "32"))
TIMEOUT = float(getenv("SENTRY_TIMEOUT", "30"))

SKIP_STAT_NAME = getenv("SENTRY_SKIP_STAT_NAME", "").split(",")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("hpack").setLevel(logging.WARNING)
logging.getLogger("h2").setLevel(logging.WARNING)

log = logging.getLogger("sentry.async")
log.setLevel(logging.DEBUG)


class SentryAPI:
    """A simple :class:`SentryAPI <SentryAPI>` to interact with Sentry's Web API.

    Used to interact with Sentry's API to do basic list operations

    Authentication tokens are passed using an auth header, and are used to authenticate
    as a user account with the API.

    Typical usage example:

      >>> from libs.sentry import SentryAPI
      >>> sentry = SentryAPI(BASE_URL, AUTH_TOKEN)
      >>> sentry.organizations()
      [{'id': '7446', 'slug': 'loggi', 'name': 'loggi', 'status': 'active'}]
    """

    def __init__(self, base_url, auth_token):
        """Inits SentryAPI with base sentry's URL and authentication token."""
        self.base_url = base_url.rstrip("/") + "/"
        self._headers = {"Authorization": f"Bearer {auth_token}"}
        self.__token = auth_token
        self.__session = requests.Session()

    async def _retrying_get(
        self,
        client: httpx.AsyncClient,
        path: str,
        limiter: asyncio.Semaphore | None = None,
    ):
        """
        GET with retry/backoff + concurrency guard.
        Raises if all retries fail.
        """
        url = self.base_url + path.lstrip("/")
        attempt = 0
        delay = DELAY

        while True:
            try:
                if limiter is not None:
                    async with limiter:
                        resp = await client.get(url)
                else:
                    resp = await client.get(url)
                resp.raise_for_status()
                return resp
            except (asyncio.TimeoutError, httpx.ReadTimeout):
                log.warning(
                    "HTTPX TIMEOUT url=%s (attempt %d/%d)",
                    url,
                    attempt + 1,
                    TRIES,
                    exc_info=True,
                )

            except httpx.ConnectTimeout:
                log.warning(
                    "HTTPX CONNECT TIMEOUT url=%s (attempt %d/%d)",
                    url,
                    attempt + 1,
                    TRIES,
                    exc_info=True,
                )

            except httpx.ConnectError as e:
                cause = repr(getattr(e, "__cause__", None))
                log.error(
                    "HTTPX CONNECT ERROR url=%s cause=%s (attempt %d/%d)",
                    url,
                    cause,
                    attempt + 1,
                    TRIES,
                    exc_info=True,
                )

            except httpx.RemoteProtocolError as e:
                log.error(
                    "HTTPX PROTOCOL ERROR url=%s (attempt %d/%d)",
                    url,
                    attempt + 1,
                    TRIES,
                    exc_info=True,
                )

            except httpx.PoolTimeout as e:
                log.warning(
                    "HTTPX POOL TIMEOUT (connection pool exhausted) url=%s (attempt %d/%d)",
                    url,
                    attempt + 1,
                    TRIES,
                    exc_info=True,
                )

            except httpx.HTTPStatusError as e:
                body_preview = e.response.text[:256] if e.response is not None else ""
                log.error(
                    "HTTP %s for %s -> %s\nBody: %r",
                    e.request.method,
                    e.request.url,
                    e.response.status_code if e.response else "?",
                    body_preview,
                )
                raise

            except Exception as e:
                log.error(
                    "HTTPX UNKNOWN ERROR url=%s type=%s (attempt %d/%d)\n%s",
                    url,
                    type(e).__name__,
                    attempt + 1,
                    TRIES,
                    "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                )

            attempt += 1
            if attempt >= TRIES:
                raise Exception("Attempted too many times")
            jitter = (2 * JITTER) * (asyncio.get_running_loop().time() % 1) - JITTER
            sleep_for = max(0.0, min(MAX_DELAY, delay) + jitter)
            await asyncio.sleep(sleep_for)
            delay *= BACKOFF

    async def project_stats(
        self,
        org_slug: str,
        project_slug: str,
        *,
        limiter: asyncio.Semaphore | None = None,
    ) -> dict:
        """
        Retrieve and reduce last hour events
        """
        now = int(datetime.today().timestamp())
        default_stat_names = ["received", "rejected", "blacklisted"]
        stat_names = []
        for stat_name in default_stat_names:
            if stat_name not in SKIP_STAT_NAME:
                stat_names.append(stat_name)
        timeout = httpx.Timeout(TIMEOUT, connect=min(10.0, TIMEOUT))
        async with httpx.AsyncClient(
            timeout=timeout,
            headers=self._headers,
            limits=httpx.Limits(
                max_connections=CONCURRENCY,
                max_keepalive_connections=CONCURRENCY,
            ),
            event_hooks={
                "request": [on_request],
                "response": [on_response],
            },
        ) as client:

            async def fetch_one(stat_name: str):
                r = await self._retrying_get(
                    client,
                    f"projects/{org_slug}/{project_slug}/stats/"
                    f"?stat={stat_name}&since={now - 3600}&until={now}&resolution=1h",
                    limiter=limiter,
                )
                return stat_name, r.json()

            pairs = await asyncio.gather(*(fetch_one(s) for s in stat_names))
            series = dict(pairs)

        project_events = {}
        for stat_name, values in series.items():
            events = 0
            for point in values:
                if not isinstance(point, str):
                    events += point[1]
            project_events[stat_name] = events

        return project_events

    @retry(requests.exceptions.HTTPError, **retry_settings)
    def __get(self, url):
        headers = {"Authorization": "Bearer " + self.__token}
        response = self.__session.get(self.base_url + url, headers=headers)
        response.raise_for_status()
        return response

    def organizations(self):
        """Return a list of organizations."""

        resp = self.__get("organizations/")
        organizations = []
        for org in resp.json():
            organization = {}
            organization.update(
                {
                    "id": org.get("id"),
                    "slug": org.get("slug"),
                    "name": org.get("name"),
                    "status": org.get("status").get("id"),
                }
            )
            organizations.append(organization)

        return organizations

    def get_org(self, org_slug):
        """Return a organization details.

        Return details on an individual organization including
        various details such as membership access, features, and teams.

        Args:
            org_slug: A organization's slug string name.

        Returns:
            A dict mapping keys to the corresponding organization
        """

        resp = self.__get("organizations/{org}/".format(org=org_slug))
        org = resp.json()
        organization = {}
        organization.update(
            {
                "id": org.get("id"),
                "slug": org.get("slug"),
                "name": org.get("name"),
                "status": org.get("status"),
                "platform": org.get("platform"),
            }
        )

        return organization

    def projects(self, org_slug):
        """Return a list of projects of the specified organization.

        Args:
            org_slug: A organization's slug string name.

        Returns:
            A list mapping with dictionary keys to the corresponding projects
        """

        resp = self.__get(
            "organizations/{org}/projects/?all_projects=1".format(org=org_slug)
        )
        projects = []
        for proj in resp.json():
            project = {}
            project.update(
                {
                    "id": proj.get("id"),
                    "slug": proj.get("slug"),
                    "name": proj.get("name"),
                    "status": proj.get("status"),
                    "platform": proj.get("platform"),
                }
            )
            projects.append(project)

        return projects

    def get_project(self, org_slug, project_slug):
        """Return details on an individual project.

        Args:
            org_slug: A organization's slug string name.
            project_slug: The project's slug string name

        Returns:
            A dict mapping keys to the corresponding project
        """

        resp = self.__get(
            "projects/{org}/{proj_slug}/".format(org=org_slug, proj_slug=project_slug)
        )
        proj = resp.json()
        project = {}
        project.update(
            {
                "id": proj.get("id"),
                "slug": proj.get("slug"),
                "name": proj.get("name"),
                "status": proj.get("status"),
                "platform": proj.get("platform"),
            }
        )

        return project

    def environments(self, org_slug, project):
        """Return a list of project's environments.

        Args:
            org_slug: A organization slug string name.
            project: dict instance of a project.

        Returns:
            A list mapping with all project's corresponding issues and each element is a dict.

        Raises:
            TypeError: An error occurred if the project instance isn't a valid dict
        """

        if not isinstance(project, dict):
            raise TypeError("project param isn't a dictionary")

        resp = self.__get(
            "projects/{org}/{proj_slug}/environments/".format(
                org=org_slug, proj_slug=project.get("slug")
            ),
        )
        if resp.status_code == 404:
            return []
        environments = [env.get("name") for env in resp.json()]
        return environments

    def issues(self, org_slug, project, environment=None, age="24h"):
        """Return a list open issues to a project.

        Retrieves the first 100 new open issues created in the past age, using the default query
        (i.e.: is:unresolved) sorted by Last Seen events

        Args:
            org_slug: A organization slug string name.
            project: dict instance of a project.
            environments: Optional;
                A sequence of strings representing the environment names.
            age: Optional;
                If age is different from default (aka 24h) query will use now - age.

        Returns:
            A list mapping with all project's corresponding issues and each element is a dict.

        Raises:
            TypeError: An error occurred if the project instance isn't a valid dict
        """

        if not isinstance(project, dict):
            raise TypeError("project param isn't a dictionary")

        issues_url = "projects/{org}/{proj_slug}/issues/?project={proj_id}&sort=date&query=age%3A-{age}".format(
            org=org_slug,
            proj_slug=project.get("slug"),
            proj_id=project.get("id"),
            age=age,
        )

        if environment:
            issues = {}
            issues_url = issues_url + "&environment={env}".format(env=environment)
            resp = self.__get(issues_url)
            if resp.status_code == 404:
                issues[environment] = []
            else:
                issues[environment] = resp.json()
            return issues
        resp = self.__get(issues_url)
        return {"all": resp.json()}

    def events(self, org_slug, project, environment=None):
        """Return a list of events bound to a project.

        Retrieves the first 100 new events, using the default query sorted by Last Seen events

        Args:
            org_slug: A organization slug string name.
            project: dict instance of a project.
            environments: Optional;
                A sequence of strings representing the environment names.

        Returns:
            A list mapping with all project's corresponding events and each element is a dict.

        Raises:
            TypeError: An error occurred if the project instance isn't a valid dict
        """

        if not isinstance(project, dict):
            raise TypeError("project param isn't a dictionary")

        events_url = (
            "projects/{org}/{proj_slug}/events/?project={proj_id}&sort=date".format(
                org=org_slug, proj_slug=project.get("slug"), proj_id=project.get("id")
            )
        )
        if environment:
            events = {}
            events_url = events_url + "&environment={env}".format(env=environment)
            resp = self.__get(events_url)
            events[environment] = resp.json()
            return events
        resp = self.__get(events_url)
        return {"all": resp.json()}

    def issue_events(self, issue_id, environment=None):
        """This method lists issue's events."""

        issue_events_url = "issues/{issue_id}/events/".format(issue_id=issue_id)

        if environment:
            issue_events = {}
            issue_events_url = issue_events_url + "&environment={env}&sort=date".format(
                env=environment
            )
            resp = self.__get(issue_events_url)
            issue_events[environment] = resp.json()
            return issue_events
        resp = self.__get(issue_events_url)
        return {"all": resp.json()}

    def issue_release(self, issue_id, environment=None):
        """This method lists issue's events."""

        issue_release_url = "issues/{issue_id}/current-release/".format(
            issue_id=issue_id
        )

        if environment:
            issue_release_url = issue_release_url + "?environment={env}".format(
                env=environment
            )
            resp = self.__get(issue_release_url)
            curr_release = resp.json().get("currentRelease")
            if curr_release:
                release = curr_release.get("release").get("version")
                return release
            return curr_release
        resp = self.__get(issue_release_url)
        release = resp.json().get("currentRelease").get("release").get("version")
        return release

    def project_releases(self, org_slug, project, environment=None):
        """Return a list of releases for a given project into the organization.

        Args:
            org_slug: A organization slug string name.
            project: dict instance of a project.
            environments: Optional;
                A sequence of strings representing the environment names.

        Returns:
            A list mapping with all project's corresponding release and each element is a dict.

        Raises:
            TypeError: An error occurred if the project instance isn't a valid dict
        """

        if not isinstance(project, dict):
            raise TypeError("project param isn't a dictionary")

        proj_releases_url = (
            "organizations/{org}/releases/?project={proj_id}&sort=date".format(
                org=org_slug, proj_id=project.get("id")
            )
        )

        if environment:
            proj_releases = {}
            proj_releases_url = proj_releases_url + "&environment={env}".format(
                env=environment
            )
            resp = self.__get(proj_releases_url)
            proj_releases[environment] = resp.json()
            return proj_releases
        resp = self.__get(proj_releases_url)
        return {"all": resp.json()}

    def rate_limit(self, org_slug, project_slug):
        """Return client key rate limits configuration on an individual project.

        Args:
            org_slug: A organization's slug string name.
            project_slug: The project's slug string name

        Returns:
            A dict corresponding of the project rate limit key
        """

        rate_limit_url = "projects/{org}/{proj_slug}/keys/".format(
            org=org_slug, proj_slug=project_slug
        )
        resp = self.__get(rate_limit_url)
        if resp.json()[0].get("rateLimit"):
            rate_limit_window = resp.json()[0].get("rateLimit").get("window")
            rate_limit_count = resp.json()[0].get("rateLimit").get("count")
            rate_limit_second = rate_limit_count / rate_limit_window
        else:
            rate_limit_second = 0

        return rate_limit_second

    async def _org_stats_v2_last_hour(
        self,
        org_slug: str,
        *,
        group_key: str,  # "outcome" or "reason"
        field: str,  # "sum(quantity)" or "sum(times_seen)"
        limiter: asyncio.Semaphore | None = None,
    ) -> dict:
        """
        Low-level helper: one call to stats_v2 for the last hour with:
          - field=<field>
          - groupBy=category & groupBy=<group_key>
          - start=<epoch secs one hour ago>, end=<epoch secs now>, resolution=1h
        Returns parsed JSON.
        """
        now = int(datetime.now(timezone.utc).timestamp())
        start = now - 3600

        # Build query with repeated keys
        params = [
            ("field", field),
            ("groupBy", "category"),
            ("groupBy", group_key),
            ("start", str(start)),
            ("end", str(now)),
            ("resolution", "1h"),
        ]
        query = urlencode(params, doseq=True)
        path = f"organizations/{org_slug}/stats_v2/?{query}"

        timeout = httpx.Timeout(TIMEOUT, connect=min(10.0, TIMEOUT))
        async with httpx.AsyncClient(
            timeout=timeout,
            headers=self._headers,
            limits=httpx.Limits(
                max_connections=CONCURRENCY,
                max_keepalive_connections=CONCURRENCY,
            ),
            event_hooks={
                "request": [on_request],
                "response": [on_response],
            },
        ) as client:
            if limiter is not None:
                async with limiter:
                    resp = await client.get(self.base_url + path.lstrip("/"))
            else:
                resp = await client.get(self.base_url + path.lstrip("/"))
            resp.raise_for_status()
            return resp.json()

    def _merge_stats_v2_groups(
        self,
        *,
        data_qty: dict,
        data_seen: dict,
        group_key: str,  # "outcome" or "reason"
    ) -> List[Dict[str, Optional[str]]]:
        """
        Merge two stats_v2 payloads (quantity & times_seen) keyed by (category, group_key).
        Produces rows: {"category": str|None, group_key: str|None, "sum_quantity": int, "sum_times_seen": int}
        """

        def extract_totals(d: dict) -> Dict[Tuple[Optional[str], Optional[str]], int]:
            out: Dict[Tuple[Optional[str], Optional[str]], int] = {}
            for g in (d or {}).get("groups", []) or []:
                by = g.get("by") or {}
                cat = by.get("category")
                grp = by.get(group_key)
                totals = g.get("totals") or {}
                val = totals.get("sum(quantity)") or totals.get("sum(times_seen)")
                if val is None:
                    # fall back to series (single bucket for 1h/1h)
                    series = g.get("series") or {}
                    # choose the matching field if present
                    fld = (
                        "sum(quantity)"
                        if "sum(quantity)" in series
                        else "sum(times_seen)"
                    )
                    arr = series.get(fld) or []
                    val = int(arr[-1]) if arr else 0
                out[(cat, grp)] = int(val)
            return out

        qty_map = extract_totals(data_qty)  # (cat, grp) -> quantity
        seen_map = extract_totals(data_seen)  # (cat, grp) -> times_seen

        keys = set(qty_map.keys()) | set(seen_map.keys())
        rows: List[Dict[str, Optional[str]]] = []
        for cat, grp in keys:
            rows.append(
                {
                    "category": cat,
                    group_key: grp,
                    "sum_quantity": int(qty_map.get((cat, grp), 0)),
                    "sum_times_seen": int(seen_map.get((cat, grp), 0)),
                }
            )
        return rows

    async def organization_event_counts_last_hour_by_outcome(
        self,
        org_slug: str,
        *,
        limiter: asyncio.Semaphore | None = None,
    ) -> List[Dict[str, Optional[str]]]:
        """
        High-level: return rows merged for groupBy=category+outcome with both fields.
        """
        qty, seen = await asyncio.gather(
            self._org_stats_v2_last_hour(
                org_slug, group_key="outcome", field="sum(quantity)", limiter=limiter
            ),
            self._org_stats_v2_last_hour(
                org_slug, group_key="outcome", field="sum(times_seen)", limiter=limiter
            ),
        )
        return self._merge_stats_v2_groups(
            data_qty=qty, data_seen=seen, group_key="outcome"
        )

    async def organization_event_counts_last_hour_by_reason(
        self,
        org_slug: str,
        *,
        limiter: asyncio.Semaphore | None = None,
    ) -> List[Dict[str, Optional[str]]]:
        """
        High-level: return rows merged for groupBy=category+reason with both fields.
        """
        qty, seen = await asyncio.gather(
            self._org_stats_v2_last_hour(
                org_slug, group_key="reason", field="sum(quantity)", limiter=limiter
            ),
            self._org_stats_v2_last_hour(
                org_slug, group_key="reason", field="sum(times_seen)", limiter=limiter
            ),
        )
        return self._merge_stats_v2_groups(
            data_qty=qty, data_seen=seen, group_key="reason"
        )
