"""Fixed-repository transport. Every mutation is issued durably before HTTP."""

import base64
import json
import re
from urllib.parse import quote

import requests

try:
    from .own_blog_content import REPO, REPO_ID, BASE
except ImportError:
    from own_blog_content import REPO, REPO_ID, BASE

PREFIX = "/repos/" + REPO
WORKFLOW = 286291890
READY = "mutation($id:ID!){markPullRequestReadyForReview(input:{pullRequestId:$id}){pullRequest{isDraft}}}"


class Stop(ValueError):
    pass


def sha(value):
    if not isinstance(value, str) or not re.fullmatch("[a-f0-9]{40}", value):
        raise Stop("github_unconfirmed")
    return value


class GitHub:
    def __init__(self, guard, finish):
        self.guard, self.finish = guard, finish

    def request(self, method, path, *, params=None, body=None, missing=False):
        token = self.guard("read")
        if not (path.startswith(PREFIX + "/") or path == PREFIX or path == "/graphql"):
            raise Stop("target_changed")
        try:
            # Streaming bounds errors as well as successes; no provider URLs enter logs.
            with requests.request(
                method,
                "https://api.github.com" + path,
                params=params,
                json=body,
                headers={
                    "Authorization": "Bearer " + token,
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                allow_redirects=False,
                stream=True,
                timeout=(3, 12),
            ) as response:
                if response.status_code == 404 and missing:
                    return None
                if response.status_code not in {200, 201, 204}:
                    raise Stop(
                        "github_denied"
                        if response.status_code in {401, 403}
                        else "github_unconfirmed"
                    )
                data = bytearray()
                for chunk in response.iter_content(65536):
                    data.extend(chunk)
                    if len(data) > 1500000:
                        raise Stop("response_limit")
                return json.loads(data) if data else {}
        except Stop:
            raise
        except Exception:
            raise Stop("github_uncertain") from None

    def get(self, path="", **params):
        return self.request("GET", PREFIX + path, params=params or None)

    def mutation(self, key, method, path, body, recover):
        known = self.guard("intent", key=key, body=[method, path, body])
        if known.get("done"):
            return known["result"]
        if known.get("issued"):
            result = recover()
            if result is None:
                raise Stop("github_uncertain")
        else:
            # A lost HTTP result can only be reconciled by a later read, never reissued.
            result = self.request(method, path, body=body)
        self.finish(key, result)
        return result

    def repository(self):
        repo = self.get()
        if (
            repo.get("id") != REPO_ID
            or repo.get("full_name") != REPO
            or repo.get("default_branch") != BASE
            or repo.get("archived")
            or repo.get("disabled")
            or (repo.get("permissions") or {}).get("push") is not True
            or repo.get("allow_merge_commit") is not True
        ):
            raise Stop("repository_required")
        return repo

    def head(self, branch=BASE):
        value = self.get("/git/ref/heads/" + quote(branch, safe=""))
        if (
            value.get("ref") != "refs/heads/" + branch
            or (value.get("object") or {}).get("type") != "commit"
        ):
            raise Stop("github_unconfirmed")
        return sha(value["object"]["sha"])

    def content(self, file, ref):
        return self.request(
            "GET",
            PREFIX + "/contents/" + quote(file, safe="/"),
            params={"ref": ref},
            missing=True,
        )

    def absent(self, files, ref):
        for file in files:
            if self.content(file["path"], ref) is not None:
                raise Stop("article_exists")

    def diff(self, base, head, files):
        value = self.get("/compare/" + sha(base) + "..." + sha(head))
        changed = value.get("files")
        if (
            not isinstance(changed, list)
            or len(changed) != len(files)
            or (value.get("merge_base_commit") or {}).get("sha") != base
        ):
            raise Stop("diff_changed")
        expected = {file["path"]: file["blob"] for file in files}
        if {f.get("filename"): f.get("sha") for f in changed} != expected or any(
            f.get("status") != "added" for f in changed
        ):
            raise Stop("diff_changed")
        for file in files:
            item = self.content(file["path"], head)
            if (
                not isinstance(item, dict)
                or item.get("type") != "file"
                or item.get("sha") != file["blob"]
                or item.get("encoding") != "base64"
            ):
                raise Stop("diff_changed")
            try:
                if base64.b64decode(item["content"]).decode("utf-8") != file["text"]:
                    raise Stop("diff_changed")
            except (KeyError, ValueError, UnicodeError):
                raise Stop("diff_changed") from None
        return value

    def pr(self, number, branch, expected_head=None):
        if type(number) is not int or number <= 0:
            raise Stop("pr_changed")
        value = self.get("/pulls/" + str(number))
        head, base = value.get("head") or {}, value.get("base") or {}
        if (
            head.get("ref") != branch
            or base.get("ref") != BASE
            or any(
                (part.get("repo") or {}).get("id") != REPO_ID for part in (head, base)
            )
            or (expected_head and head.get("sha") != expected_head)
        ):
            raise Stop("pr_changed")
        sha(head.get("sha"))
        return value

    def checks(self, number, head):
        """Require the real CI workflow and successful essential steps, not a named status."""
        value = self.get(
            "/actions/workflows/" + str(WORKFLOW) + "/runs",
            head_sha=head,
            event="pull_request",
            per_page=100,
        )
        runs = value.get("workflow_runs")
        if not isinstance(runs, list) or value.get("total_count") != len(runs):
            raise Stop("checks_incomplete")
        runs = [
            r
            for r in runs
            if r.get("head_sha") == head
            and r.get("event") == "pull_request"
            and r.get("workflow_id") == WORKFLOW
            and r.get("path") == ".github/workflows/ci.yml"
            and any(p.get("number") == number for p in r.get("pull_requests", []))
        ]
        if not runs:
            raise Stop("checks_pending")
        run = max(runs, key=lambda r: r["id"])
        if run.get("status") != "completed":
            raise Stop("checks_pending")
        if run.get("conclusion") != "success":
            raise Stop("checks_failed")
        jobs = self.get(
            "/actions/runs/" + str(run["id"]) + "/jobs", filter="latest", per_page=100
        )
        if not isinstance(jobs.get("jobs"), list) or jobs.get("total_count") != len(
            jobs["jobs"]
        ):
            raise Stop("checks_incomplete")
        matches = [
            j
            for j in jobs["jobs"]
            if j.get("name") == "checks" and j.get("head_sha") == head
        ]
        if len(matches) != 1 or matches[0].get("conclusion") != "success":
            raise Stop("checks_failed")
        steps = {
            i.get("name"): i.get("conclusion") for i in matches[0].get("steps", [])
        }
        if any(
            steps.get(name) != "success"
            for name in (
                "Compile Python",
                "Ruff",
                "Tests",
                "Dependency audit",
                "Bandit medium/high",
            )
        ):
            raise Stop("checks_failed")
        # Other reported checks may also block publication. GitHub still enforces its
        # current branch/ruleset permissions at merge; this client never bypasses them.
        checks = self.get(
            "/commits/" + head + "/check-runs", filter="latest", per_page=100
        )
        statuses = self.get("/commits/" + head + "/status", per_page=100)
        if checks.get("total_count") != len(
            checks.get("check_runs", [])
        ) or statuses.get("total_count") != len(statuses.get("statuses", [])):
            raise Stop("checks_incomplete")
        if any(
            c.get("head_sha") != head or c.get("status") != "completed"
            for c in checks["check_runs"]
        ):
            raise Stop("checks_pending")
        if any(
            c.get("conclusion") not in {"success", "neutral", "skipped"}
            for c in checks["check_runs"]
        ):
            raise Stop("checks_failed")
        if any(c.get("state") != "success" for c in statuses["statuses"]):
            raise Stop("checks_pending")
        # Read protection, rather than relying on an admin token being refused at merge.
        # Unsupported merge constraints remain a human operation in GitHub.
        protection = self.request(
            "GET", PREFIX + "/branches/main/protection", missing=True
        )
        required = set()
        if protection:
            status = protection.get("required_status_checks") or {}
            if (
                (protection.get("enforce_admins") or {}).get("enabled") is not True
                or status.get("strict") is not True
                or protection.get("required_pull_request_reviews")
                or (protection.get("required_signatures") or {}).get("enabled")
            ):
                raise Stop("branch_protection")
            required.update(status.get("contexts") or [])
            required.update(r.get("context") for r in status.get("checks", []))
        rules = self.get("/rules/branches/main")
        if not isinstance(rules, list):
            raise Stop("branch_protection")
        for rule in rules:
            if rule.get("type") in {"deletion", "non_fast_forward"}:
                continue
            if rule.get("type") != "required_status_checks" or not (
                rule.get("parameters") or {}
            ).get("strict_required_status_checks_policy"):
                raise Stop("branch_protection")
            required.update(
                r.get("context")
                for r in rule["parameters"].get("required_status_checks", [])
            )
        success = {
            r.get("name")
            for r in checks["check_runs"]
            if r.get("conclusion") == "success"
        }
        success.update(
            r.get("context")
            for r in statuses["statuses"]
            if r.get("state") == "success"
        )
        if not required <= success:
            raise Stop("checks_pending")
        return {
            "run": run["id"],
            "head": head,
            "url": "https://github.com/" + REPO + "/actions/runs/" + str(run["id"]),
        }

    def branch(self, branch, base):
        def recover():
            value = self.request(
                "GET", PREFIX + "/git/ref/heads/" + quote(branch, safe=""), missing=True
            )
            return (
                value
                if value and (value.get("object") or {}).get("sha") == base
                else None
            )

        self.mutation(
            "branch",
            "POST",
            PREFIX + "/git/refs",
            {"ref": "refs/heads/" + branch, "sha": base},
            recover,
        )
        if self.head(branch) != base:
            raise Stop("branch_changed")

    def add(self, branch, file, index, parent):
        def recover():
            item = self.content(file["path"], branch)
            if not item or item.get("sha") != file["blob"]:
                return None
            head = self.head(branch)
            commit = self.get("/git/commits/" + head)
            return (
                {"commit": {"sha": head}}
                if [p.get("sha") for p in commit.get("parents", [])] == [parent]
                else None
            )

        result = self.mutation(
            "file-" + str(index),
            "PUT",
            PREFIX + "/contents/" + quote(file["path"], safe="/"),
            {
                "message": "Noyaru: article relu (" + file["lang"] + ")",
                "branch": branch,
                "content": base64.b64encode(file["text"].encode()).decode(),
            },
            recover,
        )
        head = sha((result.get("commit") or {}).get("sha"))
        commit = self.get("/git/commits/" + head)
        if [p.get("sha") for p in commit.get("parents", [])] != [parent] or self.head(
            branch
        ) != head:
            raise Stop("branch_changed")
        return head

    def create_pr(self, branch, title, marker):
        def recover():
            values = self.get(
                "/pulls",
                state="all",
                head=REPO.split("/")[0] + ":" + branch,
                base=BASE,
                per_page=100,
            )
            if not isinstance(values, list):
                return None
            matches = [v for v in values if marker in (v.get("body") or "")]
            return matches[0] if len(matches) == 1 else None

        return self.mutation(
            "pr",
            "POST",
            PREFIX + "/pulls",
            {
                "base": BASE,
                "head": branch,
                "draft": True,
                "title": title,
                "body": "Article relu dans Noyaru. Publication soumise à une seconde validation après CI.\n\n"
                + marker,
            },
            recover,
        )

    def sync(self, branch, base, generation, files):
        def recover():
            head = self.head(branch)
            self.diff(base, head, files)
            return {"sha": head}

        result = self.mutation(
            "sync-" + str(generation),
            "POST",
            PREFIX + "/merges",
            {
                "base": branch,
                "head": base,
                "commit_message": "Noyaru: actualiser la base de l’article",
            },
            recover,
        )
        return sha(result.get("sha"))

    def ready(self, number, branch, head, attempted=False):
        value = self.pr(number, branch, head)
        if value.get("draft") is not True and not attempted:
            return
        ident = value.get("node_id")
        if not isinstance(ident, str) or not ident:
            raise Stop("pr_changed")

        def recover():
            pr = self.pr(number, branch, head)
            return {"confirmed": True} if pr.get("draft") is False else None

        result = self.mutation(
            "ready",
            "POST",
            "/graphql",
            {"query": READY, "variables": {"id": ident}},
            recover,
        )
        if result.get("errors"):
            raise Stop("github_unconfirmed")
        if self.pr(number, branch, head).get("draft") is not False:
            raise Stop("github_uncertain")

    def merge(self, number, branch, head):
        def recover():
            value = self.pr(number, branch, head)
            return (
                {"merged": True, "sha": value.get("merge_commit_sha")}
                if value.get("merged") is True
                else None
            )

        value = self.mutation(
            "merge",
            "PUT",
            PREFIX + "/pulls/" + str(number) + "/merge",
            {"sha": head, "merge_method": "merge"},
            recover,
        )
        if value.get("merged") is not True:
            raise Stop("github_unconfirmed")
        return sha(value.get("sha"))
