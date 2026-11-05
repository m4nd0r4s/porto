"""Check that commits are authored and committed with an allowed identity (CLAUDE.md, Conventions).

    uv run python scripts/check_commit_identity.py [RANGE ...]

Checks every commit in each git revision RANGE (default: the beads sync refs, `refs/dolt/data`
and `__dolt_remote_info__`, fetched from origin). CI also passes the pull request's
`base..head`. Fails when an author or committer email is not in ALLOWED_EMAILS: the usual
cause is a git config that falls back to another (global) identity. Fix it with
`git config user.email <allowed email>` in the repository that made the commit (for the beads
sync, the caches under `.beads/embeddeddolt/*/.dolt/git-remote-cache/*/repo.git`), then
rewrite the offending commits.

Exit status: 0 when every commit is allowed, 1 otherwise, 2 when a ref cannot be read.
"""

from __future__ import annotations

import subprocess
import sys

#: The project identity, and GitHub's own committer for merges made on github.com.
ALLOWED_EMAILS = frozenset(
    {
        "25236463+m4nd0r4s@users.noreply.github.com",
        "noreply@github.com",
    }
)

#: The beads (Dolt) sync refs: Dolt makes these commits itself, from its own git caches.
BEADS_REFS = {
    "refs/dolt/data": "refs/remotes/identity-check/dolt-data",
    "refs/heads/__dolt_remote_info__": "refs/remotes/identity-check/dolt-remote-info",
}


def git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def fetch_beads_refs() -> list[str]:
    """Fetches the beads sync refs from origin; returns the local names of those that exist."""
    present: list[str] = []
    for remote_ref, local_ref in BEADS_REFS.items():
        fetched = subprocess.run(
            ["git", "fetch", "--quiet", "--no-tags", "origin", f"+{remote_ref}:{local_ref}"],
            capture_output=True,
            text=True,
        )
        if fetched.returncode == 0:
            present.append(local_ref)
    return present


def bad_commits(rev_range: str) -> list[str]:
    log = git("log", "--format=%h%x00%ae%x00%ce%x00%s", rev_range)
    problems: list[str] = []
    for line in log.splitlines():
        sha, author, committer, subject = line.split("\0", 3)
        for role, email in (("author", author), ("committer", committer)):
            if email not in ALLOWED_EMAILS:
                problems.append(f"{sha} ({subject[:50]}): {role} <{email}>")
    return problems


def main(ranges: list[str]) -> int:
    if not ranges:
        ranges = fetch_beads_refs()
    problems: list[str] = []
    for rev_range in ranges:
        try:
            problems += bad_commits(rev_range)
        except subprocess.CalledProcessError as error:
            print(f"cannot read {rev_range}: {error.stderr.strip()}", file=sys.stderr)
            return 2
    if problems:
        print("Commits with an identity that is not allowed:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print(
            "Set the project identity (CLAUDE.md, Conventions) in the repository that made "
            "these commits, then rewrite them.",
            file=sys.stderr,
        )
        return 1
    print(f"commit identities ok ({', '.join(ranges) or 'no refs'})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
