"""
Every command printed in the documentation must work as written.

Documents drift. A flag gets renamed, a required option is added, and the
example in the README quietly stops working. The first person to notice is a
vendor following it. These tests parse every documented command, so that
cannot happen without a failing test.
"""
import re
import shlex
from pathlib import Path

import pytest

from run_validation import build_parser, require_connection

ROOT = Path(__file__).resolve().parent.parent
USER_DOCS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]

# What the documents tell the reader to export before running anything.
DOCUMENTED_ENV = {
    "QW_S3_ENDPOINT": "https://s3.example.com",
    "QW_S3_BUCKET": "my-test-bucket",
    "QW_S3_ACCESS_KEY": "AKIAEXAMPLE",
    "QW_S3_SECRET_KEY": "secret",
    "QW_AWS_BUCKET": "my-aws-bucket",
    "QW_AWS_ACCESS_KEY": "AKIAEXAMPLE",
    "QW_AWS_SECRET_KEY": "secret",
}


def documented_commands(path):
    """Every `python run_validation.py ...` command in one document."""
    text = path.read_text()
    commands = []
    # Consume each fence with its language tag, so a ```text block cannot
    # shift the pairing of every fence after it.
    for block in re.findall(r"```[\w-]*\n(.*?)```", text, re.S):
        joined = block.replace("\\\n", " ")
        for line in joined.splitlines():
            line = line.strip()
            if line.startswith("python run_validation.py"):
                commands.append(line)
    return commands


ALL = [(p.relative_to(ROOT), c) for p in USER_DOCS for c in documented_commands(p)]


def test_the_documents_contain_commands_to_check():
    assert len(ALL) >= 8


@pytest.mark.parametrize("doc,command", ALL, ids=[f"{d}:{c[27:60]}" for d, c in ALL])
def test_every_documented_command_works_as_written(doc, command, monkeypatch):
    for name, value in DOCUMENTED_ENV.items():
        monkeypatch.setenv(name, value)
    argv = shlex.split(command)[2:]  # drop "python run_validation.py"
    parser = build_parser()
    args = parser.parse_args(argv)  # a missing or wrong flag exits here
    # `report` reads local files only. `cleanup` takes the endpoint and the
    # bucket from the run's own records, so it needs keys but no address.
    if args.cmd == "cleanup":
        assert args.access_key and args.secret_key
    elif args.cmd != "report":
        require_connection(parser, args)  # a missing connection setting exits here


def test_commands_live_in_one_place():
    """
    Running instructions live in run_a_validation.md. The README may repeat a
    command to get people started, but only word for word, so the two can
    never disagree.
    """
    guide = set(documented_commands(ROOT / "docs" / "run_a_validation.md"))
    for path in USER_DOCS:
        if path.name in ("run_a_validation.md",):
            continue
        for command in documented_commands(path):
            assert command in guide, (
                f"{path.name} has a command that is not in run_a_validation.md:\n  {command}"
            )


def test_every_link_between_documents_points_somewhere():
    for path in USER_DOCS + sorted((ROOT / "docs" / "background").glob("*.md")):
        for target in re.findall(r"\]\(([^)#\s]+)", path.read_text()):
            if target.startswith("http"):
                continue
            assert (path.parent / target).exists(), f"{path.name} links to missing {target}"


def test_the_old_names_still_work(moto_server_endpoint, tmp_path):
    """
    The tool was `run_certification.py`, with a `certify` command. This is a
    validation, not a formal certification, so both were renamed. Scripts
    written for the old names must keep working.
    """
    import subprocess
    import sys

    from run_validation import main

    old = subprocess.run(
        [sys.executable, str(ROOT / "run_certification.py"), "--help"],
        capture_output=True, text=True, cwd=ROOT,
    )
    assert old.returncode == 0 and "validate" in old.stdout
    run = tmp_path / "old-command"
    assert main(["certify", "--endpoint", moto_server_endpoint, "--bucket", "old-names",
                 "--access-key", "testing", "--secret-key", "testing", "--run-dir", str(run),
                 "--tier", "100GB", "--duration-min", "0.02", "--levels", "1,2",
                 "--repeats", "1"]) == 0
    assert (run / "report.html").exists()


def test_reader_facing_text_never_says_certified():
    """This is a synthetic validation. The result words are PASS, FAIL, INCONCLUSIVE."""
    import re

    for path in USER_DOCS:
        for line in path.read_text().splitlines():
            for m in re.finditer(r"CERTIFIED|[Cc]ertif(?!icate)", line):
                assert "formal certification" in line or "run_certification" in line, (
                    f"{path.name}: {line.strip()[:100]}"
                )
