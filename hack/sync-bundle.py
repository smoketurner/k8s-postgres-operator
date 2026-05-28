#!/usr/bin/env python3
"""Regenerate the OLM bundle under ``bundle/``.

Wraps ``operator-sdk generate bundle`` with three things that aren't
automatic:

1. Stages a ``deploy/`` directory from the canonical artifacts in
   ``config/`` (RBAC + ServiceAccount + Deployment + webhook Service +
   ValidatingWebhookConfiguration). operator-sdk wants separate files per
   resource; the chart doesn't ship them that way.
2. Pins the operator container image in the generated CSV's Deployment to
   the chart's default (``ghcr.io/.../<version>``) instead of the dev
   placeholder (``localhost:32000/...:latest``) that lives in
   ``config/deploy/deployment.yaml``.
3. Re-injects the ``alm-examples`` annotation from the base CSV, which
   operator-sdk resets to ``'[]'`` during merge.

Run via ``make bundle-sync`` (regenerate) or ``make bundle-check`` (verify
the committed bundle matches what the script would generate, suitable for
CI).
"""

from __future__ import annotations

import argparse
import filecmp
import pathlib
import shutil
import subprocess
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parent.parent
BUNDLE_DIR = REPO / "bundle"
BUNDLE_DOCKERFILE = REPO / "bundle.Dockerfile"
CSV_FILE = BUNDLE_DIR / "manifests" / "postgres-operator.clusterserviceversion.yaml"
BASE_CSV = (
    REPO
    / "config"
    / "manifests"
    / "bases"
    / "postgres-operator.clusterserviceversion.yaml"
)
DEPLOY_SOURCES = {
    "config/rbac/role.yaml": "role.yaml",
    "config/rbac/rolebinding.yaml": "role_binding.yaml",
    "config/rbac/serviceaccount.yaml": "service_account.yaml",
    "config/deploy/deployment.yaml": "deployment.yaml",
    "config/webhook/service.yaml": "webhook_service.yaml",
    "config/webhook/validating-webhook.yaml": "webhooks.yaml",
}
DEV_IMAGE = "localhost:32000/postgres-operator:latest"
RELEASE_IMAGE_TEMPLATE = "ghcr.io/smoketurner/k8s-postgres-operator:{version}"

VERSION = "0.2.0"
PACKAGE = "postgres-operator"
CHANNEL = "alpha"


def stage_workdir(tmp_root: pathlib.Path) -> pathlib.Path:
    """Stage everything operator-sdk needs into a temp workdir.

    operator-sdk writes ``bundle.Dockerfile`` to its current working
    directory, so we run it from a sandbox to avoid clobbering the
    committed file when verifying drift.
    """
    work = tmp_root / "work"
    deploy = work / "deploy"
    deploy.mkdir(parents=True, exist_ok=True)
    for src_rel, dst_name in DEPLOY_SOURCES.items():
        src = REPO / src_rel
        if not src.exists():
            raise SystemExit(f"sync-bundle: missing {src_rel}")
        shutil.copy(src, deploy / dst_name)

    # operator-sdk reads CRDs and the kustomize base directly off disk.
    shutil.copytree(REPO / "config" / "crd", work / "crd")
    shutil.copytree(REPO / "config" / "manifests", work / "manifests")
    return work


def run_operator_sdk(work: pathlib.Path, target_dir: pathlib.Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "operator-sdk",
        "generate",
        "bundle",
        "--deploy-dir",
        str(work / "deploy"),
        "--crds-dir",
        str(work / "crd"),
        "--kustomize-dir",
        str(work / "manifests"),
        "--package",
        PACKAGE,
        "--version",
        VERSION,
        "--channels",
        CHANNEL,
        "--default-channel",
        CHANNEL,
        "--output-dir",
        str(target_dir),
        "--quiet",
    ]
    proc = subprocess.run(cmd, cwd=work, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"operator-sdk failed: exit {proc.returncode}")


def patch_csv(csv_path: pathlib.Path) -> None:
    """Apply two post-generation fixes to the CSV.

    1. Replace the dev placeholder image with the released image tag.
    2. Restore the ``alm-examples`` annotation from the base CSV (operator-sdk
       resets it to ``'[]'``).
    """
    text = csv_path.read_text()

    # Image swap. We do a literal-string replace; the chart and the bundle
    # should agree on the released image tag.
    released = RELEASE_IMAGE_TEMPLATE.format(version=VERSION)
    if DEV_IMAGE not in text:
        raise SystemExit(
            f"sync-bundle: expected dev image {DEV_IMAGE!r} in generated CSV"
        )
    text = text.replace(DEV_IMAGE, released)

    # Pull alm-examples back in from the base CSV. We locate it in the base
    # by simple line scanning rather than YAML parsing so the formatting
    # comes through unchanged.
    base_lines = BASE_CSV.read_text().splitlines(keepends=True)
    examples_block: list[str] = []
    in_block = False
    block_indent: str | None = None
    for line in base_lines:
        stripped = line.lstrip()
        if not in_block and stripped.startswith("alm-examples:"):
            in_block = True
            block_indent = line[: len(line) - len(stripped)]
            examples_block.append(line)
            continue
        if in_block:
            # The block ends at the first line that is not blank and is
            # indented at or above the annotation's column.
            assert block_indent is not None
            if line.strip() == "" or line.startswith(block_indent + " "):
                examples_block.append(line)
                continue
            break
    if not examples_block:
        raise SystemExit("sync-bundle: alm-examples block not found in base CSV")

    new_lines: list[str] = []
    out_text_lines = text.splitlines(keepends=True)
    i = 0
    replaced = False
    while i < len(out_text_lines):
        line = out_text_lines[i]
        stripped = line.lstrip()
        if stripped.startswith("alm-examples:"):
            generated_indent = line[: len(line) - len(stripped)]
            # Drop generated lines for this annotation (a single line or a
            # block, depending on YAML emitter behavior).
            i += 1
            while i < len(out_text_lines):
                nxt = out_text_lines[i]
                if nxt.strip() == "":
                    i += 1
                    continue
                if nxt.startswith(generated_indent + " "):
                    i += 1
                    continue
                break
            # Re-indent the base block to match the generated indent if
            # they differ.
            assert block_indent is not None
            if block_indent != generated_indent:
                for src_line in examples_block:
                    if src_line.strip() == "":
                        new_lines.append(src_line)
                    else:
                        new_lines.append(generated_indent + src_line[len(block_indent) :])
            else:
                new_lines.extend(examples_block)
            replaced = True
            continue
        new_lines.append(line)
        i += 1
    if not replaced:
        raise SystemExit("sync-bundle: alm-examples annotation not found in generated CSV")

    csv_path.write_text("".join(new_lines))


def patch_dockerfile(path: pathlib.Path) -> None:
    """Rewrite operator-sdk's absolute COPY paths to portable relative paths.

    operator-sdk bakes the literal ``--output-dir`` argument into the COPY
    instructions, which makes the Dockerfile unbuildable by anyone whose
    working directory differs. Replace those paths with the canonical
    ``bundle/manifests`` / ``bundle/metadata`` relative paths so
    ``docker build -f bundle.Dockerfile .`` works from the repo root.
    """
    import re

    text = path.read_text()
    text = re.sub(
        r"^COPY\s+\S+/bundle/manifests\s+/manifests/$",
        "COPY bundle/manifests /manifests/",
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^COPY\s+\S+/bundle/metadata\s+/metadata/$",
        "COPY bundle/metadata /metadata/",
        text,
        flags=re.MULTILINE,
    )
    path.write_text(text)


def regenerate(target_dir: pathlib.Path, target_dockerfile: pathlib.Path) -> None:
    with tempfile.TemporaryDirectory(prefix="bundle-sync-") as tmp:
        tmp_root = pathlib.Path(tmp)
        work = stage_workdir(tmp_root)
        run_operator_sdk(work, target_dir)
        patch_csv(
            target_dir / "manifests" / "postgres-operator.clusterserviceversion.yaml"
        )
        # operator-sdk drops bundle.Dockerfile alongside its cwd (the temp
        # workdir). Move it to the caller's target.
        produced_dockerfile = work / "bundle.Dockerfile"
        if not produced_dockerfile.exists():
            raise SystemExit("sync-bundle: operator-sdk did not produce bundle.Dockerfile")
        target_dockerfile.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(produced_dockerfile), str(target_dockerfile))
        patch_dockerfile(target_dockerfile)


def cmd_sync() -> int:
    if BUNDLE_DIR.exists():
        shutil.rmtree(BUNDLE_DIR)
    if BUNDLE_DOCKERFILE.exists():
        BUNDLE_DOCKERFILE.unlink()
    regenerate(BUNDLE_DIR, BUNDLE_DOCKERFILE)
    print(f"wrote {BUNDLE_DIR.relative_to(REPO)}/ and bundle.Dockerfile")
    return 0


def _diff(left: pathlib.Path, right: pathlib.Path) -> list[str]:
    """Recursive content diff. Returns list of relative paths that differ."""
    if not left.exists() or not right.exists():
        return [str(left.relative_to(REPO))]
    diff: list[str] = []
    cmp = filecmp.dircmp(left, right)
    queue = [(left, right, cmp)]
    while queue:
        a, b, c = queue.pop()
        for name in c.left_only + c.right_only + c.diff_files + c.funny_files:
            diff.append(str((a / name).relative_to(REPO)))
        for name in c.common_dirs:
            queue.append((a / name, b / name, c.subdirs[name]))
    return diff


def cmd_check() -> int:
    with tempfile.TemporaryDirectory(prefix="bundle-check-") as tmp:
        tmp_root = pathlib.Path(tmp)
        expected_dir = tmp_root / "bundle"
        expected_dockerfile = tmp_root / "bundle.Dockerfile"
        regenerate(expected_dir, expected_dockerfile)

        drifted: list[str] = _diff(BUNDLE_DIR, expected_dir)
        if BUNDLE_DOCKERFILE.read_bytes() != expected_dockerfile.read_bytes():
            drifted.append(str(BUNDLE_DOCKERFILE.relative_to(REPO)))

    if drifted:
        print(
            "OLM bundle is out of sync with the source artifacts:\n  "
            + "\n  ".join(sorted(set(drifted)))
            + "\nRun `make bundle-sync` to regenerate.",
            file=sys.stderr,
        )
        return 1
    print("OLM bundle is in sync with the source artifacts")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["sync", "check"])
    args = parser.parse_args()
    return cmd_sync() if args.action == "sync" else cmd_check()


if __name__ == "__main__":
    raise SystemExit(main())
