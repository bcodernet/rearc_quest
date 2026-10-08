# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

"""
Serverless runner for the unit test suite.

Exists so the tests are runnable without a local JDK + pyspark install:

    databricks bundle run rearc_quest_tests --target dev

The suite itself (tests/test_transformations.py) is plain pytest and runs locally the
same way with `pytest tests/` if you do have Spark set up. This notebook only exists to
work around two Databricks-specific wrinkles:

1. pytest cannot run directly from a /Workspace path. Its assertion rewriter writes a
   __pycache__ directory next to each test module, and Workspace files do not support
   mkdir -- you get `OSError: [Errno 95] Operation not supported`. So the sources are
   copied to a writable temp directory first, preserving the tests/ and src/ layout the
   suite's sys.path insert depends on.
2. The Jobs API does not return notebook stdout to the CLI, so pytest's report is
   captured and folded into the raised exception. A failure then shows real assertion
   output in `bundle run` instead of an opaque exit code.
"""
import contextlib
import io
import os
import shutil
import sys
import tempfile

import pytest

tests_dir = os.getcwd()
files_root = os.path.dirname(tests_dir)

# Mirror tests/ and src/ into a writable location so pytest can rewrite assertions.
work_dir = tempfile.mkdtemp(prefix="rearc_quest_tests_")
shutil.copytree(os.path.join(files_root, "tests"), os.path.join(work_dir, "tests"))
shutil.copytree(os.path.join(files_root, "src"), os.path.join(work_dir, "src"))

target = os.path.join(work_dir, "tests")
preamble = [
    f"workspace files root: {files_root}",
    f"writable work dir:    {work_dir}",
    f"python:               {sys.version.split()[0]}",
    f"pytest:               {pytest.__version__}",
    f"test files:           {sorted(f for f in os.listdir(target) if f.startswith('test_'))}",
]
print("\n".join(preamble))

buffer = io.StringIO()
with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
    exit_code = pytest.main(["-v", "--no-header", target])

report = buffer.getvalue()
print(report)
print(f"pytest exit code: {exit_code}")

if exit_code != 0:
    raise RuntimeError(
        f"Unit tests did not pass (pytest exit code {exit_code}).\n\n"
        + "\n".join(preamble)
        + "\n\n--- pytest report (tail) ---\n"
        + report[-6000:]
    )

print("All unit tests passed.")

# Surface the one-line summary as the notebook's return value, so `bundle run` and the
# Jobs API show "N passed" instead of only a green checkmark.
summary = next(
    (
        line.strip("= ")
        for line in reversed(report.splitlines())
        if " passed" in line or " failed" in line
    ),
    "pytest completed",
)
dbutils.notebook.exit(summary)  # noqa: F821 - injected by Databricks
