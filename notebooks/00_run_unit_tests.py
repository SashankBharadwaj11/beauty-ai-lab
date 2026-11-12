# Databricks notebook source
# MAGIC %md
# MAGIC # Run the unit tests inside Databricks
# MAGIC Proves two things: the `src/` code is importable from this Git folder, and its logic passes.
# MAGIC **Expected:** `14 passed`. Any failure prints the test name and what differed.

# COMMAND ----------

# MAGIC %pip install -q pytest

# COMMAND ----------

import os, sys
import pytest

sys.dont_write_bytecode = True                       # don't write __pycache__ into the Git folder
repo_root = os.path.abspath("..")                    # this notebook lives in notebooks/
sys.path.insert(0, os.path.join(repo_root, "src"))

exit_code = pytest.main([os.path.join(repo_root, "tests"), "-q", "-p", "no:cacheprovider"])
assert exit_code == 0, "Some tests failed. Read the output above."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using `src/` from any notebook in this repo
# MAGIC ```python
# MAGIC import os, sys
# MAGIC sys.path.insert(0, os.path.abspath("../src"))
# MAGIC from bcia.guards import check_claims
# MAGIC from bcia.config import fq
# MAGIC ```
