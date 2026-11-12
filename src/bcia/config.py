"""One place for names shared by notebooks, jobs and the agent."""
CATALOG = "workspace"
SCHEMA = "ai_architect_lab"
VOLUME = "lab_files"
VOL_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DATA_ROOT = f"{VOL_ROOT}/beauty_ai_lab_data"


def fq(name: str) -> str:
    """Fully qualified Unity Catalog name, e.g. workspace.ai_architect_lab.gold_sales_lines."""
    return f"{CATALOG}.{SCHEMA}.{name}"
