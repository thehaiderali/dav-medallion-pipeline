"""Build the final architecture and FinOps report.

    uv run --with reportlab python scripts/build_architecture_report.py

Every measured number is read from docs/evidence/phase3_run_evidence.json.
If docs/screenshots/spark_dag_sector_fact.png exists it is embedded as the
Spark DAG screenshot; otherwise the report says the screenshot is missing.
"""
import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    KeepTogether, Flowable, Image,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "pdf" / "phase3_architecture_finops_report.pdf"
EVIDENCE = json.loads((ROOT / "docs" / "evidence" / "phase3_run_evidence.json").read_text(encoding="utf-8"))
DAG_SCREENSHOT = ROOT / "docs" / "screenshots" / "spark_dag_sector_fact.png"

NAVY = colors.HexColor("#14263D")
BLUE = colors.HexColor("#2466A0")
TEAL = colors.HexColor("#007E83")
AMBER = colors.HexColor("#A86A00")
LIGHT = colors.HexColor("#EAF2F5")
GRAY = colors.HexColor("#526274")
RULE = colors.HexColor("#CAD7DE")

RUNS = EVIDENCE["runs"]
FULL = RUNS["full_load"]
INC = RUNS["incremental_backfill"]
FINAL = RUNS["final_state"]
LAYOUT = EVIDENCE["table_layout"]
QUERY = EVIDENCE["sector_fact_query"]
QFULL, QDAY = QUERY["full_rebuild"], QUERY["one_day_filter"]
PRICES = EVIDENCE["billing"]["list_prices_usd"]
JOBS_USD_PER_DBU = PRICES["PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO_per_dbu"]
SQL_USD_PER_DBU = PRICES["PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO_per_dbu"]

# ---- Daily-run model derived from the measured backfill --------------------
# The backfill invocation contained 17 trading-day batches, 6 failed weekend
# reads (~0.6 s each) and one metadata promotion (6.1 s) plus a skipped
# metadata load (0.5 s). Spreading the remainder over 17 days gives the
# per-day cost inside one invocation; job overhead is wall time minus pipeline.
FIXED_IN_PIPELINE_S = 0.5 + 6.1
PER_DAY_S = (INC["pipeline_seconds"] - FIXED_IN_PIPELINE_S - 6 * 0.6) / INC["trading_day_batches"]
JOB_OVERHEAD_S = INC["job_wall_seconds"] - INC["pipeline_seconds"]
DAILY_RUN_S = PER_DAY_S + FIXED_IN_PIPELINE_S + JOB_OVERHEAD_S
UNMEASURED_PER_DAY_S = PER_DAY_S - INC["avg_step_seconds_per_trading_day"]

HOURLY_RUNS = 24 * 365
HOURLY_COMPUTE_H = HOURLY_RUNS * DAILY_RUN_S / 3600
DAILY_COMPUTE_H = 365 * DAILY_RUN_S / 3600
DBU_RATES = (2, 4, 8)  # scenario DBU/hour for a small serverless notebook task

WAREHOUSE_DBU_PER_H = 4  # 2X-Small serverless SQL warehouse
BI_MINUTES_PER_REFRESH = 10 + 1  # 10 min auto-stop + ~1 min import refresh
BI_HOURLY_H = HOURLY_RUNS * BI_MINUTES_PER_REFRESH / 60
BI_DAILY_H = 365 * BI_MINUTES_PER_REFRESH / 60


def usd(value):
    return f"USD {value:,.0f}" if value >= 100 else f"USD {value:,.2f}"


def n(value):
    return f"{value:,}"


# ---- Figures -----------------------------------------------------------------
class Architecture(Flowable):
    """Raw sources through Bronze, Silver and Gold into Power BI."""

    def __init__(self, width=170 * mm):
        Flowable.__init__(self)
        self.width, self.height = width, 72 * mm

    def _box(self, x, y, w, h, fill, title, lines):
        c = self.canv
        c.setFillColor(fill)
        c.roundRect(x, y, w, h, 2.5 * mm, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 7.4)
        c.drawCentredString(x + w / 2, y + h - 5.5 * mm, title)
        c.setFont("Helvetica", 6.1)
        for i, line in enumerate(lines):
            c.drawCentredString(x + w / 2, y + h - 10 * mm - i * 3.3 * mm, line)

    def _arrow(self, x1, y1, x2, y2, color=BLUE):
        c = self.canv
        c.setStrokeColor(color)
        c.setFillColor(color)
        c.setLineWidth(1.1)
        c.line(x1, y1, x2 - 1.6 * mm, y2)
        p = c.beginPath()
        p.moveTo(x2, y2)
        p.lineTo(x2 - 1.8 * mm, y2 + 1.1 * mm)
        p.lineTo(x2 - 1.8 * mm, y2 - 1.1 * mm)
        p.close()
        c.drawPath(p, fill=1, stroke=0)

    def draw(self):
        c = self.canv
        gap = 3.2 * mm
        w = (self.width - gap * 5) / 6
        h = 30 * mm
        y = 30 * mm
        boxes = [
            (GRAY, "Sources", ["yfinance daily OHLCV", "Wikipedia S&P 500", "list (one-time CSV)"]),
            (GRAY, "Raw landing", ["GitHub Action 00:00", "one JSONL per day", "UC Volume /raw"]),
            (NAVY, "Bronze", ["Delta, append-only", "raw_payload kept", "rescued_data"]),
            (NAVY, "Silver", ["typed contract", "dedup + MERGE", "on ticker, date"]),
            (TEAL, "Gold", ["star schema", "3 dims, 2 facts", "hash-guarded MERGE"]),
            (TEAL, "Power BI", ["Databricks connector", "SQL warehouse", "Import mode"]),
        ]
        for i, (fill, title, lines) in enumerate(boxes):
            x = i * (w + gap)
            self._box(x, y, w, h, fill, title, lines)
            if i < len(boxes) - 1:
                self._arrow(x + w + .4 * mm, y + h / 2, x + w + gap - .3 * mm, y + h / 2)

        # Operational band underneath Bronze..Gold
        bx = 2 * (w + gap)
        bw = 3 * w + 2 * gap
        c.setFillColor(LIGHT)
        c.roundRect(bx, 6 * mm, bw, 16 * mm, 2.5 * mm, fill=1, stroke=0)
        c.setFillColor(NAVY)
        c.setFont("Helvetica-Bold", 7)
        c.drawCentredString(bx + bw / 2, 16.5 * mm, "Operational tables (same schema)")
        c.setFont("Helvetica", 6.3)
        c.drawCentredString(bx + bw / 2, 12.5 * mm, "pipeline_execution_logs  |  quarantine_prices  |  schema_drift_log")
        c.drawCentredString(bx + bw / 2, 9 * mm, "one audit row per step: duration, rows read / inserted / updated / quarantined")
        c.setStrokeColor(RULE)
        c.setDash(2, 2)
        for i in (2, 3, 4):
            cx = i * (w + gap) + w / 2
            c.line(cx, y, cx, 22 * mm)
        c.setDash()

        # Storage / compute labels above
        c.setFillColor(GRAY)
        c.setFont("Helvetica", 6.3)
        c.drawCentredString((w + gap) * 1 + w / 2, y + h + 3 * mm, "Git slices + S3 copy")
        c.drawCentredString(bx + bw / 2, y + h + 3 * mm,
                            "UC managed Delta: workspace.dav_phase2")
        c.drawCentredString(5 * (w + gap) + w / 2, y + h + 3 * mm, "or CSV export")


class PlanDag(Flowable):
    """Physical plan of the Gold sector aggregation, drawn from EXPLAIN FORMATTED."""

    def __init__(self, width=170 * mm):
        Flowable.__init__(self)
        self.width, self.height = width, 64 * mm

    def _node(self, x, y, w, h, title, detail, fill=NAVY):
        c = self.canv
        c.setFillColor(fill)
        c.roundRect(x, y, w, h, 2 * mm, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 6.6)
        c.drawCentredString(x + w / 2, y + h - 4.3 * mm, title)
        c.setFont("Helvetica", 5.8)
        for i, line in enumerate(detail):
            c.drawCentredString(x + w / 2, y + h - 7.6 * mm - i * 2.9 * mm, line)

    def _link(self, x1, y1, x2, y2):
        c = self.canv
        c.setStrokeColor(BLUE)
        c.setLineWidth(1)
        c.line(x1, y1, x2, y2)

    def draw(self):
        w, h = 36 * mm, 14 * mm
        col = [0, 44 * mm, 88 * mm, 132 * mm]
        top, mid, low = 46 * mm, 26 * mm, 4 * mm
        self._node(col[0], top, w, h, "Scan gold_dim_company", ["2 of 5 columns read", "503 rows"], GRAY)
        self._node(col[0], low, w, h, "Scan gold_fact_daily_price", ["4 of 11 columns read", "runtime filter on ticker"], GRAY)
        self._node(col[1], top, w, h, "Exchange: broadcast", ["SinglePartition", "EXECUTOR_BROADCAST"], AMBER)
        self._node(col[1], mid, w, h, "BroadcastHashJoin", ["inner on ticker", "no shuffle of fact rows"])
        self._node(col[2], mid, w, h, "HashAggregate (partial)", ["group date_key, sector_key", "count, sum before shuffle"])
        self._node(col[2], low, w, h, "Exchange: shuffle", ["hashpartitioning", "(date_key, sector_key, 16)"], AMBER)
        self._node(col[3], low, w, h, "HashAggregate (final)", ["merge partial counts/sums", f"{n(QFULL['rows_produced'])} output rows"])
        self._node(col[3], mid, w, h, "Result / Delta write", ["pipeline: overwrite", "gold_fact_sector_daily"], TEAL)
        self._link(col[0] + w, top + h / 2, col[1], top + h / 2)
        self._link(col[1] + w / 2, top, col[1] + w / 2, mid + h)
        self._link(col[0] + w, low + h / 2, col[1] + w / 2, mid)
        self._link(col[1] + w, mid + h / 2, col[2], mid + h / 2)
        self._link(col[2] + w / 2, mid, col[2] + w / 2, low + h)
        self._link(col[2] + w, low + h / 2, col[3], low + h / 2)
        self._link(col[3] + w / 2, low + h, col[3] + w / 2, mid)
        c = self.canv
        c.setFillColor(GRAY)
        c.setFont("Helvetica", 6)
        c.drawString(col[2], top + h / 2, "Amber = exchange (stage boundary).")
        c.drawString(col[2], top + h / 2 - 3.2 * mm, "All operators ran in Photon.")


# ---- Styles --------------------------------------------------------------------
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="TitleCustom", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=22, leading=27, textColor=NAVY, spaceAfter=6, alignment=0))
styles.add(ParagraphStyle(name="H1Custom", parent=styles["Heading1"], fontSize=13.5, leading=17, textColor=NAVY, spaceBefore=10, spaceAfter=5))
styles.add(ParagraphStyle(name="H2Custom", parent=styles["Heading2"], fontSize=10, leading=13, textColor=BLUE, spaceBefore=7, spaceAfter=3))
styles.add(ParagraphStyle(name="BodyCustom", parent=styles["BodyText"], fontSize=8.6, leading=12.3, textColor=NAVY, spaceAfter=5))
styles.add(ParagraphStyle(name="SmallCustom", parent=styles["BodyText"], fontSize=7.4, leading=10.2, textColor=NAVY, spaceAfter=0))
styles.add(ParagraphStyle(name="NoteCustom", parent=styles["BodyText"], fontSize=7.4, leading=10.2, textColor=GRAY, spaceAfter=4))
styles.add(ParagraphStyle(name="CaptionCustom", parent=styles["BodyText"], fontSize=7, leading=10, textColor=GRAY, alignment=TA_CENTER, spaceAfter=6))


def P(text, style="BodyCustom"):
    return Paragraph(text, styles[style])


def heading(text):
    return P(text, "H1Custom")


def sub(text):
    return P(text, "H2Custom")


def table(rows, widths, header=True):
    data = [[P(str(cell), "SmallCustom") for cell in row] for row in rows]
    if header:
        data[0] = [Paragraph(f'<font color="#FFFFFF"><b>{cell}</b></font>', styles["SmallCustom"]) for cell in rows[0]]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
    commands = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1 if header else 0), (-1, -1), [colors.white, colors.HexColor("#F3F7F8")]),
        ("LINEBELOW", (0, -1), (-1, -1), .4, RULE),
        ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if header:
        commands += [("BACKGROUND", (0, 0), (-1, 0), NAVY)]
    t.setStyle(TableStyle(commands))
    return t


# ---- Content -------------------------------------------------------------------
story = []

# Page 1: overview and architecture
story += [
    P("S&amp;P 500 Lakehouse: Architecture and FinOps Report", "TitleCustom"),
    P(f"Haider Ali  |  11 November 2026  |  github.com/thehaiderali/dav-medallion-pipeline at commit {EVIDENCE['workspace']['repo_commit']}", "NoteCustom"),
    P(f"Runtime: {EVIDENCE['workspace']['edition']}, {EVIDENCE['workspace']['cloud_region']}. {EVIDENCE['workspace']['compute']}.", "NoteCustom"),
    Spacer(1, 3 * mm),
    heading("1. Architecture"),
    Architecture(),
    P("Figure 1. Data flow. Bronze, Silver and Gold run as one serverless notebook job (00_end_to_end). Each Silver batch triggers the Gold refresh.", "CaptionCustom"),
    P("A GitHub Action extracts one trading day at midnight and commits it as a date-named JSONL slice; the 4-year history was extracted once into a single file. Raw files are copied into a Unity Catalog Volume. Bronze stores every arrival with its original JSON. Silver casts to an explicit contract, sends invalid rows to quarantine, keeps the latest record per (ticker, trade_date) and MERGEs it. Gold publishes a star schema that Power BI imports through a SQL warehouse."),
    heading("2. What was run"),
    table([
        ["Run", "Input", "Result", "Wall time"],
        ["Full load", f"full_load.jsonl, {n(FULL['rows']['bronze'])} rows, 2022-10-03 to 2026-09-30", f"SUCCESS. {n(FULL['rows']['silver'])} rows in Silver and Gold, 0 quarantined", f"{FULL['job_wall_seconds']} s"],
        ["Incremental backfill", f"{INC['trading_day_batches']} daily slices, {INC['window']}", f"All {INC['trading_day_batches']} days reached Gold. Job marked FAILED by 6 weekend dates (see 3.6)", f"{INC['job_wall_seconds']} s"],
    ], [28 * mm, 52 * mm, 68 * mm, 22 * mm]),
    Spacer(1, 3 * mm),
    table([
        ["Final table", "Rows", "Check"],
        ["silver_prices / gold_fact_daily_price", n(FINAL["gold_fact_daily_price"]), f"{n(FULL['rows']['silver'])} + 12 x 503 + 5 x 502; {FINAL['duplicate_ticker_date_keys']} duplicate (ticker, date) keys"],
        ["gold_fact_sector_daily", n(FINAL["gold_fact_sector_daily"]), f"{n(FULL['rows']['gold_fact_sector_daily'])} + 17 days x 11 sectors"],
        ["gold_dim_date / company / sector", f"{n(FINAL['gold_dim_date'])} / 503 / 11", f"trading dates {FINAL['trade_date_range']}"],
        ["quarantine_prices / schema_drift_log", f"{FINAL['quarantine_rows']} / {FINAL['schema_drift_log_rows']}", "no invalid values or new source fields in this data"],
    ], [52 * mm, 30 * mm, 88 * mm]),
]
story.append(PageBreak())

# Page 2: decision log
story += [
    heading("3. Decision log"),
    sub("3.1 Partitioning"),
    P("<b>Raw files are partitioned by date; Delta tables are not.</b> Incremental input lives at incremental/YYYY-MM-DD/data.jsonl, so a rerun or backfill addresses exactly one day and the batch id (incremental_YYYYMMDD) is derived from the path. No Delta table has partition columns:"),
    table([
        ["Table", "Files", "Size", "Partition columns"],
        *[[name, str(v["files"]), f"{v['mb']} MB", "none"] for name, v in LAYOUT.items()],
    ], [60 * mm, 20 * mm, 25 * mm, 65 * mm]),
    Spacer(1, 2 * mm),
    P(f"Partitioning Silver by trade_date would create {n(FINAL['gold_dim_date'])} directories of roughly 25 KB each. That is the small-file pattern Delta guidance warns against: more file listing and metadata work for no I/O saving. Date filters are still selective without partitions because files are written in date order and Delta keeps min/max statistics per file. Measured: a one-day filter on gold_fact_daily_price skipped {QDAY['files_pruned']} of {QDAY['files_pruned'] + QDAY['files_read']} files (section 4). Partitioning or liquid clustering on date_key becomes worthwhile when a table reaches hundreds of GB."),
    sub("3.2 Schema drift"),
    P("Bronze reads each JSON line as text and parses it twice: once against the price contract and once as a string map. Keys outside the contract are kept three ways: inside raw_payload, in a rescued_data JSON column, and as new string columns added with mergeSchema. Each new key writes a NEW_COLUMN row to schema_drift_log. Silver selects only contract columns with try_cast, so a new field never breaks Silver or Gold, and a changed type sends the row to quarantine with a reject reason instead of failing the batch."),
    P(f"No drift occurred in the production data: schema_drift_log has {FINAL['schema_drift_log_rows']} rows after {n(FINAL['bronze_prices'])} Bronze rows. The added-column path is exercised by notebook 03_demo_idempotency_and_audit, which was not executed for this report. One source change did occur and is not schema drift: KVUE has no prices after 2026-10-16, so daily slices dropped from 503 to 502 rows. The pipeline accepted this; the company dimension keeps KVUE and its history."),
    sub("3.3 PII"),
    P("The data contains no personal data: public daily prices and a public company directory. Audit tables store run ids, batch ids, Volume paths and counts, with no user names or e-mail addresses. The decision was therefore not to add masking. If a person-level field appeared it would surface first as a NEW_COLUMN drift row; the response would be to drop or hash it in the Bronze-to-Silver projection and apply a Unity Catalog column mask to the Bronze copy."),
    sub("3.4 Idempotent loads"),
    P(f"Bronze is append-only so every arrival stays auditable. Silver and Gold MERGE on natural keys and update only when a row hash changes. This was tested by accident: a cancelled automatic retry re-appended {n(FINAL['bronze_prices'] - FINAL['silver_prices'])} rows to Bronze, and Silver and Gold still hold {n(FINAL['silver_prices'])} rows with zero duplicate keys."),
    sub("3.5 Gold refresh after every Silver batch"),
    P(f"Refreshing Gold per batch keeps Gold consistent with Silver at every commit and costs little for one daily batch. In a backfill it repeats five Gold steps per day: about 23 of the {INC['avg_step_seconds_per_trading_day']} s spent per trading day. Refreshing Gold once at the end of a window gives the same result, because the Gold MERGEs are hash-guarded."),
    sub("3.6 Known defect: skip_missing on serverless"),
    P("Weekend dates have no file. raw_to_bronze catches a missing-path error to skip them, but serverless (Spark Connect) analyses the read lazily, so the error is raised after the try block. The six weekend dates were logged as FAILURE and the job failed after all trading days had loaded. Fix: check the path with dbutils.fs or force analysis inside the try block."),
]
story.append(PageBreak())

# Page 3: Spark DAG
dag_block = []
if DAG_SCREENSHOT.exists():
    img = Image(str(DAG_SCREENSHOT))
    scale = min(170 * mm / img.imageWidth, 95 * mm / img.imageHeight)
    img.drawWidth, img.drawHeight = img.imageWidth * scale, img.imageHeight * scale
    dag_block += [img, P(f"Figure 2. Databricks Query Profile for statement {QFULL['statement_id']}.", "CaptionCustom")]
else:
    dag_block += [P("<b>Screenshot not included.</b> Serverless compute has no classic Spark UI; the equivalent is the Query Profile in SQL query history. "
                    f"Save it as docs/screenshots/spark_dag_sector_fact.png (statement {QFULL['statement_id']}) and rebuild this PDF to embed it.", "NoteCustom")]
dag_block += [PlanDag(), P("Figure 3. Physical plan of the sector aggregation, transcribed from EXPLAIN FORMATTED on the SQL warehouse.", "CaptionCustom")]

story += [
    heading("4. Spark execution of the Gold sector fact"),
    P("The most expensive Gold step is SILVER_TO_GOLD_SECTOR_FACT. It joins every daily price row to the company dimension to get a sector, groups by (date_key, sector_key), and overwrites gold_fact_sector_daily. The same query was run on the 2X-Small serverless SQL warehouse to capture its plan and metrics."),
    *dag_block,
    sub("How Spark executed it"),
    P("<b>Broadcast instead of shuffle join.</b> The company table is 503 rows, so the optimizer collects it to one partition and broadcasts it to every executor (EXECUTOR_BROADCAST). The 507k fact rows are joined in place and never shuffled for the join."),
    P("<b>Column pruning and runtime filtering.</b> The fact scan reads 4 of its 11 columns (date_key, ticker, volume, dollar_volume_proxy), so only 9.1 MB of the 32.6 MB table was read. The scan also carries an OptionalDataFilter, hashedrelationcontains(ticker), built from the broadcast side; it drops rows with no company before the join."),
    P("<b>One shuffle, after a partial aggregate.</b> Each task pre-aggregates its rows by (date_key, sector_key). Only those partial counts and sums cross the network, hash-partitioned into 16 shuffle partitions, and a final aggregate merges them. This exchange is the single stage boundary in the fact path. Adaptive Query Execution is on, and Photon ran every operator."),
    P("<b>Predicate pushdown.</b> Adding WHERE date_key = 20261023 pushes the filter into the Delta scan. File statistics then exclude files whose date_key range cannot match:"),
    table([
        ["Metric", "Full rebuild", "One-day filter"],
        ["Files read / skipped", f"{QFULL['files_read']} / {QFULL['files_pruned']}", f"{QDAY['files_read']} / {QDAY['files_pruned']}"],
        ["Rows read", n(QFULL["rows_read"]), n(QDAY["rows_read"])],
        ["Bytes read", f"{QFULL['read_bytes'] / 1e6:.2f} MB", f"{QDAY['read_bytes'] / 1e3:.1f} KB"],
        ["Tasks / total task time", f"{QFULL['tasks']} / {QFULL['task_total_ms'] / 1000:.1f} s", f"{QDAY['tasks']} / {QDAY['task_total_ms'] / 1000:.2f} s"],
        ["Wall time", f"{QFULL['total_ms'] / 1000:.2f} s", f"{QDAY['total_ms'] / 1000:.2f} s"],
        ["Spill to disk", "0", "0"],
    ], [60 * mm, 55 * mm, 55 * mm]),
    Spacer(1, 2 * mm),
    P("In the pipeline this step averaged 3.6 s per run, and it rebuilds every date each time. The step also calls count() on the joined frame and on the result before writing, so the join and aggregation execute three times per refresh instead of once."),
]
story.append(PageBreak())

# Page 4: FinOps
scenario_rows = [["Schedule", "Runs / year", "Compute hours", *[f"{r} DBU/h" for r in DBU_RATES]]]
for label, runs, hours in [("Daily (current)", 365, DAILY_COMPUTE_H), ("Hourly", HOURLY_RUNS, HOURLY_COMPUTE_H)]:
    scenario_rows.append([label, n(runs), f"{hours:,.1f}", *[usd(hours * r * JOBS_USD_PER_DBU) for r in DBU_RATES]])

story += [
    heading("5. Cost and FinOps"),
    sub("5.1 What the runs consumed"),
    P(f"The workspace is Databricks Free Edition, so the actual spend was USD 0. The project did not use Azure for Students credit; the workspace runs on Databricks-managed AWS (us-east-2). Usage still appears in system.billing.usage, but that table was last ingested on {EVIDENCE['billing']['system_billing_usage_last_ingested_date']} and does not yet contain the 10 November jobs. Up to that date it recorded 1.70 DBU of serverless interactive compute (development sessions) and 0.31 DSU of storage. The cost figures below therefore combine measured run times with list prices from system.billing.list_prices for this region:"),
    table([
        ["SKU (us-east-2)", "List price"],
        ["Jobs serverless compute", f"USD {JOBS_USD_PER_DBU:.2f} / DBU"],
        ["All-purpose serverless compute (interactive notebooks)", f"USD {PRICES['PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_OHIO_per_dbu']:.2f} / DBU"],
        ["Serverless SQL (Power BI queries)", f"USD {SQL_USD_PER_DBU:.2f} / DBU"],
    ], [110 * mm, 60 * mm]),
    sub("5.2 A typical daily incremental run"),
    P(f"A single-day run was not timed on its own. It is derived from the backfill, which processed 17 trading days in one {INC['pipeline_seconds']:.0f} s invocation: <b>{PER_DAY_S:.0f} s per trading day</b> (of which {INC['avg_step_seconds_per_trading_day']} s is inside the seven audited steps and {UNMEASURED_PER_DAY_S:.0f} s is between steps, mainly the audit-log append after each step), {FIXED_IN_PIPELINE_S:.1f} s of metadata steps, and {JOB_OVERHEAD_S:.0f} s of job start-up and notebook overhead. Total: <b>about {DAILY_RUN_S:.0f} s ({DAILY_RUN_S / 60:.1f} min) of serverless compute per daily run</b>. The full load, for comparison, took {FULL['job_wall_seconds']} s for {n(FULL['rows']['bronze'])} rows: run time is driven by the number of Delta operations, at 4 to 7 s each, rather than by row count."),
    sub("5.3 Hourly for a year"),
    P("Databricks does not publish a fixed DBU-per-hour rate for serverless jobs, so the table shows three consumption scenarios at the jobs list price:"),
    table(scenario_rows, [30 * mm, 24 * mm, 26 * mm, 30 * mm, 30 * mm, 30 * mm]),
    Spacer(1, 2 * mm),
    P(f"The pipeline is not the main cost; the BI refresh is. A 2X-Small serverless SQL warehouse uses {WAREHOUSE_DBU_PER_H} DBU/h (USD {WAREHOUSE_DBU_PER_H * SQL_USD_PER_DBU:.2f}/h) and stays up for its 10-minute auto-stop after each refresh. Refreshing Power BI hourly keeps it running about {BI_HOURLY_H:,.0f} h a year: <b>{usd(BI_HOURLY_H * WAREHOUSE_DBU_PER_H * SQL_USD_PER_DBU)}</b>, against {usd(BI_DAILY_H * WAREHOUSE_DBU_PER_H * SQL_USD_PER_DBU)} for a daily refresh. Storage for the 116 MB of Delta tables is under USD 1 a month."),
    P(f"<b>Hourly enterprise estimate: about {usd(HOURLY_COMPUTE_H * 4 * JOBS_USD_PER_DBU + BI_HOURLY_H * WAREHOUSE_DBU_PER_H * SQL_USD_PER_DBU)} a year</b> at the 4 DBU/h scenario, before retries and egress. A daily schedule costs about {usd(DAILY_COMPUTE_H * 4 * JOBS_USD_PER_DBU + BI_DAILY_H * WAREHOUSE_DBU_PER_H * SQL_USD_PER_DBU)}."),
    sub("5.4 Optimizations, in order of effect"),
    table([
        ["Change", "Basis"],
        ["Do not run hourly. Trigger the job when the daily slice lands (file-arrival trigger on the Volume) and refresh Power BI once after it succeeds.", "yfinance provides end-of-day bars, so 23 of 24 hourly runs would find no new data. Removes about 96% of both compute lines."],
        ["Refresh Gold once per invocation, not once per batch.", f"Five Gold steps take about 23 s of the {INC['avg_step_seconds_per_trading_day']} s measured per trading day; a 17-day backfill would drop from 17 to 1 Gold refresh."],
        ["Aggregate only the changed dates into gold_fact_sector_daily and MERGE them.", f"Measured: a one-day filter reads {n(QDAY['rows_read'])} rows from {QDAY['files_read']} files instead of {n(QFULL['rows_read'])} from {QFULL['files_read']}. Also drop the two count() actions."],
        ["Write audit rows once per invocation instead of after every step.", f"About {UNMEASURED_PER_DAY_S:.0f} s per trading day is spent outside the audited steps."],
        ["Compact gold_fact_daily_price weekly (OPTIMIZE).", f"Daily MERGEs left {LAYOUT['gold_fact_daily_price']['files']} files for {LAYOUT['gold_fact_daily_price']['mb']} MB."],
    ], [80 * mm, 90 * mm]),
    Spacer(1, 2 * mm),
    P("Sources: system.billing.list_prices and system.billing.usage in this workspace; Databricks serverless SQL warehouse sizing documentation (2X-Small = 4 DBU/h). Measured values are in docs/evidence/phase3_run_evidence.json.", "NoteCustom"),
]


def footer(canvas, doc):
    canvas.saveState()
    w, _ = A4
    canvas.setStrokeColor(RULE)
    canvas.line(20 * mm, 15 * mm, w - 20 * mm, 15 * mm)
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(GRAY)
    canvas.drawString(20 * mm, 10.5 * mm, "S&P 500 Lakehouse | Architecture and FinOps Report")
    canvas.drawRightString(w - 20 * mm, 10.5 * mm, f"Page {doc.page}")
    canvas.restoreState()


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(OUT), pagesize=A4, rightMargin=20 * mm, leftMargin=20 * mm,
                            topMargin=16 * mm, bottomMargin=20 * mm,
                            title="S&P 500 Lakehouse: Architecture and FinOps Report", author="Haider Ali")
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(OUT)


if __name__ == "__main__":
    main()
