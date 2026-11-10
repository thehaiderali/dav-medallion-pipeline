"""Build the Phase 3 architecture report (uv run --with reportlab python ...)."""
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    KeepTogether, Flowable,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "pdf" / "phase3_architecture_finops_report.pdf"
NAVY = colors.HexColor("#14263D")
BLUE = colors.HexColor("#2466A0")
TEAL = colors.HexColor("#007E83")
LIGHT = colors.HexColor("#EAF2F5")
GRAY = colors.HexColor("#526274")


class Architecture(Flowable):
    def __init__(self, width=170 * mm):
        Flowable.__init__(self)
        self.width, self.height = width, 52 * mm

    def draw(self):
        c = self.canv
        labels = ["Yahoo Finance\nWikipedia", "Raw JSONL\nCSV snapshot", "Bronze\nDelta arrivals", "Silver\nvalidated MERGE", "Gold\nstar schema", "Power BI\nimport ready"]
        gap = 4 * mm
        box = (self.width - gap * 5) / 6
        y = 16 * mm
        for i, label in enumerate(labels):
            x = i * (box + gap)
            c.setFillColor(NAVY if i < 4 else TEAL)
            c.roundRect(x, y, box, 25 * mm, 4 * mm, fill=1, stroke=0)
            c.setFillColor(colors.white)
            c.setFont("Helvetica-Bold", 7)
            for j, line in enumerate(label.split("\n")):
                c.drawCentredString(x + box / 2, y + 15 * mm - j * 4 * mm, line)
            if i < 5:
                c.setStrokeColor(BLUE)
                c.setLineWidth(1.4)
                c.line(x + box + .5 * mm, y + 12.5 * mm, x + box + gap - .5 * mm, y + 12.5 * mm)
        c.setFillColor(GRAY)
        c.setFont("Helvetica", 7.5)
        c.drawCentredString(self.width / 2, 7 * mm, "Audit logs, quarantine and schema drift track each transformation")


styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="TitleCustom", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=24, leading=29, textColor=NAVY, spaceAfter=10))
styles.add(ParagraphStyle(name="H1Custom", parent=styles["Heading1"], fontSize=14, leading=18, textColor=NAVY, spaceBefore=12, spaceAfter=6))
styles.add(ParagraphStyle(name="H2Custom", parent=styles["Heading2"], fontSize=10.5, leading=14, textColor=BLUE, spaceBefore=9, spaceAfter=4))
styles.add(ParagraphStyle(name="BodyCustom", parent=styles["BodyText"], fontSize=8.6, leading=12.5, textColor=NAVY, spaceAfter=5))
styles.add(ParagraphStyle(name="SmallCustom", parent=styles["BodyText"], fontSize=7.4, leading=10.4, textColor=GRAY, spaceAfter=4))
styles.add(ParagraphStyle(name="CaptionCustom", parent=styles["BodyText"], fontSize=7, leading=10, textColor=GRAY, alignment=TA_CENTER, spaceAfter=5))


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
        ("LINEBELOW", (0, -1), (-1, -1), .4, colors.HexColor("#CAD7DE")),
        ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]
    if header:
        commands += [("BACKGROUND", (0, 0), (-1, 0), NAVY), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white)]
    t.setStyle(TableStyle(commands))
    return t


story = []
story += [P("S&amp;P 500 Lakehouse", "TitleCustom"), P("Phase 3 | Gold architecture, BI readiness and FinOps", "H1Custom"),
          P("Project report  |  25 October 2026  |  Repository: github.com/thehaiderali/dav-medallion-pipeline", "SmallCustom"),
          Spacer(1, 7 * mm), Architecture(), P("Figure 1. Implemented data path; Power BI is an import specification, not an executed connection.", "CaptionCustom")]
story += [heading("Executive summary"), P("The pipeline ingests public daily S&amp;P 500 price observations and a fixed company directory. Bronze retains arrivals, Silver validates and deduplicates them, and Gold publishes a dimensional model for trading-activity analysis. Each successful Silver batch triggers Gold refresh automatically. The Gold price fact uses an idempotent MERGE; the sector summary is rebuilt from the current price and company snapshot so sector changes do not leave stale groups."),
          P("This report separates implementation from runtime evidence. No Databricks workspace session, billing export, Spark UI capture or Power BI dashboard was supplied for this delivery. The repository contains an executable proof notebook and an import-ready semantic specification; measured production results and the required Spark UI screenshot remain to be collected in a workspace."),
          heading("Analytical questions"),
          table([["Question", "Gold answer"],
                 ["How did trading activity change over time?", "Trend of total volume and close-times-volume proxy by trading date."],
                 ["Which sectors drove activity?", "Sector-day volume, proxy value and number of priced companies."],
                 ["How did an individual ticker trade?", "Daily OHLC, adjusted close and volume by company and date."],
                 ["Was the pipeline healthy?", "Gold execution duration/counts, failures, quarantined records and schema drift."],
                ], [65 * mm, 105 * mm])]
story.append(PageBreak())

story += [heading("Gold dimensional model"), P("The main fact has one row per ticker and observed trading date. Sector activity is a separate aggregate fact at sector/date grain. The company, sector and date tables are dimensions; all relationships are one-to-many from dimension to fact. Keys are logical contracts and are checked by the proof notebook rather than enforced by Delta constraints."),
          table([["Table", "Key / relationship", "Measures or attributes"],
                 ["gold_dim_date", "date_key (PK)", "Trading date; year, quarter, month, weekday, year-month"],
                 ["gold_dim_sector", "sector_key (PK)", "Current sector name"],
                 ["gold_dim_company", "ticker (PK); sector_key (FK)", "Current name and sub-industry"],
                 ["gold_fact_daily_price", "date_key + ticker (PK/FKs)", "OHLC, adjusted close, volume, dollar-volume proxy"],
                 ["gold_fact_sector_daily", "date_key + sector_key (PK/FKs)", "Priced-company count, volume, dollar-volume proxy"],
                ], [43 * mm, 50 * mm, 77 * mm]),
          sub("Relationships"), P("gold_dim_date.date_key to both fact date_key columns; gold_dim_company.ticker to gold_fact_daily_price.ticker; gold_dim_sector.sector_key to gold_dim_company.sector_key and gold_fact_sector_daily.sector_key. The date dimension includes observed trading days; calendar gaps are expected."),
          sub("Refresh semantics"), P("The existing Bronze-to-Silver notebook now invokes Gold after each successful Silver batch. Gold dimensions and the price fact use hash-guarded MERGE. An identical replay produces zero price/dimension inserts or updates. The sector fact is a complete overwrite each run; this costs a full aggregate scan but handles changed sector assignments cleanly at this project scale. A Gold-only notebook synchronizes all current Silver rows for repair or first deployment. Every Gold step has its own audit row, duration and row counts; the sector step populates rows_aggregated."),
          sub("Metric meaning"), P("Dollar-volume proxy is close multiplied by share volume. It is not exchange-reported turnover. Company observations count valid priced tickers, not total index membership. Historical prices are classified using the current company snapshot because the source does not provide historical constituents or sector changes."),
          sub("Read optimization"), P("Delta column pruning and date/company filters support selective BI queries. The pipeline avoids daily OPTIMIZE on roughly 503-row increments; compaction can be scheduled after larger backfills if file counts justify it. No benchmark or physical clustering claim is made without workspace evidence.")]
story.append(PageBreak())

story += [heading("Engineering decision log"),
          table([["Decision", "Reason and consequence"],
                 ["Raw date slices; no physical Gold partition", "One exact-date input per incremental run simplifies replay/backfill. Gold facts are modest; date_key supports filters without tiny daily Delta partitions. Reconsider after file-size/query evidence."],
                 ["Explicit Silver contracts", "try_cast, positivity/range checks and a quarantine table prevent malformed values entering analytics."],
                 ["Schema drift in Bronze", "Additional source fields are retained in rescued_data, added as string columns and logged. Silver accepts known fields and quarantines invalid typed values."],
                 ["No PII fields", "Only public price and company directory data are collected. Source locations and audit IDs contain no person-level data; no user dimension is created."],
                 ["Idempotent facts", "A content hash protects unchanged Gold price rows and preserves their update timestamp across replays."],
                ], [44 * mm, 126 * mm]),
          heading("Spark plan and DAG analysis"),
          P("The most complex Gold step joins daily price facts to the current company dimension, groups by date_key and sector_key, and writes the sector summary. In a typical Spark physical plan, the dimension can be broadcast if small; otherwise the join may shuffle. Hash aggregation first combines rows within partitions, then an Exchange shuffles by the group keys before the final aggregate and Delta write. Date filters on the price fact can be pushed into the Delta scan for selective queries, although the current complete sector rebuild reads all price dates."),
          P("<b>Evidence status:</b> No executed Spark UI DAG screenshot was available. The explanation above is a plan expectation, not an observed stage-level measurement. Run 03_demo_idempotency_and_audit.py in Databricks, open the SQL/Spark UI for SILVER_TO_GOLD_SECTOR_FACT, export the DAG screenshot, and add it to the final submission before claiming this rubric item is complete."),
          heading("Validation and remaining evidence"),
          P("The proof notebook asserts Gold price row count equals Silver, unique fact keys, date/company dimension counts, sector-observation totals and volume reconciliation. It also checks that unchanged reruns make no Gold price/dimension updates. Local Python compilation passed; PySpark tests could not be run here because the runtime dependency is unavailable. Only an executed Databricks run can establish serverless Delta behavior. The supplied PDF therefore does not claim a measured DAG, cloud cost or dashboard outcome.")]
story.append(PageBreak())

story += [heading("Power BI import specification"),
          P("Import the five Gold Delta tables through the Azure Databricks connector using a SQL warehouse hostname and HTTP path. Configure the relationships listed on page 2 as active, single-direction, one-to-many. Import pipeline_execution_logs, quarantine_prices and schema_drift_log as separate operational tables; they have different grains from trading observations. Refresh the Power BI model after the Databricks pipeline succeeds. The repository's Gold model guide supplies full DAX definitions."),
          table([["Suggested measure", "Purpose"],
                 ["Traded Volume; Dollar Volume Proxy", "Activity totals by date, ticker or sector"],
                 ["Priced Companies; Company Observations", "Data coverage and sector breadth"],
                 ["Sector Volume; Sector Dollar Volume Proxy", "Sector comparison"],
                 ["Selected Close; Highest Daily High; Lowest Daily Low", "Ticker price investigation"],
                 ["Gold Rows Aggregated; Gold Run Seconds", "Transformation throughput and runtime"],
                 ["Failed Steps; Quarantined Records", "Reliability and data quality"],
                ], [75 * mm, 95 * mm]),
          sub("Six recommended visuals"),
          P("1. KPI cards for volume, proxy value, priced companies and failed steps. 2. Date trend for traded volume and proxy value. 3. Sector bar chart for volume. 4. Single-ticker close-price line with a company slicer. 5. Gold execution duration chart by run timestamp. 6. Quarantine-reason bar chart with schema-drift drill-through."),
          P("A close-price line should be filtered to one ticker; summing stock prices across companies is not a meaningful business measure. Audit charts use execution timestamps rather than trading dates. This delivery intentionally contains no Power BI dashboard file, as requested."),
          heading("FinOps evaluation"),
          P("No Azure/Databricks usage export or measured daily run duration was supplied. A planning scenario assumes 10 minutes of billable compute per run and an illustrative all-in compute rate of USD 2.00 per hour; it is not a quote or observed spend. Daily schedule: 365 x (10/60) x 2 = <b>USD 121.67/year</b>. Hourly schedule: 8,760 x (10/60) x 2 = <b>USD 2,920/year</b>, before storage, networking, BI query compute and retries. Hourly operation costs 24 times the assumed daily compute because it runs 24 times as often."),
          P("Actual cost should be computed as billable DBU-hours times the applicable rate, plus any separate VM, storage and networking charges for the selected SKU. Azure pricing varies by region and agreement. Export Databricks system.billing.usage or Azure Cost Management records and join job/run IDs to pipeline_execution_logs before replacing this scenario with measured cost. Reduce spend with jobs compute or serverless sizing, auto-termination, incremental aggregation, fewer full scans and compaction only when files warrant it."),
          P("Sources: Azure Databricks pricing: https://azure.microsoft.com/en-us/pricing/details/databricks/ ; Azure Databricks billing system table: https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/billing", "SmallCustom")]


def footer(canvas, doc):
    canvas.saveState()
    w, _ = A4
    canvas.setStrokeColor(colors.HexColor("#CCD9DF"))
    canvas.line(20 * mm, 17 * mm, w - 20 * mm, 17 * mm)
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(GRAY)
    canvas.drawString(20 * mm, 12 * mm, "S&P 500 Lakehouse | Phase 3")
    canvas.drawRightString(w - 20 * mm, 12 * mm, f"Page {doc.page}")
    canvas.restoreState()


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(OUT), pagesize=A4, rightMargin=20 * mm, leftMargin=20 * mm,
                            topMargin=19 * mm, bottomMargin=23 * mm, title="Phase 3 Gold Architecture and FinOps Report",
                            author="DAV Medallion Pipeline")
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(OUT)


if __name__ == "__main__":
    main()
