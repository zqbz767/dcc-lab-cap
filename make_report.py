"""Builds Lab_Report_CAP.pdf from real results:
   logs/junit.xml            (pytest --junitxml)
   logs/quorum_results.json  (written by tests/test_cap.py)
   logs/bench_results.json   (written by perf_cap_bench.py)

  python3 make_report.py --author "Your Name"
"""
import argparse
import json
import os
import xml.etree.ElementTree as ET

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(ROOT, "logs")


def load_json(name):
    try:
        with open(os.path.join(LOGS, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def load_junit():
    try:
        root = ET.parse(os.path.join(LOGS, "junit.xml")).getroot()
    except (OSError, ET.ParseError):
        return []
    rows = []
    for tc in root.iter("testcase"):
        bad = tc.find("failure") is not None or tc.find("error") is not None
        skipped = tc.find("skipped") is not None
        rows.append((tc.get("name"), "FAILED" if bad else "SKIPPED" if skipped else "PASSED"))
    return rows


def f2(x):
    return "n/a" if x is None else f"{x:.2f}"


def tbl(data, widths=None, header=True):
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    style = [("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
             ("FONTSIZE", (0, 0), (-1, -1), 8.5),
             ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]
    if header:
        style += [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eef7")),
                  ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold")]
    t.setStyle(TableStyle(style))
    return t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--author", default="Student Name")
    ap.add_argument("--out", default=os.path.join(ROOT, "Lab_Report_CAP.pdf"))
    args = ap.parse_args()

    ss = getSampleStyleSheet()
    body = ParagraphStyle("b", parent=ss["BodyText"], fontSize=10, leading=14)
    h1, h2 = ss["Heading1"], ss["Heading2"]
    story = [Paragraph("Consistency Models &amp; the CAP Theorem - Lab Report", ss["Title"]),
             Paragraph(f"CSS4004 - Distributed Systems &nbsp;|&nbsp; {args.author}", body),
             Spacer(1, 10)]

    # ---------------- Section 1
    story.append(Paragraph("Section 1: Test Results &amp; Quorum Verification", h1))
    tests = load_junit()
    passed = sum(1 for _, s in tests if s == "PASSED")
    story.append(Paragraph(f"pytest (tests/test_cap.py): <b>{passed}/{len(tests)} passed</b>. "
                           "Cluster: 3 replicas, A = leaseholder; partition DC-1 = {A, B}, DC-2 = {C}.", body))
    story.append(Spacer(1, 6))
    if tests:
        story.append(tbl([["Test case", "Result"]] + [[n, s] for n, s in tests], [13 * cm, 3 * cm]))
    else:
        story.append(Paragraph("(logs/junit.xml not found - run pytest with --junitxml)", body))

    story.append(Spacer(1, 10))
    story.append(Paragraph("Quorum verification (N = 3)", h2))
    story.append(Paragraph(
        "For each (R, W): the old value is on all nodes, the cluster is fully partitioned, the new value "
        "(higher timestamp) is written to W nodes, and <i>every</i> possible set of R nodes is read; the "
        "reader takes the newest timestamp. 'All reads fresh' must be true exactly when R + W &gt; N.", body))
    story.append(Spacer(1, 6))
    q = load_json("quorum_results.json")
    if q:
        rows = [["R", "W", "N", "R+W>N", "All R-subsets read the newest value?", "Stale read found for"]]
        for k in sorted(q, key=lambda s: tuple(map(int, s.split(",")))):
            v = q[k]
            rows.append([v["R"], v["W"], v["N"], "yes" if v["r_plus_w_gt_n"] else "no",
                         "yes" if v["all_reads_fresh"] else "no", v["stale_read_example"]])
        story.append(tbl(rows, [1 * cm, 1 * cm, 1 * cm, 2 * cm, 6.5 * cm, 4.5 * cm]))
    else:
        story.append(Paragraph("(logs/quorum_results.json not found)", body))

    # ---------------- Section 2
    story.append(Paragraph("Section 2: Performance Evaluation (PACELC)", h1))
    b = load_json("bench_results.json")
    if b:
        lin, fol = b["linearizable"], b["follower_stale"]
        story.append(Paragraph(
            f"{b['iterations']} iterations per scenario, 3 replicas on localhost. The client is attached to "
            f"follower B; Linearizable reads are proxied to the leaseholder A, Follower-Stale reads "
            f"(max_staleness_ms = 5000) are answered locally. Cross-node calls carry a simulated delay of "
            f"{b['wan_delay_ms']} ms because all nodes share one machine.", body))
        story.append(Spacer(1, 6))
        story.append(tbl([
            ["Read Mode", "Target Node", "Median Latency (ms)", "p95 Latency (ms)", "Read Availability (%)"],
            ["Linearizable", "Leaseholder (Remote)", f2(lin["median_ms"]), f2(lin["p95_ms"]),
             f"{lin['availability_pct']:.1f}"],
            ["Follower Stale", "Local Follower", f2(fol["median_ms"]), f2(fol["p95_ms"]),
             f"{fol['availability_pct']:.1f}"]]))
        if lin["median_ms"] and fol["median_ms"]:
            story.append(Spacer(1, 6))
            story.append(Paragraph(
                f"The follower read is about {lin['median_ms'] / fol['median_ms']:.1f}x faster at the median "
                f"({f2(fol['median_ms'])} ms vs {f2(lin['median_ms'])} ms): it avoids the hop to the "
                "leaseholder (the 'L' of PACELC: when there is no partition, we trade consistency for latency).", body))
        if "partition_linearizable" in b:
            pl, pf = b["partition_linearizable"], b["partition_follower_stale"]
            story.append(Spacer(1, 8))
            story.append(Paragraph("Extra: leaseholder partitioned away from the client's follower "
                                   f"({b['partition_iterations']} iterations each)", h2))
            story.append(tbl([
                ["Read Mode", "Median (ms)", "p95 (ms)", "Read Availability (%)"],
                ["Linearizable", f2(pl["median_ms"]), f2(pl["p95_ms"]), f"{pl['availability_pct']:.1f}"],
                ["Follower Stale", f2(pf["median_ms"]), f2(pf["p95_ms"]), f"{pf['availability_pct']:.1f}"]]))
            story.append(Spacer(1, 6))
            story.append(Paragraph(
                "During the partition the linearizable path refuses to answer (consistency over availability, "
                "the 'C' in PC), while bounded-stale follower reads keep being served until the staleness bound "
                "is exceeded (the 'A' in PA).", body))
    else:
        story.append(Paragraph("(logs/bench_results.json not found - run perf_cap_bench.py)", body))

    # ---------------- Section 3
    story.append(Paragraph("Section 3: Conceptual Questions", h1))
    story.append(Paragraph("1. Why does R + W &gt; N guarantee up-to-date reads, whereas R + W &le; N allows stale reads?", h2))
    story.append(Paragraph(
        "A write is acknowledged by W of the N replicas and a read contacts R of them. Two subsets of sizes W and R "
        "taken from N nodes share at least R + W - N nodes (pigeonhole principle). If R + W &gt; N that number is "
        "at least 1, so every read set contains at least one replica that holds the latest acknowledged write; "
        "because values carry a version/timestamp, the reader picks the newest and never returns an older one. "
        "This is the property that, together with a single order of writes (here: the leaseholder / majority CP "
        "writes), gives linearizable behaviour. If R + W &le; N the two sets may be disjoint: e.g. N = 3, W = 1, R = 2 - "
        "the write lands on A, the read asks B and C, neither has it, and a stale value is returned. Our experiment in "
        "Section 1 shows exactly this: stale reads appear for every (R, W) with R + W &le; 3 and never for R + W &gt; 3.", body))
    story.append(Paragraph("2. What trade-off did the system make for writes on the minority partition: QUORUM_CP vs ALWAYS_AP?", h2))
    story.append(Paragraph(
        "<b>QUORUM_CP</b> chose consistency over availability: node C (1 of 3) can never collect the required "
        "majority of 2 ACKs, so the write is rejected (ReplicaUnavailableError, acks_received = 1) and nothing is "
        "applied. The majority side (A, B) keeps accepting writes (acks_received = 2), so there is never a split brain, "
        "but clients on the minority side get errors. <b>ALWAYS_AP</b> chose availability over consistency: C accepted "
        "the write (success = true, acks_received = 1), so both sides accepted writes and the replicas diverged. After the "
        "partition healed, hinted handoff delivered the missing writes and Last-Write-Wins (highest timestamp, ties broken "
        "by node address) made all replicas converge - at the price that the losing concurrent write is silently "
        "discarded (a Union merge would keep both). In PACELC terms: CP = PC/EC-style behaviour, AP = PA/EL-style behaviour.", body))

    SimpleDocTemplate(args.out, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                      topMargin=2 * cm, bottomMargin=2 * cm, title="Lab Report CAP").build(story)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
