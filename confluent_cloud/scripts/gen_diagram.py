#!/usr/bin/env python3
"""Generates the architecture diagram docs/cc_demo_diagram.svg; render it to PNG with headless Chrome:

    python3 scripts/gen_diagram.py docs/cc_demo_diagram.svg
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1800,1130 \
        --screenshot="$PWD/docs/cc_demo_diagram.png" "file://$PWD/docs/cc_demo_diagram.svg"
"""
import sys

W, H = 1800, 1130
out = []
add = out.append

BLUE, BLUE_BG, BLUE_BOX = "#24518f", "#e6f0fc", "#f4f9ff"
PURP, PURP_BG, PURP_BOX = "#6a3fb0", "#f3ecfd", "#ece0fb"
INK, MUTED, OPT = "#14213d", "#4a5568", "#8a7aa8"


def esc(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, s, size=17, weight=400, color=INK, anchor="start", italic=False):
    st = ' font-style="italic"' if italic else ""
    add(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}" text-anchor="{anchor}"{st}>{esc(s)}</text>')


def box(x, y, w, h, fill, stroke, dashed=False, r=12, sw=2):
    d = ' stroke-dasharray="8 6"' if dashed else ""
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}/>')


def kafka_icon(cx, cy, s=1.0, color=INK):
    pts = [(0, -22), (0, 0), (0, 22), (17, -11), (17, 11)]
    for (a, b) in [(0, 1), (1, 2), (1, 3), (1, 4)]:
        add(f'<line x1="{cx+pts[a][0]*s}" y1="{cy+pts[a][1]*s}" x2="{cx+pts[b][0]*s}" y2="{cy+pts[b][1]*s}" stroke="{color}" stroke-width="{2.5*s}"/>')
    for (px, py) in pts:
        add(f'<circle cx="{cx+px*s}" cy="{cy+py*s}" r="{6*s}" fill="white" stroke="{color}" stroke-width="{2.5*s}"/>')


def card(x, y, w, h, title, lines, fill, stroke, icon=None, dashed=False, badge=None, title_size=20):
    box(x, y, w, h, fill, stroke, dashed)
    tx = x + 18
    if icon:
        icon(x + 36, y + h / 2)
        add(f'<line x1="{x+68}" y1="{y+16}" x2="{x+68}" y2="{y+h-16}" stroke="{stroke}" stroke-width="1.5"/>')
        tx = x + 82
    n = len(lines)
    top = y + h / 2 - (24 + 21 * n) / 2 + 19
    text(tx, top, title, title_size, 700)
    for i, l in enumerate(lines):
        text(tx, top + 25 + 21 * i, l, 15.5, 400, MUTED)
    if badge:
        bw = 9 * len(badge) + 18
        add(f'<rect x="{x+w-bw-10}" y="{y-12}" width="{bw}" height="24" rx="12" fill="white" stroke="{OPT}" stroke-width="1.5"/>')
        text(x + w - bw / 2 - 10, y + 5, badge, 12.5, 700, OPT, "middle")


def cylinder(x, y, w, h, title, sub=None):
    ry = 14
    add(f'<path d="M{x},{y+ry} v{h-2*ry} a{w/2},{ry} 0 0 0 {w},0 v{-(h-2*ry)}" fill="{BLUE_BOX}" stroke="{BLUE}" stroke-width="2"/>')
    add(f'<ellipse cx="{x+w/2}" cy="{y+ry}" rx="{w/2}" ry="{ry}" fill="white" stroke="{BLUE}" stroke-width="2"/>')
    text(x + w / 2, y + h / 2 + 10, title, 18, 700, INK, "middle")
    if sub:
        text(x + w / 2, y + h / 2 + 32, sub, 14, 400, MUTED, "middle")


def path(d, dashed=False, color=INK, end=True, start=False, sw=2.2):
    da = ' stroke-dasharray="7 6"' if dashed else ""
    me = ' marker-end="url(#arr)"' if end else ""
    ms = ' marker-start="url(#arrs)"' if start else ""
    add(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{sw}"{da}{me}{ms}/>')


def pill(cx, cy, lines, size=13.5):
    w = max(len(l) for l in lines) * size * 0.56 + 22
    h = 10 + 18 * len(lines)
    add(f'<rect x="{cx-w/2}" y="{cy-h/2}" width="{w}" height="{h}" rx="9" fill="white" stroke="{INK}" stroke-width="1.4"/>')
    for i, l in enumerate(lines):
        text(cx, cy - h / 2 + 19 + 18 * i, l, size, 700 if i == 0 else 400, INK if i == 0 else MUTED, "middle")


add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
    f'font-family="-apple-system, BlinkMacSystemFont, \'Segoe UI\', Helvetica, Arial, sans-serif">')
add('<defs>'
    '<marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/></marker>'
    '<marker id="arrs" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/></marker>'
    '</defs>')
add(f'<rect width="{W}" height="{H}" fill="white"/>')

# ------------------------------------------------------------------ panels
box(20, 20, 1050, 1090, BLUE_BG, BLUE, r=22, sw=3)
text(50, 72, "Your machine (Docker)", 34, 800, BLUE)
box(1100, 20, 680, 1090, PURP_BG, PURP, r=22, sw=3)
text(1130, 72, "Confluent Cloud", 34, 800, PURP)
text(1130, 102, "provisioned by Terraform, AWS Frankfurt", 17, 600, PURP)
text(1130, 126, "least-privilege RBAC: one service account per component", 15, 400, MUTED, italic=True)

# ------------------------------------------------------------------ source side
card(40, 145, 300, 120, "Python data generator", ["100 rows/table, then", "1 change/10s", "+ data_sensors: 10 rows"], BLUE_BOX, BLUE)
cylinder(375, 140, 180, 130, "SQL Server 2019", "CDC enabled")
path("M340,205 H369")
box(610, 135, 440, 140, "#dceafb", BLUE, r=14)
text(626, 158, "Kafka Connect cluster 1 · connect-source :8083", 14, 700, BLUE)
card(626, 168, 408, 92, "Debezium SQL Server", ["CDC source connector (1 task)"], BLUE_BOX, BLUE, icon=kafka_icon, title_size=18)
path("M555,205 H620")
text(584, 194, "CDC", 13, 600, MUTED, "middle")

# ------------------------------------------------------------------ sink side
box(610, 305, 440, 270, "#dceafb", BLUE, r=14)
text(626, 328, "Kafka Connect cluster 2 · connect-sink :8084", 14, 700, BLUE)
card(626, 340, 408, 100, "JDBC sink → PostgreSQL", ["postgres-sink (2 tasks), upsert + delete"], BLUE_BOX, BLUE, icon=kafka_icon, title_size=18)
card(626, 458, 408, 100, "JDBC sink → IBM Db2", ["db2-sink (2 tasks), upsert + delete"], BLUE_BOX, BLUE, icon=kafka_icon, title_size=18)
cylinder(390, 330, 150, 115, "PostgreSQL")
cylinder(390, 450, 150, 115, "IBM Db2")
path("M626,390 H546")
path("M626,508 H546")

# ------------------------------------------------------------------ Kafbat + CloudBeaver
card(610, 620, 440, 120, "Kafbat UI  :8080", ["DevOps console: connectors (pause / restart /", "edit), topics, consumer lag, schemas"], "#fff7e6", "#b7791f", title_size=20)
card(40, 620, 300, 120, "CloudBeaver  :8978", ["web SQL client for", "the 3 databases"], BLUE_BOX, BLUE)
# Kafbat -> Connect REST APIs (both clusters)
path("M830,620 V581", color="#b7791f")
path("M610,680 H585 V290 H660 V281", color="#b7791f")
text(840, 604, "Connect REST", 13, 700, "#b7791f")

# ------------------------------------------------------------------ cloud side
box(1190, 145, 570, 150, PURP_BOX, PURP, r=14)
kafka_icon(1235, 220, 1.3, PURP)
add(f'<line x1="1275" y1="165" x2="1275" y2="275" stroke="{PURP}" stroke-width="1.5"/>')
text(1292, 190, "Kafka Standard cluster", 24, 800)
text(1292, 218, "20 compacted topics cdc.<table> (Avro)", 16.5, 400, MUTED)
text(1292, 240, "+ Connect internal, license, schema history", 16.5, 400, MUTED)
text(1292, 262, "28 topics, 1 partition each", 16.5, 600, MUTED)

card(1480, 365, 280, 105, "Schema Registry", ["Stream Governance", "Essentials"], PURP_BOX, PURP)
path("M1620,301 V359", dashed=True, color=PURP, start=True)

# source -> Kafka
path("M1050,205 H1184")
pill(1120, 182, ["SASL_SSL", "source-xxxxxx"])
# Kafka -> sinks
path("M1190,250 H1150 V390 H1056")
path("M1190,272 H1130 V508 H1056")
pill(1120, 449, ["SASL_SSL", "pgsink-xxxxxx", "db2sink-xxxxxx"])
# Kafbat -> Kafka and Schema Registry
path("M1050,650 H1230 V301", color="#b7791f")
pill(1140, 650, ["SASL_SSL", "kafbat-xxxxxx"])
path("M1050,712 H1660 V476", color="#b7791f")
pill(1560, 712, ["HTTPS", "kafbat-xxxxxx"])

# ------------------------------------------------------------------ optional integrations
box(40, 790, 1010, 300, "none", OPT, dashed=True, r=16)
text(60, 822, "OPTIONAL · off by default", 15, 800, OPT)
text(60, 870, "ENABLE_LIGHTNING_TABLES=true", 15, 700, INK)
text(60, 891, "deploy-connectors.sh runs lightning-tables.sh", 14, 400, MUTED)
text(60, 1000, "ENABLE_CONFLUENT_MCP=true", 15, 700, INK)
text(60, 1021, "start.sh runs confluent-mcp.sh", 14, 400, MUTED)
card(610, 840, 350, 100, "DBeaver", ["Confluent JDBC driver"], "white", OPT, dashed=True)
card(610, 970, 350, 100, "Claude Code", ["MCP client"], "white", OPT, dashed=True)

card(1190, 840, 300, 105, "Lightning Tables", ["SQL over selected", "topics (read-only)"], "white", OPT, dashed=True, badge="OPTIONAL")
card(1190, 970, 570, 105, "Confluent managed MCP server", ["environments, clusters, connectors, metrics,", "topics, schemas, Flink SQL"], "white", OPT, dashed=True, badge="OPTIONAL")
path("M960,890 H1184", dashed=True, color=OPT)
pill(1072, 890, ["HTTPS + Global API key", "SELECT"])
path("M960,1020 H1184", dashed=True, color=OPT)
pill(1072, 1020, ["HTTPS + Global API key", "MCP tools"])
# Kafka -> Lightning Tables, Kafka/SR -> MCP
path("M1330,295 V834", dashed=True, color=OPT)
text(1340, 800, "enabled per topic", 14, 600, OPT)
path("M1720,470 V964", dashed=True, color=OPT)
path("M1750,295 V350 M1750,476 V964", dashed=True, color=OPT)

add("</svg>")
open(sys.argv[1], "w").write("\n".join(out))
