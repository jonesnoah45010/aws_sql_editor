from __future__ import annotations
import os, io, csv, time
from typing import Any
from flask import Flask, request, jsonify, send_file, render_template
from werkzeug.exceptions import BadRequest

from aws_db import aws_db_connection  # your existing module

app = Flask(__name__, template_folder="templates", static_folder="static")

# Single shared connection (simple demo)
db = aws_db_connection()
_current_db = os.getenv("DB_NAME") or "postgres"  # for display only


# ----------------------------- Helpers -----------------------------
def _exec(query: str):
    """Execute query via raw cursor to always return (columns, rows, rowcount, duration_ms)."""
    if not query or not query.strip():
        raise BadRequest("No query provided.")
    started = time.perf_counter()
    try:
        with db.conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in (cur.description or [])]
            rows = cur.fetchall() if cur.description else []
            rowcount = cur.rowcount
    except Exception as e:
        raise BadRequest(str(e))
    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    return cols or [], rows or [], rowcount, duration_ms


# ----------------------------- Pages ------------------------------
@app.get("/")
def index():
    return render_template("index.html", current_db=_current_db)


# ----------------------------- DB mgmt APIs ------------------------
@app.get("/api/databases")
def api_list_databases():
    try:
        names = db.list_databases()
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"databases": names, "current": _current_db})


@app.post("/api/databases")
def api_create_database():
    payload = request.get_json(silent=True) or {}
    dbname: str = (payload.get("name") or "").strip()
    if not dbname:
        raise BadRequest("Database name is required.")
    try:
        db.create_database(dbname)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, "created": dbname})


@app.post("/api/connect")
def api_connect_to():
    global _current_db
    payload = request.get_json(silent=True) or {}
    dbname: str = (payload.get("name") or "").strip()
    if not dbname:
        raise BadRequest("Database name is required.")
    try:
        db.connect_to(dbname)
        _current_db = dbname
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, "current": _current_db})


# ----------------------------- Tables & Schemas --------------------
@app.get("/api/table-schemas")
def api_table_schemas():
    """
    Returns all base tables in the given schema and their CREATE TABLE DDL,
    using aws_db_connection.list_table_schemas().
    """
    schema = request.args.get("schema", "public")
    try:
        mapping = db.list_table_schemas(schema=schema)  # {table_name: CREATE TABLE ...;}
        # Sort by table name for stable UI
        ordered_items = sorted(mapping.items(), key=lambda kv: kv[0])
        return jsonify({
            "schema": schema,
            "tables": [name for name, _ in ordered_items],
            "ddl": {name: ddl for name, ddl in ordered_items},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ----------------------------- SQL & CSV ---------------------------
@app.post("/api/sql")
def api_sql():
    payload = request.get_json(silent=True) or {}
    query: str = (payload.get("query") or "").strip()
    cols, rows, rowcount, duration_ms = _exec(query)

    def clip(v: Any) -> Any:
        s = "" if v is None else str(v)
        return s if len(s) <= 2000 else s[:2000] + "…"

    return jsonify({
        "columns": cols,
        "rows": [[clip(c) for c in r] for r in rows],
        "rowcount": rowcount,
        "duration_ms": duration_ms,
    })


@app.post("/api/sql/csv")
def api_sql_csv():
    payload = request.get_json(silent=True) or {}
    query: str = (payload.get("query") or "").strip()
    cols, rows, _, _ = _exec(query)

    import csv, io
    sio = io.StringIO()
    w = csv.writer(sio)
    if cols:
        w.writerow(cols)
    w.writerows(rows)
    data = io.BytesIO(sio.getvalue().encode("utf-8"))
    return send_file(
        data,
        mimetype="text/csv",
        as_attachment=True,
        download_name="query_results.csv",
    )


@app.get("/healthz")
def healthz():
    try:
        db.execute("SELECT 1")
        return {"ok": True, "db": _current_db}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
