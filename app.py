from flask import Flask, request, jsonify
import sqlite3, time, os

DB_PATH = os.environ.get("DB_PATH", "state.db")
app = Flask(__name__)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS state (
                   id INTEGER PRIMARY KEY CHECK (id=1),
                   last_processed INTEGER
                 )""")
    c.execute("""CREATE TABLE IF NOT EXISTS seen (
                   n INTEGER PRIMARY KEY
                 )""")
    c.execute("""CREATE TABLE IF NOT EXISTS error_log (
                   ts REAL, code TEXT, n INTEGER, last_processed INTEGER, message TEXT
                 )""")
    c.execute("INSERT OR IGNORE INTO state (id, last_processed) VALUES (1, NULL)")
    conn.commit()
    conn.close()

def get_last_processed(conn):
    cur = conn.execute("SELECT last_processed FROM state WHERE id=1")
    row = cur.fetchone()
    return row[0] if row else None

def set_last_processed(conn, n):
    conn.execute("UPDATE state SET last_processed=? WHERE id=1", (n,))

def log_error(conn, code, n, last_processed, message):
    conn.execute(
        "INSERT INTO error_log(ts, code, n, last_processed, message) VALUES(?,?,?,?,?)",
        (time.time(), code, n, last_processed, message)
    )

@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200

@app.route("/increment", methods=["POST"])
def increment():
    if not request.is_json:
        return jsonify(error="JSON required"), 400
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or "n" not in data:
        return jsonify(error="Body must be {\"n\": <int>}"), 400

    n = data["n"]
    if not isinstance(n, int):
        return jsonify(error="n must be integer"), 400
    if n < 0:
        return jsonify(error="n must be non-negative"), 400

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.isolation_level = "IMMEDIATE"
        cur = conn.execute("SELECT 1 FROM seen WHERE n=?", (n,))
        duplicate = cur.fetchone() is not None
        last = get_last_processed(conn)

        # Rule 1: duplicate
        if duplicate:
            log_error(conn, "DUPLICATE", n, last, "Number has already been processed")
            conn.commit()
            return jsonify(error="duplicate", n=n, last_processed=last), 409

        # Rule 2: out-of-order (-1)
        if last is not None and n == last - 1:
            log_error(conn, "OUT_OF_ORDER", n, last, "Incoming is last_processed - 1")
            conn.commit()
            return jsonify(error="out_of_order_minus_one", n=n, last_processed=last), 409

        # OK path
        conn.execute("INSERT INTO seen(n) VALUES (?)", (n,))
        set_last_processed(conn, n)
        conn.commit()
        return jsonify(received=n, result=n + 1), 200
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8000)
