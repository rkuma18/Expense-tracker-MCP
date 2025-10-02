# main.py
# Local Expense Tracker MCP server (FastMCP)
# - SQLite local storage
# - Tools: accounts, categories, transactions (+splits), search, budgets, summaries,
#          CSV import/export (dry-run), rules, attachments, goals, forecast
# - Legacy MVP endpoints kept (add_expense/list_expenses/summarize)
# - Consistent JSON envelopes: {data, meta, errors}

from fastmcp import FastMCP
import os
import io
import csv
import json
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "expenses.db")
CATEGORIES_PATH = os.path.join(APP_DIR, "categories.json")
ATTACH_DIR = os.path.join(APP_DIR, "attachments")

os.makedirs(ATTACH_DIR, exist_ok=True)

mcp = FastMCP("ExpenseTracker")

# ---------- helpers ----------

def ok(data=None, meta=None):
    return {"data": {} if data is None else data, "meta": meta or {}, "errors": []}

def err(message, meta=None):
    return {"data": None, "meta": meta or {}, "errors": [str(message)]}

def dict_rows(cur: sqlite3.Cursor) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def one_row(cur: sqlite3.Cursor) -> Optional[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    r = cur.fetchone()
    return dict(zip(cols, r)) if r else None

def coerce_date(s: str) -> str:
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return s
        for fmt in ("%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return datetime.fromisoformat(s[:10]).strftime("%Y-%m-%d")
    except Exception:
        raise ValueError(f"Invalid date format: {s}. Use YYYY-MM-DD.")

def money(x) -> float:
    try:
        return float(x)
    except Exception:
        raise ValueError(f"Invalid amount: {x}")

def get_ci(row: dict, key: str, default=None):
    """Case-insensitive dict get (for CSV headers)."""
    if key in row:
        return row[key]
    lk = (key or "").lower()
    for k, v in row.items():
        if (k or "").lower() == lk:
            return v
    return default

def get_conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    return c


# ---------- schema & migrations ----------

def init_db():
    with get_conn() as c:
        c.execute("PRAGMA foreign_keys = ON")
        # Legacy MVP table (kept)
        c.execute("""
        CREATE TABLE IF NOT EXISTS expenses(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT DEFAULT '',
            note TEXT DEFAULT ''
        )
        """)

        # Normalized schema
        c.execute("""
        CREATE TABLE IF NOT EXISTS accounts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            type TEXT DEFAULT 'cash',
            currency TEXT DEFAULT 'INR',
            opening_balance REAL DEFAULT 0,
            created_at TEXT DEFAULT (date('now'))
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent_id INTEGER,
            UNIQUE(name, parent_id),
            FOREIGN KEY(parent_id) REFERENCES categories(id) ON DELETE CASCADE
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            account_id INTEGER,
            amount REAL NOT NULL,
            currency TEXT DEFAULT 'INR',
            type TEXT NOT NULL CHECK(type IN ('expense','income','transfer')),
            merchant TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE SET NULL
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS transaction_splits(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id INTEGER NOT NULL,
            category_id INTEGER,
            amount REAL NOT NULL,
            gst_rate REAL DEFAULT 0,
            gst_amount REAL DEFAULT 0,
            tags TEXT DEFAULT '',
            FOREIGN KEY(transaction_id) REFERENCES transactions(id) ON DELETE CASCADE,
            FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS budgets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_yyyymm TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            UNIQUE(month_yyyymm, category_id),
            FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS attachments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            mime_type TEXT DEFAULT '',
            added_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(transaction_id) REFERENCES transactions(id) ON DELETE CASCADE
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS rules(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            when_json TEXT NOT NULL,
            set_json  TEXT NOT NULL,
            priority INTEGER DEFAULT 100,
            enabled INTEGER DEFAULT 1
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS goals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            target_amount REAL NOT NULL,
            target_date TEXT NOT NULL,
            created_at TEXT DEFAULT (date('now'))
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates(
            date TEXT NOT NULL,
            from_ccy TEXT NOT NULL,
            to_ccy TEXT NOT NULL,
            rate REAL NOT NULL,
            PRIMARY KEY(date, from_ccy, to_ccy)
        )
        """)

        # seed one account if empty
        cur = c.execute("SELECT COUNT(1) FROM accounts")
        if (cur.fetchone() or [0])[0] == 0:
            c.execute("INSERT INTO accounts(name, type, currency, opening_balance) VALUES (?,?,?,?)",
                      ("Cash", "cash", "INR", 0.0))

init_db()

# ---------- legacy MVP tools (unchanged interface) ----------

@mcp.tool()
def add_expense(date, amount, category, subcategory="", note=""):
    """Add a new expense to legacy 'expenses' table (for backwards-compat)."""
    try:
        d = coerce_date(date)
        amt = money(amount)
        with sqlite3.connect(DB_PATH) as c:
            cur = c.execute(
                "INSERT INTO expenses(date, amount, category, subcategory, note) VALUES (?,?,?,?,?)",
                (d, amt, category, subcategory, note)
            )
            return ok({"id": cur.lastrowid})
    except Exception as e:
        return err(str(e))

@mcp.tool()
def list_expenses(start_date, end_date):
    """List legacy 'expenses' within inclusive date range."""
    try:
        s = coerce_date(start_date); e = coerce_date(end_date)
        with sqlite3.connect(DB_PATH) as c:
            c.row_factory = sqlite3.Row
            cur = c.execute("""
                SELECT id, date, amount, category, subcategory, note
                FROM expenses
                WHERE date BETWEEN ? AND ?
                ORDER BY id ASC
            """, (s, e))
            return ok(dict_rows(cur))
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def summarize(start_date, end_date, category: Optional[str] = None):
    """Summarize legacy 'expenses' by category within range."""
    try:
        s = coerce_date(start_date); e = coerce_date(end_date)
        with sqlite3.connect(DB_PATH) as c:
            c.row_factory = sqlite3.Row
            query = """
                SELECT category, SUM(amount) AS total_amount
                FROM expenses
                WHERE date BETWEEN ? AND ?
            """
            params = [s, e]
            if category:
                query += " AND category = ?"
                params.append(category)
            query += " GROUP BY category ORDER BY category ASC"
            cur = c.execute(query, params)
            return ok(dict_rows(cur))
    except Exception as ex:
        return err(str(ex))

@mcp.resource("expense://categories", mime_type="application/json")
def categories():
    # Read fresh each time so you can edit without restart
    with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        return f.read()

# ---------- internal rules loader (no decorators, pass connection explicitly) ----------

def _load_rules(c: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = c.execute("SELECT id, when_json, set_json, priority, enabled FROM rules WHERE enabled=1")
    out: List[Dict[str, Any]] = []
    for r in dict_rows(cur):
        try:
            out.append({
                "id": r["id"],
                "when": json.loads(r["when_json"]),
                "set":  json.loads(r["set_json"]),
                "priority": r["priority"],
                "enabled": r["enabled"],
            })
        except Exception:
            continue
    return out

def _apply_rules_row(trn: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    set_accum: Dict[str, Any] = {}
    for r in sorted(rules, key=lambda x: x.get("priority", 100)):
        if not r.get("enabled", 1):
            continue
        W = r.get("when", {}) or {}
        S = r.get("set", {}) or {}
        ok_match = True
        if "merchant_regex" in W:
            if not re.search(W["merchant_regex"], trn.get("merchant","") or "", flags=re.I):
                ok_match = False
        if ok_match and "amount_min" in W and float(trn.get("amount", 0)) < float(W["amount_min"]):
            ok_match = False
        if ok_match and "amount_max" in W and float(trn.get("amount", 0)) > float(W["amount_max"]):
            ok_match = False
        if ok_match and "type" in W and str(trn.get("type")) != str(W["type"]):
            ok_match = False
        if ok_match:
            set_accum.update(S)
    return set_accum

# ---------- accounts ----------

@mcp.tool()
def add_account(name, type="cash", currency="INR", opening_balance=0.0):
    """Create an account."""
    try:
        with get_conn() as c:
            cur = c.execute(
                "INSERT INTO accounts(name, type, currency, opening_balance) VALUES (?,?,?,?)",
                (name.strip(), type, currency, money(opening_balance))
            )
            return ok({"id": cur.lastrowid})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def list_accounts():
    """List accounts."""
    with get_conn() as c:
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT id, name, type, currency, opening_balance, created_at FROM accounts ORDER BY id ASC")
        return ok(dict_rows(cur))

# ---------- categories ----------

@mcp.tool()
def list_categories():
    """List normalized categories."""
    with get_conn() as c:
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT id, name, parent_id FROM categories ORDER BY COALESCE(parent_id, id), name")
        return ok(dict_rows(cur))

@mcp.tool()
def add_category(name, parent_id: Optional[int] = None):
    """Add a (nested) category."""
    try:
        with get_conn() as c:
            cur = c.execute("INSERT INTO categories(name, parent_id) VALUES (?,?)", (name.strip(), parent_id))
            return ok({"id": cur.lastrowid})
    except Exception as ex:
        return err(str(ex))

# ---------- transactions (+splits/attachments) ----------

@mcp.tool()
def add_transaction(date,
                    amount,
                    type,                   # 'expense'|'income'|'transfer'
                    account_id: Optional[int] = None,
                    currency="INR",
                    merchant="",
                    notes="",
                    splits: Optional[List[Dict[str, Any]]] = None,
                    apply_rules: bool = True):
    """
    Add a transaction with optional splits.
    splits = [{"category_id": 1, "amount": 120, "gst_rate": 5, "tags": "food,delivery"}, ...]
    """
    try:
        d = coerce_date(date); amt = money(amount)
        if type not in ("expense","income","transfer"):
            return err("type must be 'expense'|'income'|'transfer'")
        with get_conn() as c:
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON")
            c.execute("""
                INSERT INTO transactions(date, account_id, amount, currency, type, merchant, notes)
                VALUES (?,?,?,?,?,?,?)
            """, (d, account_id, amt, currency, type, merchant, notes))
            tx_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

            rules = _load_rules(c) if apply_rules else []

            if not splits:
                set_from_rules = _apply_rules_row(
                    {"date": d, "amount": amt, "type": type, "merchant": merchant, "notes": notes}, rules
                )
                category_id = set_from_rules.get("category_id")
                tags = set_from_rules.get("tags","")
                gst_rate = float(set_from_rules.get("gst_rate",0))
                gst_amt = round(amt * gst_rate/100.0, 2) if gst_rate else 0.0
                c.execute("""
                    INSERT INTO transaction_splits(transaction_id, category_id, amount, gst_rate, gst_amount, tags)
                    VALUES (?,?,?,?,?,?)
                """, (tx_id, category_id, amt, gst_rate, gst_amt, tags))
            else:
                for sp in splits:
                    sp_amt = money(sp.get("amount", 0))
                    if sp_amt == 0:
                        continue
                    category_id = sp.get("category_id")
                    gst_rate = float(sp.get("gst_rate", 0) or 0)
                    gst_amt = round(sp_amt * gst_rate/100.0, 2) if gst_rate else 0.0
                    tags = sp.get("tags","")
                    if apply_rules:
                        set_from_rules = _apply_rules_row(
                            {"date": d, "amount": sp_amt, "type": type, "merchant": merchant, "notes": notes}, rules
                        )
                        category_id = set_from_rules.get("category_id", category_id)
                        if not tags:
                            tags = set_from_rules.get("tags","")
                        if not gst_rate:
                            gst_rate = float(set_from_rules.get("gst_rate", 0) or 0)
                            gst_amt = round(sp_amt * gst_rate/100.0, 2) if gst_rate else 0.0
                    c.execute("""
                        INSERT INTO transaction_splits(transaction_id, category_id, amount, gst_rate, gst_amount, tags)
                        VALUES (?,?,?,?,?,?)
                    """, (tx_id, category_id, sp_amt, gst_rate, gst_amt, tags))

            return ok({"id": tx_id})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def get_transaction(id: int):
    """Fetch transaction + splits + attachments."""
    try:
        with get_conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.execute("SELECT * FROM transactions WHERE id=?", (id,))
            tx = one_row(cur)
            if not tx:
                return err("Not found")
            cur = c.execute("""
                SELECT ts.*, cat.name AS category_name
                FROM transaction_splits ts
                LEFT JOIN categories cat ON cat.id = ts.category_id
                WHERE transaction_id=?
                ORDER BY id ASC
            """, (id,))
            splits = dict_rows(cur)
            cur = c.execute("""
                SELECT id, path, mime_type, added_at
                FROM attachments WHERE transaction_id=? ORDER BY id ASC
            """, (id,))
            atts = dict_rows(cur)
            return ok({"transaction": tx, "splits": splits, "attachments": atts})
    except Exception as ex:
        return err(str(ex))

# update_transaction: capture cursor
@mcp.tool()
def update_transaction(id: int, fields: dict):
    try:
        with get_conn() as c:
            allowed = {"date","amount","account_id","currency","type","merchant","notes"}
            sets, params = [], []
            for k, v in (fields or {}).items():
                if k not in allowed:
                    continue
                if k == "date": v = coerce_date(v)
                if k == "amount": v = money(v)
                sets.append(f"{k}=?"); params.append(v)
            if not sets:
                return err("No updatable fields provided")
            params.append(id)
            cur = c.execute(
                f"UPDATE transactions SET {', '.join(sets)}, updated_at=datetime('now') WHERE id=?",
                params
            )
            return ok({"updated": cur.rowcount})
    except Exception as ex:
        return err(str(ex))


# delete_transaction: use cur.rowcount
@mcp.tool()
def delete_transaction(id: int):
    with get_conn() as c:
        cur = c.execute("DELETE FROM transactions WHERE id=?", (id,))
        return ok({"deleted": cur.rowcount})


@mcp.tool()
def add_split(transaction_id: int, amount, category_id: Optional[int] = None, gst_rate: float = 0.0, tags: str = ""):
    """Add a split to a transaction."""
    try:
        amt = money(amount)
        gst_amt = round(amt * float(gst_rate)/100.0, 2) if gst_rate else 0.0
        with get_conn() as c:
            cur = c.execute("""
                INSERT INTO transaction_splits(transaction_id, category_id, amount, gst_rate, gst_amount, tags)
                VALUES (?,?,?,?,?,?)
            """, (transaction_id, category_id, amt, float(gst_rate), gst_amt, tags))
            return ok({"id": cur.lastrowid})
    except Exception as ex:
        return err(str(ex))

# delete_split: use cur.rowcount
@mcp.tool()
def delete_split(split_id: int):
    with get_conn() as c:
        cur = c.execute("DELETE FROM transaction_splits WHERE id=?", (split_id,))
        return ok({"deleted": cur.rowcount})


# ---------- search ----------

@mcp.tool()
def search_transactions(q: Optional[str] = None,
                        account_id: Optional[int] = None,
                        category_id: Optional[int] = None,
                        tags: Optional[str] = None,   # comma-separated; any match
                        merchant: Optional[str] = None,
                        type: Optional[str] = None,
                        min_amount: Optional[float] = None,
                        max_amount: Optional[float] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        limit: int = 100,
                        offset: int = 0):
    """Flexible search across transactions + splits."""
    try:
        where = []; params: List[Any] = []
        if start_date: start_date = coerce_date(start_date); where.append("t.date >= ?"); params.append(start_date)
        if end_date:   end_date   = coerce_date(end_date);   where.append("t.date <= ?"); params.append(end_date)
        if account_id: where.append("t.account_id = ?"); params.append(account_id)
        if type:       where.append("t.type = ?"); params.append(type)
        if merchant:   where.append("t.merchant LIKE ?"); params.append(f"%{merchant}%")
        if min_amount is not None: where.append("ts.amount >= ?"); params.append(money(min_amount))
        if max_amount is not None: where.append("ts.amount <= ?"); params.append(money(max_amount))
        if category_id: where.append("ts.category_id = ?"); params.append(category_id)
        if tags:
            tag_list = [t.strip().lower() for t in tags.split(",") if t.strip()]
            if tag_list:
                ors = []
                for ttag in tag_list:
                    ors.append("(','||LOWER(ts.tags)||',') LIKE ?")
                    params.append(f"%,{ttag},%")
                where.append("(" + " OR ".join(ors) + ")")
        if q:
            where.append("(LOWER(t.notes) LIKE ? OR LOWER(t.merchant) LIKE ?)")
            params.extend([f"%{q.lower()}%", f"%{q.lower()}%"])

        sql = """
        SELECT t.id as transaction_id, t.date, t.type, t.merchant, t.notes, t.amount AS gross_amount, t.currency,
               ts.id as split_id, ts.amount, ts.gst_rate, ts.gst_amount, ts.tags,
               cat.id as category_id, cat.name as category_name,
               ac.id as account_id, ac.name as account_name
        FROM transactions t
        LEFT JOIN transaction_splits ts ON ts.transaction_id = t.id
        LEFT JOIN categories cat ON cat.id = ts.category_id
        LEFT JOIN accounts ac ON ac.id = t.account_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY t.date DESC, t.id DESC, ts.id ASC LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])

        with get_conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.execute(sql, params)
            rows = dict_rows(cur)
            return ok(rows, meta={"limit": limit, "offset": offset, "returned": len(rows)})
    except Exception as ex:
        return err(str(ex))

# ---------- budgets ----------

@mcp.tool()
def set_budget(month_yyyymm: str, category_id: int, amount):
    """Set/update budget for category and month (YYYYMM)."""
    try:
        if not re.match(r"^\d{6}$", month_yyyymm):
            return err("month_yyyymm must be YYYYMM")
        amt = money(amount)
        with get_conn() as c:
            c.execute("""
                INSERT INTO budgets(month_yyyymm, category_id, amount)
                VALUES(?,?,?)
                ON CONFLICT(month_yyyymm, category_id) DO UPDATE SET amount=excluded.amount
            """, (month_yyyymm, category_id, amt))
            return ok({"upserted": True})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def get_budget_summary(month_yyyymm: str):
    """Return budget vs actual by category for a month (YYYYMM)."""
    try:
        if not re.match(r"^\d{6}$", month_yyyymm):
            return err("month_yyyymm must be YYYYMM")
        y = int(month_yyyymm[:4]); m = int(month_yyyymm[4:])
        start = datetime(y, m, 1)
        end = (datetime(y+1, 1, 1) - timedelta(days=1)) if m == 12 else (datetime(y, m+1, 1) - timedelta(days=1))
        with get_conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.execute("""
                WITH actuals AS (
                    SELECT ts.category_id, SUM(ts.amount) AS spent
                    FROM transactions t
                    JOIN transaction_splits ts ON ts.transaction_id=t.id
                    WHERE t.type='expense' AND t.date BETWEEN ? AND ?
                    GROUP BY ts.category_id
                )
                SELECT cat.id AS category_id, cat.name AS category_name,
                       b.amount AS budget_amount,
                       COALESCE(a.spent, 0) AS actual_spent,
                       (COALESCE(b.amount,0) - COALESCE(a.spent,0)) AS variance
                FROM categories cat
                LEFT JOIN budgets b ON b.category_id = cat.id AND b.month_yyyymm = ?
                LEFT JOIN actuals a ON a.category_id = cat.id
                ORDER BY cat.name
            """, (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), month_yyyymm))
            return ok(dict_rows(cur), meta={"month": month_yyyymm})
    except Exception as ex:
        return err(str(ex))

# ---------- summaries ----------

@mcp.tool()
def summary(start_date, end_date, group_by: str = "category"):
    """
    group_by: 'category' | 'month' | 'merchant' | 'account' | 'type'
    """
    try:
        s = coerce_date(start_date); e = coerce_date(end_date)
        if group_by not in ("category","month","merchant","account","type"):
            return err("group_by must be category|month|merchant|account|type")

        if group_by == "category":
            sql = """
            SELECT COALESCE(cat.name, 'Uncategorized') AS key, SUM(ts.amount) AS total
            FROM transactions t
            JOIN transaction_splits ts ON ts.transaction_id=t.id
            LEFT JOIN categories cat ON cat.id = ts.category_id
            WHERE t.date BETWEEN ? AND ?
            GROUP BY cat.id
            ORDER BY total DESC
            """

        elif group_by == "month":
            sql = """
            SELECT substr(t.date,1,7) AS key, SUM(ts.amount) AS total
            FROM transactions t
            JOIN transaction_splits ts ON ts.transaction_id=t.id
            WHERE t.date BETWEEN ? AND ?
            GROUP BY substr(t.date,1,7)
            ORDER BY key ASC
            """
        elif group_by == "merchant":
            sql = """
            SELECT t.merchant AS key, SUM(ts.amount) AS total
            FROM transactions t
            JOIN transaction_splits ts ON ts.transaction_id=t.id
            WHERE t.date BETWEEN ? AND ?
            GROUP BY t.merchant
            ORDER BY total DESC
            """
        elif group_by == "account":
            sql = """
            SELECT ac.name AS key, SUM(ts.amount) AS total
            FROM transactions t
            JOIN transaction_splits ts ON ts.transaction_id=t.id
            LEFT JOIN accounts ac ON ac.id = t.account_id
            WHERE t.date BETWEEN ? AND ?
            GROUP BY ac.name
            ORDER BY total DESC
            """
        else:  # type
            sql = """
            SELECT t.type AS key, SUM(ts.amount) AS total
            FROM transactions t
            JOIN transaction_splits ts ON ts.transaction_id=t.id
            WHERE t.date BETWEEN ? AND ?
            GROUP BY t.type
            ORDER BY total DESC
            """

        with get_conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.execute(sql, (s, e))
            return ok(dict_rows(cur))
    except Exception as ex:
        return err(str(ex))

# ---------- CSV import/export ----------

@mcp.tool()
def import_csv(path: str, mapping_preset: Optional[str] = None, dry_run: bool = True, default_type: str = "expense", account_id: Optional[int] = None):
    """
    Import from CSV.
    Expected headers: date, amount, [merchant], [notes], [category_name], [tags]
    """
    try:
        if not os.path.exists(path):
            return err(f"CSV not found: {path}")
        inserted = 0
        preview = []

        with get_conn() as c:
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON")
            rules = _load_rules(c)

            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)

                raw_headers = reader.fieldnames or []
                lower_headers = {(h or "").strip().lower() for h in raw_headers if (h or "").strip()}
                required = {"date", "amount"}
                if not required.issubset(lower_headers):
                    return err(f"CSV must include headers at least: {sorted(required)}")

                for row in reader:
                    d = coerce_date((get_ci(row, "date", "") or "").strip())
                    amt = money(get_ci(row, "amount", "0"))
                    merchant = (get_ci(row, "merchant", "") or "").strip()
                    notes = (get_ci(row, "notes", "") or "").strip()
                    category_name = (get_ci(row, "category_name", None) or None)
                    tags = (get_ci(row, "tags", "") or "").strip()

                    # category resolve (top-level only)
                    category_id = None
                    if category_name:
                        cur = c.execute("SELECT id FROM categories WHERE name=? AND parent_id IS NULL", (category_name,))
                        rr = cur.fetchone()
                        if rr:
                            category_id = rr[0]

                    set_from_rules = _apply_rules_row(
                        {"date": d, "amount": amt, "type": default_type, "merchant": merchant, "notes": notes},
                        rules
                    )
                    category_id = set_from_rules.get("category_id", category_id)
                    if not tags:
                        tags = set_from_rules.get("tags","")
                    gst_rate = float(set_from_rules.get("gst_rate", 0) or 0)
                    gst_amt  = round(amt * gst_rate/100.0, 2) if gst_rate else 0.0

                    if dry_run:
                        preview.append({
                            "date": d, "amount": amt, "type": default_type, "merchant": merchant, "notes": notes,
                            "category_id": category_id, "tags": tags, "gst_rate": gst_rate, "gst_amount": gst_amt
                        })
                    else:
                        c.execute("""
                            INSERT INTO transactions(date, account_id, amount, currency, type, merchant, notes)
                            VALUES (?,?,?,?,?,?,?)
                        """, (d, account_id, amt, "INR", default_type, merchant, notes))
                        tx_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                        c.execute("""
                            INSERT INTO transaction_splits(transaction_id, category_id, amount, gst_rate, gst_amount, tags)
                            VALUES (?,?,?,?,?,?)
                        """, (tx_id, category_id, amt, gst_rate, gst_amt, tags))
                        inserted += 1

        return ok({"inserted": inserted, "preview": preview}, meta={"dry_run": dry_run})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def export_data(format: str = "csv", start_date: Optional[str] = None, end_date: Optional[str] = None, path: Optional[str] = None):
    """
    Export joined transactions + splits.
    format: 'csv' or 'json'
    If path omitted, returns serialized content in data.
    """
    try:
        where = []; params: List[Any] = []
        if start_date: start_date = coerce_date(start_date); where.append("t.date >= ?"); params.append(start_date)
        if end_date:   end_date   = coerce_date(end_date);   where.append("t.date <= ?"); params.append(end_date)
        sql = """
        SELECT t.id as transaction_id, t.date, t.type, t.merchant, t.notes, t.amount as gross_amount, t.currency,
               ac.name as account_name,
               ts.id as split_id, ts.amount, ts.gst_rate, ts.gst_amount, ts.tags,
               cat.name as category_name
        FROM transactions t
        LEFT JOIN transaction_splits ts ON ts.transaction_id=t.id
        LEFT JOIN categories cat ON cat.id=ts.category_id
        LEFT JOIN accounts ac ON ac.id=t.account_id
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY t.date ASC, t.id ASC, ts.id ASC"

        with get_conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.execute(sql, params)
            rows = dict_rows(cur)

        if format == "json":
            payload = json.dumps(rows, ensure_ascii=False, indent=2)
            if path:
                with open(path, "w", encoding="utf-8") as f: f.write(payload)
                return ok({"written": path, "count": len(rows)})
            return ok({"content": payload, "count": len(rows)})

        if format == "csv":
            out = io.StringIO()
            headers = list(rows[0].keys()) if rows else [
                "transaction_id","date","type","merchant","notes","gross_amount","currency","account_name",
                "split_id","amount","gst_rate","gst_amount","tags","category_name"
            ]
            writer = csv.DictWriter(out, fieldnames=headers)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            content = out.getvalue()
            if path:
                with open(path, "w", encoding="utf-8", newline="") as f: f.write(content)
                return ok({"written": path, "count": len(rows)})
            return ok({"content": content, "count": len(rows)})

        return err("format must be 'csv' or 'json'")
    except Exception as ex:
        return err(str(ex))

# ---------- rules ----------

@mcp.tool()
def add_rule(when: Dict[str, Any], set: Dict[str, Any], priority: int = 100, enabled: bool = True):
    """Add a classification rule."""
    try:
        when_json = json.dumps(when, ensure_ascii=False)
        set_json  = json.dumps(set, ensure_ascii=False)
        with get_conn() as c:
            c.execute("PRAGMA foreign_keys = ON")
            c.execute("INSERT INTO rules(when_json, set_json, priority, enabled) VALUES (?,?,?,?)",
                      (when_json, set_json, int(priority), 1 if enabled else 0))
            rid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            return ok({"id": rid})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def list_rules():
    with get_conn() as c:
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT id, when_json, set_json, priority, enabled FROM rules ORDER BY priority ASC, id ASC")
        rows = []
        for r in dict_rows(cur):
            r["when"] = json.loads(r.pop("when_json"))
            r["set"]  = json.loads(r.pop("set_json"))
            rows.append(r)
        return ok(rows)

# remove_rule: use cur.rowcount
@mcp.tool()
def remove_rule(id: int):
    with get_conn() as c:
        cur = c.execute("DELETE FROM rules WHERE id=?", (id,))
        return ok({"deleted": cur.rowcount})


# ---------- attachments ----------

@mcp.tool()
def attachments_add(transaction_id: int, file_path: str, mime_type: str = ""):
    """Link a file path to a transaction (does not copy/move)."""
    try:
        norm = os.path.abspath(file_path)
        with get_conn() as c:
            cur = c.execute("INSERT INTO attachments(transaction_id, path, mime_type) VALUES (?,?,?)",
                            (transaction_id, norm, mime_type))
            return ok({"id": cur.lastrowid, "path": norm})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def attachments_list(transaction_id: int):
    with get_conn() as c:
        c.row_factory = sqlite3.Row
        cur = c.execute("SELECT id, path, mime_type, added_at FROM attachments WHERE transaction_id=? ORDER BY id ASC",
                        (transaction_id,))
        return ok(dict_rows(cur))

# ---------- goals & forecast ----------

@mcp.tool()
def goals_set(name: str, target_amount, target_date: str):
    """Create/replace a savings goal."""
    try:
        target_date = coerce_date(target_date)
        amt = money(target_amount)
        with get_conn() as c:
            c.execute("""
                INSERT INTO goals(name, target_amount, target_date)
                VALUES (?,?,?)
                ON CONFLICT(name) DO UPDATE SET target_amount=excluded.target_amount, target_date=excluded.target_date
            """, (name.strip(), amt, target_date))
            return ok({"upserted": True})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def goals_progress(name: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Progress = income - expenses in range (simple proxy for savings)."""
    try:
        if not start_date:
            start_date = f"{datetime.now().year}-01-01"
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        s = coerce_date(start_date); e = coerce_date(end_date)

        with get_conn() as c:
            c.row_factory = sqlite3.Row
            goal = one_row(c.execute("SELECT name, target_amount, target_date FROM goals WHERE name=?", (name,)))
            if not goal:
                return err("Goal not found")

            income = c.execute("""
                SELECT COALESCE(SUM(ts.amount),0) FROM transactions t
                JOIN transaction_splits ts ON ts.transaction_id=t.id
                WHERE t.type='income' AND t.date BETWEEN ? AND ?
            """, (s, e)).fetchone()[0] or 0.0

            expense = c.execute("""
                SELECT COALESCE(SUM(ts.amount),0) FROM transactions t
                JOIN transaction_splits ts ON ts.transaction_id=t.id
                WHERE t.type='expense' AND t.date BETWEEN ? AND ?
            """, (s, e)).fetchone()[0] or 0.0

            saved = round(float(income) - float(expense), 2)
            remaining = round(float(goal["target_amount"]) - saved, 2)
            return ok({
                "goal": goal,
                "period": {"start": s, "end": e},
                "income": income, "expense": expense, "saved": saved, "remaining": remaining
            })
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def forecast_cashflow(months: int = 3):
    """Naive forecast: avg monthly net over last ~6 months projected forward."""
    try:
        months = max(1, min(24, int(months)))
        today = datetime.now().date()
        start = (today.replace(day=1) - timedelta(days=180)).replace(day=1)

        with get_conn() as c:
            c.row_factory = sqlite3.Row
            cur = c.execute("""
                WITH monthly AS (
                    SELECT substr(t.date,1,7) AS ym,
                           SUM(CASE WHEN t.type='income' THEN ts.amount ELSE 0 END) AS inc,
                           SUM(CASE WHEN t.type='expense' THEN ts.amount ELSE 0 END) AS exp
                    FROM transactions t
                    JOIN transaction_splits ts ON ts.transaction_id=t.id
                    WHERE t.date >= ?
                    GROUP BY substr(t.date,1,7)
                )
                SELECT ym, inc, exp, (inc - exp) AS net
                FROM monthly
                ORDER BY ym ASC
            """, (start.strftime("%Y-%m-%d"),))
            hist = dict_rows(cur)

        if not hist:
            return ok({"history": [], "forecast": []}, meta={"note": "No history"})

        avg_net = sum((r["net"] or 0) for r in hist) / max(1, len(hist))

        # project forward
        last = datetime.strptime(hist[-1]["ym"]+"-01", "%Y-%m-%d")
        forecast = []
        y, m = last.year, last.month
        for _ in range(months):
            m += 1
            if m > 12:
                m = 1; y += 1
            forecast.append({"month": f"{y:04d}-{m:02d}", "net": round(avg_net, 2)})

        return ok({"history": hist, "forecast": forecast})
    except Exception as ex:
        return err(str(ex))
@mcp.tool()
def delete_account(id: int, reassign_to_id: Optional[int] = None):
    """
    Delete an account.
    - If reassign_to_id is provided, all transactions currently pointing to `id`
      will be reassigned to `reassign_to_id` before deletion.
    - If not provided, deletion proceeds and transactions.account_id will become NULL
      due to the FK constraint (ON DELETE SET NULL).
    Returns how many transactions were affected and whether the account was deleted.
    """
    try:
        with get_conn() as c:
            c.row_factory = sqlite3.Row

            # Ensure the account exists
            cur = c.execute("SELECT id, name FROM accounts WHERE id=?", (id,))
            acc = cur.fetchone()
            if not acc:
                return err(f"Account {id} not found")

            affected = 0
            if reassign_to_id is not None:
                # Ensure the target account exists and isn't the same
                if reassign_to_id == id:
                    return err("reassign_to_id cannot be the same as the account being deleted")
                cur = c.execute("SELECT id FROM accounts WHERE id=?", (reassign_to_id,))
                if not cur.fetchone():
                    return err(f"Target account {reassign_to_id} not found")

                # Reassign transactions
                cur = c.execute(
                    "UPDATE transactions SET account_id=? WHERE account_id=?",
                    (reassign_to_id, id)
                )
                affected = max(0, cur.rowcount or 0)

            # Delete the account
            cur = c.execute("DELETE FROM accounts WHERE id=?", (id,))
            deleted = max(0, cur.rowcount or 0)

            return ok({
                "deleted": deleted,
                "reassigned_transactions": affected,
                "account_id": id,
                "reassigned_to": reassign_to_id
            })
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def delete_budget(month_yyyymm: Optional[str] = None,
                  category_id: Optional[int] = None,
                  delete_all: bool = False):
    """
    Delete budgets.

    Use one of these modes:
    1) Specific budget: provide BOTH month_yyyymm (YYYYMM) AND category_id.
    2) All budgets for a month: provide month_yyyymm only.
    3) All budgets for a category: provide category_id only.
    4) Delete EVERYTHING: set delete_all = true.

    Returns how many rows were deleted.
    """
    try:
        # Validate inputs / mode selection
        if not delete_all:
            if month_yyyymm is None and category_id is None:
                return err("Provide month_yyyymm or category_id, or set delete_all=true.")
            if month_yyyymm is not None:
                if not re.match(r"^\d{6}$", month_yyyymm):
                    return err("month_yyyymm must be YYYYMM")
        where = []
        params: List[Any] = []

        if delete_all:
            sql = "DELETE FROM budgets"
        else:
            sql = "DELETE FROM budgets WHERE "
            if month_yyyymm is not None and category_id is not None:
                where.append("month_yyyymm = ?")
                params.append(month_yyyymm)
                where.append("category_id = ?")
                params.append(category_id)
            elif month_yyyymm is not None:
                where.append("month_yyyymm = ?")
                params.append(month_yyyymm)
            elif category_id is not None:
                where.append("category_id = ?")
                params.append(category_id)
            else:
                return err("Invalid mode")  # defensive; should not reach
            sql += " AND ".join(where)

        with get_conn() as c:
            cur = c.execute(sql, params)
            deleted = max(0, cur.rowcount or 0)
            return ok({"deleted": deleted,
                       "mode": ("all" if delete_all else
                                "month+category" if (month_yyyymm and category_id) else
                                "month" if month_yyyymm else
                                "category"),
                       "month_yyyymm": month_yyyymm,
                       "category_id": category_id})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def delete_category(id: int,
                    reassign_to_id: Optional[int] = None,
                    include_descendants: bool = True):
    """
    Delete a category.

    Options:
    - reassign_to_id: If provided, all transaction_splits with category_id in the target set
      (the category, and optionally its descendants) will be reassigned to this category
      BEFORE deletion. If omitted, those splits' category_id becomes NULL due to FK rule.
    - include_descendants (default True): also delete all child categories recursively.

    Notes:
    - Budgets linked to deleted categories are removed automatically (ON DELETE CASCADE).
    - Rules may reference category_ids in JSON; those aren't auto-updated (no FK). Review rules if needed.

    Returns: { deleted_categories, reassigned_splits, affected_category_ids, reassigned_to }
    """
    try:
        with get_conn() as c:
            c.row_factory = sqlite3.Row

            # Ensure the category exists
            cur = c.execute("SELECT id, name, parent_id FROM categories WHERE id=?", (id,))
            cat = cur.fetchone()
            if not cat:
                return err(f"Category {id} not found")

            # Build the target set of category IDs (self or self+descendants)
            if include_descendants:
                # Recursive CTE to collect subtree
                cur = c.execute("""
                    WITH RECURSIVE subtree(id) AS (
                        SELECT id FROM categories WHERE id=?
                        UNION ALL
                        SELECT c.id FROM categories c
                        JOIN subtree s ON c.parent_id = s.id
                    )
                    SELECT id FROM subtree
                """, (id,))
                target_ids = [r["id"] for r in cur.fetchall()]
            else:
                target_ids = [id]

            # Optional reassignment of splits before deletion
            reassigned = 0
            if reassign_to_id is not None:
                if reassign_to_id == id:
                    return err("reassign_to_id cannot be the same as the category being deleted")
                # Ensure target category exists
                cur = c.execute("SELECT id FROM categories WHERE id=?", (reassign_to_id,))
                if not cur.fetchone():
                    return err(f"Target category {reassign_to_id} not found")

                # Reassign all splits that reference any of the target_ids
                qmarks = ",".join("?" for _ in target_ids)
                params = [reassign_to_id] + target_ids
                cur = c.execute(
                    f"UPDATE transaction_splits SET category_id=? WHERE category_id IN ({qmarks})",
                    params
                )
                reassigned = max(0, cur.rowcount or 0)

            # Delete the categories (self or subtree)
            if include_descendants:
                cur = c.execute("""
                    WITH RECURSIVE subtree(id) AS (
                        SELECT id FROM categories WHERE id=?
                        UNION ALL
                        SELECT c.id FROM categories c
                        JOIN subtree s ON c.parent_id = s.id
                    )
                    DELETE FROM categories WHERE id IN (SELECT id FROM subtree)
                """, (id,))
            else:
                cur = c.execute("DELETE FROM categories WHERE id=?", (id,))

            deleted = max(0, cur.rowcount or 0)

            return ok({
                "deleted_categories": deleted,
                "reassigned_splits": reassigned,
                "affected_category_ids": target_ids,
                "reassigned_to": reassign_to_id
            })
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def delete_expenses(start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    category: Optional[str] = None,
                    delete_all: bool = False):
    """
    Bulk delete from legacy 'expenses'.

    Modes:
    - Specific filters: any combination of start_date/end_date/category
    - Delete everything: set delete_all = true

    Returns how many rows were deleted.
    """
    try:
        if not delete_all and not (start_date or end_date or category):
            return err("Provide a filter (start_date/end_date/category) or set delete_all=true.")

        where = []
        params: List[Any] = []

        if delete_all:
            sql = "DELETE FROM expenses"
        else:
            if start_date:
                s = coerce_date(start_date)
                where.append("date >= ?"); params.append(s)
            if end_date:
                e = coerce_date(end_date)
                where.append("date <= ?"); params.append(e)
            if category:
                where.append("category = ?"); params.append(category)
            sql = "DELETE FROM expenses WHERE " + " AND ".join(where)

        with get_conn() as c:
            cur = c.execute(sql, params)
            deleted = max(0, cur.rowcount or 0)
            return ok({
                "deleted": deleted,
                "mode": "all" if delete_all else "filtered",
                "filters": {"start_date": start_date, "end_date": end_date, "category": category}
            })
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def delete_expense(id: int):
    """
    Delete one legacy 'expenses' row by id.
    """
    try:
        with get_conn() as c:
            cur = c.execute("DELETE FROM expenses WHERE id=?", (id,))
            return ok({"deleted": max(0, cur.rowcount or 0), "id": id})
    except Exception as ex:
        return err(str(ex))


@mcp.tool()
def goals_delete(name: Optional[str] = None, delete_all: bool = False):
    """
    Delete goals by name or all goals.
    """
    try:
        if delete_all:
            with get_conn() as c:
                cur = c.execute("DELETE FROM goals")
                return ok({"deleted": max(0, cur.rowcount or 0), "mode": "all"})

        # safe strip on optional
        name_clean = (name or "").strip()
        if not name_clean:
            return err("Provide a non-empty goal name or set delete_all=true.")

        with get_conn() as c:
            cur = c.execute("DELETE FROM goals WHERE name = ?", (name_clean,))
            return ok({"deleted": max(0, cur.rowcount or 0), "mode": "by_name", "name": name_clean})
    except Exception as ex:
        return err(str(ex))

@mcp.tool()
def seed_categories_from_file(reset: bool = False):
    """
    Seed the normalized categories table from categories.json.
    - If reset=True, clears categories first (budgets linked to categories will be deleted via FK CASCADE).
    - Idempotent: uses INSERT OR IGNORE so re-running won't duplicate rows.
    Returns counts of parents/children touched.
    """
    try:
        if not os.path.exists(CATEGORIES_PATH):
            return err(f"{CATEGORIES_PATH} not found")

        with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return err("categories.json must be an object mapping parent->list(children)")

        parents = 0
        children = 0

        with sqlite3.connect(DB_PATH) as c:
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON")

            if reset:
                c.execute("DELETE FROM categories")

            for parent_name, child_list in data.items():
                if not isinstance(child_list, list):
                    return err(f"Category '{parent_name}' must map to a list of subcategories")

                # insert parent (top-level)
                c.execute("INSERT OR IGNORE INTO categories(name, parent_id) VALUES (?, NULL)", (parent_name,))
                cur = c.execute("SELECT id FROM categories WHERE name=? AND parent_id IS NULL", (parent_name,))
                row = cur.fetchone()
                if not row:
                    return err(f"Failed to insert or fetch parent category '{parent_name}'")
                parent_id = row["id"]
                parents += 1

                # insert children
                for child_name in child_list:
                    c.execute("INSERT OR IGNORE INTO categories(name, parent_id) VALUES (?, ?)", (child_name, parent_id))
                    children += 1

        return ok({"parents_processed": parents, "children_processed": children, "reset": reset})
    except Exception as ex:
        return err(str(ex))


# ---------- run ----------

if __name__ == "__main__":
    mcp.run()
