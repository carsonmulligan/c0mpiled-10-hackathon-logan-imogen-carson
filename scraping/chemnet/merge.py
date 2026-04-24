"""
Merge all per-source SQLite databases in dbs/ into a single master chemnet.sqlite3.

Strategy: attach each source DB, copy its tables into the master with the source
prefix preserved (the tables are already prefixed by source tag in each DB, so
no renaming needed — we just COPY them in). Also build a `compounds_union` view
that pulls a best-effort (name, cas, source_tag, category) tuple from each
table for cross-source search.

Safe to re-run; DROPs and rebuilds the master.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent
DB_DIR = ROOT / "dbs"
MASTER = ROOT / "chemnet.sqlite3"


# Map each source table to (name_col, cas_col, category_col, source_tag_literal)
# Non-applicable columns can be NULL. Keep this simple — evolve as schemas fix.
UNION_TABLES = [
    # source_db, table, name_col, cas_col, category_col
    ("dea", "dea_precursors", "name", "cas_numbers", "list"),
    ("dea", "dea_schedules", "name", None, "schedule"),
    ("dea", "dea_orange_book", "name", None, "schedule"),
    ("incb", "incb_red_list", "chemical_name", "cas_number", "table"),
    ("incb", "incb_yellow_list", "drug_name", "cas_number", "convention_schedule"),
    ("incb", "incb_green_list", "substance_name", "cas_number", "convention_schedule"),
    ("pubchem", "pubchem_compounds", "iupac_name", "cas_primary", "source_tag"),
    ("wikipedia", "wikipedia_compounds", "name", "cas_numbers", "schedule_or_category"),
    ("export_controls", "australia_group_chemicals", "chemical_name", "cas_number", "list_name"),
    ("export_controls", "opcw_schedules", "chemical_name", "cas_number", "schedule"),
    ("export_controls", "bis_ccl_chemicals", "chemical_name", "cas_number", "eccn"),
    ("eu_uk", "eu_precursors", "chemical_name", "cas_number", "category"),
    ("eu_uk", "uk_controlled_drugs", "drug_name", "cas_number", "schedule"),
]

MARKETPLACE_TABLES = [
    # source_db, table
    ("chemnet_com", "chemnet_products"),
    ("chemnet_com", "chemnet_suppliers"),
    ("chem_b2b", "b2b_products"),
    ("chem_b2b", "b2b_suppliers"),
    ("chem_china", "china_products"),
    ("chem_china", "china_suppliers"),
    ("chem_intl", "intl_products"),
    ("chem_intl", "intl_suppliers"),
]


def main() -> int:
    if MASTER.exists():
        MASTER.unlink()
    con = sqlite3.connect(MASTER)

    attached: list[str] = []
    for db_file in sorted(DB_DIR.glob("*.sqlite3")):
        alias = db_file.stem
        con.execute(f"ATTACH DATABASE ? AS {alias}", (str(db_file),))
        attached.append(alias)
    print(f"attached: {attached}")

    # Copy all source tables verbatim
    for alias in attached:
        tables = con.execute(
            f"SELECT name FROM {alias}.sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        for (t,) in tables:
            con.execute(f"DROP TABLE IF EXISTS {t}")
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM {alias}.{t}")
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {alias}.{t} -> main.{t}  [{n:,} rows]")

    # Build a regulatory compounds union view
    union_parts = []
    for alias, table, name_col, cas_col, cat_col in UNION_TABLES:
        if not table_exists(con, table):
            continue
        name_expr = name_col or "NULL"
        cas_expr = cas_col or "NULL"
        cat_expr = cat_col or "NULL"
        union_parts.append(
            f"SELECT '{alias}' AS source_tag, '{table}' AS source_table, "
            f"{name_expr} AS name, {cas_expr} AS cas, {cat_expr} AS category "
            f"FROM {table}"
        )
    if union_parts:
        con.execute("DROP VIEW IF EXISTS compounds_union")
        con.execute("CREATE VIEW compounds_union AS " + " UNION ALL ".join(union_parts))
        n = con.execute("SELECT COUNT(*) FROM compounds_union").fetchone()[0]
        print(f"  view compounds_union  [{n:,} rows]")

    # Marketplace listings union (suppliers + products separately)
    mk_parts = []
    for alias, table in MARKETPLACE_TABLES:
        if not table_exists(con, table):
            continue
        cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
        if "supplier_name" in cols:
            mk_parts.append(
                f"SELECT '{alias}' AS source_db, '{table}' AS source_table, "
                f"supplier_name, "
                f"{'supplier_country' if 'supplier_country' in cols else 'NULL'} AS supplier_country, "
                f"{'product_name' if 'product_name' in cols else 'NULL'} AS product_name, "
                f"{'product_cas' if 'product_cas' in cols else 'NULL'} AS product_cas, "
                f"{'query_cas' if 'query_cas' in cols else 'NULL'} AS query_cas, "
                f"{'product_url' if 'product_url' in cols else 'NULL'} AS product_url "
                f"FROM {table}"
            )
    if mk_parts:
        con.execute("DROP VIEW IF EXISTS marketplace_listings_union")
        con.execute("CREATE VIEW marketplace_listings_union AS " + " UNION ALL ".join(mk_parts))
        n = con.execute("SELECT COUNT(*) FROM marketplace_listings_union").fetchone()[0]
        print(f"  view marketplace_listings_union  [{n:,} rows]")

    con.commit()
    con.close()
    print(f"wrote {MASTER}")
    return 0


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    r = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,)
    ).fetchone()
    return r is not None


if __name__ == "__main__":
    raise SystemExit(main())
