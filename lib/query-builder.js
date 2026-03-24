class QueryBuilder {
  constructor(dialect = "mysql") {
    this.dialect = dialect;
    this.table = null;
    this.selects = [];
    this.wheres = []; // { sql, values }
    this.orderBys = [];
    this.limitNum = null;
    this.offsetNum = null;
    this.joins = [];
    this.paramIndex = 1; // for pg $N style
  }

  from(table) {
    this.table = table;
    return this;
  }

  select(...cols) {
    this.selects.push(...cols);
    return this;
  }

  join(table, leftCol, rightCol, type = "INNER") {
    this.joins.push({ table, leftCol, rightCol, type });
    return this;
  }

  leftJoin(table, leftCol, rightCol) {
    return this.join(table, leftCol, rightCol, "LEFT");
  }

  where(conditions) {
    for (const [key, value] of Object.entries(conditions)) {
      if (value === null) {
        this.wheres.push({ sql: `${this.col(key)} IS NULL`, values: [] });
      } else if (Array.isArray(value)) {
        const ph = value.map(() => this.placeholder());
        this.wheres.push({
          sql: `${this.col(key)} IN (${ph.join(", ")})`,
          values: value,
        });
      } else {
        this.wheres.push({
          sql: `${this.col(key)} = ${this.placeholder()}`,
          values: [value],
        });
      }
    }
    return this;
  }

  whereRaw(sql, values = []) {
    this.wheres.push({ sql, values });
    return this;
  }

  whereBetween(col, min, max) {
    this.wheres.push({
      sql: `${this.col(col)} BETWEEN ${this.placeholder()} AND ${this.placeholder()}`,
      values: [min, max],
    });
    return this;
  }

  whereNotNull(col) {
    this.wheres.push({ sql: `${this.col(col)} IS NOT NULL`, values: [] });
    return this;
  }

  orderBy(col, dir = "ASC") {
    const direction = dir.toUpperCase() === "DESC" ? "DESC" : "ASC";
    this.orderBys.push(`${this.col(col)} ${direction}`);
    return this;
  }

  limit(n) {
    this.limitNum = n;
    return this;
  }
  offset(n) {
    this.offsetNum = n;
    return this;
  }

  // ── Build ─────────────────────────────────────────────────────────────────

  build() {
    if (!this.table) throw new Error("QueryBuilder: .from(table) is required");

    const selectCols =
      this.selects.length > 0
        ? this.selects
            .map((c) =>
              c === "*" || c.includes(".") || c.includes(" ") ? c : this.col(c),
            )
            .join(", ")
        : "*";

    let sql = `SELECT ${selectCols} FROM ${this.tableRef(this.table)}`;

    for (const j of this.joins) {
      sql += ` ${j.type} JOIN ${this.tableRef(j.table)} ON ${j.leftCol} = ${j.rightCol}`;
    }

    const allValues = [];
    if (this.wheres.length > 0) {
      const whereSql = this.wheres
        .map((w) => {
          allValues.push(...w.values);
          return w.sql;
        })
        .join(" AND ");
      sql += ` WHERE ${whereSql}`;
    }

    if (this.orderBys.length > 0)
      sql += ` ORDER BY ${this.orderBys.join(", ")}`;
    if (this.limitNum !== null) sql += ` LIMIT ${this.limitNum}`;
    if (this.offsetNum !== null) sql += ` OFFSET ${this.offsetNum}`;

    return { sql, values: allValues };
  }

  // ── Convenience: count ────────────────────────────────────────────────────

  buildCount() {
    const saved = {
      selects: this.selects,
      orderBys: this.orderBys,
      limitNum: this.limitNum,
      offsetNum: this.offsetNum,
    };

    this.selects = ["COUNT(*) AS count"];
    this.orderBys = [];
    this.limitNum = null;
    this.offsetNum = null;

    const result = this.build();

    this.selects = saved.selects;
    this.orderBys = saved.orderBys;
    this.limitNum = saved.limitNum;
    this.offsetNum = saved.offsetNum;

    return result;
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  placeholder() {
    if (this.dialect === "postgresql") return `$${this.paramIndex++}`;
    return "?";
  }

  col(name) {
    if (this.dialect === "postgresql") return `"${name}"`;
    return `\`${name}\``;
  }

  tableRef(name) {
    if (this.dialect === "postgresql") return `"${name}"`;
    return `\`${name}\``;
  }

  async find(adapter, connection) {
    const { sql, values } = this.build();
    return adapter.executeQuery(sql, values, connection);
  }

  async count(adapter, connection) {
    const { sql, values } = this.buildCount();
    const result = await adapter.executeQuery(sql, values, connection);
    const row = Array.isArray(result)
      ? result[0]
      : result.rows
        ? result.rows[0]
        : result[0];
    return row ? parseInt(row.count, 10) : 0;
  }
}

export default QueryBuilder;
