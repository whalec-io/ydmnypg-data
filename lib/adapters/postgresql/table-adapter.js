import fs from "fs";
import mybatisMapper from "mybatis-mapper";
import { BaseTableAdapter, QueryAction } from "../base-table-adapter.js";
import {
  buildColumnDef,
  buildCreateTable,
  buildInsert,
  buildUpdate,
  buildDelete,
} from "./ddl.js";

class PgTableAdapter extends BaseTableAdapter {
  async doCreateTable(connection) {
    const { sql, constraints } = buildCreateTable(
      this.tableName,
      this.attributes,
      this.constraints,
    );
    await this.adapter.executeQuery(sql, [], connection);

    // BTREE and FULLTEXT (GIN) indexes created separately
    for (const [key, def] of Object.entries(constraints || {})) {
      if (def.type === "btree") {
        const idxSql = `CREATE INDEX "${key}" ON "${this.tableName}" (${def.keys.map((k) => `"${k}"`).join(", ")})`;
        await this.adapter
          .executeQuery(idxSql, [], connection)
          .catch((e) => this.adapter.logger.error(e.message));
      } else if (def.type === "fulltext") {
        const expr = def.keys.map((k) => `"${k}"`).join(` || ' ' || `);
        const idxSql = `CREATE INDEX "${key}" ON "${this.tableName}" USING GIN (to_tsvector('simple', ${expr}))`;
        await this.adapter
          .executeQuery(idxSql, [], connection)
          .catch((e) => this.adapter.logger.error(e.message));
      }
    }
  }

  async doDropTable(connection) {
    await this.adapter.executeQuery(
      `DROP TABLE "${this.tableName}"`,
      [],
      connection,
    );
  }

  async doSyncTable(connection) {
    const columns = await this.doGetColumns(connection);
    const existing = {};
    for (const col of columns) existing[col.column_name] = col;

    const toAdd = [];
    for (const [fieldName, attribute] of Object.entries(this.attributes)) {
      attribute.key = fieldName;
      if (!existing[fieldName]) toAdd.push({ ...attribute });
    }

    if (toAdd.length > 0) {
      const others = { name: this.tableName, addColumn: true };
      for (const col of toAdd) {
        const colDef = buildColumnDef(col, col.key, others);
        await this.adapter.executeQuery(
          `ALTER TABLE "${this.tableName}" ADD COLUMN ${colDef}`,
          [],
          connection,
        );
      }
    }
  }

  async doGetColumns(connection) {
    const result = await this.adapter.executeQuery(
      `SELECT column_name, data_type, character_maximum_length, column_default, is_nullable
       FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
      [this.tableName],
      connection,
    );
    return result.rows;
  }

  async doInitRecords(records, connection) {
    for (const values of records) {
      if (typeof values === "string") {
        if (fs.existsSync(values)) {
          const lines = fs
            .readFileSync(values)
            .toString()
            .split("\n")
            .filter(Boolean);
          for (const sql of lines) {
            if (sql) await this.adapter.executeQuery(sql, [], connection);
          }
        } else {
          await this.adapter.executeQuery(values, [], connection);
        }
      } else {
        for (const key of Object.keys(values)) {
          if (typeof values[key] === "function")
            values[key] = await values[key]();
        }
        this.applyDefaults(QueryAction.INSERT, values);
        const { sql, values: params } = buildInsert(this.tableName, values);
        await this.adapter.executeQuery(sql, params, connection);
      }
    }
  }

  async doInsert(values, connection) {
    this.applyDefaults(QueryAction.INSERT, values);
    const { sql, values: params } = buildInsert(this.tableName, values);
    const result = await this.adapter.executeQuery(sql, params, connection);
    const row = result.rows[0];
    return {
      ...result,
      affectedRows: result.rowCount,
      insertId: row ? row[this.primaryKey] : null,
    };
  }

  async doInsertIgnore(values, connection) {
    this.applyDefaults(QueryAction.INSERT, values);
    const { sql: base, values: params } = buildInsert(this.tableName, values);
    const sql = base.replace(
      " RETURNING *",
      " ON CONFLICT DO NOTHING RETURNING *",
    );
    const result = await this.adapter.executeQuery(sql, params, connection);
    return result.rowCount > 0
      ? { ...result, affectedRows: result.rowCount }
      : false;
  }

  async doInsertOnDuplicateUpdate(values, updatedKeys, connection) {
    this.applyDefaults(QueryAction.INSERT, values);
    const keys = Object.keys(values);
    const cols = keys.map((k) => `"${k}"`).join(", ");
    const params = keys.map((_, i) => `$${i + 1}`).join(", ");
    const vals = keys.map((k) => values[k]);
    const updateClauses = updatedKeys
      .map((k) => `"${k}" = EXCLUDED."${k}"`)
      .join(", ");
    const sql = `INSERT INTO "${this.tableName}" (${cols}) VALUES (${params}) ON CONFLICT ("${this.primaryKey}") DO UPDATE SET ${updateClauses} RETURNING *`;
    const result = await this.adapter.executeQuery(sql, vals, connection);
    return result.rowCount > 0
      ? { ...result, affectedRows: result.rowCount }
      : false;
  }

  async doInsertRows(rows, connection) {
    const fields = [];
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        if (!fields.includes(key)) fields.push(key);
      }
      this.applyDefaults(QueryAction.INSERT, row, fields);
    }
    fields.sort();
    const allValues = [];
    const placeholders = rows.map((row, ri) => {
      const rowPH = fields.map((f, ci) => {
        allValues.push(row[f]);
        return `$${ri * fields.length + ci + 1}`;
      });
      return `(${rowPH.join(", ")})`;
    });
    const colList = fields.map((f) => `"${f}"`).join(", ");
    const sql = `INSERT INTO "${this.tableName}" (${colList}) VALUES ${placeholders.join(", ")}`;
    const result = await this.adapter.executeQuery(sql, allValues, connection);
    return result.rowCount > 0
      ? { ...result, affectedRows: result.rowCount }
      : false;
  }

  async doInsertOnDuplicateUpdateRows(rows, condition, connection) {
    const fields = [];
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        if (!fields.includes(key)) fields.push(key);
      }
      this.applyDefaults(QueryAction.INSERT, row, fields);
    }
    fields.sort();
    const allValues = [];
    const placeholders = rows.map((row, ri) => {
      const rowPH = fields.map((f, ci) => {
        allValues.push(row[f]);
        return `$${ri * fields.length + ci + 1}`;
      });
      return `(${rowPH.join(", ")})`;
    });
    const colList = fields.map((f) => `"${f}"`).join(", ");
    const sql = `INSERT INTO "${this.tableName}" (${colList}) VALUES ${placeholders.join(", ")} ON CONFLICT ${condition}`;
    const result = await this.adapter.executeQuery(sql, allValues, connection);
    return result.rowCount > 0
      ? { ...result, affectedRows: result.rowCount }
      : false;
  }

  async doUpdate(values, condition, connection) {
    this.applyDefaults(QueryAction.UPDATE, values);
    const { sql, values: params } = buildUpdate(
      this.tableName,
      values,
      condition,
    );
    const result = await this.adapter.executeQuery(sql, params, connection);
    return { ...result, affectedRows: result.rowCount };
  }

  async doUpdateIgnore(values, condition, connection) {
    this.applyDefaults(QueryAction.UPDATE, values);
    const { sql, values: params } = buildUpdate(
      this.tableName,
      values,
      condition,
    );
    const result = await this.adapter.executeQuery(sql, params, connection);
    return result.rowCount > 0
      ? { ...result, affectedRows: result.rowCount }
      : false;
  }

  async doDelete(condition, connection) {
    const { sql, values: params } = buildDelete(this.tableName, condition);
    const result = await this.adapter.executeQuery(sql, params, connection);
    return { ...result, affectedRows: result.rowCount };
  }

  doExecute(command, params, connection) {
    let namespace, sqlId;
    if (command.includes(".")) {
      const parts = command.split(".");
      sqlId = parts.pop();
      namespace = parts.join(".");
    } else {
      sqlId = command;
      namespace = this.tableName;
    }
    const sql = mybatisMapper.getStatement(
      namespace,
      sqlId,
      { ...this.adapter.tableMapping, ...params },
      { language: "sql", indent: "  ", uppercase: true },
    );
    return this.adapter
      .executeQuery(sql, [], connection)
      .then((result) => (result.rows !== undefined ? result.rows : result));
  }
}

export default PgTableAdapter;
