import fs from "fs";
import mybatisMapper from "mybatis-mapper";
import { BaseTableAdapter, QueryAction } from "../base-table-adapter.js";
import { buildColumnDef, buildCreateTable } from "./ddl.js";

class MysqlTableAdapter extends BaseTableAdapter {
  constructor(adapter, tableName, isCreated = false) {
    super(adapter, tableName, isCreated);
    this.rowFormat = "";
  }

  setRowFormat(rowFormat) {
    const valid = ["DEFAULT", "DYNAMIC", "FIXED", "COMPRESSED", "REDUNDANT", "COMPACT"];
    if (!valid.includes(rowFormat)) {
      this.adapter.logger.warn(`Invalid ROW_FORMAT: ${rowFormat}`);
      return;
    }
    this.rowFormat = rowFormat;
  }

  async doCreateTable(connection) {
    const sql = buildCreateTable(this.tableName, this.attributes, this.constraints, this.rowFormat);
    await this.adapter.executeQuery(sql, [], connection);
  }

  async doDropTable(connection) {
    await this.adapter.executeQuery(`DROP TABLE \`${this.tableName}\``, [], connection);
  }

  async doSyncTable(connection) {
    const columns = await this.doGetColumns(connection);
    const existing = {};
    for (const col of columns) {
      const name = col.Field || col.field;
      existing[name] = col;
    }

    const toAdd = [];
    let prev = null;
    for (const [fieldName, attribute] of Object.entries(this.attributes)) {
      attribute.key = fieldName;
      if (!existing[fieldName]) toAdd.push({ ...attribute, prev });
      prev = attribute;
    }

    if (toAdd.length > 0) {
      const others = { name: this.tableName, addColumn: true };
      for (const col of toAdd) {
        const colDef = buildColumnDef("", col, col.key, others);
        const position = col.prev ? `AFTER \`${col.prev.key}\`` : "FIRST";
        await this.adapter.executeQuery(
          `ALTER TABLE \`${this.tableName}\` ADD COLUMN ${colDef} ${position}`,
          [],
          connection,
        );
      }
    }
  }

  async doGetColumns(connection) {
    return this.adapter.executeQuery(`SHOW COLUMNS FROM \`${this.tableName}\``, [], connection);
  }

  async doInitRecords(records, connection) {
    for (const values of records) {
      if (typeof values === "string") {
        if (fs.existsSync(values)) {
          const lines = fs.readFileSync(values).toString().split("\n").filter(Boolean);
          for (const sql of lines) {
            if (sql) await this.adapter.executeQuery(sql, [], connection);
          }
        } else {
          await this.adapter.executeQuery(values, [], connection);
        }
      } else {
        for (const key of Object.keys(values)) {
          if (typeof values[key] === "function") values[key] = await values[key]();
        }
        this.applyDefaults(QueryAction.INSERT, values);
        await this.adapter.executeQuery(`INSERT INTO \`${this.tableName}\` SET ?`, values, connection);
      }
    }
  }

  async doInsert(values, connection) {
    this.applyDefaults(QueryAction.INSERT, values);
    return this.adapter.executeQuery(`INSERT INTO \`${this.tableName}\` SET ?`, values, connection);
  }

  async doInsertIgnore(values, connection) {
    this.applyDefaults(QueryAction.INSERT, values);
    const res = await this.adapter.executeQuery(`INSERT IGNORE INTO \`${this.tableName}\` SET ?`, values, connection);
    return res.affectedRows > 0 && res.warningCount === 0 ? res : false;
  }

  async doInsertOnDuplicateUpdate(values, updatedKeys, connection) {
    this.applyDefaults(QueryAction.INSERT, values);
    const keyCondition = updatedKeys.includes(this.primaryKey)
      ? `\`${this.primaryKey}\` = \`${this.primaryKey}\`, `
      : "";
    const updatedValues = {};
    for (const k of updatedKeys) {
      if (values[k] !== undefined) updatedValues[k] = values[k];
    }
    const sql = `INSERT INTO \`${this.tableName}\` SET ? ON DUPLICATE KEY UPDATE ${keyCondition} ?`;
    const res = await this.adapter.executeQuery(sql, [values, updatedValues], connection);
    return res.affectedRows > 0 ? res : false;
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
    const values = rows.map(row => fields.map(f => row[f]));
    const sql = `INSERT INTO \`${this.tableName}\` (${fields.map(f => `\`${f}\``).join(",")}) VALUES ?`;
    const res = await this.adapter.executeQuery(sql, [values], connection);
    return res.affectedRows > 0 ? res : false;
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
    const values = rows.map(row => fields.map(f => row[f]));
    const sql = `INSERT INTO \`${this.tableName}\` (${fields.map(f => `\`${f}\``).join(",")}) VALUES ? ON DUPLICATE KEY UPDATE ${condition}`;
    const res = await this.adapter.executeQuery(sql, [values], connection);
    return res.affectedRows > 0 ? res : false;
  }

  buildWhereClause(condition) {
    const keys = Object.keys(condition);
    const clause = keys.map(k => `\`${k}\` = ?`).join(" AND ");
    const values = keys.map(k => condition[k]);
    return { clause, values };
  }

  async doUpdate(values, condition, connection) {
    this.applyDefaults(QueryAction.UPDATE, values);
    const where = this.buildWhereClause(condition);
    return this.adapter.executeQuery(
      `UPDATE \`${this.tableName}\` SET ? WHERE ${where.clause}`,
      [values, ...where.values],
      connection,
    );
  }

  async doUpdateIgnore(values, condition, connection) {
    this.applyDefaults(QueryAction.UPDATE, values);
    const where = this.buildWhereClause(condition);
    const res = await this.adapter.executeQuery(
      `UPDATE IGNORE \`${this.tableName}\` SET ? WHERE ${where.clause}`,
      [values, ...where.values],
      connection,
    );
    return res.affectedRows > 0 && res.warningCount === 0 ? res : false;
  }

  async doDelete(condition, connection) {
    const where = this.buildWhereClause(condition);
    return this.adapter.executeQuery(
      `DELETE FROM \`${this.tableName}\` WHERE ${where.clause}`,
      where.values,
      connection,
    );
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

    return this.adapter.executeQuery(sql, [], connection);
  }
}

export default MysqlTableAdapter;
