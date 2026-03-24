import fs from "fs";
import mysql from "mysql2/promise";
import mybatisMapper from "mybatis-mapper";
import BaseAdapter from "../base-adapter.js";
import MysqlTableAdapter from "./table-adapter.js";
import { QueryError } from "../../errors.js";

class MysqlAdapter extends BaseAdapter {
  get type() {
    return "mysql";
  }

  normalizeTimezone(tz) {
    if (!tz || tz === "local") return "local";
    if (tz === "utc" || tz === "UTC") return "+00:00";
    return tz;
  }

  createPool(options) {
    const {
      database,
      host = "localhost",
      port = 3306,
      user,
      password,
      charset = "utf8mb4",
      timezone = "local",
      connectionLimit = 10,
      waitForConnections = true,
      queueLimit = 0,
      multipleStatements = false,
      sslCA,
    } = options;

    const config = {
      database,
      host,
      port,
      user,
      password,
      charset,
      timezone: this.normalizeTimezone(timezone),
      connectionLimit,
      waitForConnections,
      queueLimit,
      multipleStatements,
    };

    if (sslCA) config.ssl = { ca: fs.readFileSync(sslCA) };

    return mysql.createPool(config);
  }

  async getConnection() {
    return this.pool.getConnection();
  }

  releaseConnection(client) {
    try {
      client.release();
    } catch (_) {}
  }

  async ping(client) {
    await client.ping();
  }

  async destroyPool(pool) {
    await pool.end();
  }

  async fetchTableNames(database, connection) {
    const [rows] = await connection.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = ?`,
      [database],
    );
    return rows.map((r) => r.table_name || r.TABLE_NAME);
  }

  createTableAdapter(tableName, isCreated) {
    return new MysqlTableAdapter(this, tableName, isCreated);
  }

  async beginTransaction(client) {
    await client.beginTransaction();
  }

  async commitTransaction(client) {
    await client.commit();
  }

  async rollbackTransaction(client) {
    await client.rollback();
  }

  async createView(tableAdapter, view, connection) {
    let sql;
    if (!view.toUpperCase().includes("SELECT") && view.includes(".")) {
      const parts = view.split(".");
      const sqlId = parts.pop();
      const namespace = parts.join(".");
      const viewQuery = mybatisMapper.getStatement(
        namespace,
        sqlId,
        { ...this.tableMapping },
        { language: "sql", indent: "  ", uppercase: true },
      );
      sql = `CREATE OR REPLACE VIEW \`${tableAdapter.tableName}\` AS (${viewQuery})`;
    } else {
      sql = `CREATE OR REPLACE VIEW \`${tableAdapter.tableName}\` AS (${view})`;
    }
    await this.executeQuery(sql, [], connection);
    tableAdapter.ready = true;
  }

  // ── Query execution ────────────────────────────────────────────────────────

  async executeQuery(sql, values, connection) {
    if (!connection) throw new QueryError("connection is null", sql);

    const qid = this.logger.queryStart({ sql, values });
    try {
      const hasValues =
        values != null && (!Array.isArray(values) || values.length > 0);
      const [result] = hasValues
        ? await connection.query(sql, values)
        : await connection.query(sql);
      this.logger.queryEnd(qid);
      return result;
    } catch (e) {
      this.logger.queryEnd(qid, { error: e });
      throw new QueryError(e.message, sql, e);
    }
  }
}

export default MysqlAdapter;
