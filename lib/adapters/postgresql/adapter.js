import pg from "pg";
const { Pool } = pg;
import BaseAdapter from "../base-adapter.js";
import PgTableAdapter from "./table-adapter.js";
import { QueryError } from "../../errors.js";

class PgAdapter extends BaseAdapter {
  get type() {
    return "postgresql";
  }

  schemaKey(tableName) {
    return tableName;
  }

  createPool(options) {
    const {
      database,
      host = "localhost",
      port = 5432,
      user,
      password,
      connectionLimit: max = 10,
      idleTimeoutMillis = 30000,
      connectionTimeoutMillis = 10000,
      ssl,
    } = options;

    return new Pool({
      database,
      host,
      port,
      user,
      password,
      max,
      idleTimeoutMillis,
      connectionTimeoutMillis,
      ssl,
    });
  }

  async getConnection() {
    return this.pool.connect();
  }

  releaseConnection(client) {
    try {
      client.release();
    } catch (_) {}
  }

  async ping(client) {
    await client.query("SELECT 1");
  }

  async destroyPool(pool) {
    await pool.end();
  }

  async fetchTableNames(database, connection) {
    const result = await connection.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_catalog = $1`,
      [database],
    );
    return result.rows.map((r) => r.table_name);
  }

  createTableAdapter(tableName, isCreated) {
    return new PgTableAdapter(this, tableName, isCreated);
  }

  async beginTransaction(client) {
    await client.query("BEGIN");
  }

  async commitTransaction(client) {
    await client.query("COMMIT");
  }

  async rollbackTransaction(client) {
    await client.query("ROLLBACK");
  }

  async executeQuery(sql, values, connection) {
    if (!connection) throw new QueryError("connection is null", sql);

    const qid = this.logger.queryStart({ sql, values });
    try {
      const hasValues =
        values != null && (!Array.isArray(values) || values.length > 0);
      const result = hasValues
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

export default PgAdapter;
