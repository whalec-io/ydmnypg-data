import Connection from "./connection.js";
import { defaultRegistry } from "./registry.js";

class Repository {
  constructor(dbConnection, registry) {
    const reg = registry || defaultRegistry;
    this.dbConnection =
      typeof dbConnection === "string"
        ? Connection.get(dbConnection, reg)
        : dbConnection;
  }

  get tableName() {
    throw new Error(`${this.constructor.name} must define tableName`);
  }

  // ── Lifecycle hooks ────────────────────────────────────────────────────────

  async beforeInsert(data) {
    return data;
  }
  async afterInsert(row) {}
  async beforeUpdate(data, condition) {
    return data;
  }
  async afterUpdate(result) {}
  async beforeDelete(condition) {
    return true;
  }
  async afterDelete(result) {}

  // ── Adapter access ────────────────────────────────────────────────────────

  get db() {
    return this.dbConnection.client;
  }
  get dbTable() {
    return this.db.table(this.tableName);
  }

  // ── CRUD ──────────────────────────────────────────────────────────────────

  query(sql, values, connection) {
    return this.db.executeQuery(sql, values, connection);
  }

  async insert(data, options = {}) {
    const values = await this.beforeInsert({ ...data });
    const result = await this.dbTable.insert(values, options.connection);
    await this.afterInsert(result);
    return result;
  }

  async insertIgnore(data, options = {}) {
    const values = await this.beforeInsert({ ...data });
    return this.dbTable.insertIgnore(values, options.connection);
  }

  async upsert(data, updatedKeys, options = {}) {
    const values = await this.beforeInsert({ ...data });
    return this.dbTable.insertOnDuplicateUpdate(
      values,
      updatedKeys,
      options.connection,
    );
  }

  async insertRows(rows, options = {}) {
    return this.dbTable.insertRows(rows, options.connection);
  }

  async update(condition, data, options = {}) {
    const values = await this.beforeUpdate({ ...data }, condition);
    const result = await this.dbTable.update(
      values,
      condition,
      options.connection,
    );
    await this.afterUpdate(result);
    return result;
  }

  archive(condition, options = {}) {
    return this.dbTable.archive(condition, options.connection);
  }

  async delete(condition, options = {}) {
    const proceed = await this.beforeDelete(condition);
    if (proceed === false) return false;
    const result = await this.dbTable.delete(condition, options.connection);
    await this.afterDelete(result);
    return result;
  }

  execute(command, params = {}, options = {}) {
    return this.dbTable.execute(command, params, options.connection);
  }

  transaction(callback) {
    return this.db.transaction(callback);
  }

  task(callback) {
    return this.db.task(callback);
  }
}

export default Repository;
