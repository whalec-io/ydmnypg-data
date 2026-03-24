import { v4 as uuid } from "uuid";

const QueryAction = {
  INSERT: "INSERT",
  UPDATE: "UPDATE",
};

class BaseTableAdapter {
  constructor(adapter, tableName, isCreated = false) {
    this.adapter = adapter;
    this.tableName = tableName;
    this.ready = isCreated;
    this.creating = false;
    this.dropping = false;
    this.attributes = {};
    this.constraints = {};
    this.primaryKey = "";
    this.useAutoIncrement = false;
    this.lastError = null;
  }

  get isCreated() {
    return this.ready;
  }

  handleError(error) {
    this.adapter.logger.error(error.message || String(error));
    this.lastError = error;
  }

  retryWhenReady(methodName, args, delayMs = 100, timeoutMs = 10000) {
    this.adapter.logger.warn(
      `Table '${this.tableName}' not ready — delaying ${methodName}...`,
    );
    return new Promise((resolve, reject) => {
      const deadline = Date.now() + timeoutMs;
      const attempt = () => {
        if (this.ready) {
          this[methodName](...args)
            .then(resolve)
            .catch(reject);
        } else if (Date.now() > deadline) {
          reject(
            new Error(
              `Table '${this.tableName}' not ready after ${timeoutMs}ms — ${methodName} timed out`,
            ),
          );
        } else {
          setTimeout(attempt, delayMs);
        }
      };
      setTimeout(attempt, delayMs);
    });
  }

  generateUUID() {
    return uuid().replace(/-/g, "");
  }
  generateTimestamp(ts = new Date()) {
    return ts.toISOString();
  }

  toJSON(data, defaultValue = "{}") {
    if (typeof data === "string") return data;
    if (data !== null && typeof data === "object") {
      try {
        return JSON.stringify(data);
      } catch (_) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  applyDefaults(action, values, touchedFields = []) {
    for (const [key, def] of Object.entries(this.attributes)) {
      if (!def) continue;
      if (def.json === true) {
        if (
          action === QueryAction.INSERT &&
          (!values[key] || values[key] === "")
        ) {
          values[key] = this.toJSON(values[key], "{}");
        } else if (
          values[key] !== undefined &&
          typeof values[key] === "object"
        ) {
          values[key] = this.toJSON(values[key], "{}");
        }
      } else if (def.defaultValue === "UUID") {
        if (
          action === QueryAction.INSERT &&
          (!values[key] || values[key] === "")
        ) {
          values[key] = this.generateUUID();
        }
      } else if (def.type === "timestamp" || def.type === "datetime") {
        if (values[key] === "CURRENT_TIMESTAMP") {
          values[key] = this.generateTimestamp();
        } else if (
          typeof def.triggerKey === "string" &&
          values[def.triggerKey] !== undefined
        ) {
          values[key] = this.generateTimestamp();
        } else if (
          action === QueryAction.INSERT &&
          (key === "createdAt" || key === "updatedAt")
        ) {
          if (!touchedFields.includes(key)) touchedFields.push(key);
          values[key] = this.generateTimestamp();
        } else if (action === QueryAction.UPDATE && key === "updatedAt") {
          if (!touchedFields.includes(key)) touchedFields.push(key);
          values[key] = this.generateTimestamp();
        }
      } else if (values[key] === undefined) {
        delete values[key];
      }
    }
  }

  setAttributes(attributes) {
    for (const [fieldName, attribute] of Object.entries(attributes)) {
      attribute.key = fieldName;
      if (attribute.primaryKey === true) {
        this.primaryKey = fieldName;
        this.useAutoIncrement =
          attribute.autoIncrement === true ||
          attribute.autoIncrement === "auto";
      }
    }
    this.attributes = attributes;
  }

  setConstraints(constraints = {}) {
    this.constraints = constraints;
  }

  async createTable(connection) {
    if (this.creating) {
      this.adapter.logger.warn(
        `createTable '${this.tableName}' ignored — already creating`,
      );
      return false;
    }
    if (this.tableName === "*")
      throw new Error("Wildcard table does not support DDL");
    this.creating = true;
    try {
      await this.doCreateTable(connection);
      this.ready = true;
      return true;
    } catch (e) {
      this.handleError(e);
      throw e;
    } finally {
      this.creating = false;
    }
  }

  async dropTable(connection) {
    if (this.dropping) {
      this.adapter.logger.warn(
        `dropTable '${this.tableName}' ignored — already dropping`,
      );
      return false;
    }
    if (this.tableName === "*")
      throw new Error("Wildcard table does not support DDL");
    this.dropping = true;
    try {
      await this.doDropTable(connection);
      this.ready = false;
      return true;
    } catch (e) {
      this.handleError(e);
      throw e;
    } finally {
      this.dropping = false;
    }
  }

  syncTable(connection) {
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table does not support DDL"));
    return this.doSyncTable(connection);
  }

  getColumns(connection) {
    if (!this.ready) return this.retryWhenReady("getColumns", [connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doGetColumns(connection);
  }

  init() {
    this.ready = true;
    return Promise.resolve();
  }

  async initRecords(records, connection) {
    if (!this.ready)
      return this.retryWhenReady("initRecords", [records, connection]);
    if (this.tableName === "*") throw new Error("Wildcard table");
    return this.doInitRecords(records, connection);
  }

  insert(values, connection) {
    if (!this.ready) return this.retryWhenReady("insert", [values, connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doInsert(values, connection);
  }

  insertIgnore(values, connection) {
    if (!this.ready)
      return this.retryWhenReady("insertIgnore", [values, connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doInsertIgnore(values, connection);
  }

  insertOnDuplicateUpdate(values, updatedKeys, connection) {
    if (!this.ready)
      return this.retryWhenReady("insertOnDuplicateUpdate", [
        values,
        updatedKeys,
        connection,
      ]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doInsertOnDuplicateUpdate(values, updatedKeys, connection);
  }

  insertRows(rows, connection) {
    if (!this.ready)
      return this.retryWhenReady("insertRows", [rows, connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doInsertRows(rows, connection);
  }

  insertOnDuplicateUpdateRows(rows, condition, connection) {
    if (!this.ready)
      return this.retryWhenReady("insertOnDuplicateUpdateRows", [
        rows,
        condition,
        connection,
      ]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doInsertOnDuplicateUpdateRows(rows, condition, connection);
  }

  update(values, condition, connection) {
    if (!this.ready)
      return this.retryWhenReady("update", [values, condition, connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doUpdate(values, condition, connection);
  }

  updateIgnore(values, condition, connection) {
    if (!this.ready)
      return this.retryWhenReady("updateIgnore", [
        values,
        condition,
        connection,
      ]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doUpdateIgnore(values, condition, connection);
  }

  archive(condition, connection) {
    if (!this.ready)
      return this.retryWhenReady("archive", [condition, connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    const values = { archived: true };
    this.applyDefaults("UPDATE", values);
    return this.doUpdate(values, condition, connection);
  }

  delete(condition, connection) {
    if (!this.ready)
      return this.retryWhenReady("delete", [condition, connection]);
    if (this.tableName === "*")
      return Promise.reject(new Error("Wildcard table"));
    return this.doDelete(condition, connection);
  }

  execute(command, params = {}, connection) {
    if (!this.ready)
      return this.retryWhenReady("execute", [command, params, connection]);
    return this.doExecute(command, params, connection);
  }
}

export { BaseTableAdapter, QueryAction };
