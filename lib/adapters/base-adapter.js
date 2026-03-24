import mybatisMapper from "mybatis-mapper";
import { Logger } from "./logger.js";

class BaseAdapter {
  constructor(dbConnection, options = {}) {
    this.dbConnection = dbConnection;
    this.options = options;
    this.logger = new Logger(`dao:${this.type}`, options.logger);
    this.tableAdapters = new Map();
    this.tableMapping = {};
    this.autoCreate = options.autoCreate === true;
    this.preloaded = false;
    this.preloading = false;
    this.preloadWaiters = [];
    this.keepAliveTimer = null;
    this.pool = this.createPool(options);
    this.startKeepAlive(options.keepAlive || 60);
  }

  startKeepAlive(intervalSeconds) {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
    this.keepAliveTimer = setInterval(async () => {
      let client;
      try {
        client = await this.getConnection();
        await this.ping(client);
        this.logger.log("keep-alive ping ok");
      } catch (err) {
        this.logger.error(
          "keep-alive ping failed, recreating pool:",
          err.message,
        );
        try {
          await this.destroyPool(this.pool);
        } catch (_) {}
        this.pool = this.createPool(this.options);
      } finally {
        if (client) this.releaseConnection(client);
      }
    }, intervalSeconds * 1000);
    if (this.keepAliveTimer.unref) this.keepAliveTimer.unref();
  }

  async destroy() {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
    await this.destroyPool(this.pool);
  }

  schemaKey(tableName) {
    return `${this.dbConnection.database}.${tableName}`;
  }

  async preloadSchemas() {
    if (this.preloaded) return true;

    if (this.preloading) {
      return new Promise((resolve) => {
        this.preloadWaiters.push(resolve);
      });
    }

    this.preloading = true;
    this.logger.initializing = true;
    let client;
    try {
      client = await this.getConnection();
      const tableNames = await this.fetchTableNames(
        this.dbConnection.database,
        client,
      );
      for (const name of tableNames) {
        const key = this.schemaKey(name);
        if (!this.tableAdapters.has(key)) {
          this.tableAdapters.set(key, this.createTableAdapter(name, true));
        } else {
          this.tableAdapters.get(key).ready = true;
        }
      }
      this.preloaded = true;
      this.logger.info(
        `preloaded ${tableNames.length} tables from '${this.dbConnection.database}'`,
      );
    } catch (e) {
      this.logger.error("preloadSchemas failed:", e.message);
    } finally {
      if (client) this.releaseConnection(client);
      this.preloading = false;
      this.logger.initializing = false;
      for (const resolve of this.preloadWaiters) resolve(this.preloaded);
      this.preloadWaiters = [];
    }
    return this.preloaded;
  }

  initialize(options = {}) {
    const { mappers, autoCreate } = options;
    this.autoCreate = autoCreate === true;
    if (Array.isArray(mappers) && mappers.length > 0) {
      try {
        mybatisMapper.createMapper(mappers);
      } catch (e) {
        this.logger.error("failed to load mappers:", e.message);
      }
    }
  }

  async loadSchemas(design, schemas) {
    let client;
    try {
      client = await this.getConnection();
      for (const schema of schemas) {
        await this.initSchema(design, schema.name, schema, client);
      }
      return true;
    } catch (e) {
      this.logger.error("loadSchemas failed:", e.message);
      return false;
    } finally {
      if (client) this.releaseConnection(client);
    }
  }

  async initSchema(design, schemaName, schema, connection) {
    this.logger.initializing = true;
    const { mappingName, view, attributes, constraints, rowFormat, records } =
      schema;
    const database = this.dbConnection.database;

    if (design.dbConnection.database !== database) {
      throw new Error(
        `Connection database mismatch: expected '${database}', got '${design.dbConnection.database}'`,
      );
    }

    const key = this.schemaKey(schemaName);
    if (!this.tableAdapters.has(key)) {
      this.tableAdapters.set(key, this.createTableAdapter(schemaName, false));
    }
    const ta = this.tableAdapters.get(key);

    if (mappingName) this.tableMapping[mappingName] = key;

    if (view) {
      await this.createView(ta, view, connection);
    } else {
      if (rowFormat && typeof ta.setRowFormat === "function")
        ta.setRowFormat(rowFormat.toUpperCase());
      if (attributes) ta.setAttributes(attributes);
      if (constraints) ta.setConstraints(constraints);

      if (this.autoCreate) {
        if (schema.truncate === true && ta.isCreated)
          await ta.dropTable(connection);
        if (!ta.isCreated) {
          await ta.createTable(connection);
          if (records && records.length > 0)
            await ta.initRecords(records, connection);
        } else {
          await ta.init(connection);
          await ta.syncTable(connection);
        }
      } else {
        await ta.init(connection);
      }
    }

    this.logger.initializing = false;
    return ta;
  }

  async createView(tableAdapter, view, connection) {
    this.logger.warn(`VIEW not supported for '${this.type}'`);
  }

  schema(tableName = "*") {
    const key = this.schemaKey(tableName);
    if (!this.tableAdapters.has(key)) {
      this.tableAdapters.set(
        key,
        this.createTableAdapter(tableName, tableName === "*"),
      );
    }
    return this.tableAdapters.get(key);
  }

  table(tableName = "*") {
    return this.schema(tableName);
  }
  query(sql, values, connection) {
    return this.executeQuery(sql, values, connection);
  }

  async task(callback) {
    const taskId = this.logger.taskStart();
    let client;
    try {
      client = await this.getConnection();
      return await callback(client);
    } catch (e) {
      this.logger.error("task error:", e.message);
      throw e;
    } finally {
      this.logger.taskEnd(taskId);
      if (client) this.releaseConnection(client);
    }
  }

  async transaction(callback) {
    const txId = this.logger.transactionStart();
    let client;
    try {
      client = await this.getConnection();
      await this.beginTransaction(client);
      try {
        const result = await callback(client);
        if (result === false) {
          await this.rollbackTransaction(client);
          this.logger.transactionEnd(txId, "rollback");
        } else {
          await this.commitTransaction(client);
          this.logger.transactionEnd(txId, "commit");
        }
        return result;
      } catch (e) {
        await this.rollbackTransaction(client);
        this.logger.transactionEnd(txId, "rollback");
        throw e;
      }
    } catch (e) {
      this.logger.error("transaction error:", e.message);
      throw e;
    } finally {
      if (client) this.releaseConnection(client);
    }
  }
}

export default BaseAdapter;
