import MysqlAdapter from "./adapters/mysql/adapter.js";
import PgAdapter from "./adapters/postgresql/adapter.js";
import { defaultRegistry } from "./registry.js";
import { SchemaError } from "./errors.js";

class Connection {
  constructor(key, options = {}) {
    this.key = key;
    this.database = options.database;
    this.options = options;
    this.adapterType = options.adapter;
    this.client = this.createAdapter(options.adapter, options);
  }

  get type() {
    return this.adapterType;
  }

  createAdapter(type, options) {
    switch (type) {
      case "mysql":
        return new MysqlAdapter(this, options);
      case "postgresql":
      case "postgres":
      case "pg":
        return new PgAdapter(this, options);
      default:
        throw new SchemaError(
          `Unknown adapter type: '${type}'. Use 'mysql' or 'postgresql'.`,
        );
    }
  }

  async initialize() {
    return this.client.preloadSchemas();
  }

  async destroy() {
    if (typeof this.client.destroy === "function") {
      await this.client.destroy();
    }
  }

  // ─── Static helpers that operate on the default shared registry ───

  static async register(key, options = {}, registry = defaultRegistry) {
    if (registry.has(key)) return;
    const conn = new Connection(key, options);
    registry.add(key, conn);
    await conn.initialize();
  }

  static async registerConnections(
    connectionOptions,
    registry = defaultRegistry,
  ) {
    const connections = [];
    for (const key of Object.keys(connectionOptions)) {
      if (!registry.has(key)) {
        const conn = new Connection(key, connectionOptions[key]);
        registry.add(key, conn);
        connections.push(conn);
      }
    }
    // Initialize in parallel
    await Promise.all(connections.map((c) => c.initialize()));
  }

  static get(key, registry = defaultRegistry) {
    return registry.get(key); // throws ConnectionNotFoundError if missing
  }
}

export default Connection;
