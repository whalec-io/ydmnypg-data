import Connection from "./connection.js";
import Collection from "./collection.js";
import { defaultRegistry } from "./registry.js";

class Design {
  constructor(options = {}) {
    const { dbConnection, registry } = options;
    const reg = registry || defaultRegistry;

    this.dbConnection =
      typeof dbConnection === "string"
        ? Connection.get(dbConnection, reg)
        : dbConnection;

    this.options = options;
    this.schemas = this.normalizeSchemas(options);
    this.defaultSchema = this.findDefaultSchema(this.schemas);
  }

  get db() {
    return this.dbConnection ? this.dbConnection.client : null;
  }

  get connection() {
    return null;
  }

  normalizeSchemas({ schema, schemas }) {
    if (Array.isArray(schemas)) return schemas;
    if (!schema) return [];
    const arr = Array.isArray(schema) ? schema : [schema];
    if (arr.length === 1) arr[0].default = true;
    return arr;
  }

  findDefaultSchema(schemas) {
    if (schemas.length === 0) return {};
    return schemas.find((s) => s.default === true) || schemas[0];
  }

  async initialize(overrides = {}) {
    if (!this.db) return false;

    this.db.initialize({ ...this.options, ...overrides });

    if (this.schemas.length > 0) {
      return this.db.loadSchemas(this, this.schemas);
    }
    return true;
  }

  createCollection(connection) {
    return new Collection({ dbConnection: this.dbConnection, connection });
  }
}

export default Design;
