import Connection from "./connection.js";
import Collection from "./collection.js";
import Design from "./design.js";
import Repository from "./repository.js";
import QueryBuilder from "./query-builder.js";
import { defaultRegistry, ConnectionRegistry } from "./registry.js";
import * as errors from "./errors.js";
import Migration from "./migration/migration.js";
import MigrationRunner from "./migration/runner.js";

class DAO {
  static Collection = Collection;
  static Design = Design;
  static Repository = Repository;
  static QueryBuilder = QueryBuilder;
  static Migration = Migration;
  static MigrationRunner = MigrationRunner;
  static ConnectionRegistry = ConnectionRegistry;
  static errors = errors;

  static connection(key) {
    return Connection.get(key);
  }

  static registerConnection(key, options = {}) {
    return Connection.register(key, options);
  }

  static registerConnections(connectionOptions) {
    return Connection.registerConnections(connectionOptions);
  }

  static unregisterConnection(key) {
    return defaultRegistry.remove(key);
  }

  static connectionKeys() {
    return defaultRegistry.keys();
  }
}

export default DAO;
