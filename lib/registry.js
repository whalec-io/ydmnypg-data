import { ConnectionNotFoundError } from "./errors.js";

class ConnectionRegistry {
  constructor() {
    this.connections = new Map();
  }

  add(key, connection) {
    if (this.connections.has(key)) return false;
    this.connections.set(key, connection);
    return true;
  }

  get(key) {
    if (!this.connections.has(key)) throw new ConnectionNotFoundError(key);
    return this.connections.get(key);
  }

  has(key) {
    return this.connections.has(key);
  }

  async remove(key) {
    const conn = this.connections.get(key);
    if (conn && typeof conn.destroy === "function") {
      await conn.destroy();
    }
    this.connections.delete(key);
  }

  keys() {
    return [...this.connections.keys()];
  }
}

// Default shared registry for application-level use
const defaultRegistry = new ConnectionRegistry();

export { ConnectionRegistry, defaultRegistry };
