import { MigrationError } from "../errors.js";

class MigrationRunner {
  constructor(adapter, options = {}) {
    this.adapter = adapter;
    this.historyTable = options.historyTable || "__migrations";
    this.migrations = (options.migrations || [])
      .slice()
      .sort((a, b) => a.version - b.version);
  }

  // ── Ensure history table exists ───────────────────────────────────────────

  async ensureHistoryTable(connection) {
    const isMysql = this.adapter.type === "mysql";
    const sql = isMysql
      ? `CREATE TABLE IF NOT EXISTS \`${this.historyTable}\` (
           version INT NOT NULL,
           description VARCHAR(255),
           applied_at DATETIME DEFAULT CURRENT_TIMESTAMP,
           checksum VARCHAR(64),
           PRIMARY KEY (version)
         )`
      : `CREATE TABLE IF NOT EXISTS "${this.historyTable}" (
           version INTEGER NOT NULL,
           description VARCHAR(255),
           applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           checksum VARCHAR(64),
           PRIMARY KEY (version)
         )`;
    await this.adapter.executeQuery(sql, [], connection);
  }

  async appliedVersions(connection) {
    const isMysql = this.adapter.type === "mysql";
    const sql = isMysql
      ? `SELECT version FROM \`${this.historyTable}\` ORDER BY version ASC`
      : `SELECT version FROM "${this.historyTable}" ORDER BY version ASC`;
    const rows = await this.adapter.executeQuery(sql, [], connection);
    const list = Array.isArray(rows) ? rows : rows.rows || [];
    return list.map((r) => r.version);
  }

  async recordApplied(migration, connection) {
    const isMysql = this.adapter.type === "mysql";
    const sql = isMysql
      ? `INSERT INTO \`${this.historyTable}\` (version, description) VALUES (?, ?)`
      : `INSERT INTO "${this.historyTable}" (version, description) VALUES ($1, $2)`;
    await this.adapter.executeQuery(
      sql,
      [migration.version, migration.description || ""],
      connection,
    );
  }

  async removeRecord(migration, connection) {
    const isMysql = this.adapter.type === "mysql";
    const sql = isMysql
      ? `DELETE FROM \`${this.historyTable}\` WHERE version = ?`
      : `DELETE FROM "${this.historyTable}" WHERE version = $1`;
    await this.adapter.executeQuery(sql, [migration.version], connection);
  }

  // ── Public API ────────────────────────────────────────────────────────────

  async migrate(target = "latest") {
    return this.adapter.task(async (connection) => {
      await this.ensureHistoryTable(connection);
      const applied = await this.appliedVersions(connection);
      const maxVersion = target === "latest" ? Infinity : target;

      const pending = this.migrations.filter(
        (m) => !applied.includes(m.version) && m.version <= maxVersion,
      );

      for (const migration of pending) {
        try {
          await migration.up(this.adapter, connection);
          await this.recordApplied(migration, connection);
          console.log(
            `[migration] applied v${migration.version}: ${migration.description || ""}`,
          );
        } catch (e) {
          throw new MigrationError(
            `Migration v${migration.version} failed: ${e.message}`,
            e,
          );
        }
      }

      return { applied: pending.map((m) => m.version) };
    });
  }

  async rollback(steps = 1) {
    return this.adapter.task(async (connection) => {
      await this.ensureHistoryTable(connection);
      const applied = await this.appliedVersions(connection);
      const toRollback = applied.slice(-steps).reverse();

      const rolledBack = [];
      for (const version of toRollback) {
        const migration = this.migrations.find((m) => m.version === version);
        if (!migration) {
          throw new MigrationError(`No migration found for version ${version}`);
        }
        try {
          await migration.down(this.adapter, connection);
          await this.removeRecord(migration, connection);
          console.log(
            `[migration] rolled back v${migration.version}: ${migration.description || ""}`,
          );
          rolledBack.push(version);
        } catch (e) {
          throw new MigrationError(
            `Rollback v${version} failed: ${e.message}`,
            e,
          );
        }
      }

      return { rolledBack };
    });
  }

  async status() {
    return this.adapter.task(async (connection) => {
      await this.ensureHistoryTable(connection);
      const applied = await this.appliedVersions(connection);
      return this.migrations.map((m) => ({
        version: m.version,
        description: m.description || "",
        status: applied.includes(m.version) ? "applied" : "pending",
      }));
    });
  }
}

export default MigrationRunner;
