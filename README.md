# @ydmnypg/data

Database DAO layer for MySQL and PostgreSQL with schema management, MyBatis XML mapper support, and migrations.

## Installation

```bash
npm install @ydmnypg/data
```

## Quick Start

```js
import DAO from '@ydmnypg/data';

// Register a connection
await DAO.registerConnection('mysql-connection', {
  adapter: 'mysql',
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: 'secret',
  database: 'mydb',
});

// Use the connection
const conn = DAO.connection('mysql-connection');
await conn.client.transaction(async (client) => {
  await conn.client.table('users').insert({ name: 'Alice' }, client);
});
```

## Supported Databases

| Adapter | Value |
|---------|-------|
| MySQL | `'mysql'` |
| PostgreSQL | `'postgresql'`, `'postgres'`, or `'pg'` |

## Design (Schema Management)

Define table schemas and let the framework handle creation and synchronization:

```js
const design = new DAO.Design({
  dbConnection: 'mysql-connection',
  mappers: ['path/to/mappers/user.xml'],
  schema: {
    name: 'users',
    default: true,
    attributes: {
      id: { type: 'int', length: 11, autoIncrement: true, primaryKey: true },
      name: { type: 'varchar', length: 100, notNull: true },
      email: { type: 'varchar', length: 200, notNull: true, unique: true },
      status: { type: 'tinyint', length: 1, defaultValue: 1 },
      metadata: { type: 'json', json: true },
      created_at: { type: 'datetime', notNull: true, defaultValue: 'CURRENT_TIMESTAMP' },
      updated_at: { type: 'datetime', notNull: true, defaultOnUpdate: true },
    },
    constraints: {
      idx_status: { type: 'btree', keys: ['status'] },
      uq_email: { type: 'unique', keys: ['email'] },
    },
  },
});

// Initialize (auto-create tables if needed)
await design.initialize({ autoCreate: true });
```

### Supported Column Types

`int`, `bigint`, `smallint`, `tinyint`, `mediumint`, `float`, `double`, `bit`, `varchar`, `char`, `text`, `tinytext`, `mediumtext`, `longtext`, `json`, `jsonb`, `datetime`, `timestamp`, `date`, `time`, `year`, `enum`, `boolean`

### Attribute Options

| Option | Type | Description |
|--------|------|-------------|
| `type` | `string` | Column type |
| `length` | `number` | Column length |
| `primaryKey` | `boolean` | Mark as primary key |
| `autoIncrement` | `boolean` | Auto-increment |
| `notNull` | `boolean` | NOT NULL constraint |
| `unique` | `boolean` | UNIQUE constraint |
| `defaultValue` | `any` | Default value (`'CURRENT_TIMESTAMP'`, `'UUID'`, `'NULL'`, or literal) |
| `defaultOnUpdate` | `boolean` | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP |
| `json` | `boolean` | Auto-serialize/deserialize JSON |
| `hidden` | `boolean` | Exclude from Swagger docs |
| `triggerKey` | `string` | Auto-update timestamp when this field changes |

## Repository

Base class for data access with lifecycle hooks:

```js
class UserRepository extends DAO.Repository {
  get tableName() { return 'users'; }

  async beforeInsert(data) {
    data.id = crypto.randomUUID();
    return data;
  }
}

const repo = new UserRepository('mysql-connection');

await repo.insert({ name: 'Alice', email: 'alice@example.com' });
await repo.update({ id: '...' }, { name: 'Bob' });
await repo.delete({ id: '...' });
await repo.execute('users.findByEmail', { email: 'alice@example.com' });

await repo.transaction(async (conn) => {
  await repo.insert(data1, { connection: conn });
  await repo.insert(data2, { connection: conn });
});
```

### Lifecycle Hooks

| Hook | Description |
|------|-------------|
| `beforeInsert(data)` | Transform data before insert |
| `afterInsert(result)` | Called after insert |
| `beforeUpdate(data, condition)` | Transform data before update |
| `afterUpdate(result)` | Called after update |
| `beforeDelete(condition)` | Return `false` to cancel |
| `afterDelete(result)` | Called after delete |

## QueryBuilder

Chainable SQL builder with parameterized queries:

```js
const qb = new DAO.QueryBuilder('mysql');

const { sql, values } = qb
  .from('users')
  .select('id', 'name', 'email')
  .where({ status: 1 })
  .whereNotNull('email')
  .orderBy('created_at', 'DESC')
  .limit(20)
  .offset(0)
  .build();

// Execute directly
const rows = await qb.find(adapter, connection);
const count = await qb.count(adapter, connection);
```

## Migrations

```js
import DAO from '@ydmnypg/data';

class CreateUsers extends DAO.Migration {
  get version() { return 1; }
  get description() { return 'create users table'; }

  async up(db, connection) {
    await db.executeQuery(
      `CREATE TABLE users (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100))`,
      [], connection
    );
  }

  async down(db, connection) {
    await db.executeQuery('DROP TABLE users', [], connection);
  }
}

const conn = DAO.connection('mysql-connection');
const runner = new DAO.MigrationRunner(conn.client, {
  migrations: [new CreateUsers()],
});

await runner.migrate('latest');   // Apply pending migrations
await runner.rollback(1);         // Roll back last migration
const status = await runner.status();
```

## Connection Management

```js
// Register multiple connections
await DAO.registerConnections({
  'mysql-connection': { adapter: 'mysql', host: '...', database: '...' },
  'pg-connection': { adapter: 'postgresql', host: '...', database: '...' },
});

// List registered keys
DAO.connectionKeys();  // ['mysql-connection', 'pg-connection']

// Remove a connection
await DAO.unregisterConnection('mysql-connection');
```

## Debug Logging

```bash
DEBUG=dao:mysql:debug    # MySQL adapter logs
DEBUG=dao:postgresql:debug  # PostgreSQL adapter logs
DEBUG=dao:*              # All adapter logs
```

## Exports

```js
import DAO from '@ydmnypg/data';

DAO.Collection        // Execution context for task/transaction
DAO.Design            // Schema management
DAO.Repository        // Base repository class
DAO.QueryBuilder      // SQL query builder
DAO.Migration         // Base migration class
DAO.MigrationRunner   // Migration runner
DAO.ConnectionRegistry // Connection registry
DAO.errors            // { DaoError, ConnectionNotFoundError, QueryError, ... }
```

## Requirements

- Node.js >= 18.0.0

## License

MIT
