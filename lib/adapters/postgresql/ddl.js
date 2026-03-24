function buildColumnDef(def, key, others) {
  const type = def.type ? def.type.toLowerCase() : null;
  let col = `"${key}" `;

  switch (type) {
    case "int":
    case "integer":
      col += !others.addColumn && def.autoIncrement ? "SERIAL" : "INTEGER";
      break;
    case "smallint":
    case "tinyint":
      col += "SMALLINT";
      break;
    case "mediumint":
      col += "INTEGER";
      break;
    case "bigint":
      col += !others.addColumn && def.autoIncrement ? "BIGSERIAL" : "BIGINT";
      break;
    case "float":
      col += "REAL";
      break;
    case "double":
      col += "DOUBLE PRECISION";
      break;
    case "bit":
      col += `BIT(${def.length || 1})`;
      break;
    case "timestamp":
    case "datetime":
      col += "TIMESTAMP";
      break;
    case "date":
      col += "DATE";
      break;
    case "time":
      col += "TIME";
      break;
    case "char":
      col += `CHAR(${def.length || 10})`;
      break;
    case "varchar":
      col += `VARCHAR(${def.length || 45})`;
      break;
    case "text":
    case "tinytext":
    case "mediumtext":
    case "longtext":
      col += "TEXT";
      break;
    case "json":
    case "jsonb":
      col += "JSONB";
      break;
    case "enum": {
      const enums = (def.enum || []).map((e) => `'${e}'`).join(", ");
      col += `VARCHAR(255) CHECK ("${key}" IN (${enums}))`;
      break;
    }
    default:
      throw new Error(
        `PostgreSQL: unsupported column type '${type}' on ${others.name}.${key}`,
      );
  }

  if (def.notNull === true) {
    col += " NOT NULL";
  } else if (def.notNull === false || def.defaultValue === "NULL") {
    col += " NULL";
  }

  if (def.defaultOnUpdate !== true && def.defaultValue !== undefined) {
    if (def.defaultValue === "CURRENT_TIMESTAMP") {
      col += " DEFAULT CURRENT_TIMESTAMP";
    } else if (def.defaultValue === "NULL") {
      col += " DEFAULT NULL";
    } else if (def.defaultValue === "UUID") {
      col += ` DEFAULT ''`;
    } else if (typeof def.defaultValue === "number") {
      col += ` DEFAULT ${def.defaultValue}`;
    } else {
      col += ` DEFAULT '${def.defaultValue}'`;
    }
  }

  if (def.unique) col += " UNIQUE";
  if (def.primaryKey) {
    others.primaryKey = others.primaryKey || [];
    others.primaryKey.push(key);
  }

  return col;
}

function buildCreateTable(tableName, attributes, constraints) {
  const others = { name: tableName };
  const columns = [];

  for (const [key, def] of Object.entries(attributes)) {
    columns.push(buildColumnDef(def, key, others));
  }

  for (const [key, def] of Object.entries(constraints || {})) {
    if (def.type === "unique") {
      columns.push(
        `CONSTRAINT "${key}" UNIQUE (${def.keys.map((k) => `"${k}"`).join(", ")})`,
      );
    }
  }

  if (others.primaryKey) {
    columns.push(
      `CONSTRAINT "PK_${others.primaryKey.join("_")}" PRIMARY KEY (${others.primaryKey.map((k) => `"${k}"`).join(", ")})`,
    );
  }

  if (columns.length === 0)
    throw new Error(`Table '${tableName}' has no attribute definitions`);

  return {
    sql: `CREATE TABLE "${tableName}" (${columns.join(", ")})`,
    constraints,
  };
}

// Parameterized query builders
function buildInsert(tableName, values) {
  const keys = Object.keys(values);
  const cols = keys.map((k) => `"${k}"`).join(", ");
  const params = keys.map((_, i) => `$${i + 1}`).join(", ");
  return {
    sql: `INSERT INTO "${tableName}" (${cols}) VALUES (${params}) RETURNING *`,
    values: keys.map((k) => values[k]),
  };
}

function buildUpdate(tableName, values, condition) {
  const valKeys = Object.keys(values);
  const condKeys = Object.keys(condition);
  let idx = 1;
  const setClauses = valKeys.map((k) => `"${k}" = $${idx++}`).join(", ");
  const whereClauses = condKeys.map((k) => `"${k}" = $${idx++}`).join(" AND ");
  return {
    sql: `UPDATE "${tableName}" SET ${setClauses} WHERE ${whereClauses}`,
    values: [
      ...valKeys.map((k) => values[k]),
      ...condKeys.map((k) => condition[k]),
    ],
  };
}

function buildDelete(tableName, condition) {
  const condKeys = Object.keys(condition);
  let idx = 1;
  const whereClauses = condKeys.map((k) => `"${k}" = $${idx++}`).join(" AND ");
  return {
    sql: `DELETE FROM "${tableName}" WHERE ${whereClauses}`,
    values: condKeys.map((k) => condition[k]),
  };
}

export {
  buildColumnDef,
  buildCreateTable,
  buildInsert,
  buildUpdate,
  buildDelete,
};
