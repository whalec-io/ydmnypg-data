function buildColumnDef(q, def, key, others) {
  const type = def.type ? def.type.toLowerCase() : null;
  if (q) q += ",";

  switch (type) {
    case "float":
    case "double": {
      const len = def.length || 11;
      const dec = def.decimals || 2;
      q += `${key} ${type}(${len}, ${dec})`;
      break;
    }
    case "int": {
      const len = def.length || 11;
      q += `${key} int(${len})`;
      if (def.unsigned) q += " UNSIGNED";
      if (others.addColumn !== true && def.autoIncrement)
        q += " AUTO_INCREMENT";
      break;
    }
    case "smallint": {
      const len = def.length || 6;
      q += `${key} smallint(${len})`;
      if (def.unsigned) q += " UNSIGNED";
      break;
    }
    case "mediumint": {
      const len = def.length || 7;
      q += `${key} mediumint(${len})`;
      if (def.unsigned) q += " UNSIGNED";
      break;
    }
    case "tinyint": {
      const len = def.length || 4;
      q += `${key} tinyint(${len})`;
      if (def.unsigned) q += " UNSIGNED";
      break;
    }
    case "bigint": {
      const len = def.length || 20;
      q += `${key} bigint(${len})`;
      if (def.unsigned) q += " UNSIGNED";
      if (others.addColumn !== true && def.autoIncrement)
        q += " AUTO_INCREMENT";
      break;
    }
    case "bit": {
      const len = def.length || 1;
      q += `${key} bit(${len})`;
      break;
    }
    case "timestamp":
    case "datetime":
      q += `${key} ${type}`;
      break;
    case "date":
    case "time":
    case "year":
      q += `${key} ${type}`;
      break;
    case "char": {
      const len = def.length || 10;
      q += `${key} char(${len})`;
      break;
    }
    case "varchar": {
      const len = def.length || 45;
      q += `${key} varchar(${len})`;
      break;
    }
    case "text":
    case "tinytext":
    case "mediumtext":
    case "longtext":
      q += `${key} ${type}`;
      break;
    case "json":
      q += `${key} json`;
      break;
    case "enum": {
      const enums = (def.enum || []).join("','");
      q += `${key} enum('${enums}')`;
      break;
    }
    default:
      throw new Error(
        `MySQL: unsupported column type '${type}' on ${others.name}.${key}`,
      );
  }

  if (def.notNull === true) {
    q += " NOT NULL";
  } else if (def.notNull === false || def.defaultValue === "NULL") {
    q += " NULL";
  }

  if (def.defaultOnUpdate === true) {
    q += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
  } else if (def.defaultValue !== undefined) {
    if (def.defaultValue === "CURRENT_TIMESTAMP") {
      q += " DEFAULT CURRENT_TIMESTAMP";
    } else if (def.defaultValue === "NULL") {
      q += " DEFAULT NULL";
    } else if (def.defaultValue === "UUID") {
      q += ` DEFAULT ''`;
    } else if (typeof def.defaultValue === "number") {
      q += ` DEFAULT ${def.defaultValue}`;
    } else {
      q += ` DEFAULT '${def.defaultValue}'`;
    }
  }

  if (def.unique) q += " UNIQUE";
  if (def.primaryKey) {
    others.primaryKey = others.primaryKey || [];
    others.primaryKey.push(key);
  }

  return q;
}

function buildCreateTable(tableName, attributes, constraints, rowFormat) {
  const others = { name: tableName };
  let statement = "";

  for (const [key, def] of Object.entries(attributes)) {
    statement = buildColumnDef(statement, def, key, others);
  }

  for (const [key, def] of Object.entries(constraints || {})) {
    if (def.type === "unique") {
      statement += `, CONSTRAINT ${key} UNIQUE (${def.keys.join(",")})`;
    } else if (def.type === "btree") {
      statement += `, KEY ${key} (${def.keys.join(",")}) USING BTREE`;
    } else if (def.type === "fulltext") {
      statement += `, FULLTEXT KEY ${key} (${def.keys.join(",")})`;
    }
  }

  if (others.primaryKey) {
    statement += `, CONSTRAINT PK_${others.primaryKey.join("_")} PRIMARY KEY (${others.primaryKey.join(",")})`;
  }

  if (!statement)
    throw new Error(`Table '${tableName}' has no attribute definitions`);

  const option = rowFormat ? ` ROW_FORMAT=${rowFormat}` : "";
  return `CREATE TABLE \`${tableName}\` (${statement})${option}`;
}

export { buildColumnDef, buildCreateTable };
