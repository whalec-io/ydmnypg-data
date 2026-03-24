class DaoError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
    if (Error.captureStackTrace)
      Error.captureStackTrace(this, this.constructor);
  }
}

class ConnectionNotFoundError extends DaoError {
  constructor(key) {
    super(
      `Connection '${key}' is not registered. Call DAO.registerConnection('${key}', options) first.`,
    );
    this.key = key;
  }
}

class ConnectionFailedError extends DaoError {
  constructor(message, cause) {
    super(message);
    this.cause = cause;
  }
}

class QueryError extends DaoError {
  constructor(message, sql, cause) {
    super(message);
    this.sql = sql;
    this.cause = cause;
  }
}

class ValidationError extends DaoError {
  constructor(message, fields) {
    super(message);
    this.fields = fields || {};
  }
}

class MigrationError extends DaoError {
  constructor(message, cause) {
    super(message);
    this.cause = cause;
  }
}

class SchemaError extends DaoError {}

export {
  DaoError,
  ConnectionNotFoundError,
  ConnectionFailedError,
  QueryError,
  ValidationError,
  MigrationError,
  SchemaError,
};
