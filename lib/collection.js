class Collection {
  constructor({ dbConnection, connection }) {
    this.dbConnection = dbConnection;
    this.connection = connection;
  }

  get db() {
    return this.dbConnection ? this.dbConnection.client : null;
  }

  setConnection(connection) {
    this.connection = connection;
  }
}

export default Collection;
