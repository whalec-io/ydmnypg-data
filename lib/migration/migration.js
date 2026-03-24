class Migration {
  get version() {
    throw new Error(`${this.constructor.name} must define version`);
  }

  get description() {
    return "";
  }

  async up(db, connection) {
    throw new Error(`${this.constructor.name}.up() not implemented`);
  }

  async down(db, connection) {
    throw new Error(`${this.constructor.name}.down() not implemented`);
  }
}

export default Migration;
