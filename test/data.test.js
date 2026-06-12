import assert from "node:assert/strict";
import test from "node:test";
import DAO from "../index.js";

test("exports data layer primitives", () => {
  assert.equal(typeof DAO.Design, "function");
  assert.equal(typeof DAO.Repository, "function");
  assert.equal(typeof DAO.QueryBuilder, "function");
});

test("builds parameterized mysql queries", () => {
  const query = new DAO.QueryBuilder("mysql")
    .from("users")
    .select("id", "email")
    .where({ status: 1 })
    .orderBy("id", "DESC")
    .limit(10)
    .build();

  assert.equal(query.sql, "SELECT `id`, `email` FROM `users` WHERE `status` = ? ORDER BY `id` DESC LIMIT 10");
  assert.deepEqual(query.values, [1]);
});

test("builds parameterized postgresql queries", () => {
  const query = new DAO.QueryBuilder("postgresql").from("users").where({ id: 7 }).build();

  assert.equal(query.sql, 'SELECT * FROM "users" WHERE "id" = $1');
  assert.deepEqual(query.values, [7]);
});
