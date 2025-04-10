"use strict";

const { Client } = require("pg");
const fs = require("fs");
const groupsSqlInsert = fs.readFileSync("seedGroups.sql").toString();
const userGroupsSqlInsert = fs.readFileSync("seedUserGroups.sql").toString();
const usersSqlInsert = fs.readFileSync("seedUsers.sql").toString();
const salesSqlInsert = fs.readFileSync("seedSales.sql").toString();

const pgclient = new Client({
  host: "db",
  port: "5432",
  user: "user",
  password: "pass",
  database: "actifai",
});

pgclient.connect();

// Create tables
const createUsersTableQuery = `
    CREATE TABLE IF NOT EXISTS "users" (
	    "id" SERIAL,
	    "name" VARCHAR(50) NOT NULL,
	    "role" VARCHAR(50) NOT NULL,
	    PRIMARY KEY ("id")
    );`;

const createGroupsTableQuery = `
    CREATE TABLE IF NOT EXISTS "groups" (
	    "id" SERIAL,
	    "name" VARCHAR(50) NOT NULL,
	    PRIMARY KEY ("id")
    );`;

const createUserGroupsTableQuery = `
    CREATE TABLE IF NOT EXISTS "user_groups" (
	    "user_id" SERIAL,
	    "group_id" SERIAL,
	    FOREIGN KEY(user_id) REFERENCES users(id),
	    FOREIGN KEY(group_id) REFERENCES groups(id)
    );`;

const createSalesTableQuery = `
    CREATE TABLE IF NOT EXISTS "sales" (
      "id" SERIAL,
	    "user_id" SERIAL,
	    "amount" INTEGER,
	    "date" DATE,
	    FOREIGN KEY(user_id) REFERENCES users(id),
	    PRIMARY KEY ("id")
    );`;

const seedDatabase = async function () {
  const usersTableExistsResult = await pgclient.query(
    "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users');"
  );
  const usersTableExists = usersTableExistsResult.rows[0].exists;

  // Check if users table exists already. If so, we assume the seeders have already run successfully
  if (usersTableExists) {
    console.log("Skipping seeders.");
    pgclient.end();
    return;
  } else {
    console.log("Seeding database...");
  }

  await pgclient.query(createUsersTableQuery);
  console.log("Created users table.");

  await pgclient.query(usersSqlInsert);
  console.log("Seeded users table.");

  await pgclient.query(createGroupsTableQuery);
  console.log("Created groups table.");

  await pgclient.query(groupsSqlInsert);
  console.log("Seeded groups table.");

  await pgclient.query(createUserGroupsTableQuery);
  console.log("Created user_groups table.");

  await pgclient.query(userGroupsSqlInsert);
  console.log("Seeded user_group table.");

  await pgclient.query(createSalesTableQuery);
  console.log("Created sales table.");

  await pgclient.query(salesSqlInsert);
  console.log("Seeded sales table.");

  pgclient.end();
};

module.exports = {
  seedDatabase,
};
