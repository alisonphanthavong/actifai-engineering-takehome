"use strict";

const express = require("express");
const seeder = require("./seed");
const { Client } = require("pg");

const pgclient = new Client({
  host: "db",
  port: "5432",
  user: "user",
  password: "pass",
  database: "actifai",
});

// Constants
const PORT = 3000;
const HOST = "0.0.0.0";

async function start() {
  // Seed the database
  await seeder.seedDatabase();

  // Connect the database
  pgclient.connect();

  // App
  const app = express();

  // Health check
  app.get("/health", (req, res) => {
    res.send("Hello World");
  });

  // Write your endpoints here

  // This endpoint returns average revenue and total revenue by user and group for a given month, with options to sort by avg or total revenue
  // Potential optimization: add limit and offset to paginate large datasets
  // Extension idea: fetch avg revenue and total revenue by user and group for a custom timeframe
  app.get("/sales", async (req, res) => {
    try {
      const {
        date,
        group_by = "user",
        sort_by = "total_revenue",
        sort_order = "desc",
      } = req.query;

      // YYYY-MM format with valid months
      const dateFormat = /^\d{4}-(0[1-9]|1[0-2])$/;
      if (!dateFormat.test(date)) {
        return res.status(400).json({
          error:
            "Invalid date format. Please use 'YYYY-MM' format with valid months (01 to 12).",
        });
      }

      // Validate group_by, sort_by and sort_order parameters
      const validGroupBy = ["user", "group"];
      const validSortBy = ["total_revenue", "avg_revenue"];
      const validSortOrder = ["asc", "desc"];

      if (!validGroupBy.includes(group_by)) {
        return res.status(400).json({
          error: `Invalid group_by value. Valid options are 'user' or 'group'.`,
        });
      }
      if (!validSortBy.includes(sort_by)) {
        return res.status(400).json({
          error: `Invalid sort_by value. Valid options are 'total_revenue' or 'avg_revenue'.`,
        });
      }
      if (!validSortOrder.includes(sort_order)) {
        return res.status(400).json({
          error: `Invalid sort_order value. Valid options are 'asc' or 'desc'.`,
        });
      }

      // Convert dates
      const startDate = new Date(`${date}-01`);
      const endDate = new Date(`${date}-01`);
      endDate.setMonth(endDate.getMonth() + 1);

      let query = "";
      if (group_by === "user") {
        query = `
          SELECT
            u.id AS user_id,
            u.name AS user_name,
            TO_CHAR($1::date, 'YYYY-MM') AS month,
            COUNT(s.id) AS num_sales,
            SUM(s.amount) AS total_revenue,
            AVG(s.amount) AS avg_revenue
          FROM users u
          JOIN sales s ON s.user_id = u.id
          WHERE s.date >= $1 AND s.date < $2
          GROUP BY u.id, u.name, month
          ORDER BY ${sort_by} ${sort_order};
        `;
      } else if (group_by === "group") {
        query = `
          SELECT
            g.id AS group_id,
            g.name AS group_name,
            TO_CHAR($1::date, 'YYYY-MM') AS month,
            COUNT(s.id) AS num_sales,
            SUM(s.amount) AS total_revenue,
            AVG(s.amount) AS avg_revenue
          FROM groups g
          JOIN user_groups ug ON g.id = ug.group_id
          JOIN users u ON ug.user_id = u.id
          JOIN sales s ON s.user_id = u.id
          WHERE s.date >= $1 AND s.date < $2
          GROUP BY g.id, g.name, month
          ORDER BY ${sort_by} ${sort_order};
        `;
      }

      // Run query
      const result = await pgclient.query(query, [startDate, endDate]);

      if (result.rows.length === 0) {
        return res.status(404).json({
          error: "No sales data found for the given parameters.",
        });
      }

      return res.json(result.rows);
    } catch (err) {
      console.error("Error querying sales:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  // This endpoint would be useful to fetch historical monthly sales data for specific users or groups
  // Potential optimizations: add limit and offset to paginate large datasets, validate so that date range cannot exceed a specific range (12 months)
  // Extension ideas: add interval parameters for day, week, and year, accept username or group name as parameters instead of ids
  app.get("/sales/trends", async (req, res) => {
    try {
      const { user_id, group_id, start_date, end_date } = req.query;

      if (!start_date || !end_date) {
        return res.status(400).json({
          error:
            "Please provide both start_date and end_date parameters in 'YYYY-MM' format.",
        });
      }

      // YYYY-MM format with valid months
      const dateFormat = /^\d{4}-(0[1-9]|1[0-2])$/;
      if (!dateFormat.test(start_date) || !dateFormat.test(end_date)) {
        return res.status(400).json({
          error:
            "Invalid date format. Please use 'YYYY-MM' format with valid months (01 to 12).",
        });
      }

      // Convert dates
      const startDate = new Date(`${start_date}-01`);
      const endDate = new Date(`${end_date}-01`);
      endDate.setMonth(endDate.getMonth() + 1);

      if (startDate > endDate) {
        return res.status(400).json({
          error:
            "Invalid date range. Start date must be before or equal to end date.",
        });
      }

      let query = `
        SELECT
          u.id AS user_id,
          u.name AS user_name,
          g.id AS group_id,
          g.name AS group_name,
          TO_CHAR(DATE_TRUNC('month', s.date), 'YYYY-MM') AS period,
          COUNT(s.id) AS num_sales,
          SUM(s.amount) AS total_revenue,
          AVG(s.amount) AS avg_revenue
        FROM sales s
        JOIN users u ON s.user_id = u.id
        LEFT JOIN user_groups ug ON u.id = ug.user_id
        LEFT JOIN groups g ON ug.group_id = g.id
        WHERE s.date >= $1 AND s.date < $2
      `;

      const values = [startDate, endDate];

      // Validate user_id and group_id
      if (user_id && !Number.isInteger(Number(user_id))) {
        return res.status(400).json({
          error: "Invalid user_id. Must be an integer.",
        });
      }

      if (group_id && !Number.isInteger(Number(group_id))) {
        return res.status(400).json({
          error: "Invalid group_id. Must be an integer.",
        });
      }

      // Add filters based on user_id and group_id
      if (user_id) {
        query += ` AND u.id = $3`;
        values.push(Number(user_id));
      }

      if (group_id) {
        const groupParamIndex = user_id ? 4 : 3;
        query += ` AND g.id = $${groupParamIndex}`;
        values.push(Number(group_id));
      }

      query += `
        GROUP BY u.id, u.name, g.id, g.name, period
        ORDER BY period;
      `;

      const result = await pgclient.query(query, values);

      if (result.rows.length === 0) {
        return res.status(404).json({
          error: "No sales data found for the given parameters.",
        });
      }

      return res.json(result.rows);
    } catch (err) {
      console.error("Error querying sales:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  // Additional business metrics that would be useful to track: number of calls to calculate conversion rates (num_sales/num_calls) per user and average conversion rates for groups, sales streaks for longest consecutive number of days with at least 1 sale to incentivize users (gamification in general), client data (repeat sales, dropped sales)

  app.listen(PORT, HOST);
  console.log(`Server is running on http://${HOST}:${PORT}`);
}

start();
