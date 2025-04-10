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

pgclient.connect();

// Constants
const PORT = 3000;
const HOST = "0.0.0.0";

async function start() {
  // Seed the database
  await seeder.seedDatabase();

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
        month,
        year,
        group_by = "user",
        sort_by = "total_revenue",
        sort_order = "desc",
      } = req.query;
  
      if (!month || !year) {
        return res.status(400).json({
          error: "Please provide both month and year parameters.",
        });
      }

      // Validate month
      const monthInt = parseInt(month, 10);
      if (isNaN(monthInt) || monthInt < 1 || monthInt > 12) {
        return res.status(400).json({
          error: "Invalid month. Month must be a number between 1 and 12.",
        });
      }
  
      // Validate sort_by and sort_order parameters
      const validSortBy = ["total_revenue", "avg_revenue"];
      const validSortOrder = ["asc", "desc"];
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
      
      // Convert month and year into start and end dates for the month
      const startDate = new Date(year, month - 1);
      const endDate = new Date(startDate);
      endDate.setMonth(endDate.getMonth() + 1);
      endDate.setDate(0);
  
      let query = "";
      if (group_by === "user") {
        query = `
          SELECT
            u.id AS user_id,
            u.name AS user_name,
            DATE_TRUNC('month', s.date) AS period,
            COUNT(s.id) AS num_sales,
            SUM(s.amount) AS total_revenue,
            AVG(s.amount) AS avg_revenue
          FROM users u
          JOIN sales s ON s.user_id = u.id
          WHERE s.date BETWEEN $1 AND $2
          GROUP BY u.id, u.name, period
          ORDER BY ${sort_by} ${sort_order};
        `;
      } else if (group_by === "group") {
        query = `
          SELECT
            g.id AS group_id,
            g.name AS group_name,
            DATE_TRUNC('month', s.date) AS period,
            COUNT(s.id) AS num_sales,
            SUM(s.amount) AS total_revenue,
            AVG(s.amount) AS avg_revenue
          FROM groups g
          JOIN user_groups ug ON g.id = ug.group_id
          JOIN users u ON ug.user_id = u.id
          JOIN sales s ON s.user_id = u.id
          WHERE s.date BETWEEN $1 AND $2
          GROUP BY g.id, g.name, period
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
  
      res.json(result.rows);
  
    } catch (err) {
      console.error("Error querying sales:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });  

  // This endpoint would be useful to fetch historical monthly sales data for specific users or groups
  // Potential optimizations: add limit and offset to paginate large datasets, validate so that date range cannot exceed a specific range (12 months)
  // Extension idea: add interval parameters for day, week, and year
  app.get("/sales/trends", async (req, res) => {
    try {
      const { user_id, group_id, start_date, end_date } = req.query;
  
      if (!start_date || !end_date) {
        return res.status(400).json({
          error: "Please provide both start_date and end_date parameters in 'YYYY-MM' format.",
        });
      }
  
      // YYYY-MM format with valid months
      const dateFormat = /^\d{4}-(0[1-9]|1[0-2])$/;
      if (!dateFormat.test(start_date) || !dateFormat.test(end_date)) {
        return res.status(400).json({
          error: "Invalid date format. Please use 'YYYY-MM' format with valid months (01 to 12).",
        });
      }

      const startDate = new Date(`${start_date}-01`);
      const endDate = new Date(`${end_date}-01`);
      endDate.setMonth(endDate.getMonth() + 1);
      endDate.setDate(0);

      if (startDate > endDate) {
        return res.status(400).json({
          error: "Invalid date range. Start date must be before or equal to end date.",
        });
      }
  
      let query = `
        SELECT
          u.id AS user_id,
          u.name AS user_name,
          g.id AS group_id,
          g.name AS group_name,
          DATE_TRUNC('month', s.date) AS period, -- Group by month
          COUNT(s.id) AS num_sales,
          SUM(s.amount) AS total_revenue,
          AVG(s.amount) AS avg_revenue
        FROM sales s
        JOIN users u ON s.user_id = u.id
        LEFT JOIN user_groups ug ON u.id = ug.user_id
        LEFT JOIN groups g ON ug.group_id = g.id
        WHERE s.date BETWEEN $1 AND $2
      `;
      
      let values = [startDate, endDate];
  
      // Add filters based on user_id and group_id
      if (user_id) {
        query += ` AND u.id = $3`;
        values.push(user_id);
      }
      
      if (group_id) {
        // Placeholder for group_id should depend on values length
        const groupParamIndex = user_id ? 4 : 3; 
        query += ` AND g.id = $${groupParamIndex}`;
        values.push(group_id);
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
  
      res.json(result.rows);

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
