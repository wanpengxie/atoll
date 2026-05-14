import { Router } from 'express';
import mysql from 'mysql2/promise';

const router = Router();

const dbConfig = {
  host: process.env.DB_HOST || 'bj-cynosdbmysql-grp-9812vsxq.sql.tencentcdb.com',
  port: parseInt(process.env.DB_PORT || '26210'),
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD,
  database: 'snowland',
  ssl: false,
  connectTimeout: 10000,
};

// GET /api/jobs?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
router.get('/', async (req, res) => {
  const { start_date, end_date } = req.query;

  if (!start_date || !end_date) {
    return res.status(400).json({ error: '请提供 start_date 和 end_date 参数（格式：YYYY-MM-DD）' });
  }

  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRegex.test(start_date) || !dateRegex.test(end_date)) {
    return res.status(400).json({ error: '日期格式错误，请使用 YYYY-MM-DD' });
  }

  let conn;
  try {
    conn = await mysql.createConnection(dbConfig);
    const [rows] = await conn.execute(
      `SELECT company_name, COUNT(*) AS job_count
       FROM job_details
       WHERE is_valid = 'valid'
         AND job_type = '校招'
         AND DATE(created_at) BETWEEN ? AND ?
       GROUP BY company_name
       ORDER BY job_count DESC`,
      [start_date, end_date]
    );

    const [countRow] = await conn.execute(
      `SELECT COUNT(*) AS total
       FROM job_details
       WHERE is_valid = 'valid'
         AND job_type = '校招'
         AND DATE(created_at) BETWEEN ? AND ?`,
      [start_date, end_date]
    );

    res.json({
      total: countRow[0].total,
      companies: rows.length,
      start_date,
      end_date,
      data: rows,
    });
  } catch (err) {
    console.error('[job-query] DB error:', err);
    res.status(500).json({ error: '数据库查询失败: ' + err.message });
  } finally {
    if (conn) await conn.end();
  }
});

export default router;
