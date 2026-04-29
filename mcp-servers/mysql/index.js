#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import mysql from 'mysql2/promise';

const pool = mysql.createPool({
  host:     process.env.DB_HOST     ?? 'localhost',
  port:     Number(process.env.DB_PORT ?? 3306),
  user:     process.env.DB_USER     ?? 'root',
  password: process.env.DB_PASSWORD ?? '',
  database: process.env.DB_NAME     ?? 'snowland',
  waitForConnections: true,
  connectionLimit: 3,
});

const server = new McpServer({ name: 'mysql-jobs', version: '0.1.0' });

// ── search_jobs ───────────────────────────────────────────────────────────────
server.tool('search_jobs',
  '搜索职位。支持按关键词、公司名、职位类型、工作地点过滤。返回匹配的职位列表（不含完整描述）。',
  {
    keyword:  z.string().optional().describe('搜索关键词，匹配职位名称或描述'),
    company:  z.string().optional().describe('公司名（模糊匹配）'),
    job_type: z.string().optional().describe('职位类型，如 实习、校招、社招'),
    location: z.string().optional().describe('工作地点，如 北京、上海'),
    limit:    z.number().optional().describe('返回数量，默认 10，最大 50'),
  },
  async ({ keyword, company, job_type, location, limit = 10 }) => {
    const max = Math.min(limit, 50);
    const conditions = ['is_valid != "false"'];
    const params = [];

    if (keyword) {
      conditions.push('(job_title LIKE ? OR job_description LIKE ?)');
      params.push(`%${keyword}%`, `%${keyword}%`);
    }
    if (company) {
      conditions.push('company_name LIKE ?');
      params.push(`%${company}%`);
    }
    if (job_type) {
      conditions.push('job_type LIKE ?');
      params.push(`%${job_type}%`);
    }
    if (location) {
      conditions.push('work_location LIKE ?');
      params.push(`%${location}%`);
    }

    params.push(max);
    const sql = `
      SELECT job_id, job_title, company_name, salary, work_location, job_type, degree, tags, publish_date
      FROM job_details
      WHERE ${conditions.join(' AND ')}
      ORDER BY created_at DESC
      LIMIT ?
    `;

    console.error(`[mysql-mcp] search_jobs: ${sql.replace(/\s+/g, ' ').trim()} | params: ${JSON.stringify(params)}`);
    const [rows] = await pool.query(sql, params);
    if (rows.length === 0) {
      return { content: [{ type: 'text', text: '未找到匹配的职位。' }] };
    }

    const text = rows.map(r => [
      `job_id: ${r.job_id}`,
      `职位: ${r.job_title} @ ${r.company_name}`,
      `类型: ${r.job_type ?? '-'}  地点: ${r.work_location ?? '-'}  学历: ${r.degree ?? '-'}`,
      `薪资: ${r.salary || '未披露'}`,
      `标签: ${(() => { try { return r.tags ? JSON.parse(r.tags).join(', ') : '-'; } catch { return r.tags ?? '-'; } })()}`,
      `发布日期: ${r.publish_date ?? '-'}`,
    ].join('\n')).join('\n\n---\n\n');

    return { content: [{ type: 'text', text: `共 ${rows.length} 条结果：\n\n${text}` }] };
  }
);

// ── get_job_detail ────────────────────────────────────────────────────────────
server.tool('get_job_detail',
  '根据 job_id 获取职位完整详情，包括职位描述全文。用于写手生成文案前获取完整信息。',
  {
    job_id: z.string().describe('职位 ID（从 search_jobs 结果中获取）'),
  },
  async ({ job_id }) => {
    const detailSql = `SELECT jd.*, c.industry, c.scale, c.type as company_type, c.logo_url FROM job_details jd LEFT JOIN companies c ON jd.company_id = c.company_id WHERE jd.job_id = ?`;
    console.error(`[mysql-mcp] get_job_detail: ${detailSql} | params: ${JSON.stringify([job_id])}`);
    const [rows] = await pool.query(detailSql, [job_id]);

    if (rows.length === 0) {
      return { content: [{ type: 'text', text: `未找到 job_id=${job_id} 的职位。` }] };
    }

    const r = rows[0];
    const tags = (() => { try { return r.tags ? JSON.parse(r.tags) : []; } catch { return []; } })();
    const text = [
      `【职位详情】`,
      `职位名称: ${r.job_title}`,
      `公司: ${r.company_name}${r.industry ? `（${r.industry}）` : ''}`,
      `公司规模: ${r.scale ?? '-'}  公司类型: ${r.company_type ?? '-'}`,
      `薪资: ${r.salary || '未披露'}`,
      `工作地点: ${r.work_location ?? '-'}`,
      `职位类型: ${r.job_type ?? '-'}`,
      `学历要求: ${r.degree ?? '-'}`,
      `技能标签: ${tags.join(', ') || '-'}`,
      `发布日期: ${r.publish_date ?? '-'}`,
      `截止日期: ${r.close_date ?? '-'}`,
      `原始链接: ${r.job_detail_url}`,
      ``,
      `【职位描述】`,
      r.job_description ?? '（无描述）',
    ].join('\n');

    return { content: [{ type: 'text', text }] };
  }
);

// ── list_jobs ─────────────────────────────────────────────────────────────────
server.tool('list_jobs',
  '列出最新职位，支持按类型、地点过滤。用于浏览近期职位。',
  {
    job_type: z.string().optional().describe('职位类型过滤，如 实习、校招、社招'),
    location: z.string().optional().describe('工作地点过滤'),
    limit:    z.number().optional().describe('返回数量，默认 20，最大 50'),
    offset:   z.number().optional().describe('跳过前 N 条，用于分页'),
  },
  async ({ job_type, location, limit = 20, offset = 0 }) => {
    const max = Math.min(limit, 50);
    const conditions = ['is_valid != "false"'];
    const params = [];

    if (job_type) { conditions.push('job_type LIKE ?'); params.push(`%${job_type}%`); }
    if (location) { conditions.push('work_location LIKE ?'); params.push(`%${location}%`); }

    params.push(max, offset);
    const listSql = `SELECT job_id, job_title, company_name, salary, work_location, job_type, degree, tags FROM job_details WHERE ${conditions.join(' AND ')} ORDER BY created_at DESC LIMIT ? OFFSET ?`;
    console.error(`[mysql-mcp] list_jobs: ${listSql} | params: ${JSON.stringify(params)}`);
    const [rows] = await pool.query(listSql, params);

    if (rows.length === 0) {
      return { content: [{ type: 'text', text: '暂无职位数据。' }] };
    }

    const text = rows.map(r =>
      `${r.job_id} | ${r.job_title} @ ${r.company_name} | ${r.job_type ?? '-'} | ${r.work_location ?? '-'} | ${r.salary || '薪资未披露'}`
    ).join('\n');

    return { content: [{ type: 'text', text }] };
  }
);

// ── start ─────────────────────────────────────────────────────────────────────
const transport = new StdioServerTransport();
await server.connect(transport);
console.error('[mysql-mcp] Server started');
