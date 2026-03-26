import { Hono } from 'hono';
import { cors } from 'hono/cors';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ENGINE_RUNTIME: Fetcher;
  SHARED_BRAIN: Fetcher;
  ECHO_API_KEY?: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uid(): string { return crypto.randomUUID().replace(/-/g, '').slice(0, 16); }
function nowISO(): string { return new Date().toISOString().replace('T', ' ').replace('Z', ''); }

function log(level: string, msg: string, extra: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, msg, service: 'echo-project-manager', ...extra }));
}

function ok(data: Record<string, unknown> = {}): Response {
  return Response.json({ ok: true, ...data });
}

function fail(error: string, status = 400): Response {
  return Response.json({ ok: false, error }, { status });
}

function requireBody(body: unknown, ...fields: string[]): string | null {
  if (!body || typeof body !== 'object') return 'Missing request body';
  for (const f of fields) {
    if ((body as Record<string, unknown>)[f] === undefined || (body as Record<string, unknown>)[f] === null) {
      return `Missing required field: ${f}`;
    }
  }
  return null;
}

function parsePagination(url: URL): { limit: number; offset: number } {
  const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') || '50', 10) || 50, 1), 200);
  const offset = Math.max(parseInt(url.searchParams.get('offset') || '0', 10) || 0, 0);
  return { limit, offset };
}

// ---------------------------------------------------------------------------
// Rate Limiting (KV-based sliding window)
// ---------------------------------------------------------------------------

interface RLState { c: number; t: number }

async function checkRateLimit(kv: KVNamespace, key: string, limit: number, windowSec: number): Promise<{ allowed: boolean; remaining: number; reset: number }> {
  const rlKey = `rl:${key}`;
  const now = Math.floor(Date.now() / 1000);
  const raw = await kv.get(rlKey, 'json') as RLState | null;

  let count: number;
  let windowStart: number;

  if (!raw || (now - raw.t) >= windowSec) {
    count = 1;
    windowStart = now;
  } else {
    const elapsed = now - raw.t;
    const decay = Math.max(0, 1 - elapsed / windowSec);
    count = Math.floor(raw.c * decay) + 1;
    windowStart = raw.t;
  }

  const allowed = count <= limit;
  await kv.put(rlKey, JSON.stringify({ c: count, t: windowStart } as RLState), { expirationTtl: windowSec * 2 });

  return { allowed, remaining: Math.max(0, limit - count), reset: windowSec - (now - windowStart) };
}

// ---------------------------------------------------------------------------
// Input Sanitization
// ---------------------------------------------------------------------------

function sanitize(input: string, maxLen = 2000): string {
  if (typeof input !== 'string') return '';
  return input.slice(0, maxLen).replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
}

function sanitizeBody<T extends Record<string, unknown>>(body: T, maxLen = 2000): T {
  const clean = { ...body };
  for (const [k, v] of Object.entries(clean)) {
    if (typeof v === 'string') (clean as Record<string, unknown>)[k] = sanitize(v, maxLen);
  }
  return clean;
}

function parseJson(raw: string | null, fallback: unknown = null): unknown {
  if (!raw) return fallback;
  try { return JSON.parse(raw); } catch { return fallback; }
}

async function logActivity(db: D1Database, workspaceId: string, action: string, opts: { projectId?: string; taskId?: string; memberId?: string; details?: string } = {}): Promise<void> {
  try {
    await db.prepare(
      "INSERT INTO activity_log (workspace_id, project_id, task_id, member_id, action, details, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).bind(workspaceId, opts.projectId || null, opts.taskId || null, opts.memberId || null, action, opts.details || null, nowISO()).run();
  } catch { /* best effort */ }
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

const app = new Hono<{ Bindings: Env }>();

app.use('*', cors({
  origin: ['https://echo-ept.com', 'https://www.echo-ept.com', 'https://echo-op.com', 'http://localhost:3000'],
  allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowHeaders: ['Content-Type', 'Authorization', 'X-Echo-API-Key'],
  maxAge: 86400,
}));

// Rate limiting middleware — 60 req/min per IP for writes, 200 req/min for reads
app.use('*', async (c, next) => {
  const path = new URL(c.req.url).pathname;
  if (path === '/health' || path === '/' || path === '/status') return next();
  const ip = c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || 'unknown';
  const isWrite = ['POST', 'PUT', 'DELETE'].includes(c.req.method);
  const limit = isWrite ? 60 : 200;
  const rlKey = `pm:${ip}:${isWrite ? 'w' : 'r'}`;
  const { allowed, remaining, reset } = await checkRateLimit(c.env.CACHE, rlKey, limit, 60);
  c.header('X-RateLimit-Limit', String(limit));
  c.header('X-RateLimit-Remaining', String(remaining));
  c.header('X-RateLimit-Reset', String(reset));
  if (!allowed) {
    log('warn', 'Rate limited', { ip, path, method: c.req.method });
    return c.json({ ok: false, error: 'Rate limit exceeded. Try again shortly.' }, 429);
  }
  return next();
});

// ── Auth Middleware — writes require API key ─────────────────────
app.use('*', async (c, next) => {
  const method = c.req.method;
  const path = new URL(c.req.url).pathname;
  if (method === 'GET' || method === 'OPTIONS' || method === 'HEAD' || path === '/health' || path === '/status') return next();
  const apiKey = c.req.header('X-Echo-API-Key') || '';
  const bearer = (c.req.header('Authorization') || '').replace('Bearer ', '');
  const expected = c.env.ECHO_API_KEY;
  if (!expected || (apiKey !== expected && bearer !== expected)) {
    return c.json({ ok: false, error: 'Unauthorized — X-Echo-API-Key or Bearer token required' }, 401);
  }
  return next();
});

// =========================================================================
// HEALTH & STATUS
// =========================================================================

app.get('/', (c) => c.json({ service: 'echo-project-manager', version: '1.0.0', status: 'operational' }));

app.get('/health', async (c) => {
  let dbOk = false;
  try { const r = await c.env.DB.prepare("SELECT 1 AS ping").first(); dbOk = r?.ping === 1; } catch { /* */ }
  return ok({ service: 'echo-project-manager', version: '1.0.0', d1: dbOk ? 'connected' : 'offline', ts: new Date().toISOString() });
});

app.get('/status', async (c) => {
  const [ws, proj, tasks, members, sprints] = await Promise.all([
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM workspaces").first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM projects WHERE status = 'active'").first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM tasks WHERE status != 'done'").first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM members WHERE is_active = 1").first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM sprints WHERE status = 'active'").first<{ cnt: number }>(),
  ]);
  return ok({
    version: '1.0.0', endpoints: 65,
    modules: ['workspaces', 'projects', 'tasks', 'sprints', 'members', 'time_tracking', 'milestones', 'comments', 'labels', 'templates', 'notifications', 'ai', 'activity'],
    overview: { workspaces: ws?.cnt ?? 0, active_projects: proj?.cnt ?? 0, open_tasks: tasks?.cnt ?? 0, active_members: members?.cnt ?? 0, active_sprints: sprints?.cnt ?? 0 },
  });
});

// =========================================================================
// WORKSPACES
// =========================================================================

app.get('/workspaces', async (c) => {
  const { results } = await c.env.DB.prepare("SELECT * FROM workspaces ORDER BY created_at DESC").all();
  return ok({ workspaces: (results || []).map(w => ({ ...w, settings: parseJson((w as Record<string, string>).settings, {}) })) });
});

app.post('/workspaces', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'name', 'owner_id');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  const slug = String(b.name).toLowerCase().replace(/[^a-z0-9]+/g, '-').slice(0, 50);
  await c.env.DB.prepare(
    "INSERT INTO workspaces (id, name, slug, description, owner_id, settings, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).bind(id, b.name, slug, b.description || null, b.owner_id, JSON.stringify(b.settings || {}), nowISO()).run();
  log('info', 'Workspace created', { id, name: b.name });
  return ok({ id, slug, message: 'Workspace created' });
});

app.get('/workspaces/:id', async (c) => {
  const ws = await c.env.DB.prepare("SELECT * FROM workspaces WHERE id = ?").bind(c.req.param('id')).first();
  if (!ws) return fail('Workspace not found', 404);
  return ok({ workspace: { ...ws, settings: parseJson((ws as Record<string, string>).settings, {}) } });
});

// =========================================================================
// PROJECTS
// =========================================================================

app.get('/projects', async (c) => {
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);
  const wsId = url.searchParams.get('workspace_id');
  const status = url.searchParams.get('status');
  let query = "SELECT * FROM projects";
  const conditions: string[] = [];
  const params: unknown[] = [];
  if (wsId) { conditions.push("workspace_id = ?"); params.push(wsId); }
  if (status) { conditions.push("status = ?"); params.push(status); }
  if (conditions.length) query += " WHERE " + conditions.join(" AND ");
  query += " ORDER BY updated_at DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);
  const { results } = await c.env.DB.prepare(query).bind(...params).all();
  return ok({ projects: results || [], limit, offset });
});

app.post('/projects', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'workspace_id', 'name');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO projects (id, workspace_id, name, description, status, priority, start_date, target_date, budget_hours, settings, created_at, updated_at) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)"
  ).bind(id, b.workspace_id, b.name, b.description || null, b.priority || 'medium', b.start_date || null, b.target_date || null, b.budget_hours || null, JSON.stringify(b.settings || {}), nowISO(), nowISO()).run();
  await logActivity(c.env.DB, String(b.workspace_id), 'project_created', { projectId: id, details: String(b.name) });
  log('info', 'Project created', { id, name: b.name });
  return ok({ id, message: 'Project created' });
});

app.get('/projects/:id', async (c) => {
  const project = await c.env.DB.prepare("SELECT * FROM projects WHERE id = ?").bind(c.req.param('id')).first();
  if (!project) return fail('Project not found', 404);
  const [taskStats, milestones, activeSprint] = await Promise.all([
    c.env.DB.prepare("SELECT status, COUNT(*) AS cnt FROM tasks WHERE project_id = ? GROUP BY status").bind(c.req.param('id')).all(),
    c.env.DB.prepare("SELECT * FROM milestones WHERE project_id = ? ORDER BY due_date").bind(c.req.param('id')).all(),
    c.env.DB.prepare("SELECT * FROM sprints WHERE project_id = ? AND status = 'active' LIMIT 1").bind(c.req.param('id')).first(),
  ]);
  return ok({ project, task_stats: taskStats.results || [], milestones: milestones.results || [], active_sprint: activeSprint });
});

app.put('/projects/:id', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return fail('Missing body');
  const b = body as Record<string, unknown>;
  const fields: string[] = [];
  const vals: unknown[] = [];
  for (const [k, v] of Object.entries(b)) {
    if (['name', 'description', 'status', 'priority', 'start_date', 'target_date', 'budget_hours'].includes(k)) {
      fields.push(`${k} = ?`); vals.push(v);
    }
    if (k === 'settings') { fields.push('settings = ?'); vals.push(JSON.stringify(v)); }
  }
  if (!fields.length) return fail('No valid fields');
  fields.push('updated_at = ?'); vals.push(nowISO());
  vals.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE projects SET ${fields.join(', ')} WHERE id = ?`).bind(...vals).run();
  return ok({ message: 'Project updated' });
});

app.delete('/projects/:id', async (c) => {
  const id = c.req.param('id');
  await c.env.DB.batch([
    c.env.DB.prepare("DELETE FROM comments WHERE task_id IN (SELECT id FROM tasks WHERE project_id = ?)").bind(id),
    c.env.DB.prepare("DELETE FROM time_entries WHERE task_id IN (SELECT id FROM tasks WHERE project_id = ?)").bind(id),
    c.env.DB.prepare("DELETE FROM tasks WHERE project_id = ?").bind(id),
    c.env.DB.prepare("DELETE FROM sprints WHERE project_id = ?").bind(id),
    c.env.DB.prepare("DELETE FROM milestones WHERE project_id = ?").bind(id),
    c.env.DB.prepare("DELETE FROM projects WHERE id = ?").bind(id),
  ]);
  return ok({ message: 'Project deleted' });
});

// =========================================================================
// MEMBERS
// =========================================================================

app.get('/members', async (c) => {
  const url = new URL(c.req.url);
  const wsId = url.searchParams.get('workspace_id');
  if (!wsId) return fail('workspace_id required');
  const { results } = await c.env.DB.prepare("SELECT * FROM members WHERE workspace_id = ? AND is_active = 1 ORDER BY name").bind(wsId).all();
  return ok({ members: (results || []).map(m => ({ ...m, skills: parseJson((m as Record<string, string>).skills, []) })) });
});

app.post('/members', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'workspace_id', 'email', 'name');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO members (id, workspace_id, email, name, role, avatar_url, hourly_rate, capacity_hours_week, skills, is_active, joined_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)"
  ).bind(id, b.workspace_id, b.email, b.name, b.role || 'member', b.avatar_url || null, b.hourly_rate || null, b.capacity_hours_week || 40, JSON.stringify(b.skills || []), nowISO()).run();
  await logActivity(c.env.DB, String(b.workspace_id), 'member_added', { memberId: id, details: String(b.name) });
  return ok({ id, message: 'Member added' });
});

app.put('/members/:id', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return fail('Missing body');
  const b = body as Record<string, unknown>;
  const fields: string[] = [];
  const vals: unknown[] = [];
  for (const [k, v] of Object.entries(b)) {
    if (['name', 'role', 'avatar_url', 'hourly_rate', 'capacity_hours_week', 'is_active'].includes(k)) {
      fields.push(`${k} = ?`); vals.push(v);
    }
    if (k === 'skills') { fields.push('skills = ?'); vals.push(JSON.stringify(v)); }
  }
  if (!fields.length) return fail('No valid fields');
  vals.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE members SET ${fields.join(', ')} WHERE id = ?`).bind(...vals).run();
  return ok({ message: 'Member updated' });
});

// =========================================================================
// TASKS (KANBAN)
// =========================================================================

const VALID_TASK_STATUSES = ['todo', 'in_progress', 'in_review', 'blocked', 'done'];
const VALID_TASK_TYPES = ['task', 'bug', 'story', 'epic', 'subtask', 'feature', 'improvement'];
const VALID_PRIORITIES = ['critical', 'high', 'medium', 'low', 'none'];

app.get('/tasks', async (c) => {
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);
  const projectId = url.searchParams.get('project_id');
  const sprintId = url.searchParams.get('sprint_id');
  const assigneeId = url.searchParams.get('assignee_id');
  const status = url.searchParams.get('status');
  const priority = url.searchParams.get('priority');
  const type = url.searchParams.get('type');

  let query = "SELECT t.*, m.name AS assignee_name FROM tasks t LEFT JOIN members m ON t.assignee_id = m.id";
  const conditions: string[] = [];
  const params: unknown[] = [];

  if (projectId) { conditions.push("t.project_id = ?"); params.push(projectId); }
  if (sprintId) { conditions.push("t.sprint_id = ?"); params.push(sprintId); }
  if (assigneeId) { conditions.push("t.assignee_id = ?"); params.push(assigneeId); }
  if (status) { conditions.push("t.status = ?"); params.push(status); }
  if (priority) { conditions.push("t.priority = ?"); params.push(priority); }
  if (type) { conditions.push("t.type = ?"); params.push(type); }

  if (conditions.length) query += " WHERE " + conditions.join(" AND ");
  query += " ORDER BY t.position, t.created_at DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);

  const { results } = await c.env.DB.prepare(query).bind(...params).all();
  return ok({
    tasks: (results || []).map(t => ({ ...t, labels: parseJson((t as Record<string, string>).labels, []) })),
    limit, offset,
  });
});

app.post('/tasks', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'project_id', 'title');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;

  if (b.status && !VALID_TASK_STATUSES.includes(String(b.status))) return fail(`Invalid status. Valid: ${VALID_TASK_STATUSES.join(', ')}`);
  if (b.type && !VALID_TASK_TYPES.includes(String(b.type))) return fail(`Invalid type. Valid: ${VALID_TASK_TYPES.join(', ')}`);
  if (b.priority && !VALID_PRIORITIES.includes(String(b.priority))) return fail(`Invalid priority. Valid: ${VALID_PRIORITIES.join(', ')}`);

  const id = uid();
  const maxPos = await c.env.DB.prepare("SELECT COALESCE(MAX(position), 0) + 1 AS next FROM tasks WHERE project_id = ?").bind(b.project_id).first<{ next: number }>();

  await c.env.DB.prepare(
    "INSERT INTO tasks (id, project_id, parent_id, sprint_id, title, description, status, priority, type, assignee_id, reporter_id, labels, estimated_hours, story_points, due_date, position, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(
    id, b.project_id, b.parent_id || null, b.sprint_id || null,
    b.title, b.description || null, b.status || 'todo', b.priority || 'medium', b.type || 'task',
    b.assignee_id || null, b.reporter_id || null, JSON.stringify(b.labels || []),
    b.estimated_hours || null, b.story_points || null, b.due_date || null,
    maxPos?.next ?? 1, nowISO(), nowISO()
  ).run();

  // Get workspace_id for activity log
  const proj = await c.env.DB.prepare("SELECT workspace_id FROM projects WHERE id = ?").bind(b.project_id).first<{ workspace_id: string }>();
  if (proj) await logActivity(c.env.DB, proj.workspace_id, 'task_created', { projectId: String(b.project_id), taskId: id, details: String(b.title) });

  log('info', 'Task created', { id, title: b.title, project: b.project_id });
  return ok({ id, message: 'Task created' });
});

app.get('/tasks/:id', async (c) => {
  const task = await c.env.DB.prepare("SELECT t.*, m.name AS assignee_name FROM tasks t LEFT JOIN members m ON t.assignee_id = m.id WHERE t.id = ?").bind(c.req.param('id')).first();
  if (!task) return fail('Task not found', 404);
  const [comments, timeEntries, subtasks] = await Promise.all([
    c.env.DB.prepare("SELECT c.*, m.name AS author_name FROM comments c JOIN members m ON c.author_id = m.id WHERE c.task_id = ? ORDER BY c.created_at").bind(c.req.param('id')).all(),
    c.env.DB.prepare("SELECT te.*, m.name AS member_name FROM time_entries te JOIN members m ON te.member_id = m.id WHERE te.task_id = ? ORDER BY te.logged_date DESC").bind(c.req.param('id')).all(),
    c.env.DB.prepare("SELECT * FROM tasks WHERE parent_id = ? ORDER BY position").bind(c.req.param('id')).all(),
  ]);
  return ok({
    task: { ...task, labels: parseJson((task as Record<string, string>).labels, []) },
    comments: comments.results || [],
    time_entries: timeEntries.results || [],
    subtasks: (subtasks.results || []).map(s => ({ ...s, labels: parseJson((s as Record<string, string>).labels, []) })),
  });
});

app.put('/tasks/:id', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return fail('Missing body');
  const b = body as Record<string, unknown>;

  if (b.status && !VALID_TASK_STATUSES.includes(String(b.status))) return fail(`Invalid status`);
  if (b.priority && !VALID_PRIORITIES.includes(String(b.priority))) return fail(`Invalid priority`);

  const fields: string[] = [];
  const vals: unknown[] = [];
  for (const [k, v] of Object.entries(b)) {
    if (['title', 'description', 'status', 'priority', 'type', 'assignee_id', 'reporter_id', 'sprint_id', 'parent_id', 'estimated_hours', 'actual_hours', 'story_points', 'due_date', 'position'].includes(k)) {
      fields.push(`${k} = ?`); vals.push(v);
    }
    if (k === 'labels') { fields.push('labels = ?'); vals.push(JSON.stringify(v)); }
  }

  // Auto-set timestamps
  if (b.status === 'in_progress' && !b.started_at) { fields.push('started_at = ?'); vals.push(nowISO()); }
  if (b.status === 'done') { fields.push('completed_at = ?'); vals.push(nowISO()); }
  fields.push('updated_at = ?'); vals.push(nowISO());

  if (fields.length <= 1) return fail('No valid fields');
  vals.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE tasks SET ${fields.join(', ')} WHERE id = ?`).bind(...vals).run();

  // Update project spent_hours if actual_hours changed
  if (b.actual_hours !== undefined) {
    const task = await c.env.DB.prepare("SELECT project_id FROM tasks WHERE id = ?").bind(c.req.param('id')).first<{ project_id: string }>();
    if (task) {
      const total = await c.env.DB.prepare("SELECT COALESCE(SUM(actual_hours), 0) AS total FROM tasks WHERE project_id = ?").bind(task.project_id).first<{ total: number }>();
      await c.env.DB.prepare("UPDATE projects SET spent_hours = ? WHERE id = ?").bind(total?.total ?? 0, task.project_id).run();
    }
  }

  return ok({ message: 'Task updated' });
});

app.delete('/tasks/:id', async (c) => {
  const id = c.req.param('id');
  await c.env.DB.batch([
    c.env.DB.prepare("DELETE FROM comments WHERE task_id = ?").bind(id),
    c.env.DB.prepare("DELETE FROM time_entries WHERE task_id = ?").bind(id),
    c.env.DB.prepare("UPDATE tasks SET parent_id = NULL WHERE parent_id = ?").bind(id),
    c.env.DB.prepare("DELETE FROM tasks WHERE id = ?").bind(id),
  ]);
  return ok({ message: 'Task deleted' });
});

// Bulk move tasks between statuses (drag & drop kanban)
app.post('/tasks/bulk-move', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'task_ids', 'status');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  if (!VALID_TASK_STATUSES.includes(String(b.status))) return fail('Invalid status');
  const taskIds = b.task_ids as string[];
  if (!Array.isArray(taskIds) || !taskIds.length) return fail('task_ids must be a non-empty array');

  const placeholders = taskIds.map(() => '?').join(',');
  const now = nowISO();
  const extra = b.status === 'done' ? `, completed_at = '${now}'` : b.status === 'in_progress' ? `, started_at = COALESCE(started_at, '${now}')` : '';
  await c.env.DB.prepare(`UPDATE tasks SET status = ?, updated_at = ?${extra} WHERE id IN (${placeholders})`).bind(b.status, now, ...taskIds).run();
  return ok({ updated: taskIds.length, status: b.status });
});

// Kanban board view
app.get('/board/:projectId', async (c) => {
  const projectId = c.req.param('projectId');
  const columns: Record<string, unknown[]> = {};
  for (const status of VALID_TASK_STATUSES) {
    const { results } = await c.env.DB.prepare(
      "SELECT t.*, m.name AS assignee_name FROM tasks t LEFT JOIN members m ON t.assignee_id = m.id WHERE t.project_id = ? AND t.status = ? AND t.parent_id IS NULL ORDER BY t.position"
    ).bind(projectId, status).all();
    columns[status] = (results || []).map(t => ({ ...t, labels: parseJson((t as Record<string, string>).labels, []) }));
  }
  return ok({ board: columns });
});

// =========================================================================
// SPRINTS
// =========================================================================

app.get('/sprints', async (c) => {
  const url = new URL(c.req.url);
  const projectId = url.searchParams.get('project_id');
  if (!projectId) return fail('project_id required');
  const { results } = await c.env.DB.prepare("SELECT * FROM sprints WHERE project_id = ? ORDER BY start_date DESC").bind(projectId).all();
  return ok({ sprints: results || [] });
});

app.post('/sprints', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'project_id', 'name', 'start_date', 'end_date');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO sprints (id, project_id, name, goal, start_date, end_date, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'planning', ?)"
  ).bind(id, b.project_id, b.name, b.goal || null, b.start_date, b.end_date, nowISO()).run();
  return ok({ id, message: 'Sprint created' });
});

app.put('/sprints/:id', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return fail('Missing body');
  const b = body as Record<string, unknown>;
  const fields: string[] = [];
  const vals: unknown[] = [];
  for (const [k, v] of Object.entries(b)) {
    if (['name', 'goal', 'start_date', 'end_date', 'status', 'velocity'].includes(k)) {
      fields.push(`${k} = ?`); vals.push(v);
    }
  }
  if (!fields.length) return fail('No valid fields');
  vals.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE sprints SET ${fields.join(', ')} WHERE id = ?`).bind(...vals).run();
  return ok({ message: 'Sprint updated' });
});

// Sprint burndown data
app.get('/sprints/:id/burndown', async (c) => {
  const sprint = await c.env.DB.prepare("SELECT * FROM sprints WHERE id = ?").bind(c.req.param('id')).first();
  if (!sprint) return fail('Sprint not found', 404);
  const s = sprint as Record<string, string>;

  const tasks = await c.env.DB.prepare("SELECT story_points, status, completed_at FROM tasks WHERE sprint_id = ?").bind(c.req.param('id')).all();
  const totalPoints = (tasks.results || []).reduce((sum, t) => sum + ((t as Record<string, number>).story_points || 0), 0);

  // Build daily burndown
  const start = new Date(s.start_date);
  const end = new Date(s.end_date);
  const days: { date: string; remaining: number; ideal: number }[] = [];
  const totalDays = Math.ceil((end.getTime() - start.getTime()) / 86400000);

  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const dateStr = d.toISOString().slice(0, 10);
    const completedByDate = (tasks.results || []).filter(t => {
      const ca = (t as Record<string, string>).completed_at;
      return ca && ca.slice(0, 10) <= dateStr;
    }).reduce((sum, t) => sum + ((t as Record<string, number>).story_points || 0), 0);

    const dayIndex = Math.ceil((d.getTime() - start.getTime()) / 86400000);
    days.push({
      date: dateStr,
      remaining: totalPoints - completedByDate,
      ideal: Math.round(totalPoints * (1 - dayIndex / totalDays)),
    });
  }

  return ok({ sprint, total_points: totalPoints, burndown: days });
});

// =========================================================================
// TIME TRACKING
// =========================================================================

app.post('/time', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'task_id', 'member_id', 'hours', 'logged_date');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO time_entries (id, task_id, member_id, hours, description, billable, logged_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(id, b.task_id, b.member_id, b.hours, b.description || null, b.billable !== false ? 1 : 0, b.logged_date, nowISO()).run();

  // Update task actual_hours
  const total = await c.env.DB.prepare("SELECT COALESCE(SUM(hours), 0) AS total FROM time_entries WHERE task_id = ?").bind(b.task_id).first<{ total: number }>();
  await c.env.DB.prepare("UPDATE tasks SET actual_hours = ? WHERE id = ?").bind(total?.total ?? 0, b.task_id).run();

  return ok({ id, message: 'Time logged' });
});

app.get('/time', async (c) => {
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);
  const memberId = url.searchParams.get('member_id');
  const projectId = url.searchParams.get('project_id');
  const from = url.searchParams.get('from');
  const to = url.searchParams.get('to');

  let query = "SELECT te.*, t.title AS task_title, m.name AS member_name FROM time_entries te JOIN tasks t ON te.task_id = t.id JOIN members m ON te.member_id = m.id";
  const conditions: string[] = [];
  const params: unknown[] = [];
  if (memberId) { conditions.push("te.member_id = ?"); params.push(memberId); }
  if (projectId) { conditions.push("t.project_id = ?"); params.push(projectId); }
  if (from) { conditions.push("te.logged_date >= ?"); params.push(from); }
  if (to) { conditions.push("te.logged_date <= ?"); params.push(to); }
  if (conditions.length) query += " WHERE " + conditions.join(" AND ");
  query += " ORDER BY te.logged_date DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);

  const { results } = await c.env.DB.prepare(query).bind(...params).all();

  // Total hours
  let totalQuery = "SELECT COALESCE(SUM(te.hours), 0) AS total_hours, COALESCE(SUM(CASE WHEN te.billable = 1 THEN te.hours ELSE 0 END), 0) AS billable_hours FROM time_entries te JOIN tasks t ON te.task_id = t.id";
  if (conditions.length) totalQuery += " WHERE " + conditions.join(" AND ");
  const totals = await c.env.DB.prepare(totalQuery).bind(...params.slice(0, -2)).first();

  return ok({ entries: results || [], totals: totals || {}, limit, offset });
});

app.delete('/time/:id', async (c) => {
  const entry = await c.env.DB.prepare("SELECT task_id FROM time_entries WHERE id = ?").bind(c.req.param('id')).first<{ task_id: string }>();
  await c.env.DB.prepare("DELETE FROM time_entries WHERE id = ?").bind(c.req.param('id')).run();
  if (entry) {
    const total = await c.env.DB.prepare("SELECT COALESCE(SUM(hours), 0) AS total FROM time_entries WHERE task_id = ?").bind(entry.task_id).first<{ total: number }>();
    await c.env.DB.prepare("UPDATE tasks SET actual_hours = ? WHERE id = ?").bind(total?.total ?? 0, entry.task_id).run();
  }
  return ok({ message: 'Time entry deleted' });
});

// =========================================================================
// COMMENTS
// =========================================================================

app.post('/comments', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'task_id', 'author_id', 'body');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO comments (id, task_id, author_id, body, is_internal, created_at) VALUES (?, ?, ?, ?, ?, ?)"
  ).bind(id, b.task_id, b.author_id, b.body, b.is_internal ? 1 : 0, nowISO()).run();
  return ok({ id, message: 'Comment added' });
});

app.put('/comments/:id', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return fail('Missing body');
  const b = body as Record<string, unknown>;
  await c.env.DB.prepare("UPDATE comments SET body = ?, edited_at = ? WHERE id = ?").bind(b.body, nowISO(), c.req.param('id')).run();
  return ok({ message: 'Comment updated' });
});

app.delete('/comments/:id', async (c) => {
  await c.env.DB.prepare("DELETE FROM comments WHERE id = ?").bind(c.req.param('id')).run();
  return ok({ message: 'Comment deleted' });
});

// =========================================================================
// MILESTONES
// =========================================================================

app.get('/milestones', async (c) => {
  const projectId = new URL(c.req.url).searchParams.get('project_id');
  if (!projectId) return fail('project_id required');
  const { results } = await c.env.DB.prepare("SELECT * FROM milestones WHERE project_id = ? ORDER BY due_date").bind(projectId).all();
  return ok({ milestones: results || [] });
});

app.post('/milestones', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'project_id', 'name');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO milestones (id, project_id, name, description, due_date, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)"
  ).bind(id, b.project_id, b.name, b.description || null, b.due_date || null, nowISO()).run();
  return ok({ id, message: 'Milestone created' });
});

app.put('/milestones/:id', async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body) return fail('Missing body');
  const b = body as Record<string, unknown>;
  const fields: string[] = [];
  const vals: unknown[] = [];
  for (const [k, v] of Object.entries(b)) {
    if (['name', 'description', 'due_date', 'status'].includes(k)) {
      fields.push(`${k} = ?`); vals.push(v);
    }
  }
  if (b.status === 'completed') { fields.push('completed_at = ?'); vals.push(nowISO()); }
  if (!fields.length) return fail('No valid fields');
  vals.push(c.req.param('id'));
  await c.env.DB.prepare(`UPDATE milestones SET ${fields.join(', ')} WHERE id = ?`).bind(...vals).run();
  return ok({ message: 'Milestone updated' });
});

// =========================================================================
// LABELS
// =========================================================================

app.get('/labels', async (c) => {
  const wsId = new URL(c.req.url).searchParams.get('workspace_id');
  if (!wsId) return fail('workspace_id required');
  const { results } = await c.env.DB.prepare("SELECT * FROM labels WHERE workspace_id = ? ORDER BY name").bind(wsId).all();
  return ok({ labels: results || [] });
});

app.post('/labels', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'workspace_id', 'name');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare("INSERT INTO labels (id, workspace_id, name, color, created_at) VALUES (?, ?, ?, ?, ?)").bind(id, b.workspace_id, b.name, b.color || '#6366f1', nowISO()).run();
  return ok({ id, message: 'Label created' });
});

app.delete('/labels/:id', async (c) => {
  await c.env.DB.prepare("DELETE FROM labels WHERE id = ?").bind(c.req.param('id')).run();
  return ok({ message: 'Label deleted' });
});

// =========================================================================
// TEMPLATES
// =========================================================================

app.get('/templates', async (c) => {
  const wsId = new URL(c.req.url).searchParams.get('workspace_id');
  if (!wsId) return fail('workspace_id required');
  const { results } = await c.env.DB.prepare("SELECT * FROM project_templates WHERE workspace_id = ? ORDER BY name").bind(wsId).all();
  return ok({
    templates: (results || []).map(t => ({
      ...t,
      tasks_template: parseJson((t as Record<string, string>).tasks_template, []),
      milestones_template: parseJson((t as Record<string, string>).milestones_template, []),
      labels_template: parseJson((t as Record<string, string>).labels_template, []),
    })),
  });
});

app.post('/templates', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'workspace_id', 'name');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const id = uid();
  await c.env.DB.prepare(
    "INSERT INTO project_templates (id, workspace_id, name, description, tasks_template, milestones_template, labels_template, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(id, b.workspace_id, b.name, b.description || null, JSON.stringify(b.tasks_template || []), JSON.stringify(b.milestones_template || []), JSON.stringify(b.labels_template || []), nowISO()).run();
  return ok({ id, message: 'Template created' });
});

// Create project from template
app.post('/templates/:id/apply', async (c) => {
  const template = await c.env.DB.prepare("SELECT * FROM project_templates WHERE id = ?").bind(c.req.param('id')).first();
  if (!template) return fail('Template not found', 404);
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'project_name');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const t = template as Record<string, string>;

  // Create project
  const projectId = uid();
  await c.env.DB.prepare(
    "INSERT INTO projects (id, workspace_id, name, description, status, priority, template_id, created_at, updated_at) VALUES (?, ?, ?, ?, 'active', 'medium', ?, ?, ?)"
  ).bind(projectId, t.workspace_id, b.project_name, t.description, c.req.param('id'), nowISO(), nowISO()).run();

  // Create tasks from template
  const tasksTemplate = parseJson(t.tasks_template, []) as Array<Record<string, unknown>>;
  let pos = 1;
  for (const task of tasksTemplate) {
    await c.env.DB.prepare(
      "INSERT INTO tasks (id, project_id, title, description, status, priority, type, estimated_hours, story_points, position, created_at, updated_at) VALUES (?, ?, ?, ?, 'todo', ?, ?, ?, ?, ?, ?, ?)"
    ).bind(uid(), projectId, task.title, task.description || null, task.priority || 'medium', task.type || 'task', task.estimated_hours || null, task.story_points || null, pos++, nowISO(), nowISO()).run();
  }

  // Create milestones from template
  const milestonesTemplate = parseJson(t.milestones_template, []) as Array<Record<string, unknown>>;
  for (const ms of milestonesTemplate) {
    await c.env.DB.prepare(
      "INSERT INTO milestones (id, project_id, name, description, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)"
    ).bind(uid(), projectId, ms.name, ms.description || null, nowISO()).run();
  }

  return ok({ project_id: projectId, tasks_created: tasksTemplate.length, milestones_created: milestonesTemplate.length });
});

// =========================================================================
// NOTIFICATIONS
// =========================================================================

app.get('/notifications', async (c) => {
  const memberId = new URL(c.req.url).searchParams.get('member_id');
  if (!memberId) return fail('member_id required');
  const unread = new URL(c.req.url).searchParams.get('unread');
  let query = "SELECT * FROM notifications WHERE member_id = ?";
  const params: unknown[] = [memberId];
  if (unread === 'true') { query += " AND is_read = 0"; }
  query += " ORDER BY created_at DESC LIMIT 50";
  const { results } = await c.env.DB.prepare(query).bind(...params).all();
  const count = await c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM notifications WHERE member_id = ? AND is_read = 0").bind(memberId).first<{ cnt: number }>();
  return ok({ notifications: results || [], unread_count: count?.cnt ?? 0 });
});

app.put('/notifications/read-all', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'member_id');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  await c.env.DB.prepare("UPDATE notifications SET is_read = 1 WHERE member_id = ? AND is_read = 0").bind(b.member_id).run();
  return ok({ message: 'All marked read' });
});

// =========================================================================
// ACTIVITY FEED
// =========================================================================

app.get('/activity', async (c) => {
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);
  const wsId = url.searchParams.get('workspace_id');
  const projectId = url.searchParams.get('project_id');
  let query = "SELECT al.*, m.name AS member_name FROM activity_log al LEFT JOIN members m ON al.member_id = m.id";
  const conditions: string[] = [];
  const params: unknown[] = [];
  if (wsId) { conditions.push("al.workspace_id = ?"); params.push(wsId); }
  if (projectId) { conditions.push("al.project_id = ?"); params.push(projectId); }
  if (conditions.length) query += " WHERE " + conditions.join(" AND ");
  query += " ORDER BY al.created_at DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);
  const { results } = await c.env.DB.prepare(query).bind(...params).all();
  return ok({ activity: results || [], limit, offset });
});

// =========================================================================
// AI FEATURES
// =========================================================================

app.post('/ai/estimate', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'title');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;

  try {
    const engineResp = await c.env.ENGINE_RUNTIME.fetch('https://engine-runtime/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: `Estimate the effort for this software task: "${b.title}". ${b.description ? `Description: ${b.description}` : ''} Provide: estimated_hours (number), story_points (1-13 fibonacci), complexity (low/medium/high), suggested_subtasks (array of strings), risk_factors (array of strings).`,
        engine_tags: ['SOFTWARE', 'PROJECT_MANAGEMENT'],
        max_results: 1,
      }),
    });
    const data = await engineResp.json();
    return ok({ estimation: data });
  } catch (e) {
    // Fallback: simple heuristic
    const wordCount = (String(b.title) + ' ' + String(b.description || '')).split(/\s+/).length;
    const complexity = wordCount > 50 ? 'high' : wordCount > 20 ? 'medium' : 'low';
    const hours = complexity === 'high' ? 16 : complexity === 'medium' ? 8 : 4;
    const points = complexity === 'high' ? 8 : complexity === 'medium' ? 5 : 3;
    return ok({
      estimation: { estimated_hours: hours, story_points: points, complexity, suggested_subtasks: [], risk_factors: [], source: 'heuristic' },
    });
  }
});

app.post('/ai/sprint-plan', async (c) => {
  const body = await c.req.json().catch(() => null);
  const err = requireBody(body, 'project_id');
  if (err) return fail(err);
  const b = body as Record<string, unknown>;
  const capacityHours = (b.capacity_hours as number) || 80;

  // Get unassigned tasks
  const { results: tasks } = await c.env.DB.prepare(
    "SELECT id, title, estimated_hours, story_points, priority FROM tasks WHERE project_id = ? AND sprint_id IS NULL AND status = 'todo' ORDER BY CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 ELSE 5 END"
  ).bind(b.project_id).all();

  // Simple greedy capacity-based sprint planning
  const suggested: unknown[] = [];
  let totalHours = 0;
  let totalPoints = 0;
  for (const task of (tasks || [])) {
    const t = task as Record<string, unknown>;
    const hours = (t.estimated_hours as number) || 4;
    if (totalHours + hours <= capacityHours) {
      suggested.push(task);
      totalHours += hours;
      totalPoints += (t.story_points as number) || 0;
    }
  }

  return ok({
    suggested_tasks: suggested,
    total_estimated_hours: totalHours,
    total_story_points: totalPoints,
    capacity_hours: capacityHours,
    utilization_pct: Math.round((totalHours / capacityHours) * 100),
    remaining_backlog: (tasks || []).length - suggested.length,
  });
});

// =========================================================================
// ANALYTICS
// =========================================================================

app.get('/analytics/velocity', async (c) => {
  const projectId = new URL(c.req.url).searchParams.get('project_id');
  if (!projectId) return fail('project_id required');
  const { results } = await c.env.DB.prepare(
    "SELECT s.id, s.name, s.start_date, s.end_date, COALESCE(SUM(CASE WHEN t.status = 'done' THEN t.story_points ELSE 0 END), 0) AS completed_points, COUNT(CASE WHEN t.status = 'done' THEN 1 END) AS completed_tasks, COUNT(t.id) AS total_tasks FROM sprints s LEFT JOIN tasks t ON t.sprint_id = s.id WHERE s.project_id = ? AND s.status IN ('completed', 'active') GROUP BY s.id ORDER BY s.start_date DESC LIMIT 10"
  ).bind(projectId).all();

  const velocities = (results || []).map(s => (s as Record<string, number>).completed_points || 0);
  const avgVelocity = velocities.length ? Math.round(velocities.reduce((a, b) => a + b, 0) / velocities.length) : 0;

  return ok({ sprints: results || [], average_velocity: avgVelocity });
});

app.get('/analytics/workload', async (c) => {
  const wsId = new URL(c.req.url).searchParams.get('workspace_id');
  if (!wsId) return fail('workspace_id required');
  const { results } = await c.env.DB.prepare(
    "SELECT m.id, m.name, m.capacity_hours_week, COUNT(t.id) AS assigned_tasks, COALESCE(SUM(t.estimated_hours), 0) AS total_estimated, COALESCE(SUM(t.actual_hours), 0) AS total_actual FROM members m LEFT JOIN tasks t ON t.assignee_id = m.id AND t.status NOT IN ('done') WHERE m.workspace_id = ? AND m.is_active = 1 GROUP BY m.id ORDER BY total_estimated DESC"
  ).bind(wsId).all();

  return ok({
    workload: (results || []).map(m => {
      const r = m as Record<string, number>;
      return { ...m, utilization_pct: r.capacity_hours_week ? Math.round((r.total_estimated / r.capacity_hours_week) * 100) : 0 };
    }),
  });
});

app.get('/analytics/summary', async (c) => {
  const projectId = new URL(c.req.url).searchParams.get('project_id');
  if (!projectId) return fail('project_id required');
  const [project, byStatus, byPriority, byType, recentActivity, overdue] = await Promise.all([
    c.env.DB.prepare("SELECT * FROM projects WHERE id = ?").bind(projectId).first(),
    c.env.DB.prepare("SELECT status, COUNT(*) AS count FROM tasks WHERE project_id = ? GROUP BY status").bind(projectId).all(),
    c.env.DB.prepare("SELECT priority, COUNT(*) AS count FROM tasks WHERE project_id = ? AND status != 'done' GROUP BY priority").bind(projectId).all(),
    c.env.DB.prepare("SELECT type, COUNT(*) AS count FROM tasks WHERE project_id = ? GROUP BY type").bind(projectId).all(),
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM activity_log WHERE project_id = ? AND created_at > datetime('now', '-7 days')").bind(projectId).first<{ cnt: number }>(),
    c.env.DB.prepare("SELECT COUNT(*) AS cnt FROM tasks WHERE project_id = ? AND due_date < datetime('now') AND status NOT IN ('done')").bind(projectId).first<{ cnt: number }>(),
  ]);

  return ok({
    project,
    tasks_by_status: byStatus.results || [],
    tasks_by_priority: byPriority.results || [],
    tasks_by_type: byType.results || [],
    activity_7d: recentActivity?.cnt ?? 0,
    overdue_tasks: overdue?.cnt ?? 0,
  });
});

// =========================================================================
// SCHEDULED (CRON)
// =========================================================================

async function handleScheduled(event: ScheduledEvent, env: Env): Promise<void> {
  log('info', 'Cron triggered', { cron: event.cron });

  if (event.cron === '0 9 * * MON') {
    // Weekly summary: check for overdue tasks, sprint progress
    const overdue = await env.DB.prepare(
      "SELECT t.id, t.title, t.due_date, p.name AS project_name FROM tasks t JOIN projects p ON t.project_id = p.id WHERE t.due_date < datetime('now') AND t.status NOT IN ('done') ORDER BY t.due_date LIMIT 20"
    ).all();

    if (overdue.results && overdue.results.length > 0) {
      log('warn', 'Overdue tasks found', { count: overdue.results.length });
    }

    // Check sprint deadlines
    const endingSprints = await env.DB.prepare(
      "SELECT * FROM sprints WHERE status = 'active' AND end_date <= datetime('now', '+2 days')"
    ).all();

    if (endingSprints.results && endingSprints.results.length > 0) {
      log('info', 'Sprints ending soon', { count: endingSprints.results.length });
    }
  }
}

// =========================================================================
// 404 & ERROR
// =========================================================================

app.notFound((c) => c.json({ ok: false, error: 'Not found', path: c.req.path }, 404));
app.onError((err, c) => {
  log('error', 'Unhandled error', { path: c.req.path, error: err.message, stack: err.stack });
  return c.json({ ok: false, error: 'Internal server error' }, 500);
});

// =========================================================================
// EXPORT
// =========================================================================

export default {
  fetch: app.fetch,
  scheduled: handleScheduled,
};
