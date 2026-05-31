#!/usr/bin/env node
/**
 * AI 观影伴侣 MCP 服务器 v2.0
 *
 * 新增功能：
 *  1. MCP over HTTP+SSE transport —— 安卓 Operit/任何 MCP 客户端可通过局域网远程连接
 *     连接地址：http://<局域网IP>:3765/sse
 *  2. 字幕变化主动推送 —— 字幕更新时，通过 SSE 向所有 MCP 客户端发送 notification，
 *     触发 AI 主动收到新字幕上下文（无需手动发消息）
 *  3. 保留原有：网页状态桥接 /state、OpenAI 代理 /v1/chat/completions、stdio MCP
 */

const http = require('http');
const https = require('https');
const readline = require('readline');
const os = require('os');

// ── 共享状态 ──
let movieState = {
  connected: false,
  videoFile: null,
  currentTime: 0,
  duration: 0,
  isPaused: true,
  currentSubtitle: '',
  recentSubtitles: [],
  screenshot: null,
  screenshotTime: 0,
  messages: [],
  updatedAt: null,
};

let pendingMessages = [];
const BRIDGE_PORT = 3765;

// ── SSE 客户端管理（MCP over HTTP+SSE） ──
// 每个连接的 MCP 客户端对应一个 SSE 连接
const sseClients = new Map(); // clientId -> { res, sessionId, pendingRequests: Map }
let clientIdCounter = 0;

const fmtTime = s => {
  if (!s || isNaN(s)) return '0:00';
  const m = Math.floor(s / 60), sec = Math.floor(s % 60);
  return m + ':' + String(sec).padStart(2, '0');
};

// ── MCP 工具定义 ──
const tools = [
  {
    name: 'get_movie_status',
    description: '获取当前电影播放状态，包括片名、时间进度和当前字幕。适合随时查询当前剧情进展。',
    inputSchema: { type: 'object', properties: {} }
  },
  {
    name: 'get_recent_subtitles',
    description: '获取最近一段时间（约前后30秒）的字幕列表，了解更完整的剧情上下文。',
    inputSchema: {
      type: 'object',
      properties: {
        count: {
          type: 'number',
          description: '返回的字幕条数，默认7条'
        }
      }
    }
  },
  {
    name: 'get_screenshot',
    description: '获取当前电影画面截图（base64 JPEG），可用于分析画面内容、角色表情、场景构图等。',
    inputSchema: { type: 'object', properties: {} }
  },
  {
    name: 'send_comment_to_screen',
    description: '将 AI 的实时评论发送到电脑屏幕上的聊天面板，让观众看到 AI 的主动弹幕式点评。',
    inputSchema: {
      type: 'object',
      required: ['comment'],
      properties: {
        comment: {
          type: 'string',
          description: '要显示的评论内容（建议50字以内，简短犀利）'
        }
      }
    }
  }
];

async function handleToolCall(name, args) {
  if (name === 'get_movie_status') {
    return {
      content: [{
        type: 'text',
        text: `📽️ 观影状态\n` +
              `片名：${movieState.videoFile || '未知'}\n` +
              `当前进度：${fmtTime(movieState.currentTime)} / ${fmtTime(movieState.duration)}\n` +
              `播放状态：${movieState.isPaused ? '⏸ 暂停' : '▶ 播放中'}\n` +
              `当前字幕：${movieState.currentSubtitle || '（无）'}`
      }]
    };
  }

  if (name === 'get_recent_subtitles') {
    const count = args?.count || 7;
    if (!movieState.recentSubtitles || movieState.recentSubtitles.length === 0) {
      return { content: [{ type: 'text', text: '暂无字幕数据，请确认前端已加载字幕文件并同步。' }] };
    }
    const subs = movieState.recentSubtitles.slice(-count);
    const text = subs.map(s => `[${s.time}] ${s.text}`).join('\n');
    return {
      content: [{
        type: 'text',
        text: `📋 近期字幕（${subs.length} 条，当前进度 ${fmtTime(movieState.currentTime)}）\n\n${text}`
      }]
    };
  }

  if (name === 'get_screenshot') {
    if (movieState.screenshot) {
      return {
        content: [{
          type: 'image',
          data: movieState.screenshot,
          mimeType: 'image/jpeg'
        }]
      };
    }
    return { content: [{ type: 'text', text: '当前没有可用截图，请确保视频正在播放且前端已同步。' }] };
  }

  if (name === 'send_comment_to_screen') {
    const comment = args?.comment || '';
    if (comment) {
      pendingMessages.push({
        id: Date.now(),
        role: 'mcp-ai',
        type: 'proactive',
        text: comment,
        timestamp: new Date().toISOString(),
        videoTime: movieState.currentTime
      });
    }
    return { content: [{ type: 'text', text: `✅ 评论已发送到屏幕：${comment}` }] };
  }

  return { content: [{ type: 'text', text: `未知工具: ${name}` }] };
}

// ── 向所有 SSE 客户端广播 ──
function broadcastToSseClients(event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const [id, client] of sseClients) {
    try {
      client.res.write(msg);
    } catch (e) {
      sseClients.delete(id);
    }
  }
}

// ── 字幕变化检测：通过 sampling/createMessage 主动触发 AI ──
let lastPushedSubtitle = '';
let samplingMsgId = 1;
function checkAndPushSubtitleChange() {
  const current = movieState.currentSubtitle;
  if (current && current !== lastPushedSubtitle && !movieState.isPaused) {
    lastPushedSubtitle = current;

    // 向所有支持 sampling 的客户端发送 sampling/createMessage 请求
    // 这是 MCP 唯一能让服务器主动触发 AI 响应的机制
    const recentSubs = (movieState.recentSubtitles || []).slice(-5)
      .map(s => `[${s.time}] ${s.text}`).join('\n');

    const samplingRequest = {
      jsonrpc: '2.0',
      id: samplingMsgId++,
      method: 'sampling/createMessage',
      params: {
        messages: [{
          role: 'user',
          content: {
            type: 'text',
            text: `🎬 [${fmtTime(movieState.currentTime)}] 新字幕：${current}\n` +
                  `近期字幕：\n${recentSubs}\n` +
                  `片名：${movieState.videoFile || '未知'}\n` +
                  `请根据这条字幕简短点评（50字内，可以不回复如果内容普通）。`
          }
        }],
        maxTokens: 200,
        includeContext: 'none',  // 不带历史对话，避免上下文里的图片格式导致报错
        systemPrompt: '你是AI观影伴侣，请用简短犀利的语言对当前字幕内容做实时点评，不超过50字。如果字幕内容平淡无聊则回复空字符串。'
      }
    };

    for (const [id, client] of sseClients) {
      if (!client.supportsSampling) continue;
      try {
        client.res.write(`event: message\ndata: ${JSON.stringify(samplingRequest)}\n\n`);
        process.stderr.write(`[Sampling] 已向客户端 #${id} 发送字幕采样请求：${current.slice(0,20)}...\n`);
      } catch (e) {
        sseClients.delete(id);
      }
    }
  }
}

// ── HTTP 桥接服务器 ──
const bridgeServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ════════════════════════════════════════════
  //  MCP over HTTP+SSE transport (新增)
  // ════════════════════════════════════════════

  // 1. SSE 端点：MCP 客户端建立长连接，接收服务端推送
  if (req.method === 'GET' && req.url === '/sse') {
    const clientId = ++clientIdCounter;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });

    // 告知客户端用哪个端点发消息
    // 优先用请求的 Host 头（手机连的就是这个地址），回送同样的 host 保证可达
    const reqHost = req.headers['host'] || `localhost:${BRIDGE_PORT}`;
    const origin = `http://${reqHost}`;
    res.write(`event: endpoint\ndata: ${JSON.stringify({ uri: `${origin}/message?sessionId=${clientId}` })}\n\n`);

    sseClients.set(clientId, { res, sessionId: clientId });
    process.stderr.write(`[MCP SSE] 客户端 #${clientId} 已连接（共 ${sseClients.size} 个）\n`);

    // 保活心跳
    const heartbeat = setInterval(() => {
      try { res.write(`: heartbeat\n\n`); } catch (e) { clearInterval(heartbeat); }
    }, 15000);

    req.on('close', () => {
      clearInterval(heartbeat);
      sseClients.delete(clientId);
      process.stderr.write(`[MCP SSE] 客户端 #${clientId} 断开（剩余 ${sseClients.size} 个）\n`);
    });
    return;
  }

  // 2. Message 端点：MCP 客户端发送 JSON-RPC 请求
  if (req.method === 'POST' && req.url?.startsWith('/message')) {
    const urlParams = new URLSearchParams(req.url.split('?')[1] || '');
    const sessionId = parseInt(urlParams.get('sessionId') || '0');

    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      res.writeHead(202); res.end('Accepted');

      let request;
      try { request = JSON.parse(body); } catch (e) { return; }

      const client = sseClients.get(sessionId);
      if (!client) return;

      const { id, method, params } = request;
      let result;

      try {
        if (method === 'initialize') {
          // 记录客户端是否支持 sampling（rikkahub 支持）
          if (params?.capabilities?.sampling) {
            sseClients.get(sessionId).supportsSampling = true;
          }
          result = {
            protocolVersion: params?.protocolVersion || '2024-11-05',
            capabilities: {
              tools: {},
              logging: {},
              sampling: {}   // 声明服务器需要使用 sampling 能力
            },
            serverInfo: { name: 'movie-mcp-server', version: '2.0.0' }
          };
        } else if (method === 'tools/list') {
          result = { tools };
        } else if (method === 'tools/call') {
          result = await handleToolCall(params.name, params.arguments);
        } else if (method === 'ping') {
          result = {};
        } else if (method === 'notifications/initialized' || method === 'initialized') {
          // 客户端初始化完成通知 —— 这是单向通知，不需要返回 result
          // 但主动推送一条欢迎 notification，让客户端确认链路畅通
          const connectMsg = `event: message\ndata: ${JSON.stringify({
            jsonrpc: '2.0',
            method: 'notifications/message',
            params: {
              level: 'info',
              logger: 'movie-companion',
              data: `🎬 AI观影伴侣已连接！当前视频：${movieState.videoFile || '未加载'}，进度：${fmtTime(movieState.currentTime)}`
            }
          })}\n\n`;
          try { client.res.write(connectMsg); } catch (e) {}
          // 如果带了 id（某些客户端实现），也回一个空 result 防止超时
          if (id !== undefined && id !== null) {
            const ackMsg = `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id, result: {} })}\n\n`;
            try { client.res.write(ackMsg); } catch (e) {}
          }
          return;
        } else {
          // 未知方法，返回错误
          const errMsg = `event: message\ndata: ${JSON.stringify({
            jsonrpc: '2.0', id,
            error: { code: -32601, message: `Method not found: ${method}` }
          })}\n\n`;
          try { client.res.write(errMsg); } catch (e) {}
          return;
        }
      } catch (e) {
        const errMsg = `event: message\ndata: ${JSON.stringify({
          jsonrpc: '2.0', id,
          error: { code: -32603, message: e.message }
        })}\n\n`;
        try { client.res.write(errMsg); } catch (e2) {}
        return;
      }

      // 通过 SSE 回送响应
      const responseMsg = `event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id, result })}\n\n`;
      try { client.res.write(responseMsg); } catch (e) {}
    });
    return;
  }

  // ════════════════════════════════════════════
  //  原有网页桥接端点
  // ════════════════════════════════════════════

  // 网页客户端：更新观影状态
  if (req.method === 'POST' && req.url === '/state') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        movieState = { ...movieState, ...data, connected: true, updatedAt: new Date().toISOString() };

        // 检测字幕变化并推送给 SSE 客户端
        checkAndPushSubtitleChange();

        res.writeHead(200, { 'Content-Type': 'application/json' });
        const toSend = [...pendingMessages];
        pendingMessages = [];
        res.end(JSON.stringify({ ok: true, pendingMessages: toSend, mcpClients: sseClients.size }));
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // 网页客户端：轮询新消息
  if (req.method === 'GET' && req.url === '/messages') {
    const toSend = [...pendingMessages];
    pendingMessages = [];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ messages: toSend }));
    return;
  }

  // 健康检查
  if (req.method === 'GET' && req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      connected: movieState.connected,
      video: movieState.videoFile,
      mcpClients: sseClients.size,
      currentSubtitle: movieState.currentSubtitle
    }));
    return;
  }

  // OpenAI 兼容端点
  if (req.method === 'POST' && (req.url === '/v1/chat/completions' || req.url === '/chat/completions')) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const openAiReq = JSON.parse(body);
        handleMobileAiProxy(req, res, openAiReq);
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: '请求解析失败: ' + e.message } }));
      }
    });
    return;
  }

  // 模型列表
  if (req.method === 'GET' && (req.url === '/v1/models' || req.url === '/models')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      data: [
        { id: 'deepseek-chat', object: 'model' },
        { id: 'deepseek-reasoner', object: 'model' },
        { id: 'gpt-4o', object: 'model' }
      ]
    }));
    return;
  }

  res.writeHead(404); res.end('Not found');
});

// ── OpenAI 代理（原有逻辑保留）──
function handleMobileAiProxy(req, res, openAiReq) {
  const authHeader = req.headers['authorization'];
  const movieContext = `\n\n[实时观影同伴系统注入]\n` +
    `当前播放影片: ${movieState.videoFile || '未知'}\n` +
    `当前时间轴进度: ${fmtTime(movieState.currentTime)}\n` +
    `当前行字幕: "${movieState.currentSubtitle || '无'}"\n` +
    `近期字幕历史:\n${(movieState.recentSubtitles || []).map(s => `[${s.time}] ${s.text}`).join('\n')}\n` +
    `请结合上述正在播放的情节、字幕上下文和观众唠嗑。你的设定是和观众坐在同一张沙发上并排看电影的毒舌/资深影评人老铁，回复要自然、简短。`;

  let modifiedMessages = JSON.parse(JSON.stringify(openAiReq.messages));
  if (modifiedMessages.length > 0) {
    let lastUserMsg = [...modifiedMessages].reverse().find(m => m.role === 'user');
    if (lastUserMsg) {
      if (typeof lastUserMsg.content === 'string') {
        lastUserMsg.content += movieContext;
      } else if (Array.isArray(lastUserMsg.content)) {
        lastUserMsg.content.push({ type: 'text', text: movieContext });
      }
    }
  }

  const targetModel = openAiReq.model || 'deepseek-chat';
  let targetHost = 'api.deepseek.com';
  let targetPath = '/v1/chat/completions';
  let targetHeaders = {
    'Content-Type': 'application/json',
    'Authorization': authHeader || 'Bearer '
  };

  if (targetModel.includes('/')) {
    targetHost = 'openrouter.ai';
    targetPath = '/api/v1/chat/completions';
    targetHeaders = {
      'Content-Type': 'application/json',
      'Authorization': authHeader || '',
      'HTTP-Referer': 'https://github.com/ai-movie-companion',
      'X-Title': 'AI Movie Companion Mobile Bridge'
    };
  }

  const payload = {
    model: targetModel,
    messages: modifiedMessages,
    stream: openAiReq.stream || false,
    temperature: openAiReq.temperature ?? 0.7,
    max_tokens: openAiReq.max_tokens ?? 400
  };

  const proxyReq = https.request({ hostname: targetHost, path: targetPath, method: 'POST', headers: targetHeaders }, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    let accumulatedText = '';
    proxyRes.on('data', (chunk) => {
      res.write(chunk);
      if (openAiReq.stream) {
        for (let line of chunk.toString().split('\n')) {
          line = line.trim();
          if (line.startsWith('data: ') && line !== 'data: [DONE]') {
            try { accumulatedText += JSON.parse(line.slice(6)).choices?.[0]?.delta?.content || ''; } catch (e) {}
          }
        }
      } else {
        accumulatedText += chunk.toString();
      }
    });
    proxyRes.on('end', () => {
      res.end();
      if (!openAiReq.stream && accumulatedText) {
        try { accumulatedText = JSON.parse(accumulatedText).choices?.[0]?.message?.content || ''; } catch (e) {}
      }
      if (accumulatedText && !accumulatedText.startsWith('{')) {
        pendingMessages.push({
          id: Date.now(), role: 'mcp-ai', type: 'analysis',
          text: accumulatedText, timestamp: new Date().toISOString(),
          videoTime: movieState.currentTime
        });
      }
    });
  });

  proxyReq.on('error', (err) => {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: '代理请求失败: ' + err.message } }));
  });
  proxyReq.write(JSON.stringify(payload));
  proxyReq.end();
}

// ── stdio MCP（保留，供桌面 Claude 使用）──
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', async (line) => {
  if (!line.trim()) return;
  let req;
  try { req = JSON.parse(line); } catch (e) { return; }
  const { id, method, params } = req;
  try {
    if (method === 'initialize') {
      sendMcpResponse(id, {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {}, logging: {} },
        serverInfo: { name: 'movie-mcp-server', version: '2.0.0' }
      });
    } else if (method === 'tools/list') {
      sendMcpResponse(id, { tools });
    } else if (method === 'tools/call') {
      const result = await handleToolCall(params.name, params.arguments);
      sendMcpResponse(id, result);
    } else if (method === 'ping') {
      sendMcpResponse(id, {});
    }
  } catch (e) {
    process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32603, message: e.message } }) + '\n');
  }
});
function sendMcpResponse(id, result) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n');
}

// ── 启动 ──
bridgeServer.listen(BRIDGE_PORT, '0.0.0.0', () => {
  let localIp = 'localhost';
  for (const name of Object.keys(os.networkInterfaces())) {
    for (const net of os.networkInterfaces()[name]) {
      if (net.family === 'IPv4' && !net.internal) { localIp = net.address; break; }
    }
  }
  process.stderr.write(`\n${'='.repeat(58)}\n`);
  process.stderr.write(`🎬 AI观影伴侣 MCP 服务器 v2.0 已启动\n\n`);
  process.stderr.write(`📡 网页前端同步地址：\n`);
  process.stderr.write(`   http://localhost:${BRIDGE_PORT}\n\n`);
  process.stderr.write(`📱 安卓 Operit / MCP 客户端连接地址（HTTP+SSE）：\n`);
  process.stderr.write(`   👉 http://${localIp}:${BRIDGE_PORT}/sse\n\n`);
  process.stderr.write(`🖥️  桌面 Claude MCP 配置（stdio，原有方式不变）：\n`);
  process.stderr.write(`   command: node\n`);
  process.stderr.write(`   args: ["/path/to/movie-mcp-server.js"]\n\n`);
  process.stderr.write(`💡 字幕变化时会自动推送 notification 给所有 MCP 客户端\n`);
  process.stderr.write(`${'='.repeat(58)}\n\n`);
});
