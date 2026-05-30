#!/usr/bin/env node
/**
 * AI 观影伴侣 MCP & OpenAI/OpenRouter 双模流式桥接服务器
 * 使用纯 Node.js 内置模块，无需任何 npm 包
 * * 功能：
 * 1. [MCP 模式] 供电脑/手机端 Claude 客户端通过 stdio 连接
 * 2. [Web 状态桥接] 接收电脑浏览器前端发送的实时画面、字幕、时间进度
 * 3. [OpenAI/SSE 代理模式] 允许手机端任何 AI 客户端将其设为自定义 API Base，
 * 自动将当前电影上下文注入到你的 DeepSeek/OpenRouter 聊天中，支持 SSE 流式返回。
 */

const http = require('http');
const https = require('https');
const readline = require('readline');

// ── 共享状态（由网页 POST 过来更新）──
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
  aiName: 'AI 伴侣',
  activeLore: '',
  longTermMemories: '',
  updatedAt: null,
};

// ── 待发给网页的消息队列 ──
let pendingMessages = [];

const BRIDGE_PORT = 3765;

// 时间格式化助手
const fmtTime = s => {
  if (!s || isNaN(s)) return '0:00';
  const m = Math.floor(s / 60), sec = Math.floor(s % 60);
  return m + ':' + String(sec).padStart(2, '0');
};

// ── HTTP 桥接与代理服务器 ──
const bridgeServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // 1. 网页客户端：更新观影状态
  if (req.method === 'POST' && req.url === '/state') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        movieState = { ...movieState, ...data, connected: true, updatedAt: new Date().toISOString() };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        const toSend = [...pendingMessages];
        pendingMessages = [];
        res.end(JSON.stringify({ ok: true, pendingMessages: toSend }));
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // 2. 网页客户端：轮询新消息
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
    res.end(JSON.stringify({ ok: true, connected: movieState.connected, video: movieState.videoFile }));
    return;
  }

  // 3. OpenAI 兼容端点 - 手机端 AI 通过这里连接
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

  // 获取模型列表（可选）
  if (req.method === 'GET' && (req.url === '/v1/models' || req.url === '/models')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      data: [
        { id: 'deepseek-chat', object: 'model' },
        { id: 'gpt-4o', object: 'model' },
        { id: 'claude-3-sonnet', object: 'model' }
      ]
    }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// ── 处理手机端 AI 请求，注入观影上下文 ──
function handleMobileAiProxy(req, res, openAiReq) {
  const authHeader = req.headers['authorization'];
  const targetModel = openAiReq.model || 'deepseek-chat';
  const supportsVision = /gpt-4o|gpt-4\.1|vision|vl|qwen.*vl|gemini|claude-3|llava|pixtral|glm-4v/i.test(targetModel);

  const movieContext = `\n\n[实时观影同伴系统注入]\n` +
    `AI名字/人设: ${movieState.aiName || 'AI 观影伴侣'}\n` +
    `当前播放影片: ${movieState.videoFile || '未知'}\n` +
    `当前时间轴进度: ${fmtTime(movieState.currentTime)} / ${fmtTime(movieState.duration)}\n` +
    `播放状态: ${movieState.isPaused ? '暂停' : '播放中'}\n` +
    `当前行字幕: "${movieState.currentSubtitle || '无'}"\n` +
    `近期字幕历史:\n${(movieState.recentSubtitles || []).map(s => `[${s.time}] ${s.text}`).join('\n') || '无'}\n` +
    `世界书/设定触发:\n${movieState.activeLore || '无'}\n` +
    `长期剧情记忆:\n${movieState.longTermMemories || '无'}\n` +
    (movieState.screenshot
      ? (supportsVision ? `已附加当前画面截图。\n` : `当前画面已捕获，但当前模型名看起来不支持视觉输入，因此只注入字幕和时间轴。\n`)
      : `当前无可用画面截图。\n`) +
    `请结合上述正在播放的情节、字幕上下文和观众唠嗑。回复要自然、简短，避免剧透未出现内容。`;

  let modifiedMessages = JSON.parse(JSON.stringify(openAiReq.messages || []));
  if (!modifiedMessages.length) modifiedMessages.push({ role: 'user', content: '聊聊当前剧情。' });

  const lastUserMsg = [...modifiedMessages].reverse().find(m => m.role === 'user');
  if (lastUserMsg) {
    const imagePart = supportsVision && movieState.screenshot
      ? { type: 'image_url', image_url: { url: `data:image/jpeg;base64,${movieState.screenshot}` } }
      : null;

    if (typeof lastUserMsg.content === 'string') {
      const originalText = lastUserMsg.content;
      if (imagePart) {
        lastUserMsg.content = [
          { type: 'text', text: originalText + movieContext },
          imagePart
        ];
      } else {
        lastUserMsg.content = originalText + movieContext;
      }
    } else if (Array.isArray(lastUserMsg.content)) {
      lastUserMsg.content.push({ type: 'text', text: movieContext });
      if (imagePart) lastUserMsg.content.push(imagePart);
    }
  }

  let targetHost = 'api.deepseek.com';
  let targetPath = '/v1/chat/completions';
  let targetHeaders = {
    'Content-Type': 'application/json',
    'Authorization': authHeader || 'Bearer '
  };

  // 如果是 OpenRouter 格式的模型名，例如 openai/gpt-4o-mini，就转发到 OpenRouter
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

  const proxyOptions = {
    hostname: targetHost,
    path: targetPath,
    method: 'POST',
    headers: targetHeaders
  };

  const proxyReq = https.request(proxyOptions, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);

    let accumulatedText = '';

    proxyRes.on('data', (chunk) => {
      res.write(chunk);
      if (openAiReq.stream) {
        const lines = chunk.toString().split('\n');
        for (let line of lines) {
          line = line.trim();
          if (line.startsWith('data: ') && line !== 'data: [DONE]') {
            try {
              const parsed = JSON.parse(line.slice(6));
              const content = parsed.choices?.[0]?.delta?.content || '';
              accumulatedText += content;
            } catch (e) {}
          }
        }
      } else {
        accumulatedText += chunk.toString();
      }
    });

    proxyRes.on('end', () => {
      res.end();
      if (!openAiReq.stream && accumulatedText) {
        try {
          const parsed = JSON.parse(accumulatedText);
          accumulatedText = parsed.choices?.[0]?.message?.content || '';
        } catch (e) {}
      }
      if (accumulatedText && !accumulatedText.startsWith('{')) {
        pendingMessages.push({
          id: Date.now(),
          role: 'mcp-ai',
          type: 'analysis',
          text: accumulatedText,
          timestamp: new Date().toISOString(),
          videoTime: movieState.currentTime
        });
      }
    });
  });

  proxyReq.on('error', (err) => {
    process.stderr.write(`[Proxy Error] 请求失败: ${err.message}\n`);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: { message: '代理请求失败: ' + err.message } }));
  });

  proxyReq.write(JSON.stringify(payload));
  proxyReq.end();
}

// ── MCP 协议实现 (stdio) ──
const tools = [
  {
    name: 'get_movie_status',
    description: '获取当前电影播放状态，包括片名、时间进度、以及当前字幕。',
    inputSchema: { type: 'object', properties: {} }
  },
  {
    name: 'get_screenshot',
    description: '获取当前电影画面的截图（base64格式）。',
    inputSchema: { type: 'object', properties: {} }
  }
];

async function handleToolCall(name, args) {
  if (name === 'get_movie_status') {
    return {
      content: [{
        type: 'text',
        text: `📽️ 观影状态\n片名：${movieState.videoFile || '未知'}\n当前进度：${fmtTime(movieState.currentTime)}\n当前字幕：${movieState.currentSubtitle || '（无）'}`
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
    } else {
      return {
        content: [{
          type: 'text',
          text: '当前没有可用的截图，请确保视频正在播放且已同步。'
        }]
      };
    }
  }
  return { content: [{ type: 'text', text: `未知工具: ${name}` }] };
}

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', async (line) => {
  if (!line.trim()) return;
  let req;
  try {
    req = JSON.parse(line);
  } catch (e) {
    return;
  }
  const { id, method, params } = req;
  try {
    if (method === 'initialize') {
      sendMcpResponse(id, {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: 'movie-mcp-server', version: '1.5.0' }
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
    sendMcpResponse(id, -32603, e.message);
  }
});

function sendMcpResponse(id, result) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id, result }) + '\n');
}

// ── 启动服务 ──
bridgeServer.listen(BRIDGE_PORT, '0.0.0.0', () => {
  const os = require('os');
  const networkInterfaces = os.networkInterfaces();
  let localIp = 'localhost';
  for (const name of Object.keys(networkInterfaces)) {
    for (const net of networkInterfaces[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        localIp = net.address;
        break;
      }
    }
  }
  process.stderr.write(`\n======================================================\n`);
  process.stderr.write(`🎬 AI观影伴侣多模态中转服务已在局域网就绪！\n`);
  process.stderr.write(`📡 网页同步桥接端口 → http://localhost:${BRIDGE_PORT}\n`);
  process.stderr.write(`📱 手机 AI 客户端接口自定义 Base URL 请填写：\n`);
  process.stderr.write(`   👉 http://${localIp}:${BRIDGE_PORT}/v1\n`);
  process.stderr.write(`======================================================\n\n`);
});