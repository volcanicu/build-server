// unified-server.js (v16 - API Key Sanitization Fix)

// --- 核心依赖 ---
const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const { EventEmitter } = require('events');
const fs = require('fs');
const path = require('path');
const { firefox } = require('playwright');
const os = require('os');


// ===================================================================================
// AUTH SOURCE MANAGEMENT MODULE
// ===================================================================================

class AuthSource {
  constructor(logger) {
    this.logger = logger;
    this.authMode = 'file'; // Default mode
    this.maxIndex = 0;

    if (process.env.AUTH_JSON_1) {
      this.authMode = 'env';
      this.logger.info('[Auth] 检测到 AUTH_JSON_1 环境变量，切换到环境变量认证模式。');
    } else {
      this.logger.info('[Auth] 未检测到环境变量认证，将使用 "auth/" 目录下的文件。');
    }
    
    this._calculateMaxIndex();
    
    if (this.maxIndex === 0) {
      this.logger.error(`[Auth] 致命错误：在 '${this.authMode}' 模式下未找到任何有效的认证源。`);
      throw new Error("No valid authentication sources found.");
    }
  }

  _calculateMaxIndex() {
    if (this.authMode === 'env') {
      let i = 1;
      while (process.env[`AUTH_JSON_${i}`]) {
        i++;
      }
      this.maxIndex = i - 1;
    } else { // 'file' mode
      const authDir = path.join(__dirname, 'auth');
      if (!fs.existsSync(authDir)) {
        this.logger.warn('[Auth] "auth/" 目录不存在。');
        this.maxIndex = 0;
        return;
      }
      try {
        const files = fs.readdirSync(authDir);
        const authFiles = files.filter(file => /^auth-\d+\.json$/.test(file));
        const indices = authFiles.map(file => parseInt(file.match(/^auth-(\d+)\.json$/)[1], 10));
        this.maxIndex = indices.length > 0 ? Math.max(...indices) : 0;
      } catch (error) {
        this.logger.error(`[Auth] 扫描 "auth/" 目录失败: ${error.message}`);
        this.maxIndex = 0;
      }
    }
    this.logger.info(`[Auth] 在 '${this.authMode}' 模式下，检测到 ${this.maxIndex} 个认证源。`);
  }

  getMaxIndex() {
    return this.maxIndex;
  }

  getAuth(index) {
    if (index > this.maxIndex || index < 1) {
      this.logger.error(`[Auth] 请求了无效的认证索引: ${index}`);
      return null;
    }

    let jsonString;
    let sourceDescription;

    if (this.authMode === 'env') {
      jsonString = process.env[`AUTH_JSON_${index}`];
      sourceDescription = `环境变量 AUTH_JSON_${index}`;
    } else { // 'file' mode
      const authFilePath = path.join(__dirname, 'auth', `auth-${index}.json`);
      sourceDescription = `文件 ${authFilePath}`;
      if (!fs.existsSync(authFilePath)) {
          this.logger.error(`[Auth] ${sourceDescription} 不存在。`);
          return null;
      }
      try {
        jsonString = fs.readFileSync(authFilePath, 'utf-8');
      } catch (e) {
        this.logger.error(`[Auth] 读取 ${sourceDescription} 失败: ${e.message}`);
        return null;
      }
    }
    
    try {
      return JSON.parse(jsonString);
    } catch (e) {
      this.logger.error(`[Auth] 解析来自 ${sourceDescription} 的JSON内容失败: ${e.message}`);
      return null;
    }
  }
}


// ===================================================================================
// BROWSER MANAGEMENT MODULE
// ===================================================================================

class BrowserManager {
  constructor(logger, config, authSource) {
    this.logger = logger;
    this.config = config;
    this.authSource = authSource;
    this.browser = null;
    this.context = null;
    this.page = null;
    this.currentAuthIndex = 0;
    this.scriptFileName = 'dark-browser.js';
    
    if (this.config.browserExecutablePath) {
      this.browserExecutablePath = this.config.browserExecutablePath;
      this.logger.info(`[System] 使用环境变量 CAMOUFOX_EXECUTABLE_PATH 指定的浏览器路径。`);
    } else {
      const platform = os.platform();
      if (platform === 'win32') {
        this.browserExecutablePath = path.join(__dirname, 'camoufox', 'camoufox.exe');
        this.logger.info(`[System] 检测到操作系统: Windows. 将使用 'camoufox' 目录下的浏览器。`);
      } else if (platform === 'linux') {
        this.browserExecutablePath = path.join(__dirname, 'camoufox-linux', 'camoufox');
        this.logger.info(`[System] 检测到操作系统: Linux. 将使用 'camoufox-linux' 目录下的浏览器。`);
      } else {
        this.logger.error(`[System] 不支持的操作系统: ${platform}.`);
        throw new Error(`Unsupported operating system: ${platform}`);
      }
    }
  }

  async launchBrowser(authIndex) {
    if (this.browser) {
      this.logger.warn('尝试启动一个已在运行的浏览器实例，操作已取消。');
      return;
    }
    
    const sourceDescription = this.authSource.authMode === 'env' ? `环境变量 AUTH_JSON_${authIndex}` : `文件 auth-${authIndex}.json`;
    this.logger.info('==================================================');
    this.logger.info(`🚀 [Browser] 准备启动浏览器`);
    this.logger.info(`   • 认证源: ${sourceDescription}`);
    this.logger.info(`   • 浏览器路径: ${this.browserExecutablePath}`);
    this.logger.info('==================================================');

    if (!fs.existsSync(this.browserExecutablePath)) {
        this.logger.error(`❌ [Browser] 找不到浏览器可执行文件: ${this.browserExecutablePath}`);
        throw new Error(`Browser executable not found at path: ${this.browserExecutablePath}`);
    }
    
    const storageStateObject = this.authSource.getAuth(authIndex);
    if (!storageStateObject) {
        this.logger.error(`❌ [Browser] 无法获取或解析索引为 ${authIndex} 的认证信息。`);
        throw new Error(`Failed to get or parse auth source for index ${authIndex}.`);
    }

    let buildScriptContent;
    try {
      const scriptFilePath = path.join(__dirname, this.scriptFileName);
      buildScriptContent = fs.readFileSync(scriptFilePath, 'utf-8');
      this.logger.info(`✅ [Browser] 成功读取注入脚本 "${this.scriptFileName}"`);
    } catch (error) {
      this.logger.error(`❌ [Browser] 无法读取注入脚本 "${this.scriptFileName}"！`);
      throw error;
    }

    try {
      this.browser = await firefox.launch({
        headless: true,
        executablePath: this.browserExecutablePath,
      });
      this.browser.on('disconnected', () => {
        this.logger.error('❌ [Browser] 浏览器意外断开连接！服务器可能需要重启。');
        this.browser = null; this.context = null; this.page = null;
      });
      this.context = await this.browser.newContext({
        storageState: storageStateObject,
        viewport: { width: 1920, height: 1080 },
      });
      this.page = await this.context.newPage();
      this.logger.info(`[Browser] 正在加载账户 ${authIndex} 并访问目标网页...`);
      const targetUrl = 'https://aistudio.google.com/u/0/apps/bundled/blank?showPreview=true&showCode=true&showAssistant=true';
      await this.page.goto(targetUrl, { timeout: 60000, waitUntil: 'networkidle' });
      this.logger.info('[Browser] 网页加载完成，正在注入客户端脚本...');
      
      const editorContainerLocator = this.page.locator('div.monaco-editor').first();
      
      this.logger.info('[Browser] 等待编辑器出现，最长60秒...');
      await editorContainerLocator.waitFor({ state: 'visible', timeout: 60000 });
      this.logger.info('[Browser] 编辑器已出现，准备粘贴脚本。');

      await editorContainerLocator.click();
      await this.page.evaluate(text => navigator.clipboard.writeText(text), buildScriptContent);
      const isMac = os.platform() === 'darwin';
      const pasteKey = isMac ? 'Meta+V' : 'Control+V';
      await this.page.keyboard.press(pasteKey);
      this.logger.info('[Browser] 脚本已粘贴。浏览器端初始化完成。');


      this.currentAuthIndex = authIndex;
      this.logger.info('==================================================');
      this.logger.info(`✅ [Browser] 账户 ${authIndex} 初始化成功！`);
      this.logger.info('✅ [Browser] 浏览器客户端已准备就绪。');
      this.logger.info('==================================================');
    } catch (error) {
        this.logger.error(`❌ [Browser] 账户 ${authIndex} 初始化失败: ${error.message}`);
        if (this.browser) {
            await this.browser.close();
            this.browser = null;
        }
        throw error;
    }
  }

  async closeBrowser() {
    if (this.browser) {
      this.logger.info('[Browser] 正在关闭当前浏览器实例...');
      await this.browser.close();
      this.browser = null; this.context = null; this.page = null;
      this.logger.info('[Browser] 浏览器已关闭。');
    }
  }

  async switchAccount(newAuthIndex) {
    this.logger.info(`🔄 [Browser] 开始账号切换: 从 ${this.currentAuthIndex} 到 ${newAuthIndex}`);
    await this.closeBrowser();
    await this.launchBrowser(newAuthIndex);
    this.logger.info(`✅ [Browser] 账号切换完成，当前账号: ${this.currentAuthIndex}`);
  }
}

// ===================================================================================
// PROXY SERVER MODULE
// ===================================================================================

class LoggingService {
  constructor(serviceName = 'ProxyServer') {
    this.serviceName = serviceName;
  }
  _formatMessage(level, message) {
    const timestamp = new Date().toISOString();
    return `[${level}] ${timestamp} [${this.serviceName}] - ${message}`;
  }
  info(message) { console.log(this._formatMessage('INFO', message)); }
  error(message) { console.error(this._formatMessage('ERROR', message)); }
  warn(message) { console.warn(this._formatMessage('WARN', message)); }
  debug(message) { console.debug(this._formatMessage('DEBUG', message)); }
}

class MessageQueue extends EventEmitter {
  constructor(timeoutMs = 600000) {
    super();
    this.messages = [];
    this.waitingResolvers = [];
    this.defaultTimeout = timeoutMs;
    this.closed = false;
  }
  enqueue(message) {
    if (this.closed) return;
    if (this.waitingResolvers.length > 0) {
      const resolver = this.waitingResolvers.shift();
      resolver.resolve(message);
    } else {
      this.messages.push(message);
    }
  }
  async dequeue(timeoutMs = this.defaultTimeout) {
    if (this.closed) {
      throw new Error('Queue is closed');
    }
    return new Promise((resolve, reject) => {
      if (this.messages.length > 0) {
        resolve(this.messages.shift());
        return;
      }
      const resolver = { resolve, reject };
      this.waitingResolvers.push(resolver);
      const timeoutId = setTimeout(() => {
        const index = this.waitingResolvers.indexOf(resolver);
        if (index !== -1) {
          this.waitingResolvers.splice(index, 1);
          reject(new Error('Queue timeout'));
        }
      }, timeoutMs);
      resolver.timeoutId = timeoutId;
    });
  }
  close() {
    this.closed = true;
    this.waitingResolvers.forEach(resolver => {
      clearTimeout(resolver.timeoutId);
      resolver.reject(new Error('Queue closed'));
    });
    this.waitingResolvers = [];
    this.messages = [];
  }
}

class ConnectionRegistry extends EventEmitter {
  constructor(logger) {
    super();
    this.logger = logger;
    this.connections = new Set();
    this.messageQueues = new Map();
  }
  addConnection(websocket, clientInfo) {
    this.connections.add(websocket);
    this.logger.info(`[Server] 内部WebSocket客户端已连接 (来自: ${clientInfo.address})`);
    websocket.on('message', (data) => this._handleIncomingMessage(data.toString()));
    websocket.on('close', () => this._removeConnection(websocket));
    websocket.on('error', (error) => this.logger.error(`[Server] 内部WebSocket连接错误: ${error.message}`));
    this.emit('connectionAdded', websocket);
  }
  _removeConnection(websocket) {
    this.connections.delete(websocket);
    this.logger.warn('[Server] 内部WebSocket客户端连接断开');
    this.messageQueues.forEach(queue => queue.close());
    this.messageQueues.clear();
    this.emit('connectionRemoved', websocket);
  }
  _handleIncomingMessage(messageData) {
    try {
      const parsedMessage = JSON.parse(messageData);
      const requestId = parsedMessage.request_id;
      if (!requestId) {
        this.logger.warn('[Server] 收到无效消息：缺少request_id');
        return;
      }
      const queue = this.messageQueues.get(requestId);
      if (queue) {
        this._routeMessage(parsedMessage, queue);
      } else {
        this.logger.warn(`[Server] 收到未知请求ID的消息: ${requestId}`);
      }
    } catch (error) {
      this.logger.error('[Server] 解析内部WebSocket消息失败');
    }
  }
  _routeMessage(message, queue) {
    const { event_type } = message;
    switch (event_type) {
      case 'response_headers': case 'chunk': case 'error':
        queue.enqueue(message);
        break;
      case 'stream_close':
        queue.enqueue({ type: 'STREAM_END' });
        break;
      default:
        this.logger.warn(`[Server] 未知的内部事件类型: ${event_type}`);
    }
  }
  hasActiveConnections() { return this.connections.size > 0; }
  getFirstConnection() { return this.connections.values().next().value; }
  createMessageQueue(requestId) {
    const queue = new MessageQueue();
    this.messageQueues.set(requestId, queue);
    return queue;
  }
  removeMessageQueue(requestId) {
    const queue = this.messageQueues.get(requestId);
    if (queue) {
      queue.close();
      this.messageQueues.delete(requestId);
    }
  }
}

class RequestHandler {
  constructor(serverSystem, connectionRegistry, logger, browserManager, config, authSource) {
    this.serverSystem = serverSystem;
    this.connectionRegistry = connectionRegistry;
    this.logger = logger;
    this.browserManager = browserManager;
    this.config = config;
    this.authSource = authSource;
    this.maxRetries = this.config.maxRetries;
    this.retryDelay = this.config.retryDelay;
    this.failureCount = 0;
    this.isAuthSwitching = false;
  }
  
  get currentAuthIndex() {
    return this.browserManager.currentAuthIndex;
  }
  
  _getMaxAuthIndex() {
    return this.authSource.getMaxIndex();
  }
  
  _getNextAuthIndex() {
    const maxIndex = this._getMaxAuthIndex();
    if (maxIndex === 0) return 0; // Should not happen if initial check passes
    return this.currentAuthIndex >= maxIndex ? 1 : this.currentAuthIndex + 1;
  }
  
  async _switchToNextAuth() {
    if (this.isAuthSwitching) {
      this.logger.info('🔄 [Auth] 正在切换auth文件，跳过重复切换');
      return;
    }
    
    this.isAuthSwitching = true;
    const nextAuthIndex = this._getNextAuthIndex();
    const maxAuthIndex = this._getMaxAuthIndex();
    
    this.logger.info('==================================================');
    this.logger.info(`🔄 [Auth] 开始账号切换流程`);
    this.logger.info(`   • 失败次数: ${this.failureCount}/${this.config.failureThreshold}`);
    this.logger.info(`   • 当前账号索引: ${this.currentAuthIndex}`);
    this.logger.info(`   • 目标账号索引: ${nextAuthIndex}`);
    this.logger.info(`   • 可用账号总数: ${maxAuthIndex}`);
    this.logger.info('==================================================');
    
    try {
      await this.browserManager.switchAccount(nextAuthIndex);
      this.failureCount = 0;
      this.logger.info('==================================================');
      this.logger.info(`✅ [Auth] 成功切换到账号索引 ${this.currentAuthIndex}`);
      this.logger.info(`✅ [Auth] 失败计数已重置为0`);
      this.logger.info('==================================================');
    } catch (error) {
      this.logger.error('==================================================');
      this.logger.error(`❌ [Auth] 切换账号失败: ${error.message}`);
      this.logger.error('==================================================');
      throw error;
    } finally {
      this.isAuthSwitching = false;
    }
  }

  async processRequest(req, res) {
    this.logger.info(`[Request] 处理请求: ${req.method} ${req.path}`);
    if (!this.connectionRegistry.hasActiveConnections()) {
      return this._sendErrorResponse(res, 503, '没有可用的浏览器连接');
    }
    const requestId = this._generateRequestId();
    const proxyRequest = this._buildProxyRequest(req, requestId);
    const messageQueue = this.connectionRegistry.createMessageQueue(requestId);
    try {
      if (this.serverSystem.streamingMode === 'fake') {
        await this._handlePseudoStreamResponse(proxyRequest, messageQueue, req, res);
      } else {
        await this._handleRealStreamResponse(proxyRequest, messageQueue, res);
      }
    } catch (error) {
      this._handleRequestError(error, res);
    } finally {
      this.connectionRegistry.removeMessageQueue(requestId);
    }
  }
  _generateRequestId() { return `${Date.now()}_${Math.random().toString(36).substring(2, 11)}`; }
  _buildProxyRequest(req, requestId) {
    let requestBody = '';
    if (Buffer.isBuffer(req.body)) requestBody = req.body.toString('utf-8');
    else if (typeof req.body === 'string') requestBody = req.body;
    else if (req.body) requestBody = JSON.stringify(req.body);
    return {
      path: req.path, method: req.method, headers: req.headers, query_params: req.query,
      body: requestBody, request_id: requestId, streaming_mode: this.serverSystem.streamingMode
    };
  }
  _forwardRequest(proxyRequest) {
    const connection = this.connectionRegistry.getFirstConnection();
    if (connection) {
      connection.send(JSON.stringify(proxyRequest));
    } else {
      throw new Error("无法转发请求：没有可用的WebSocket连接。");
    }
  }
  _sendErrorChunkToClient(res, errorMessage) {
    const errorPayload = {
      error: { message: `[代理系统提示] ${errorMessage}`, type: 'proxy_error', code: 'proxy_error' }
    };
    const chunk = `data: ${JSON.stringify(errorPayload)}\n\n`;
    if (res && !res.writableEnded) {
      res.write(chunk);
      this.logger.info(`[Request] 已向客户端发送标准错误信号: ${errorMessage}`);
    }
  }
  async _handlePseudoStreamResponse(proxyRequest, messageQueue, req, res) {
    res.status(200).set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
    this.logger.info('[Request] 已向客户端发送初始响应头，假流式计时器已启动。');
    let connectionMaintainer = null;
    try {
      const keepAliveChunk = this._getKeepAliveChunk(req);
      connectionMaintainer = setInterval(() => { if (!res.writableEnded) { res.write(keepAliveChunk); } }, 1000);
      let lastMessage, requestFailed = false;
      for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
        this.logger.info(`[Request] 请求尝试 #${attempt}/${this.maxRetries}...`);
        this._forwardRequest(proxyRequest);
        lastMessage = await messageQueue.dequeue();
        if (lastMessage.event_type === 'error' && lastMessage.status >= 400 && lastMessage.status <= 599) {
          const errorText = `收到 ${lastMessage.status} 错误。${attempt < this.maxRetries ? `将在 ${this.retryDelay / 1000}秒后重试...` : '已达到最大重试次数。'}`;
          this._sendErrorChunkToClient(res, errorText);
          if (attempt < this.maxRetries) {
            await new Promise(resolve => setTimeout(resolve, this.retryDelay));
            continue;
          }
          requestFailed = true;
        }
        break;
      }
      if (lastMessage.event_type === 'error' || requestFailed) {
        this.failureCount++;
        this.logger.warn(`⚠️ [Auth] 请求失败 - 失败计数: ${this.failureCount}/${this.config.failureThreshold} (当前账号索引: ${this.currentAuthIndex})`);
        if (this.failureCount >= this.config.failureThreshold) {
          this.logger.warn(`🔴 [Auth] 达到失败阈值！准备切换账号...`);
          this._sendErrorChunkToClient(res, `连续失败${this.failureCount}次，正在尝试切换账号...`);
          try {
            await this._switchToNextAuth();
            this._sendErrorChunkToClient(res, `已切换到账号索引 ${this.currentAuthIndex}，请重试`);
          } catch (switchError) {
            this.logger.error(`🔴 [Auth] 账号切换失败: ${switchError.message}`);
            this._sendErrorChunkToClient(res, `切换账号失败: ${switchError.message}`);
          }
        }
        throw new Error(lastMessage.message || '请求失败');
      }
      if (this.failureCount > 0) {
        this.logger.info(`✅ [Auth] 请求成功 - 失败计数已从 ${this.failureCount} 重置为 0`);
      }
      this.failureCount = 0;
      const dataMessage = await messageQueue.dequeue();
      const endMessage = await messageQueue.dequeue();
      if (dataMessage.data) {
        res.write(`data: ${dataMessage.data}\n\n`);
        this.logger.info('[Request] 已将完整响应体作为SSE事件发送。');
      }
      if (endMessage.type !== 'STREAM_END') this.logger.warn('[Request] 未收到预期的流结束信号。');
    } finally {
      if (connectionMaintainer) clearInterval(connectionMaintainer);
      if (!res.writableEnded) res.end();
      this.logger.info('[Request] 假流式响应处理结束。');
    }
  }
  async _handleRealStreamResponse(proxyRequest, messageQueue, res) {
    let headerMessage, requestFailed = false;
    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      this.logger.info(`[Request] 请求尝试 #${attempt}/${this.maxRetries}...`);
      this._forwardRequest(proxyRequest);
      headerMessage = await messageQueue.dequeue();
      if (headerMessage.event_type === 'error' && headerMessage.status >= 400 && headerMessage.status <= 599) {
        this.logger.warn(`[Request] 收到 ${headerMessage.status} 错误，将在 ${this.retryDelay / 1000}秒后重试...`);
        if (attempt < this.maxRetries) {
          await new Promise(resolve => setTimeout(resolve, this.retryDelay));
          continue;
        }
        requestFailed = true;
      }
      break;
    }
    if (headerMessage.event_type === 'error' || requestFailed) {
      this.failureCount++;
      this.logger.warn(`⚠️ [Auth] 请求失败 - 失败计数: ${this.failureCount}/${this.config.failureThreshold} (当前账号索引: ${this.currentAuthIndex})`);
      if (this.failureCount >= this.config.failureThreshold) {
        this.logger.warn(`🔴 [Auth] 达到失败阈值！准备切换账号...`);
        try {
          await this._switchToNextAuth();
        } catch (switchError) {
          this.logger.error(`🔴 [Auth] 账号切换失败: ${switchError.message}`);
        }
      }
      return this._sendErrorResponse(res, headerMessage.status, headerMessage.message);
    }
    if (this.failureCount > 0) {
      this.logger.info(`✅ [Auth] 请求成功 - 失败计数已从 ${this.failureCount} 重置为 0`);
    }
    this.failureCount = 0;
    this._setResponseHeaders(res, headerMessage);
    this.logger.info('[Request] 已向客户端发送真实响应头，开始流式传输...');
    try {
      while (true) {
        const dataMessage = await messageQueue.dequeue(30000);
        if (dataMessage.type === 'STREAM_END') { this.logger.info('[Request] 收到流结束信号。'); break; }
        if (dataMessage.data) res.write(dataMessage.data);
      }
    } catch (error) {
      if (error.message !== 'Queue timeout') throw error;
      this.logger.warn('[Request] 真流式响应超时，可能流已正常结束。');
    } finally {
      if (!res.writableEnded) res.end();
      this.logger.info('[Request] 真流式响应连接已关闭。');
    }
  }
  _getKeepAliveChunk(req) {
    if (req.path.includes('chat/completions')) {
      const payload = { id: `chatcmpl-${this._generateRequestId()}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "gpt-4", choices: [{ index: 0, delta: {}, finish_reason: null }] };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }
    if (req.path.includes('generateContent') || req.path.includes('streamGenerateContent')) {
      const payload = { candidates: [{ content: { parts: [{ text: "" }], role: "model" }, finishReason: null, index: 0, safetyRatings: [] }] };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }
    return 'data: {}\n\n';
  }
  _setResponseHeaders(res, headerMessage) {
    res.status(headerMessage.status || 200);
    const headers = headerMessage.headers || {};
    Object.entries(headers).forEach(([name, value]) => {
      if (name.toLowerCase() !== 'content-length') res.set(name, value);
    });
  }
  _handleRequestError(error, res) {
    if (res.headersSent) {
      this.logger.error(`[Request] 请求处理错误 (头已发送): ${error.message}`);
      if (this.serverSystem.streamingMode === 'fake') this._sendErrorChunkToClient(res, `处理失败: ${error.message}`);
      if (!res.writableEnded) res.end();
    } else {
      this.logger.error(`[Request] 请求处理错误: ${error.message}`);
      const status = error.message.includes('超时') ? 504 : 500;
      this._sendErrorResponse(res, status, `代理错误: ${error.message}`);
    }
  }
  _sendErrorResponse(res, status, message) {
    if (!res.headersSent) res.status(status || 500).type('text/plain').send(message);
  }
}

class ProxyServerSystem extends EventEmitter {
  constructor() {
    super();
    this.logger = new LoggingService('ProxySystem');
    this._loadConfiguration();
    this.streamingMode = this.config.streamingMode;
    
    this.authSource = new AuthSource(this.logger);
    this.browserManager = new BrowserManager(this.logger, this.config, this.authSource);
    this.connectionRegistry = new ConnectionRegistry(this.logger);
    this.requestHandler = new RequestHandler(this, this.connectionRegistry, this.logger, this.browserManager, this.config, this.authSource);
    
    this.httpServer = null;
    this.wsServer = null;
  }
  
  _loadConfiguration() {
    let config = {
      httpPort: 8889, host: '0.0.0.0', wsPort: 9998, streamingMode: 'real',
      failureThreshold: 3, maxRetries: 3, retryDelay: 2000, browserExecutablePath: null,
      apiKeys: [],
    };
    
    const configPath = path.join(__dirname, 'config.json');
    try {
      if (fs.existsSync(configPath)) {
        const fileConfig = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
        config = { ...config, ...fileConfig };
        this.logger.info('[System] 已从 config.json 加载配置。');
      }
    } catch (error) {
      this.logger.warn(`[System] 无法读取或解析 config.json: ${error.message}`);
    }

    if (process.env.PORT) config.httpPort = parseInt(process.env.PORT, 10) || config.httpPort;
    if (process.env.HOST) config.host = process.env.HOST;
    if (process.env.STREAMING_MODE) config.streamingMode = process.env.STREAMING_MODE;
    if (process.env.FAILURE_THRESHOLD) config.failureThreshold = parseInt(process.env.FAILURE_THRESHOLD, 10) || config.failureThreshold;
    if (process.env.MAX_RETRIES) config.maxRetries = parseInt(process.env.MAX_RETRIES, 10) || config.maxRetries;
    if (process.env.RETRY_DELAY) config.retryDelay = parseInt(process.env.RETRY_DELAY, 10) || config.retryDelay;
    if (process.env.CAMOUFOX_EXECUTABLE_PATH) config.browserExecutablePath = process.env.CAMOUFOX_EXECUTABLE_PATH;
    if (process.env.API_KEYS) {
        config.apiKeys = process.env.API_KEYS.split(',');
    }

    // --- CRITICAL FIX: Sanitize the apiKeys array to remove empty/whitespace-only keys ---
    if (Array.isArray(config.apiKeys)) {
        config.apiKeys = config.apiKeys.map(k => String(k).trim()).filter(k => k);
    } else {
        config.apiKeys = []; // Ensure it's always an array
    }
    
    this.config = config;
    this.logger.info('================ [ EFFECTIVE CONFIGURATION ] ================');
    this.logger.info(`  HTTP Port: ${this.config.httpPort}`);
    this.logger.info(`  Host: ${this.config.host}`);
    this.logger.info(`  Streaming Mode: ${this.config.streamingMode}`);
    this.logger.info(`  Failure Threshold: ${this.config.failureThreshold}`);
    this.logger.info(`  Max Retries: ${this.config.maxRetries}`);
    this.logger.info(`  Retry Delay: ${this.config.retryDelay}ms`);
    if (this.config.apiKeys && this.config.apiKeys.length > 0) {
        this.logger.info(`  API Key Auth: Enabled (${this.config.apiKeys.length} keys loaded)`);
    } else {
        this.logger.info(`  API Key Auth: Disabled`);
    }
    this.logger.info('=============================================================');
  }
  
  async start(initialAuthIndex = 1) {
    try {
      await this.browserManager.launchBrowser(initialAuthIndex);
      await this._startHttpServer();
      await this._startWebSocketServer();
      this.logger.info(`[System] 代理服务器系统启动完成。`);
      this.emit('started');
    } catch (error) {
      this.logger.error(`[System] 启动失败: ${error.message}`);
      this.emit('error', error);
      throw error;
    }
  }
  
  _createAuthMiddleware() {
    return (req, res, next) => {
      const serverApiKeys = this.config.apiKeys;
      if (!serverApiKeys || serverApiKeys.length === 0) {
        return next();
      }

      let clientKey = null;
      let keySource = null;
      
      const headers = req.headers;

      if (headers['x-goog-api-key']) {
        clientKey = headers['x-goog-api-key'];
        keySource = 'x-goog-api-key Header';
      } else if (headers.authorization && headers.authorization.startsWith('Bearer ')) {
        clientKey = headers.authorization.substring(7);
        keySource = 'Authorization Header';
      } else if (headers['x-api-key']) {
        clientKey = headers['x-api-key'];
        keySource = 'X-API-Key Header';
      } else if (req.query.key) {
        clientKey = req.query.key;
        keySource = 'Query Parameter';
      }
      
      if (clientKey) {
        if (serverApiKeys.includes(clientKey)) {
          this.logger.info(`[Auth] API Key 在 '${keySource}' 中找到，验证通过。`);
          
          // --- CRITICAL FIX: Clean up the request object if key was in query ---
          if (keySource === 'Query Parameter') {
              delete req.query.key;
              this.logger.debug(`[Auth-Cleanup] 已从 req.query 中移除 API Key，以确保请求纯净。`);
          }
          return next();
        } else {
          this.logger.warn(`[Auth] 拒绝请求: 无效的 API Key。IP: ${req.ip}, Source: ${keySource}, Key: '${clientKey}'`);
          return res.status(401).json({ error: { message: "Invalid API key provided." } });
        }
      }

      this.logger.warn(`[Auth] 拒绝受保护的请求: 缺少 API Key。IP: ${req.ip}, Path: ${req.path}`);
      this.logger.debug(`[Auth-Debug] 未在任何标准位置找到API Key。`);
      this.logger.debug(`[Auth-Debug] 搜索的Headers: ${JSON.stringify(headers)}`);
      this.logger.debug(`[Auth-Debug] 搜索的Query: ${JSON.stringify(req.query)}`);
      this.logger.debug(`[Auth-Debug] 已加载的API Keys: [${serverApiKeys.join(', ')}]`);

      return res.status(401).json({ error: { message: "Access denied. A valid API key was not found in headers or query parameters." } });
    };
  }
  
  async _startHttpServer() {
    const app = this._createExpressApp();
    this.httpServer = http.createServer(app);
    return new Promise((resolve) => {
      this.httpServer.listen(this.config.httpPort, this.config.host, () => {
        this.logger.info(`[System] HTTP服务器已在 http://${this.config.host}:${this.config.httpPort} 上监听`);
        resolve();
      });
    });
  }
  
  _createExpressApp() {
    const app = express();
    app.use(express.json({ limit: '100mb' }));
    app.use(express.raw({ type: '*/*', limit: '100mb' }));

    app.get('/admin/set-mode', (req, res) => {
      const newMode = req.query.mode;
      if (newMode === 'fake' || newMode === 'real') {
        this.streamingMode = newMode;
        res.status(200).send(`流式模式已切换为: ${this.streamingMode}`);
      } else {
        res.status(400).send('无效模式. 请用 "fake" 或 "real".');
      }
    });
    
    app.get('/health', (req, res) => {
      res.status(200).json({
        status: 'healthy',
        uptime: process.uptime(),
        config: {
            streamingMode: this.streamingMode,
            failureThreshold: this.config.failureThreshold,
            maxRetries: this.config.maxRetries,
            authMode: this.authSource.authMode,
            apiKeyAuth: (this.config.apiKeys && this.config.apiKeys.length > 0) ? 'Enabled' : 'Disabled',
        },
        auth: {
          currentAuthIndex: this.requestHandler.currentAuthIndex,
          maxAuthIndex: this.authSource.getMaxIndex(),
          failureCount: this.requestHandler.failureCount,
          isAuthSwitching: this.requestHandler.isAuthSwitching,
        },
        browser: {
          connected: !!this.browserManager.browser,
        },
        websocket: {
          internalClients: this.connectionRegistry.connections.size
        }
      });
    });
    
    app.use(this._createAuthMiddleware());
    
    app.all(/(.*)/, (req, res) => {
      if (req.path === '/favicon.ico') return res.status(204).send();
      this.requestHandler.processRequest(req, res);
    });
    
    return app;
  }
  
  async _startWebSocketServer() {
    this.wsServer = new WebSocket.Server({ port: this.config.wsPort, host: this.config.host });
    this.wsServer.on('connection', (ws, req) => {
      this.connectionRegistry.addConnection(ws, { address: req.socket.remoteAddress });
    });
  }
}

// ===================================================================================
// MAIN INITIALIZATION
// ===================================================================================

async function initializeServer() {
  const initialAuthIndex = parseInt(process.env.INITIAL_AUTH_INDEX, 10) || 1;
  try {
    const serverSystem = new ProxyServerSystem();
    await serverSystem.start(initialAuthIndex);
  } catch (error) {
    console.error('❌ 服务器启动失败:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  initializeServer();
}

module.exports = { ProxyServerSystem, BrowserManager, initializeServer };
