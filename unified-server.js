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
    this.availableIndices = []; // 不再使用 maxIndex，而是存储所有可用索引

    if (process.env.AUTH_JSON_1) {
      this.authMode = 'env';
      this.logger.info('[Auth] 检测到 AUTH_JSON_1 环境变量，切换到环境变量认证模式。');
    } else {
      this.logger.info('[Auth] 未检测到环境变量认证，将使用 "auth/" 目录下的文件。');
    }

    this._discoverAvailableIndices();

    if (this.availableIndices.length === 0) {
      this.logger.error(`[Auth] 致命错误：在 '${this.authMode}' 模式下未找到任何有效的认证源。`);
      throw new Error("No valid authentication sources found.");
    }
  }

  _discoverAvailableIndices() {
    let indices = [];
    if (this.authMode === 'env') {
      const regex = /^AUTH_JSON_(\d+)$/;
      for (const key in process.env) {
        const match = key.match(regex);
        if (match && match[1]) {
          indices.push(parseInt(match[1], 10));
        }
      }
    } else { // 'file' mode
      const authDir = path.join(__dirname, 'auth');
      if (!fs.existsSync(authDir)) {
        this.logger.warn('[Auth] "auth/" 目录不存在。');
        this.availableIndices = [];
        return;
      }
      try {
        const files = fs.readdirSync(authDir);
        const authFiles = files.filter(file => /^auth-\d+\.json$/.test(file));
        indices = authFiles.map(file => parseInt(file.match(/^auth-(\d+)\.json$/)[1], 10));
      } catch (error) {
        this.logger.error(`[Auth] 扫描 "auth/" 目录失败: ${error.message}`);
        this.availableIndices = [];
        return;
      }
    }

    // 排序并去重，确保索引列表干净有序
    this.availableIndices = [...new Set(indices)].sort((a, b) => a - b);

    this.logger.info(`[Auth] 在 '${this.authMode}' 模式下，检测到 ${this.availableIndices.length} 个认证源。`);
    if (this.availableIndices.length > 0) {
      this.logger.info(`[Auth] 可用索引列表: [${this.availableIndices.join(', ')}]`);
    }
  }

  getAvailableIndices() {
    return this.availableIndices;
  }

  getFirstAvailableIndex() {
    return this.availableIndices.length > 0 ? this.availableIndices[0] : null;
  }

  getAuth(index) {
    // 检查请求的索引是否存在于我们的可用列表中
    if (!this.availableIndices.includes(index)) {
      this.logger.error(`[Auth] 请求了无效或不存在的认证索引: ${index}`);
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
      // 虽然 _discoverAvailableIndices 已确认文件存在，但为了健壮性，再次检查
      if (!fs.existsSync(authFilePath)) {
        this.logger.error(`[Auth] ${sourceDescription} 在读取时突然消失。`);
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

    // --- START: 自动修复 Cookie 的 sameSite 属性 (健壮版) ---
    if (storageStateObject.cookies && Array.isArray(storageStateObject.cookies)) {
      let fixedCount = 0;
      const validSameSiteValues = ['Lax', 'Strict', 'None'];
      storageStateObject.cookies.forEach(cookie => {
        // 检查 sameSite 的值是否在有效列表里
        if (!validSameSiteValues.includes(cookie.sameSite)) {
          // 如果无效 (比如是 'lax', '', null, undefined), 则修正为 'Lax'
          this.logger.warn(`[Auth] 发现无效的 sameSite 值: '${cookie.sameSite}'，正在自动修正为 'None'。`);
          cookie.sameSite = 'None';
          fixedCount++;
        }
      });
      if (fixedCount > 0) {
        this.logger.info(`[Auth] 自动修正了 ${fixedCount} 个无效的 Cookie 'sameSite' 属性。`);
      }
    }
    // --- END: 自动修复 ---

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
        storageState: storageStateObject, // 使用修复后的 storageState
        viewport: { width: 1280, height: 720 },
      });
      this.page = await this.context.newPage();
      this.logger.info(`[Browser] 正在加载账户 ${authIndex} 并访问目标网页...`);
      const targetUrl = 'https://aistudio.google.com/u/0/apps/bundled/blank?showPreview=true&showCode=true&showAssistant=true';
      await this.page.goto(targetUrl, { timeout: 120000, waitUntil: 'networkidle' });
      this.logger.info('[Browser] 网页加载完成，正在注入客户端脚本...');

      const editorContainerLocator = this.page.locator('div.monaco-editor').first();

      this.logger.info('[Browser] 等待编辑器出现，最长120秒...');
      await editorContainerLocator.waitFor({ state: 'visible', timeout: 120000 });
      this.logger.info('[Browser] 编辑器已出现，准备粘贴脚本。');

      // --- START: 新增的点击逻辑 ---
      this.logger.info('[Browser] 等待5秒，之后将在页面下方执行一次模拟点击以确保页面激活...');
      await this.page.waitForTimeout(5000); // 等待5秒

      const viewport = this.page.viewportSize();
      if (viewport) {
        const clickX = viewport.width / 2;
        const clickY = viewport.height - 120;
        this.logger.info(`[Browser] 在页面底部中心位置 (x≈${Math.round(clickX)}, y=${clickY}) 执行点击。`);
        await this.page.mouse.click(clickX, clickY);
      } else {
        this.logger.warn('[Browser] 无法获取视窗大小，跳过页面底部模拟点击。');
      }
      // --- END: 新增的点击逻辑 ---

      await editorContainerLocator.click({ timeout: 120000 });
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
  constructor(timeoutMs = 1200000) {
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
        //this.logger.warn(`[Server] 收到未知请求ID的消息: ${requestId}`);
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

  _getNextAuthIndex() {
    const available = this.authSource.getAvailableIndices();
    if (available.length === 0) return null; // 没有可用的auth
    if (available.length === 1) return available[0]; // 只有一个，切给自己

    const currentIndexInArray = available.indexOf(this.currentAuthIndex);

    // 如果当前索引不知为何不在可用列表里，安全起见返回第一个
    if (currentIndexInArray === -1) {
      this.logger.warn(`[Auth] 当前索引 ${this.currentAuthIndex} 不在可用列表中，将切换到第一个可用索引。`);
      return available[0];
    }

    // 计算下一个索引在数组中的位置，使用模运算实现循环
    const nextIndexInArray = (currentIndexInArray + 1) % available.length;

    return available[nextIndexInArray];
  }

  async _switchToNextAuth() {
    if (this.isAuthSwitching) {
      this.logger.info('🔄 [Auth] 正在切换auth文件，跳过重复切换');
      return;
    }

    this.isAuthSwitching = true;
    const nextAuthIndex = this._getNextAuthIndex();
    const totalAuthCount = this.authSource.getAvailableIndices().length;

    if (nextAuthIndex === null) {
      this.logger.error('🔴 [Auth] 无法切换账号，因为没有可用的认证源！');
      this.isAuthSwitching = false;
      // 抛出错误以便调用者可以捕获它
      throw new Error('No available authentication sources to switch to.');
    }

    this.logger.info('==================================================');
    this.logger.info(`🔄 [Auth] 开始账号切换流程`);
    this.logger.info(`   • 失败次数: ${this.failureCount}/${this.config.failureThreshold > 0 ? this.config.failureThreshold : 'N/A'}`);
    this.logger.info(`   • 当前账号索引: ${this.currentAuthIndex}`);
    this.logger.info(`   • 目标账号索引: ${nextAuthIndex}`);
    this.logger.info(`   • 可用账号总数: ${totalAuthCount}`);
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

  // NEW: Error parsing and correction utility
  _parseAndCorrectErrorDetails(errorDetails) {
    // 创建一个副本以避免修改原始对象
    const correctedDetails = { ...errorDetails };
    this.logger.debug(`[ErrorParser] 原始错误详情: status=${correctedDetails.status}, message="${correctedDetails.message}"`);

    // 只有在错误消息存在时才尝试解析
    if (correctedDetails.message && typeof correctedDetails.message === 'string') {
      // 正则表达式匹配 "HTTP xxx" 或 "status code xxx" 等模式
      const regex = /(?:HTTP|status code)\s+(\d{3})/;
      const match = correctedDetails.message.match(regex);

      if (match && match[1]) {
        const parsedStatus = parseInt(match[1], 10);
        // 确保解析出的状态码是有效的 HTTP 错误码
        if (parsedStatus >= 400 && parsedStatus <= 599) {
          if (correctedDetails.status !== parsedStatus) {
            this.logger.warn(`[ErrorParser] 修正了错误状态码！原始: ${correctedDetails.status}, 从消息中解析得到: ${parsedStatus}`);
            correctedDetails.status = parsedStatus; // 使用解析出的更准确的状态码
          } else {
            this.logger.debug(`[ErrorParser] 解析的状态码 (${parsedStatus}) 与原始状态码一致，无需修正。`);
          }
        }
      }
    }
    return correctedDetails;
  }

  async _handleRequestFailureAndSwitch(errorDetails, res) {
    // 创建一个副本进行操作，并进行深度解析
    const correctedDetails = { ...errorDetails };
    if (correctedDetails.message && typeof correctedDetails.message === 'string') {
      // 增强版正则表达式，能匹配 "HTTP 429" 或 JSON 中的 "code":429 等多种模式
      const regex = /(?:HTTP|status code)\s*(\d{3})|"code"\s*:\s*(\d{3})/;
      const match = correctedDetails.message.match(regex);

      // match[1] 对应 (?:HTTP|status code)\s*(\d{3})
      // match[2] 对应 "code"\s*:\s*(\d{3})
      const parsedStatusString = match ? (match[1] || match[2]) : null;

      if (parsedStatusString) {
        const parsedStatus = parseInt(parsedStatusString, 10);
        if (parsedStatus >= 400 && parsedStatus <= 599 && correctedDetails.status !== parsedStatus) {
          this.logger.warn(`[Auth] 修正了错误状态码！原始: ${correctedDetails.status}, 从消息中解析得到: ${parsedStatus}`);
          correctedDetails.status = parsedStatus;
        }
      }
    }

    // --- 后续逻辑使用修正后的 correctedDetails ---

    const isImmediateSwitch = this.config.immediateSwitchStatusCodes.includes(correctedDetails.status);

    if (isImmediateSwitch) {
      this.logger.warn(`🔴 [Auth] 收到状态码 ${correctedDetails.status} (已修正)，触发立即切换账号...`);
      if (res) this._sendErrorChunkToClient(res, `收到状态码 ${correctedDetails.status}，正在尝试切换账号...`);
      try {
        await this._switchToNextAuth();
        if (res) this._sendErrorChunkToClient(res, `已切换到账号索引 ${this.currentAuthIndex}，请重试`);
      } catch (switchError) {
        this.logger.error(`🔴 [Auth] 账号切换失败: ${switchError.message}`);
        if (res) this._sendErrorChunkToClient(res, `切换账号失败: ${switchError.message}`);
      }
      return; // 结束函数，外层循环将进行重试
    }

    // 基于失败计数的切换逻辑
    if (this.config.failureThreshold > 0) {
      this.failureCount++;
      this.logger.warn(`⚠️ [Auth] 请求失败 - 失败计数: ${this.failureCount}/${this.config.failureThreshold} (当前账号索引: ${this.currentAuthIndex}, 状态码: ${correctedDetails.status})`);
      if (this.failureCount >= this.config.failureThreshold) {
        this.logger.warn(`🔴 [Auth] 达到失败阈值！准备切换账号...`);
        if (res) this._sendErrorChunkToClient(res, `连续失败${this.failureCount}次，正在尝试切换账号...`);
        try {
          await this._switchToNextAuth();
          if (res) this._sendErrorChunkToClient(res, `已切换到账号索引 ${this.currentAuthIndex}，请重试`);
        } catch (switchError) {
          this.logger.error(`🔴 [Auth] 账号切换失败: ${switchError.message}`);
          if (res) this._sendErrorChunkToClient(res, `切换账号失败: ${switchError.message}`);
        }
      }
    } else {
      this.logger.warn(`[Auth] 请求失败 (状态码: ${correctedDetails.status})。基于计数的自动切换已禁用 (failureThreshold=0)`);
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

  //========================================================
  // START: MODIFIED SECTION
  //========================================================

  _getKeepAliveChunk(req) {
    if (req.path.includes('chat/completions')) {
      const payload = { id: `chatcmpl-${this._generateRequestId()}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "gpt-4", choices: [{ index: 0, delta: {}, finish_reason: null }] };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }
    if (req.path.includes('generateContent') || req.path.includes('streamGenerateContent')) {
      const payload = { candidates: [{ content: { parts: [{ text: "" }], role: "model" }, finishReason: null, index: 0, safetyRatings: [] }] };
      return `data: ${JSON.stringify(payload)}\n\n`;
    }
    // Provide a generic, harmless default
    return 'data: {}\n\n';
  }

  async _handlePseudoStreamResponse(proxyRequest, messageQueue, req, res) {
    // 关键决策点: 通过请求路径判断客户端期望的是流还是普通JSON
    const originalPath = req.path;
    const isStreamRequest = originalPath.includes(':stream');

    this.logger.info(`[Request] 假流式处理流程启动，路径: "${originalPath}"，判定为: ${isStreamRequest ? '流式请求' : '非流式请求'}`);

    let connectionMaintainer = null;

    // 只有在确定是流式请求时，才立即发送头并启动心跳
    if (isStreamRequest) {
      res.status(200).set({
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });
      const keepAliveChunk = this._getKeepAliveChunk(req);
      connectionMaintainer = setInterval(() => { if (!res.writableEnded) res.write(keepAliveChunk); }, 2000);
    }

    try {
      let lastMessage, requestFailed = false;
      for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
        this.logger.info(`[Request] 请求尝试 #${attempt}/${this.maxRetries}...`);
        this._forwardRequest(proxyRequest);
        lastMessage = await messageQueue.dequeue();

        if (lastMessage.event_type === 'error' && lastMessage.status >= 400 && lastMessage.status <= 599) {
          const correctedMessage = this._parseAndCorrectErrorDetails(lastMessage);
          await this._handleRequestFailureAndSwitch(correctedMessage, isStreamRequest ? res : null); // 仅在流模式下才向客户端发送错误块

          const errorText = `收到 ${correctedMessage.status} 错误。${attempt < this.maxRetries ? `将在 ${this.retryDelay / 1000}秒后重试...` : '已达到最大重试次数。'}`;
          this.logger.warn(`[Request] ${errorText}`);

          // 如果是流式请求，则通过数据块通知客户端错误
          if (isStreamRequest) {
            this._sendErrorChunkToClient(res, errorText);
          }

          if (attempt < this.maxRetries) {
            await new Promise(resolve => setTimeout(resolve, this.retryDelay));
            continue;
          }
          requestFailed = true;
        }
        break; // 成功则跳出循环
      }

      // 如果所有重试都失败
      if (lastMessage.event_type === 'error' || requestFailed) {
        const finalError = this._parseAndCorrectErrorDetails(lastMessage);
        // 对于非流式请求，现在可以安全地发送一个完整的错误响应
        if (!res.headersSent) {
          this._sendErrorResponse(res, finalError.status, `请求失败: ${finalError.message}`);
        } else { // 对于流式请求，只能发送最后一个错误块
          this._sendErrorChunkToClient(res, `请求最终失败 (状态码: ${finalError.status}): ${finalError.message}`);
        }
        return; // 结束函数
      }

      // 请求成功
      if (this.failureCount > 0) {
        this.logger.info(`✅ [Auth] 请求成功 - 失败计数已从 ${this.failureCount} 重置为 0`);
      }
      this.failureCount = 0;

      const dataMessage = await messageQueue.dequeue();
      const endMessage = await messageQueue.dequeue();
      if (endMessage.type !== 'STREAM_END') this.logger.warn('[Request] 未收到预期的流结束信号。');

      // ======================= 核心逻辑：根据请求类型格式化最终响应 =======================
      if (isStreamRequest) {
        // 客户端想要一个流，我们发送SSE数据块
        if (dataMessage.data) {
          res.write(`data: ${dataMessage.data}\n\n`);
        }
        res.write('data: [DONE]\n\n');
        this.logger.info('[Request] 已将完整响应作为模拟SSE事件发送。');
      } else {
        // 客户端想要一个普通JSON，我们直接返回它
        this.logger.info('[Request] 准备发送 application/json 响应。');
        if (dataMessage.data) {
          try {
            // 确保我们发送的是有效的JSON
            const jsonData = JSON.parse(dataMessage.data);
            res.status(200).json(jsonData);
          } catch (e) {
            this.logger.error(`[Request] 无法将来自浏览器的响应解析为JSON: ${e.message}`);
            this._sendErrorResponse(res, 500, '代理内部错误：无法解析来自后端的响应。');
          }
        } else {
          this._sendErrorResponse(res, 500, '代理内部错误：后端未返回有效数据。');
        }
      }
      // =================================================================================

    } catch (error) {
      // 这个 catch 块处理意外错误，比如队列超时
      this.logger.error(`[Request] 假流式处理期间发生意外错误: ${error.message}`);
      if (!res.headersSent) {
        this._handleRequestError(error, res);
      } else {
        this._sendErrorChunkToClient(res, `处理失败: ${error.message}`);
      }
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

        // --- START: MODIFICATION ---
        const correctedMessage = this._parseAndCorrectErrorDetails(headerMessage);
        await this._handleRequestFailureAndSwitch(correctedMessage, null); // res is not available
        this.logger.warn(`[Request] 收到 ${correctedMessage.status} 错误，将在 ${this.retryDelay / 1000}秒后重试...`);
        // --- END: MODIFICATION ---

        if (attempt < this.maxRetries) {
          await new Promise(resolve => setTimeout(resolve, this.retryDelay));
          continue;
        }
        requestFailed = true;
      }
      break;
    }
    if (headerMessage.event_type === 'error' || requestFailed) {
      // --- START: MODIFICATION ---
      const finalError = this._parseAndCorrectErrorDetails(headerMessage);
      // 使用修正后的状态码和消息返回给客户端
      return this._sendErrorResponse(res, finalError.status, finalError.message);
      // --- END: MODIFICATION ---
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
      failureThreshold: 0,
      maxRetries: 3, retryDelay: 2000, browserExecutablePath: null,
      apiKeys: [],
      immediateSwitchStatusCodes: [],
      initialAuthIndex: null,
      debugMode: false, // [新增] 调试模式默认关闭
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
    if (process.env.DEBUG_MODE) { // [新增] 从环境变量读取调试模式
      config.debugMode = process.env.DEBUG_MODE === 'true';
    }
    // 新增：处理环境变量，它会覆盖 config.json 中的设置
    if (process.env.INITIAL_AUTH_INDEX) {
      const envIndex = parseInt(process.env.INITIAL_AUTH_INDEX, 10);
      if (!isNaN(envIndex) && envIndex > 0) {
        config.initialAuthIndex = envIndex;
      }
    }


    // NEW: 统一处理 immediateSwitchStatusCodes，环境变量优先于 config.json
    let rawCodes = process.env.IMMEDIATE_SWITCH_STATUS_CODES;
    let codesSource = '环境变量';

    if (!rawCodes && config.immediateSwitchStatusCodes && Array.isArray(config.immediateSwitchStatusCodes)) {
      rawCodes = config.immediateSwitchStatusCodes.join(',');
      codesSource = 'config.json 文件';
    }

    if (rawCodes && typeof rawCodes === 'string') {
      config.immediateSwitchStatusCodes = rawCodes
        .split(',')
        .map(code => parseInt(String(code).trim(), 10))
        .filter(code => !isNaN(code) && code >= 400 && code <= 599);
      if (config.immediateSwitchStatusCodes.length > 0) {
        this.logger.info(`[System] 已从 ${codesSource} 加载“立即切换状态码”。`);
      }
    } else {
      config.immediateSwitchStatusCodes = [];
    }

    if (Array.isArray(config.apiKeys)) {
      config.apiKeys = config.apiKeys.map(k => String(k).trim()).filter(k => k);
    } else {
      config.apiKeys = [];
    }

    this.config = config;
    this.logger.info('================ [ 生效配置 ] ================');
    this.logger.info(`  HTTP 服务端口: ${this.config.httpPort}`);
    this.logger.info(`  监听地址: ${this.config.host}`);
    this.logger.info(`  流式模式: ${this.config.streamingMode}`);
    this.logger.info(`  调试模式: ${this.config.debugMode ? '已开启' : '已关闭'}`); // [新增] 打印调试模式状态
    // 新增：在日志中显示初始索引的配置
    if (this.config.initialAuthIndex) {
      this.logger.info(`  指定初始认证索引: ${this.config.initialAuthIndex}`);
    }
    // MODIFIED: 日志输出已汉化
    this.logger.info(`  失败计数切换: ${this.config.failureThreshold > 0 ? `连续 ${this.config.failureThreshold} 次失败后切换` : '已禁用'}`);
    this.logger.info(`  立即切换状态码: ${this.config.immediateSwitchStatusCodes.length > 0 ? this.config.immediateSwitchStatusCodes.join(', ') : '已禁用'}`);
    this.logger.info(`  单次请求最大重试: ${this.config.maxRetries}次`);
    this.logger.info(`  重试间隔: ${this.config.retryDelay}ms`);
    if (this.config.apiKeys && this.config.apiKeys.length > 0) {
      this.logger.info(`  API 密钥认证: 已启用 (${this.config.apiKeys.length} 个密钥)`);
    } else {
      this.logger.info(`  API 密钥认证: 已禁用`);
    }
    this.logger.info('=============================================================');
  }

  async start() {
    try {
      // 决定启动时使用的认证索引
      let startupIndex = this.authSource.getFirstAvailableIndex();
      // 修改：从 this.config 读取，而不是直接从 process.env
      const suggestedIndex = this.config.initialAuthIndex;

      if (suggestedIndex) {
        if (this.authSource.getAvailableIndices().includes(suggestedIndex)) {
          this.logger.info(`[System] 使用配置中指定的有效启动索引: ${suggestedIndex}`);
          startupIndex = suggestedIndex;
        } else {
          this.logger.warn(`[System] 配置中指定的启动索引 ${suggestedIndex} 无效或不存在，将使用第一个可用索引: ${startupIndex}`);
        }
      } else {
        this.logger.info(`[System] 未指定启动索引，将自动使用第一个可用索引: ${startupIndex}`);
      }

      await this.browserManager.launchBrowser(startupIndex);
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

  // [新增] 调试日志中间件
  _createDebugLogMiddleware() {
    return (req, res, next) => {
      if (!this.config.debugMode) {
        return next();
      }

      const requestId = this.requestHandler._generateRequestId();
      const log = this.logger.info.bind(this.logger); // 使用 info 级别以保证显示

      log(`\n\n--- [DEBUG] START INCOMING REQUEST (${requestId}) ---`);
      log(`[DEBUG][${requestId}] Client IP: ${req.ip}`);
      log(`[DEBUG][${requestId}] Method: ${req.method}`);
      log(`[DEBUG][${requestId}] URL: ${req.originalUrl}`);
      log(`[DEBUG][${requestId}] Headers: ${JSON.stringify(req.headers, null, 2)}`);

      // 智能处理请求体
      let bodyContent = 'N/A or empty';
      if (req.body) {
        if (Buffer.isBuffer(req.body) && req.body.length > 0) {
          // 对于 buffer，尝试以 utf-8 解码，如果失败则显示原始 buffer 信息
          try {
            bodyContent = req.body.toString('utf-8');
          } catch (e) {
            bodyContent = `[Non-UTF8 Buffer, size: ${req.body.length} bytes]`;
          }
        } else if (typeof req.body === 'object' && Object.keys(req.body).length > 0) {
          bodyContent = JSON.stringify(req.body, null, 2);
        } else if (typeof req.body === 'string' && req.body.length > 0) {
          bodyContent = req.body;
        }
      }

      log(`[DEBUG][${requestId}] Body:\n${bodyContent}`);
      log(`--- [DEBUG] END INCOMING REQUEST (${requestId}) ---\n\n`);

      next();
    };
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
      const xGoogApiKey = headers['x-goog-api-key'] || headers['x_goog_api_key'];
      const xApiKey = headers['x-api-key'] || headers['x_api_key'];
      const authHeader = headers.authorization;

      if (xGoogApiKey) {
        clientKey = xGoogApiKey;
        keySource = 'x-goog-api-key Header';
      } else if (authHeader && authHeader.startsWith('Bearer ')) {
        clientKey = authHeader.substring(7);
        keySource = 'Authorization Header';
      } else if (xApiKey) {
        clientKey = xApiKey;
        keySource = 'X-API-Key Header';
      } else if (req.query.key) {
        clientKey = req.query.key;
        keySource = 'Query Parameter';
      }

      // --- 认证逻辑开始 ---

      if (clientKey) {
        // 情况1: 客户端提供了密钥
        if (serverApiKeys.includes(clientKey)) {
          // 密钥有效，通过
          if (this.config.debugMode) {
              this.logger.debug(`[Auth][Debug] API Key 在 '${keySource}' 中找到，验证通过。`);
          }
          if (keySource === 'Query Parameter') {
            delete req.query.key;
          }
          return next();
        } else {
          // 密钥无效，拒绝
          if (this.config.debugMode) {
            this.logger.warn(`[Auth][Debug] 拒绝请求: 无效的 API Key。IP: ${req.ip}, Path: ${req.path}`);
            this.logger.debug(`[Auth][Debug] 来源: ${keySource}`);
            this.logger.debug(`[Auth][Debug] 提供的错误密钥: '${clientKey}'`);
            this.logger.debug(`[Auth][Debug] 已加载的有效密钥: [${serverApiKeys.join(', ')}]`);
          } else {
            this.logger.warn(`[Auth] 拒绝请求: 无效的 API Key。IP: ${req.ip}, Path: ${req.path}`);
          }
          return res.status(401).json({ error: { message: "Invalid API key provided." } });
        }
      }

      // 情况2: 客户端未提供密钥
      // 无论是否在调试模式下，都记录此基本警告
      this.logger.warn(`[Auth] 拒绝受保护的请求: 缺少 API Key。IP: ${req.ip}, Path: ${req.path}`);
      
      // 仅在调试模式下，才记录额外的详细信息
      if (this.config.debugMode) {
        this.logger.debug(`[Auth][Debug] 未在任何标准位置找到API Key。`);
        this.logger.debug(`[Auth][Debug] 搜索的 Headers: ${JSON.stringify(headers, null, 2)}`);
        this.logger.debug(`[Auth][Debug] 搜索的 Query: ${JSON.stringify(req.query)}`);
        this.logger.debug(`[Auth][Debug] 已加载的有效密钥: [${serverApiKeys.join(', ')}]`);
      }

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
    // [修改] body-parser 中间件需要先于我们的调试中间件
    app.use(express.json({ limit: '100mb' }));
    app.use(express.raw({ type: '*/*', limit: '100mb' }));

    // [新增] 插入调试日志中间件。它会在body解析后，但在任何业务逻辑之前运行。
    app.use(this._createDebugLogMiddleware());

    app.get('/admin/set-mode', (req, res) => {
      const newMode = req.query.mode;
      if (newMode === 'fake' || newMode === 'real') {
        this.streamingMode = newMode;
        this.logger.info(`[Admin] 流式模式已切换为: ${this.streamingMode}`);
        res.status(200).send(`流式模式已切换为: ${this.streamingMode}`);
      } else {
        res.status(400).send('无效模式. 请用 "fake" 或 "real".');
      }
    });

    // [新增] 切换调试模式的管理端点
    app.get('/admin/set-debug', (req, res) => {
      const enable = req.query.enable;
      if (enable === 'true') {
        this.config.debugMode = true;
        this.logger.info('[Admin] 调试模式已开启 (Debug Mode ON)');
        res.status(200).send('调试模式已开启 (Debug Mode ON)');
      } else if (enable === 'false') {
        this.config.debugMode = false;
        this.logger.info('[Admin] 调试模式已关闭 (Debug Mode OFF)');
        res.status(200).send('调试模式已关闭 (Debug Mode OFF)');
      } else {
        res.status(400).send('无效的参数. 请使用 ?enable=true 或 ?enable=false');
      }
    });

    app.get('/health', (req, res) => {
      res.status(200).json({
        status: 'healthy',
        uptime: process.uptime(),
        config: {
          streamingMode: this.streamingMode,
          debugMode: this.config.debugMode, // [新增] 在健康检查中报告调试模式状态
          failureThreshold: this.config.failureThreshold,
          immediateSwitchStatusCodes: this.config.immediateSwitchStatusCodes,
          maxRetries: this.config.maxRetries,
          authMode: this.authSource.authMode,
          apiKeyAuth: (this.config.apiKeys && this.config.apiKeys.length > 0) ? 'Enabled' : 'Disabled',
        },
        auth: {
          currentAuthIndex: this.requestHandler.currentAuthIndex,
          availableIndices: this.authSource.getAvailableIndices(),
          totalAuthSources: this.authSource.getAvailableIndices().length,
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

    // --- 新增的 /switch 端点 ---
    app.get('/switch', async (req, res) => {
      this.logger.info('[Admin] 接到 /switch 请求，手动触发账号切换。');

      if (this.requestHandler.isAuthSwitching) {
        const msg = '账号切换已在进行中，请稍后。';
        this.logger.warn(`[Admin] /switch 请求被拒绝: ${msg}`);
        return res.status(429).send(msg);
      }

      const oldIndex = this.requestHandler.currentAuthIndex;

      try {
        await this.requestHandler._switchToNextAuth();
        const newIndex = this.requestHandler.currentAuthIndex;

        const message = `成功将账号从索引 ${oldIndex} 切换到 ${newIndex}。`;
        this.logger.info(`[Admin] 手动切换成功。 ${message}`);
        res.status(200).send(message);
      } catch (error) {
        const errorMessage = `切换账号失败: ${error.message}`;
        this.logger.error(`[Admin] 手动切换失败。错误: ${errorMessage}`);
        res.status(500).send(errorMessage);
      }
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
  try {
    const serverSystem = new ProxyServerSystem();
    // 不再传递 initialAuthIndex，start 方法内部会自行决定
    await serverSystem.start();
  } catch (error) {
    console.error('❌ 服务器启动失败:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  initializeServer();
}

module.exports = { ProxyServerSystem, BrowserManager, initializeServer };
