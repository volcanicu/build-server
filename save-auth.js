const { firefox } = require('playwright');
const fs = require('fs');
const path = require('path');

// --- 配置常量 ---
const browserExecutablePath = path.join(__dirname, 'camoufox', 'camoufox.exe');
const VALIDATION_LINE_THRESHOLD = 200; // 定义验证的行数阈值
const AUTH_DIR = 'auth'; // 格式化认证文件的文件夹
const SINGLE_LINE_AUTH_DIR = 'single-line-auth'; // 单行认证文件的文件夹

/**
 * 确保指定的目录存在，如果不存在则创建它。
 * @param {string} dirPath - 要检查和创建的目录的路径。
 */
function ensureDirectoryExists(dirPath) {
  if (!fs.existsSync(dirPath)) {
    console.log(`📂 目录 "${path.basename(dirPath)}" 不存在，正在创建...`);
    fs.mkdirSync(dirPath);
  }
}

/**
 * 从 'auth' 目录中获取下一个可用的认证文件索引。
 * @returns {number} - 下一个可用的索引值。
 */
function getNextAuthIndex() {
  const directory = path.join(__dirname, AUTH_DIR);
  // 如果 'auth' 目录还不存在，说明是第一次运行，直接返回索引 1
  if (!fs.existsSync(directory)) {
    return 1;
  }

  const files = fs.readdirSync(directory);
  const authRegex = /^auth-(\d+)\.json$/;

  let maxIndex = 0;
  files.forEach(file => {
    const match = file.match(authRegex);
    if (match) {
      const currentIndex = parseInt(match[1], 10);
      if (currentIndex > maxIndex) {
        maxIndex = currentIndex;
      }
    }
  });
  return maxIndex + 1;
}

(async () => {
  // 1. 脚本启动时，首先确保目标文件夹存在
  const authDirPath = path.join(__dirname, AUTH_DIR);
  const singleLineAuthDirPath = path.join(__dirname, SINGLE_LINE_AUTH_DIR);
  ensureDirectoryExists(authDirPath);
  ensureDirectoryExists(singleLineAuthDirPath);

  // 2. 获取新的文件索引和文件名
  const newIndex = getNextAuthIndex();
  const newAuthFileName = `auth-${newIndex}.json`;
  const newSingleLineAuthFileName = `auth-single-${newIndex}.json`;

  console.log(`▶️  准备为账户 #${newIndex} 创建新的认证文件...`);
  console.log(`▶️  启动浏览器: ${browserExecutablePath}`);

  const browser = await firefox.launch({
    headless: false,
    executablePath: browserExecutablePath,
  });

  const context = await browser.newContext();
  const page = await context.newPage();

  console.log('\n--- 请在新打开的 Camoufox 窗口中完成以下操作 ---');
  console.log('1. 在网页上【完全登录】您的Google账户。');
  console.log('2. 登录成功后，请不要关闭浏览器窗口。');
  console.log('3. 回到这个终端，然后按 "Enter" 键继续...');

  await page.goto('https://accounts.google.com/');

  await new Promise(resolve => process.stdin.once('data', resolve));

  // ==================== 智能验证与双文件保存逻辑 ====================
  console.log('\n正在获取并验证登录状态...');
  
  // 1. 获取状态到内存中
  const currentState = await context.storageState();

  // 2. 将状态对象格式化为带缩进的JSON字符串，用于验证和保存为可读文件
  const prettyStateString = JSON.stringify(currentState, null, 2);
  const lineCount = prettyStateString.split('\n').length;

  // 3. 检查行数是否达到阈值
  if (lineCount > VALIDATION_LINE_THRESHOLD) {
    console.log(`✅ 状态验证通过 (${lineCount} 行 > ${VALIDATION_LINE_THRESHOLD} 行).`);
    
    // 生成单行JSON字符串
    const singleLineStateString = JSON.stringify(currentState);

    // 定义两个文件的完整路径
    const prettyAuthFilePath = path.join(authDirPath, newAuthFileName);
    const singleLineAuthFilePath = path.join(singleLineAuthDirPath, newSingleLineAuthFileName);
    
    // 写入格式化的文件
    fs.writeFileSync(prettyAuthFilePath, prettyStateString);
    console.log(`   📄 格式化文件已保存到: ${path.join(AUTH_DIR, newAuthFileName)}`);
    
    // 写入单行文件
    fs.writeFileSync(singleLineAuthFilePath, singleLineStateString);
    console.log(`    单行压缩文件已保存到: ${path.join(SINGLE_LINE_AUTH_DIR, newSingleLineAuthFileName)}`);

  } else {
    console.log(`❌ 状态验证失败 (${lineCount} 行 <= ${VALIDATION_LINE_THRESHOLD} 行).`);
    console.log('   登录状态似乎为空或无效，文件未被保存。');
    console.log('   请确保您已完全登录账户后再按回车。');
  }
  // ===================================================================
  
  await browser.close();
  console.log('\n浏览器已关闭。');

  process.exit(0);
})();
