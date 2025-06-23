const { firefox } = require('playwright');
const fs = require('fs');
const path = require('path');

const browserExecutablePath = path.join(__dirname, 'camoufox', 'camoufox.exe');
const VALIDATION_LINE_THRESHOLD = 200; // 定义验证的行数阈值

function getNextAuthIndex() {
  const directory = __dirname;
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
  const newIndex = getNextAuthIndex();
  const newAuthFileName = `auth-${newIndex}.json`;

  console.log(`▶️  准备为账户 #${newIndex} 创建新的认证文件: "${newAuthFileName}"`);
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

  // ==================== 智能验证逻辑 ====================
  console.log('\n正在获取并验证登录状态...');
  
  // 1. 先获取状态到内存中，而不是直接写入文件
  const currentState = await context.storageState();

  // 2. 将状态对象格式化为带缩进的JSON字符串，以便计算行数
  const stateString = JSON.stringify(currentState, null, 2);
  const lineCount = stateString.split('\n').length;

  // 3. 检查行数是否达到阈值
  if (lineCount > VALIDATION_LINE_THRESHOLD) {
    console.log(`✅ 状态验证通过 (${lineCount} 行 > ${VALIDATION_LINE_THRESHOLD} 行).`);
    const authFilePath = path.join(__dirname, 'auth', newAuthFileName);
    // 验证通过后，才将字符串写入文件
    fs.writeFileSync(authFilePath, stateString);
    console.log(`   认证信息已成功保存到: ${newAuthFileName}`);
  } else {
    console.log(`❌ 状态验证失败 (${lineCount} 行 <= ${VALIDATION_LINE_THRESHOLD} 行).`);
    console.log('   登录状态似乎为空或无效，文件未被保存。');
    console.log('   请确保您已完全登录账户后再按回车。');
  }
  // ======================================================
  
  await browser.close();
  console.log('浏览器已关闭。');

  process.exit(0);
})();
