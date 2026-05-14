// 测试1：IIFE 格式
var test1 = (function () {
  console.log('IIFE format test');
  return 'test1';
})();

// 测试2：直接代码
console.log('Direct code test');

// 测试3：正常的 Service Worker 代码
chrome.runtime.onInstalled.addListener(() => {
  console.log('Service Worker installed');
});
