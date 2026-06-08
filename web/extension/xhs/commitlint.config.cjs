module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [
      2,
      'always',
      [
        'feat',     // 新功能
        'fix',      // 修复bug
        'docs',     // 文档
        'style',    // 格式化，不影响代码运行的变动
        'refactor', // 重构
        'test',     // 测试
        'chore',    // 构建过程或辅助工具的变动
        'perf',     // 性能优化
        'revert',   // 回退
        'build',    // 构建相关
        'ci'        // CI/CD相关
      ]
    ],
    'subject-case': [0],
    'subject-full-stop': [0, 'never'],
    'header-max-length': [2, 'always', 100]
  }
};