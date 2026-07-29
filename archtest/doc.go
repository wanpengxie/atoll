// Package archtest is the repository's architecture wall: machine checks for
// invariants the compiler cannot express.
//
// 本包 2026-07 起从零重建（旧墙在 _archtest_legacy/，重建完成后删除）。
// 每堵墙入包前必须过准入三问，答案写在墙自己的头注里：
//
//  1. 它拦哪条系统不变量？（一句话，脱离任何 spec 独立成立）
//  2. 违反了会发生什么坏事？（具体后果——谁的写权被绕过 / 哪份真相长出第二个所有者）
//  3. 为什么爬不到更高强度？（能用编译器/internal 目录做到的恒不写测试）
//
// 判据一句话：这堵墙红了，必须意味着"你改错了"，恒不是仅仅"你改了"。
// 机制只允许四类：import 方向、门面墙（唯一门/白名单调用点）、能力面与 wire
// 词表闭集（黄金表——加方法=扩权，逼显式改表过 review）、黄金清单。
// 恒不钉：实现形状、符号名字、文件位置。全文见 .claude/rules/archtest-discipline.md
// 与 .dalek/pm/archtest-refactor-decisions.md。
package archtest
