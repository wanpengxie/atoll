// Package claudecode adapts the Claude Code SDK to base.Engine's asynchronous
// EventPort contract. SDK turns run outside the mailbox arbitrator; session ids
// and complete tool/turn phases are reported back to base.
package claudecode
