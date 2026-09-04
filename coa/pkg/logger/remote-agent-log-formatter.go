/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package logger

import (
	"encoding/json"
	"fmt"
	"strings"
)

type RemoteAgentLogFormatter struct {
	Logger Logger
}

type RemoteAgentLogEntry struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
	Scope   string `json:"scope"`
	Caller  string `json:"caller"`
	Time    string `json:"time"`
}

func NewRemoteAgentLogFormatter() *RemoteAgentLogFormatter {
	return &RemoteAgentLogFormatter{Logger: NewLogger("remote-agent")}
}

func (f *RemoteAgentLogFormatter) LogRemoteAgentLogs(operationID string, lines []string) {
	if f == nil || f.Logger == nil {
		return
	}
	for _, line := range lines {
		entry, ok := parseRemoteAgentLog(line)
		message := strings.TrimSpace(line)
		level := "info"
		if ok {
			message = entry.Message
			level = entry.Level
			if entry.Scope != "" {
				message = fmt.Sprintf("[%s] %s", entry.Scope, message)
			}
		}
		message = fmt.Sprintf("remote operation %s: %s", operationID, message)
		switch strings.ToLower(level) {
		case "debug":
			f.Logger.Debug(message)
		case "warn", "warning":
			f.Logger.Warn(message)
		case "error", "fatal":
			f.Logger.Error(message)
		default:
			f.Logger.Info(message)
		}
	}
}

func parseRemoteAgentLog(line string) (RemoteAgentLogEntry, bool) {
	var entry RemoteAgentLogEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.Message == "" {
		return RemoteAgentLogEntry{}, false
	}
	return entry, true
}
