//go:build remote

/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package logger

func NewLogger(name string) Logger {
	globalLoggersLock.Lock()
	defer globalLoggersLock.Unlock()

	for _, existing := range globalLoggers {
		return existing
	}
	log, err := newFileLogger(name)
	if err != nil {
		fallback := newCoaLogger(name, defaultContextHookOptions())
		fallback.Errorf("failed to initialize remote-agent file logging: %v", err)
		globalLoggers[name] = fallback
		return fallback
	}
	globalLoggers[name] = log
	return log
}
