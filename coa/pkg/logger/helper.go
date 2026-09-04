//go:build !remote

/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package logger

import "github.com/eclipse-symphony/symphony/coa/pkg/logger/hooks"

func NewLogger(name string) Logger {
	globalLoggersLock.Lock()
	defer globalLoggersLock.Unlock()

	log, ok := globalLoggers[name]
	if !ok {
		log = newCoaLogger(name, hooks.ContextHookOptions{DiagnosticLogContextEnabled: true, ActivityLogContextEnabled: false, Folding: true})
		globalLoggers[name] = log
	}
	return log
}
