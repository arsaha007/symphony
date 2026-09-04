//go:build remote

/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package logger

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger/hooks"
)

const (
	defaultRemoteLogName      = "symphony-remote-agent.log"
	defaultRemoteLogSizeMB    = 10
	defaultRemoteLogBackups   = 5
	defaultRemoteLogAgeDays   = 7
	maxRemoteLogResponseBytes = 1024 * 1024
	remoteLogPathEnv          = "SYMPHONY_REMOTE_AGENT_LOG_FILE_PATH"
	remoteLogSizeEnv          = "SYMPHONY_REMOTE_AGENT_LOG_MAX_SIZE_MB"
	remoteLogBackupsEnv       = "SYMPHONY_REMOTE_AGENT_LOG_MAX_BACKUPS"
	remoteLogAgeEnv           = "SYMPHONY_REMOTE_AGENT_LOG_MAX_AGE_DAYS"
)

type FileLogger struct {
	*coaLogger
	file    *rollingFile
	capture *captureWriter
}

func defaultContextHookOptions() hooks.ContextHookOptions {
	return hooks.ContextHookOptions{DiagnosticLogContextEnabled: true, ActivityLogContextEnabled: false, Folding: true}
}

func newFileLogger(name string) (*FileLogger, error) {
	path := os.Getenv(remoteLogPathEnv)
	if path == "" {
		path = filepath.Join(os.TempDir(), defaultRemoteLogName)
	}
	file, err := newRollingFile(
		path,
		envInt(remoteLogSizeEnv, defaultRemoteLogSizeMB)*1024*1024,
		envInt(remoteLogBackupsEnv, defaultRemoteLogBackups),
		time.Duration(envInt(remoteLogAgeEnv, defaultRemoteLogAgeDays))*24*time.Hour,
	)
	if err != nil {
		return nil, err
	}
	capture := &captureWriter{maxBytes: maxRemoteLogResponseBytes}
	base := newCoaLogger(name, defaultContextHookOptions())
	base.logger.SetOutput(io.MultiWriter(os.Stdout, file, capture))
	return &FileLogger{coaLogger: base, file: file, capture: capture}, nil
}

func (l *FileLogger) CollectLogs() []string {
	return l.capture.DrainLines()
}

func (l *FileLogger) Close() error {
	return l.file.Close()
}

type captureWriter struct {
	mu       sync.Mutex
	buffer   []byte
	maxBytes int
}

func (w *captureWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer = append(w.buffer, data...)
	if len(w.buffer) > w.maxBytes {
		w.buffer = append([]byte(nil), w.buffer[len(w.buffer)-w.maxBytes:]...)
		if newline := bytes.IndexByte(w.buffer, '\n'); newline >= 0 {
			w.buffer = w.buffer[newline+1:]
		}
	}
	return len(data), nil
}

func (w *captureWriter) DrainLines() []string {
	w.mu.Lock()
	data := append([]byte(nil), w.buffer...)
	w.buffer = w.buffer[:0]
	w.mu.Unlock()

	lines := make([]string, 0)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

type rollingFile struct {
	mu         sync.Mutex
	path       string
	file       *os.File
	size       int64
	maxBytes   int64
	maxBackups int
	maxAge     time.Duration
}

func newRollingFile(path string, maxBytes, maxBackups int, maxAge time.Duration) (*rollingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &rollingFile{
		path: path, file: file, size: info.Size(), maxBytes: int64(maxBytes), maxBackups: maxBackups, maxAge: maxAge,
	}, nil
}

func (w *rollingFile) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.maxBytes > 0 && w.size+int64(len(data)) > w.maxBytes {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	written, err := w.file.Write(data)
	w.size += int64(written)
	return written, err
}

func (w *rollingFile) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *rollingFile) rotate() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	rotated := fmt.Sprintf("%s.%s", w.path, time.Now().UTC().Format("20060102-150405.000000000"))
	if err := os.Rename(w.path, rotated); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	w.file = file
	w.size = 0
	w.prune()
	return nil
}

func (w *rollingFile) prune() {
	matches, _ := filepath.Glob(w.path + ".*")
	type backup struct {
		path string
		info os.FileInfo
	}
	backups := make([]backup, 0, len(matches))
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			continue
		}
		if w.maxAge > 0 && time.Since(info.ModTime()) > w.maxAge {
			_ = os.Remove(match)
			continue
		}
		backups = append(backups, backup{path: match, info: info})
	}
	sort.Slice(backups, func(left, right int) bool {
		return backups[left].info.ModTime().After(backups[right].info.ModTime())
	})
	for index := w.maxBackups; w.maxBackups >= 0 && index < len(backups); index++ {
		_ = os.Remove(backups[index].path)
	}
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
