/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	target "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
)

type Agent struct {
	Providers  map[string]target.ITargetProvider
	Logger     logger.Logger
	Executions *ExecutionCache
}

func (a *Agent) Handle(payload []byte, ctx context.Context) model.AsyncResult {
	request, err := model.DecodeAgentRequest(payload)
	if err != nil {
		return failure(request.OperationID, err)
	}
	if request.OperationID != "" && a.Executions != nil {
		return a.Executions.Do(ctx, request.OperationID, func() model.AsyncResult {
			return a.handle(request, payload, ctx)
		})
	}
	return a.handle(request, payload, ctx)
}

func (a *Agent) handle(request model.AgentRequest, payload []byte, ctx context.Context) model.AsyncResult {

	provider, ok := a.Providers[request.Provider]
	if !ok || provider == nil {
		return failure(request.OperationID, fmt.Errorf("provider role %q not found", request.Provider))
	}

	switch request.Action {
	case "get":
		var getRequest model.ProviderGetRequest
		if err := json.Unmarshal(payload, &getRequest); err != nil {
			return failure(request.OperationID, err)
		}
		specs, providerErr := provider.Get(ctx, getRequest.Deployment, getRequest.References)
		return encodeResult(request.OperationID, specs, providerErr)
	case "apply":
		var applyRequest model.ProviderApplyRequest
		if err := json.Unmarshal(payload, &applyRequest); err != nil {
			return failure(request.OperationID, err)
		}
		results, providerErr := provider.Apply(ctx, applyRequest.Deployment, applyRequest.Step, applyRequest.IsDryRun)
		return encodeResult(request.OperationID, results, providerErr)
	case "getValidationRule":
		return encodeResult(request.OperationID, provider.GetValidationRule(ctx), nil)
	default:
		return failure(request.OperationID, fmt.Errorf("action %q not found", request.Action))
	}
}

type executionEntry struct {
	created time.Time
	done    chan struct{}
	result  model.AsyncResult
}

type ExecutionCache struct {
	mu         sync.Mutex
	entries    map[string]*executionEntry
	ttl        time.Duration
	maxEntries int
	now        func() time.Time
}

func NewExecutionCache(maxEntries int, ttl time.Duration) *ExecutionCache {
	return &ExecutionCache{entries: make(map[string]*executionEntry), ttl: ttl, maxEntries: maxEntries, now: time.Now}
}

func (c *ExecutionCache) Do(ctx context.Context, operationID string, execute func() model.AsyncResult) model.AsyncResult {
	c.mu.Lock()
	c.purgeLocked()
	if entry, ok := c.entries[operationID]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return failure(operationID, ctx.Err())
		case <-entry.done:
			return entry.result
		}
	}
	c.enforceCapacityLocked()
	if c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		c.mu.Unlock()
		return failure(operationID, fmt.Errorf("execution cache capacity reached"))
	}
	entry := &executionEntry{created: c.now(), done: make(chan struct{})}
	c.entries[operationID] = entry
	c.mu.Unlock()

	entry.result = execute()
	close(entry.done)
	return entry.result
}

func (c *ExecutionCache) purgeLocked() {
	if c.ttl <= 0 {
		return
	}
	for operationID, entry := range c.entries {
		select {
		case <-entry.done:
			if c.now().Sub(entry.created) > c.ttl {
				delete(c.entries, operationID)
			}
		default:
		}
	}
}

func (c *ExecutionCache) enforceCapacityLocked() {
	if c.maxEntries <= 0 || len(c.entries) < c.maxEntries {
		return
	}
	type completedEntry struct {
		operationID string
		created     time.Time
	}
	completed := make([]completedEntry, 0, len(c.entries))
	for operationID, entry := range c.entries {
		select {
		case <-entry.done:
			completed = append(completed, completedEntry{operationID: operationID, created: entry.created})
		default:
		}
	}
	sort.Slice(completed, func(left, right int) bool { return completed[left].created.Before(completed[right].created) })
	for _, entry := range completed {
		if len(c.entries) < c.maxEntries {
			break
		}
		delete(c.entries, entry.operationID)
	}
}

func encodeResult(operationID string, value interface{}, providerErr error) model.AsyncResult {
	body, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return failure(operationID, marshalErr)
	}

	result := model.AsyncResult{OperationID: operationID, Body: body}
	if providerErr != nil {
		result.Error = providerErr.Error()
	}
	return result
}

func failure(operationID string, err error) model.AsyncResult {
	return model.AsyncResult{OperationID: operationID, Error: err.Error()}
}
