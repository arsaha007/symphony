/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger/contexts"
	"github.com/eclipse-symphony/symphony/remote-agent/agent"
)

type Poller struct {
	Agent         agent.Agent
	Client        *http.Client
	RequestURL    string
	ResponseURL   string
	Target        string
	Namespace     string
	Interval      time.Duration
	PollParallel  int
	PageSize      int
	Logger        logger.Logger
	CollectLogs   func() []string
	MaxConcurrent int
	ResultRetries int
	RetryBackoff  time.Duration
}

func (p *Poller) Run(ctx context.Context) error {
	if err := p.validate(); err != nil {
		return err
	}
	if err := p.Recover(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(p.pollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.PollOnce(ctx); err != nil && p.Logger != nil {
				p.Logger.ErrorfCtx(ctx, "remote-agent HTTP poll failed: %v", err)
			}
		}
	}
}

func (p *Poller) Recover(ctx context.Context) error {
	preindex := "0"
	for {
		page, err := p.fetch(ctx, true, preindex)
		if err != nil {
			return err
		}
		if err := p.process(ctx, page.RequestList); err != nil {
			return err
		}
		if page.LastMessageID == "" {
			return nil
		}
		preindex = page.LastMessageID
	}
}

func (p *Poller) PollOnce(ctx context.Context) error {
	parallel := p.PollParallel
	if parallel <= 0 {
		parallel = 3
	}

	requests := make([]map[string]interface{}, 0, parallel)
	var requestsMu sync.Mutex
	var firstErr error
	var errMu sync.Mutex
	var waitGroup sync.WaitGroup
	for index := 0; index < parallel; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			page, err := p.fetch(ctx, false, "")
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			requestsMu.Lock()
			requests = append(requests, page.RequestList...)
			requestsMu.Unlock()
		}()
	}
	waitGroup.Wait()
	return errors.Join(firstErr, p.process(ctx, requests))
}

func (p *Poller) fetch(ctx context.Context, getAll bool, preindex string) (model.ProviderPagingRequest, error) {
	endpoint, err := url.Parse(p.RequestURL)
	if err != nil {
		return model.ProviderPagingRequest{}, fmt.Errorf("parse request endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("target", p.Target)
	query.Set("namespace", p.Namespace)
	if getAll {
		query.Set("getAll", "true")
		query.Set("preindex", preindex)
		query.Set("size", strconv.Itoa(p.pageSize()))
	}
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return model.ProviderPagingRequest{}, fmt.Errorf("create task request: %w", err)
	}
	response, err := p.Client.Do(request)
	if err != nil {
		return model.ProviderPagingRequest{}, fmt.Errorf("poll tasks: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return model.ProviderPagingRequest{}, fmt.Errorf("poll tasks returned %s: %s", response.Status, string(body))
	}

	var page model.ProviderPagingRequest
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		return model.ProviderPagingRequest{}, fmt.Errorf("decode task response: %w", err)
	}
	return page, nil
}

func (p *Poller) process(ctx context.Context, requests []map[string]interface{}) error {
	if len(requests) == 0 {
		return nil
	}

	limit := p.MaxConcurrent
	if limit <= 0 {
		limit = 3
	}
	semaphore := make(chan struct{}, limit)
	var firstErr error
	var errMu sync.Mutex
	var waitGroup sync.WaitGroup
	for _, request := range requests {
		request := request
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}
			if err := p.handle(ctx, request); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	waitGroup.Wait()
	return firstErr
}

func (p *Poller) handle(ctx context.Context, request map[string]interface{}) error {
	operationID, err := model.TaskOperationID(request)
	if err != nil {
		return fmt.Errorf("reject uncorrelated task: %w", err)
	}
	correlationID, ok := request[contexts.ConstructHttpHeaderKeyForActivityLogContext(contexts.Activity_CorrelationId)].(string)
	if !ok || correlationID == "" {
		correlationID = operationID
	}
	requestContext := context.WithValue(ctx, contexts.Activity_CorrelationId, correlationID)
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode agent request: %w", err)
	}

	result := p.Agent.Handle(payload, requestContext)
	if result.OperationID != operationID {
		return fmt.Errorf("agent result operation ID %q does not match task %q", result.OperationID, operationID)
	}
	result.Namespace = p.Namespace
	if p.CollectLogs != nil {
		result.Logs = p.CollectLogs()
	}
	return p.sendResult(requestContext, result)
}

func (p *Poller) sendResult(ctx context.Context, result model.AsyncResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode agent result: %w", err)
	}
	endpoint, err := url.Parse(p.ResponseURL)
	if err != nil {
		return fmt.Errorf("parse response endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("target", p.Target)
	query.Set("namespace", p.Namespace)
	endpoint.RawQuery = query.Encode()

	var lastErr error
	for attempt := 0; attempt < p.resultRetries(); attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("create result request: %w", err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := p.Client.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 64*1024))
			response.Body.Close()
			if readErr == nil && response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
				return nil
			}
			if readErr != nil {
				lastErr = readErr
			} else {
				lastErr = fmt.Errorf("post agent result returned %s: %s", response.Status, string(body))
			}
		} else {
			lastErr = fmt.Errorf("post agent result: %w", err)
		}
		if attempt+1 < p.resultRetries() {
			timer := time.NewTimer(p.retryBackoff() * time.Duration(attempt+1))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func (p *Poller) validate() error {
	if p.Client == nil {
		return fmt.Errorf("HTTP client is required")
	}
	if p.RequestURL == "" || p.ResponseURL == "" {
		return fmt.Errorf("request and response endpoints are required")
	}
	if p.Target == "" || p.Namespace == "" {
		return fmt.Errorf("target and namespace are required")
	}
	return nil
}

func (p *Poller) pollInterval() time.Duration {
	if p.Interval <= 0 {
		return 10 * time.Second
	}
	return p.Interval
}

func (p *Poller) pageSize() int {
	if p.PageSize <= 0 {
		return 10
	}
	return p.PageSize
}

func (p *Poller) resultRetries() int {
	if p.ResultRetries <= 0 {
		return 3
	}
	return p.ResultRetries
}

func (p *Poller) retryBackoff() time.Duration {
	if p.RetryBackoff <= 0 {
		return time.Second
	}
	return p.RetryBackoff
}
