/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger/contexts"
	"github.com/eclipse-symphony/symphony/remote-agent/agent"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type Client interface {
	Publish(topic string, qos byte, retained bool, payload interface{}) paho.Token
	Subscribe(topic string, qos byte, callback paho.MessageHandler) paho.Token
	IsConnected() bool
}

type Poller struct {
	Agent         agent.Agent
	Client        Client
	RequestTopic  string
	ResponseTopic string
	Target        string
	Namespace     string
	Interval      time.Duration
	Timeout       time.Duration
	PollParallel  int
	PageSize      int
	MaxConcurrent int
	ResultRetries int
	RetryBackoff  time.Duration
	Logger        logger.Logger
	CollectLogs   func() []string
	Tracker       *RequestTracker
	trackerOnce   sync.Once
}

func (p *Poller) Subscribe() error {
	if err := p.validate(); err != nil {
		return err
	}
	token := p.Client.Subscribe(p.ResponseTopic, 0, p.handleResponse)
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("subscribe to %s: %w", p.ResponseTopic, err)
	}
	return nil
}

func (p *Poller) Run(ctx context.Context) error {
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
				p.Logger.ErrorfCtx(ctx, "remote-agent MQTT poll failed: %v", err)
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

func (p *Poller) UpdateTopology(ctx context.Context, topology []byte) error {
	response, err := p.request(ctx, v1alpha2.COARequest{
		Method:      "POST",
		Route:       "updatetopology",
		ContentType: "application/json",
		Parameters: map[string]string{
			"target":    p.Target,
			"__name":    p.Target,
			"namespace": p.Namespace,
		},
		Body: topology,
	})
	if err != nil {
		return err
	}
	return responseError("update topology", response)
}

func (p *Poller) fetch(ctx context.Context, getAll bool, preindex string) (model.ProviderPagingRequest, error) {
	parameters := map[string]string{"target": p.Target, "namespace": p.Namespace}
	if getAll {
		parameters["getAll"] = "true"
		parameters["preindex"] = preindex
		parameters["size"] = fmt.Sprintf("%d", p.pageSize())
	}
	response, err := p.request(ctx, v1alpha2.COARequest{Method: "GET", Route: "tasks", Parameters: parameters})
	if err != nil {
		return model.ProviderPagingRequest{}, err
	}
	if err := responseError("poll tasks", response); err != nil {
		return model.ProviderPagingRequest{}, err
	}
	var page model.ProviderPagingRequest
	if err := json.Unmarshal(response.Body, &page); err != nil {
		return model.ProviderPagingRequest{}, fmt.Errorf("decode task response: %w", err)
	}
	return page, nil
}

func (p *Poller) process(ctx context.Context, requests []map[string]interface{}) error {
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
			if err := p.handleRequest(ctx, request); err != nil {
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

func (p *Poller) handleRequest(ctx context.Context, request map[string]interface{}) error {
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
	resultPayload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode agent result: %w", err)
	}
	resultRequest := v1alpha2.COARequest{
		Method:      "POST",
		Route:       "getResult",
		ContentType: "application/json",
		Body:        resultPayload,
		Parameters:  map[string]string{"target": p.Target, "namespace": p.Namespace},
	}
	var lastErr error
	for attempt := 0; attempt < p.resultRetries(); attempt++ {
		response, err := p.request(requestContext, resultRequest)
		if err == nil {
			err = responseError("post agent result", response)
		}
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt+1 < p.resultRetries() {
			timer := time.NewTimer(p.retryBackoff() * time.Duration(attempt+1))
			select {
			case <-requestContext.Done():
				timer.Stop()
				return requestContext.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func (p *Poller) request(ctx context.Context, request v1alpha2.COARequest) (v1alpha2.COAResponse, error) {
	if !p.Client.IsConnected() {
		return v1alpha2.COAResponse{}, fmt.Errorf("MQTT client is not connected")
	}
	tracker := p.requestTracker()
	requestID, responseChannel, err := tracker.Register()
	if err != nil {
		return v1alpha2.COAResponse{}, err
	}
	defer tracker.Delete(requestID)
	if request.Metadata == nil {
		request.Metadata = make(map[string]string)
	}
	request.Metadata["request-id"] = requestID
	payload, err := json.Marshal(request)
	if err != nil {
		return v1alpha2.COAResponse{}, fmt.Errorf("encode MQTT request: %w", err)
	}
	token := p.Client.Publish(p.RequestTopic, 0, false, payload)
	token.Wait()
	if err := token.Error(); err != nil {
		return v1alpha2.COAResponse{}, fmt.Errorf("publish MQTT request: %w", err)
	}
	timer := time.NewTimer(p.responseTimeout())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return v1alpha2.COAResponse{}, ctx.Err()
	case <-timer.C:
		return v1alpha2.COAResponse{}, fmt.Errorf("timed out waiting for MQTT response")
	case response := <-responseChannel:
		return response, nil
	}
}

func (p *Poller) handleResponse(_ paho.Client, message paho.Message) {
	var response v1alpha2.COAResponse
	if err := json.Unmarshal(message.Payload(), &response); err != nil {
		if p.Logger != nil {
			p.Logger.Errorf("decode MQTT response: %v", err)
		}
		return
	}
	requestID := response.Metadata["request-id"]
	if requestID == "" || !p.requestTracker().Resolve(requestID, response) {
		if p.Logger != nil {
			p.Logger.Warnf("ignoring MQTT response with unknown request-id %q", requestID)
		}
	}
}

func responseError(operation string, response v1alpha2.COAResponse) error {
	if response.State == v1alpha2.OK || response.State == v1alpha2.Accepted {
		return nil
	}
	return fmt.Errorf("%s failed with state %d: %s", operation, response.State, string(response.Body))
}

func (p *Poller) validate() error {
	if p.Client == nil {
		return fmt.Errorf("MQTT client is required")
	}
	if p.RequestTopic == "" || p.ResponseTopic == "" {
		return fmt.Errorf("request and response topics are required")
	}
	if p.Target == "" || p.Namespace == "" {
		return fmt.Errorf("target and namespace are required")
	}
	p.requestTracker()
	return nil
}

func (p *Poller) requestTracker() *RequestTracker {
	p.trackerOnce.Do(func() {
		if p.Tracker == nil {
			p.Tracker = NewRequestTracker(10_000, 5*time.Minute)
		}
	})
	return p.Tracker
}

func (p *Poller) pollInterval() time.Duration {
	if p.Interval <= 0 {
		return 3 * time.Second
	}
	return p.Interval
}

func (p *Poller) responseTimeout() time.Duration {
	if p.Timeout <= 0 {
		return 30 * time.Second
	}
	return p.Timeout
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

type trackedRequest struct {
	created  time.Time
	response chan v1alpha2.COAResponse
}

type RequestTracker struct {
	mu         sync.Mutex
	items      map[string]trackedRequest
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
}

func NewRequestTracker(maxEntries int, ttl time.Duration) *RequestTracker {
	return &RequestTracker{items: make(map[string]trackedRequest), maxEntries: maxEntries, ttl: ttl, now: time.Now}
}

func (t *RequestTracker) Register() (string, <-chan v1alpha2.COAResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.purgeExpiredLocked()
	if t.maxEntries > 0 && len(t.items) >= t.maxEntries {
		return "", nil, fmt.Errorf("MQTT request tracker capacity reached")
	}
	id := uuid.NewString()
	response := make(chan v1alpha2.COAResponse, 1)
	t.items[id] = trackedRequest{created: t.now(), response: response}
	return id, response, nil
}

func (t *RequestTracker) Resolve(id string, response v1alpha2.COAResponse) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	item, ok := t.items[id]
	if !ok || t.expired(item) {
		delete(t.items, id)
		return false
	}
	select {
	case item.response <- response:
	default:
	}
	return true
}

func (t *RequestTracker) Delete(id string) {
	t.mu.Lock()
	delete(t.items, id)
	t.mu.Unlock()
}

func (t *RequestTracker) PurgeExpired() {
	t.mu.Lock()
	t.purgeExpiredLocked()
	t.mu.Unlock()
}

func (t *RequestTracker) purgeExpiredLocked() {
	for id, item := range t.items {
		if t.expired(item) {
			delete(t.items, id)
		}
	}
}

func (t *RequestTracker) expired(item trackedRequest) bool {
	return t.ttl > 0 && t.now().Sub(item.created) > t.ttl
}
