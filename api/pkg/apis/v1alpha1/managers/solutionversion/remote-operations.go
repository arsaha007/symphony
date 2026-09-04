/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package solutionversion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	loggercontexts "github.com/eclipse-symphony/symphony/coa/pkg/logger/contexts"
)

const remoteOperationResource = "RemoteOperation"

var remoteClaimLock sync.Mutex

func (s *SolutionVersionManager) executeRemoteGet(ctx context.Context, deployment model.DeploymentSpec, step model.DeploymentStep, namespace string) ([]model.ComponentSpec, error) {
	request := model.ProviderGetRequest{
		AgentRequest: model.AgentRequest{
			OperationID: remoteOperationID(deployment, "get", step),
			Provider:    step.Role,
			Action:      "get",
		},
		Deployment: deployment,
		References: step.Components,
	}
	result, err := s.executeRemoteOperation(ctx, step.Target, namespace, request.OperationID, request)
	if err != nil {
		return nil, err
	}
	var components []model.ComponentSpec
	if len(result.Body) > 0 {
		if err := json.Unmarshal(result.Body, &components); err != nil {
			return nil, fmt.Errorf("decode remote get result: %w", err)
		}
	}
	if result.Error != "" {
		return components, errors.New(result.Error)
	}
	return components, nil
}

func (s *SolutionVersionManager) executeRemoteApply(ctx context.Context, deployment model.DeploymentSpec, step model.DeploymentStep, namespace string) (map[string]model.ComponentResultSpec, error) {
	request := model.ProviderApplyRequest{
		AgentRequest: model.AgentRequest{
			OperationID: remoteOperationID(deployment, "apply", step),
			Provider:    step.Role,
			Action:      "apply",
		},
		Deployment: deployment,
		Step:       step,
		IsDryRun:   deployment.IsDryRun,
	}
	result, err := s.executeRemoteOperation(ctx, step.Target, namespace, request.OperationID, request)
	if err != nil {
		return nil, err
	}
	components := make(map[string]model.ComponentResultSpec)
	if len(result.Body) > 0 {
		if err := json.Unmarshal(result.Body, &components); err != nil {
			return nil, fmt.Errorf("decode remote apply result: %w", err)
		}
	}
	if result.Error != "" {
		return components, errors.New(result.Error)
	}
	return components, nil
}

func (s *SolutionVersionManager) executeRemoteOperation(ctx context.Context, target, namespace, operationID string, request interface{}) (model.AsyncResult, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return model.AsyncResult{}, err
	}
	if err := s.ensureRemoteOperation(ctx, model.RemoteOperation{
		OperationID: operationID,
		Target:      target,
		Namespace:   namespace,
		Request:     payload,
		Status:      model.RemoteOperationQueued,
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		return model.AsyncResult{}, err
	}
	result, err := s.waitForRemoteOperation(ctx, operationID, namespace)
	if err != nil {
		return model.AsyncResult{}, err
	}
	return result, nil
}

func (s *SolutionVersionManager) ensureRemoteOperation(ctx context.Context, operation model.RemoteOperation) error {
	existing, err := s.getRemoteOperation(ctx, operation.OperationID, operation.Namespace)
	if err == nil {
		if existing.Target != operation.Target {
			return fmt.Errorf("remote operation %s target mismatch", operation.OperationID)
		}
		return nil
	}
	if !v1alpha2.IsNotFound(err) {
		return err
	}
	return s.upsertRemoteOperation(ctx, operation)
}

func (s *SolutionVersionManager) waitForRemoteOperation(ctx context.Context, operationID, namespace string) (model.AsyncResult, error) {
	timeout := durationProperty(s.Config.Properties, "remoteAgent.operationTimeout", 10*time.Minute)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		operation, err := s.getRemoteOperation(ctx, operationID, namespace)
		if err == nil && operation.Status == model.RemoteOperationCompleted && operation.Result != nil {
			return *operation.Result, nil
		}
		if err != nil && !v1alpha2.IsNotFound(err) {
			return model.AsyncResult{}, err
		}
		select {
		case <-ctx.Done():
			return model.AsyncResult{}, ctx.Err()
		case <-timer.C:
			return model.AsyncResult{}, fmt.Errorf("timed out waiting for remote operation %s", operationID)
		case <-ticker.C:
		}
	}
}

func (s *SolutionVersionManager) GetRemoteTasks(ctx context.Context, target, namespace string, getAll bool, start string, size int) (model.ProviderPagingRequest, error) {
	if size <= 0 {
		size = 1
	}
	remoteClaimLock.Lock()
	defer remoteClaimLock.Unlock()
	if err := s.cleanupCompletedRemoteOperations(ctx, namespace); err != nil {
		log.WarnfCtx(ctx, "failed to clean completed remote operations: %v", err)
	}
	operations, err := s.listRemoteOperations(ctx, namespace)
	if err != nil {
		return model.ProviderPagingRequest{}, err
	}
	now := time.Now().UTC()
	eligible := make([]model.RemoteOperation, 0)
	for _, operation := range operations {
		if operation.Target != target || operation.Status == model.RemoteOperationCompleted {
			continue
		}
		if operation.Status == model.RemoteOperationQueued || operation.LeaseUntil.Before(now) {
			eligible = append(eligible, operation)
		}
	}
	sort.Slice(eligible, func(left, right int) bool {
		if eligible[left].CreatedAt.Equal(eligible[right].CreatedAt) {
			return eligible[left].OperationID < eligible[right].OperationID
		}
		return eligible[left].CreatedAt.Before(eligible[right].CreatedAt)
	})
	startIndex := 0
	if start != "" && start != "0" {
		for index, operation := range eligible {
			if operation.OperationID == start {
				startIndex = index + 1
				break
			}
		}
	}
	if startIndex >= len(eligible) {
		return model.ProviderPagingRequest{RequestList: []map[string]interface{}{}}, nil
	}
	end := startIndex + size
	if end > len(eligible) {
		end = len(eligible)
	}
	page := model.ProviderPagingRequest{RequestList: make([]map[string]interface{}, 0, end-startIndex)}
	leaseDuration := durationProperty(s.Config.Properties, "remoteAgent.leaseDuration", 2*time.Minute)
	for index := startIndex; index < end; index++ {
		operation := eligible[index]
		operation.Status = model.RemoteOperationLeased
		operation.LeaseUntil = now.Add(leaseDuration)
		operation.Attempts++
		if err := s.upsertRemoteOperation(ctx, operation); err != nil {
			return model.ProviderPagingRequest{}, err
		}
		var request map[string]interface{}
		if err := json.Unmarshal(operation.Request, &request); err != nil {
			return model.ProviderPagingRequest{}, fmt.Errorf("decode remote operation %s: %w", operation.OperationID, err)
		}
		if err := model.SetTaskOperationID(request, operation.OperationID); err != nil {
			return model.ProviderPagingRequest{}, fmt.Errorf("attach tracked operation ID %s: %w", operation.OperationID, err)
		}
		correlationKey := loggercontexts.ConstructHttpHeaderKeyForActivityLogContext(loggercontexts.Activity_CorrelationId)
		if correlationID, ok := request[correlationKey].(string); !ok || correlationID == "" {
			request[correlationKey] = operation.OperationID
		}
		page.RequestList = append(page.RequestList, request)
	}
	if end < len(eligible) {
		page.LastMessageID = eligible[end-1].OperationID
	}
	return page, nil
}

func (s *SolutionVersionManager) HandleRemoteAgentExecuteResult(ctx context.Context, result model.AsyncResult) error {
	return s.handleRemoteAgentExecuteResult(ctx, result, "")
}

func (s *SolutionVersionManager) HandleRemoteAgentExecuteResultForTarget(ctx context.Context, result model.AsyncResult, target string) error {
	return s.handleRemoteAgentExecuteResult(ctx, result, target)
}

func (s *SolutionVersionManager) handleRemoteAgentExecuteResult(ctx context.Context, result model.AsyncResult, expectedTarget string) error {
	if result.OperationID == "" || result.Namespace == "" {
		return fmt.Errorf("remote result operationID and namespace are required")
	}
	operation, err := s.getRemoteOperation(ctx, result.OperationID, result.Namespace)
	if err != nil {
		return err
	}
	if expectedTarget != "" && operation.Target != expectedTarget {
		return v1alpha2.NewCOAError(nil, "remote operation target does not match caller target", v1alpha2.Forbidden)
	}
	if len(result.Logs) > 0 {
		logger.NewRemoteAgentLogFormatter().LogRemoteAgentLogs(result.OperationID, result.Logs)
	}
	operation.Status = model.RemoteOperationCompleted
	operation.LeaseUntil = time.Time{}
	operation.Result = &result
	return s.upsertRemoteOperation(ctx, operation)
}

func (s *SolutionVersionManager) upsertRemoteOperation(ctx context.Context, operation model.RemoteOperation) error {
	_, err := s.StateProvider.Upsert(ctx, states.UpsertRequest{
		Value:    states.StateEntry{ID: operation.OperationID, Body: operation},
		Metadata: remoteOperationMetadata(operation.Namespace),
	})
	return err
}

func (s *SolutionVersionManager) getRemoteOperation(ctx context.Context, operationID, namespace string) (model.RemoteOperation, error) {
	entry, err := s.StateProvider.Get(ctx, states.GetRequest{ID: operationID, Metadata: remoteOperationMetadata(namespace)})
	if err != nil {
		return model.RemoteOperation{}, err
	}
	return decodeRemoteOperation(entry.Body)
}

func (s *SolutionVersionManager) listRemoteOperations(ctx context.Context, namespace string) ([]model.RemoteOperation, error) {
	entries, _, err := s.StateProvider.List(ctx, states.ListRequest{Metadata: remoteOperationMetadata(namespace)})
	if err != nil {
		return nil, err
	}
	operations := make([]model.RemoteOperation, 0, len(entries))
	for _, entry := range entries {
		operation, err := decodeRemoteOperation(entry.Body)
		if err != nil || operation.OperationID == "" {
			continue
		}
		operations = append(operations, operation)
	}
	return operations, nil
}

func (s *SolutionVersionManager) deleteRemoteOperation(ctx context.Context, operationID, namespace string) error {
	return s.StateProvider.Delete(ctx, states.DeleteRequest{ID: operationID, Metadata: remoteOperationMetadata(namespace)})
}

func (s *SolutionVersionManager) cleanupCompletedRemoteOperations(ctx context.Context, namespace string) error {
	retention := durationProperty(s.Config.Properties, "remoteAgent.completedRetention", 24*time.Hour)
	operations, err := s.listRemoteOperations(ctx, namespace)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-retention)
	for _, operation := range operations {
		if operation.Status != model.RemoteOperationCompleted {
			continue
		}
		if operation.CreatedAt.IsZero() || !operation.CreatedAt.Before(cutoff) {
			continue
		}
		if err := s.deleteRemoteOperation(ctx, operation.OperationID, namespace); err != nil && !v1alpha2.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func decodeRemoteOperation(value interface{}) (model.RemoteOperation, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return model.RemoteOperation{}, err
	}
	var operation model.RemoteOperation
	if err := json.Unmarshal(payload, &operation); err != nil {
		return model.RemoteOperation{}, err
	}
	return operation, nil
}

func remoteOperationMetadata(namespace string) map[string]interface{} {
	return map[string]interface{}{
		"namespace": namespace,
		"group":     model.SolutionVersionGroup,
		"version":   "v1",
		"resource":  remoteOperationResource,
	}
}

func remoteOperationID(deployment model.DeploymentSpec, action string, step model.DeploymentStep) string {
	payload, _ := json.Marshal(struct {
		Instance   string               `json:"instance"`
		Generation string               `json:"generation"`
		Hash       string               `json:"hash"`
		JobID      string               `json:"jobID"`
		Action     string               `json:"action"`
		Step       model.DeploymentStep `json:"step"`
	}{
		Instance: deployment.Instance.ObjectMeta.Name, Generation: deployment.Generation,
		Hash: deployment.Hash, JobID: deployment.JobID, Action: action, Step: step,
	})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func stepTargetIsRemoteTarget(deployment model.DeploymentSpec, targetName string) bool {
	target, ok := deployment.Targets[targetName]
	if !ok || target.Spec == nil {
		return false
	}
	for _, component := range target.Spec.Components {
		if component.Type == "remote-agent" {
			return true
		}
	}
	return false
}

func deploymentNamespace(deployment model.DeploymentSpec) string {
	if deployment.Instance.ObjectMeta.Namespace != "" {
		return deployment.Instance.ObjectMeta.Namespace
	}
	return "default"
}

func durationProperty(properties map[string]string, name string, fallback time.Duration) time.Duration {
	if properties == nil {
		return fallback
	}
	value, ok := properties[name]
	if !ok || value == "" {
		return fallback
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
