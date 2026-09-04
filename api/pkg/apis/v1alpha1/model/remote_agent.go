/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const (
	OperationIDField       = "operationId"
	LegacyOperationIDField = "operationID"
)

func RemoteAgentCredentialName(namespace, target string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + target))
	return "remote-agent-" + hex.EncodeToString(digest[:16])
}

type AgentRequest struct {
	OperationID string `json:"operationId"`
	Provider    string `json:"provider"`
	Action      string `json:"action"`
}

type ProviderGetRequest struct {
	AgentRequest
	Deployment DeploymentSpec  `json:"deployment"`
	References []ComponentStep `json:"references"`
}

type ProviderApplyRequest struct {
	AgentRequest
	Deployment DeploymentSpec `json:"deployment"`
	Step       DeploymentStep `json:"step"`
	IsDryRun   bool           `json:"isDryRun,omitempty"`
}

type ProviderGetValidationRuleRequest struct {
	AgentRequest
}

type ProviderPagingRequest struct {
	RequestList   []map[string]interface{} `json:"requestList"`
	LastMessageID string                   `json:"lastMessageID"`
}

type AsyncResult struct {
	OperationID string   `json:"operationId"`
	Namespace   string   `json:"namespace"`
	Error       string   `json:"error,omitempty"`
	Body        []byte   `json:"body,omitempty"`
	Logs        []string `json:"logs,omitempty"`
}

type SymphonyEndpoint struct {
	BaseURL          string `json:"baseUrl"`
	RequestEndpoint  string `json:"requestEndpoint,omitempty"`
	ResponseEndpoint string `json:"responseEndpoint,omitempty"`
}

type RemoteOperationStatus string

const (
	RemoteOperationQueued    RemoteOperationStatus = "queued"
	RemoteOperationLeased    RemoteOperationStatus = "leased"
	RemoteOperationCompleted RemoteOperationStatus = "completed"
)

type RemoteOperation struct {
	OperationID string                `json:"operationId"`
	Target      string                `json:"target"`
	Namespace   string                `json:"namespace"`
	Request     json.RawMessage       `json:"request"`
	Status      RemoteOperationStatus `json:"status"`
	CreatedAt   time.Time             `json:"createdAt"`
	LeaseUntil  time.Time             `json:"leaseUntil,omitempty"`
	Attempts    int                   `json:"attempts,omitempty"`
	Result      *AsyncResult          `json:"result,omitempty"`
}

func TaskOperationID(task map[string]interface{}) (string, error) {
	canonical, canonicalPresent := task[OperationIDField]
	legacy, legacyPresent := task[LegacyOperationIDField]
	canonicalID, err := operationIDValue(OperationIDField, canonical, canonicalPresent)
	if err != nil {
		return "", err
	}
	legacyID, err := operationIDValue(LegacyOperationIDField, legacy, legacyPresent)
	if err != nil {
		return "", err
	}
	if canonicalID != "" && legacyID != "" && canonicalID != legacyID {
		return "", fmt.Errorf("conflicting %s and %s values", OperationIDField, LegacyOperationIDField)
	}
	if canonicalID != "" {
		return canonicalID, nil
	}
	if legacyID != "" {
		return legacyID, nil
	}
	return "", fmt.Errorf("task is missing %s", OperationIDField)
}

func SetTaskOperationID(task map[string]interface{}, operationID string) error {
	if operationID == "" {
		return fmt.Errorf("operation ID is required")
	}
	_, canonicalPresent := task[OperationIDField]
	_, legacyPresent := task[LegacyOperationIDField]
	existing, err := TaskOperationID(task)
	if err != nil {
		if canonicalPresent || legacyPresent {
			return err
		}
	} else if existing != operationID {
		return fmt.Errorf("task operation ID %q does not match tracked operation %q", existing, operationID)
	}
	task[OperationIDField] = operationID
	task[LegacyOperationIDField] = operationID
	return nil
}

func DecodeAgentRequest(payload []byte) (AgentRequest, error) {
	var task map[string]interface{}
	if err := json.Unmarshal(payload, &task); err != nil {
		return AgentRequest{}, err
	}
	operationID, err := TaskOperationID(task)
	if err != nil {
		return AgentRequest{}, err
	}
	provider, _ := task["provider"].(string)
	action, _ := task["action"].(string)
	return AgentRequest{OperationID: operationID, Provider: provider, Action: action}, nil
}

func DecodeAsyncResult(payload []byte) (AsyncResult, error) {
	var result AsyncResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return AsyncResult{}, err
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(payload, &fields); err != nil {
		return AsyncResult{}, err
	}
	operationID, err := TaskOperationID(fields)
	if err != nil {
		return AsyncResult{}, err
	}
	result.OperationID = operationID
	return result, nil
}

func operationIDValue(field string, value interface{}, present bool) (string, error) {
	if !present {
		return "", nil
	}
	operationID, ok := value.(string)
	if !ok || operationID == "" {
		return "", fmt.Errorf("%s must be a non-empty string", field)
	}
	return operationID, nil
}
