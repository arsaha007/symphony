/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package remoteagent

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/targets"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	coa_utils "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/utils"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
)

type RemoteAgentSchedulerManager struct {
	managers.Manager
	TargetsManager *targets.TargetsManager
}

var schedulerLog = logger.NewLogger("remote-agent-scheduler")

func (s *RemoteAgentSchedulerManager) Init(ctx *contexts.VendorContext, config managers.ManagerConfig, configuredProviders map[string]providers.IProvider) error {
	if err := s.Manager.Init(ctx, config, configuredProviders); err != nil {
		return err
	}
	s.TargetsManager = &targets.TargetsManager{}
	return s.TargetsManager.Init(ctx, config, configuredProviders)
}

func (s *RemoteAgentSchedulerManager) Enabled() bool {
	return !strings.EqualFold(s.Config.Properties["enabled"], "false")
}

func (s *RemoteAgentSchedulerManager) Reconcil() []error { return nil }

func (s *RemoteAgentSchedulerManager) Poll() []error {
	ctx := context.Background()
	targetStates, err := s.TargetsManager.ListState(ctx, "")
	if err != nil {
		return []error{err}
	}
	desiredVersion := os.Getenv("AGENT_VERSION")
	agentPath := os.Getenv("AGENT_PATH")
	errorsFound := make([]error, 0)
	for _, targetState := range targetStates {
		componentIndex := remoteAgentComponentIndex(targetState)
		if componentIndex < 0 {
			continue
		}
		status, ok := remoteAgentStatus(targetState, targetState.Spec.Components[componentIndex].Name)
		if !ok {
			continue
		}
		desiredThumbprint := ""
		if s.TargetsManager.SecretProvider != nil {
			certificateNamespace := os.Getenv("POD_NAMESPACE")
			if certificateNamespace == "" {
				certificateNamespace = targetState.ObjectMeta.Namespace
			}
			certificate, readErr := s.TargetsManager.SecretProvider.Read(
				ctx,
				model.RemoteAgentCredentialName(targetState.ObjectMeta.Namespace, targetState.ObjectMeta.Name)+"-tls",
				"tls.crt",
				coa_utils.EvaluationContext{Namespace: certificateNamespace},
			)
			if readErr == nil {
				desiredThumbprint, readErr = certificateThumbprint([]byte(certificate))
			}
			if readErr != nil {
				schedulerLog.WarnfCtx(ctx, "failed to read desired certificate for target %s: %v", targetState.ObjectMeta.Name, readErr)
			}
		}
		desiredChecksum := ""
		if desiredVersion != "" && status["version"] != desiredVersion && !strings.EqualFold(status["os"], "windows") {
			desiredChecksum, err = packagedAgentChecksum(agentPath, status["os"])
			if err != nil {
				schedulerLog.WarnfCtx(ctx, "failed to checksum upgrade artifact for target %s: %v", targetState.ObjectMeta.Name, err)
				continue
			}
		}
		updated, changed := desiredRemoteAgentComponent(targetState.Spec.Components[componentIndex], status, desiredVersion, desiredThumbprint, desiredChecksum)
		if !changed {
			continue
		}
		targetState.Spec.Components[componentIndex] = updated
		if err := s.TargetsManager.UpsertState(ctx, targetState.ObjectMeta.Name, targetState); err != nil {
			errorsFound = append(errorsFound, fmt.Errorf("update remote-agent target %s: %w", targetState.ObjectMeta.Name, err))
		}
	}
	return errorsFound
}

func remoteAgentComponentIndex(target model.TargetState) int {
	if target.Spec == nil {
		return -1
	}
	for index, component := range target.Spec.Components {
		if component.Type == "remote-agent" {
			return index
		}
	}
	return -1
}

func remoteAgentStatus(target model.TargetState, componentName string) (map[string]string, bool) {
	value := target.Status.GetComponentStatus(target.ObjectMeta.Name, componentName)
	if value == "" && target.Status.Properties != nil {
		value = target.Status.Properties[fmt.Sprintf("targets.%s.%s", target.ObjectMeta.Name, componentName)]
	}
	start := strings.Index(value, "{")
	if start < 0 {
		return nil, false
	}
	var status map[string]string
	if err := json.Unmarshal([]byte(value[start:]), &status); err != nil {
		return nil, false
	}
	return status, true
}

func desiredRemoteAgentComponent(component model.ComponentSpec, status map[string]string, desiredVersion, desiredThumbprint, desiredChecksum string) (model.ComponentSpec, bool) {
	properties := make(map[string]interface{}, len(component.Properties)+2)
	for key, value := range component.Properties {
		properties[key] = value
	}
	if desiredVersion != "" && status["version"] != desiredVersion {
		if strings.EqualFold(status["os"], "windows") {
			return component, false
		}
		if desiredChecksum == "" {
			return component, false
		}
		properties["action"] = "upgrade"
		properties["version"] = desiredVersion
		if desiredChecksum != "" {
			properties["sha256"] = desiredChecksum
		}
		delete(properties, "thumbprint")
		component.Properties = properties
		return component, true
	}
	if desiredThumbprint != "" && status["certificateThumbprint"] != "" && !strings.EqualFold(status["certificateThumbprint"], desiredThumbprint) {
		properties["action"] = "secretrotation"
		properties["thumbprint"] = desiredThumbprint
		delete(properties, "version")
		delete(properties, "sha256")
		component.Properties = properties
		return component, true
	}
	_, hadAction := properties["action"]
	delete(properties, "action")
	delete(properties, "version")
	delete(properties, "thumbprint")
	delete(properties, "sha256")
	component.Properties = properties
	return component, hadAction
}

func packagedAgentChecksum(directory, operatingSystem string) (string, error) {
	if directory == "" {
		return "", fmt.Errorf("AGENT_PATH is not configured")
	}
	name := "remote-agent"
	if strings.EqualFold(operatingSystem, "windows") {
		name += ".exe"
	}
	file, err := os.Open(filepath.Join(directory, name))
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func certificateThumbprint(data []byte) (string, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", v1alpha2.NewCOAError(nil, "invalid certificate PEM", v1alpha2.BadConfig)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(certificate.Raw)
	return hex.EncodeToString(digest[:]), nil
}
