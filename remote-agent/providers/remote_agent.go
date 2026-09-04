/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package providers

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	coaProviders "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
)

const defaultVersion = "0.0.0.1"

type RemoteAgentProviderConfig struct {
	Version        string   `json:"version,omitempty"`
	PublicCertPath string   `json:"publicCertPath,omitempty"`
	PrivateKeyPath string   `json:"privateKeyPath,omitempty"`
	BaseURL        string   `json:"baseUrl,omitempty"`
	Namespace      string   `json:"namespace,omitempty"`
	TargetName     string   `json:"targetName,omitempty"`
	ExecutablePath string   `json:"executablePath,omitempty"`
	StartupArgs    []string `json:"-"`
}

type RemoteAgentProvider struct {
	Config          RemoteAgentProviderConfig
	Client          *http.Client
	ScheduleRestart func()
	Logger          logger.Logger
}

func (p *RemoteAgentProvider) Init(config coaProviders.IProviderConfig) error {
	payload, err := json.Marshal(config)
	if err != nil {
		return err
	}
	var parsed RemoteAgentProviderConfig
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return fmt.Errorf("decode remote-agent provider config: %w", err)
	}
	if parsed.Version == "" {
		parsed.Version = defaultVersion
	}
	p.Config = parsed
	return nil
}

func (p *RemoteAgentProvider) Get(ctx context.Context, _ model.DeploymentSpec, references []model.ComponentStep) ([]model.ComponentSpec, error) {
	status, err := p.status()
	if err != nil {
		return nil, err
	}
	result := make([]model.ComponentSpec, 0, len(references))
	for _, reference := range references {
		component := reference.Component
		component.Properties = status
		result = append(result, component)
	}
	return result, nil
}

func (p *RemoteAgentProvider) Apply(ctx context.Context, _ model.DeploymentSpec, step model.DeploymentStep, isDryRun bool) (map[string]model.ComponentResultSpec, error) {
	result := step.PrepareResultMap()
	if isDryRun {
		return result, nil
	}
	for _, componentStep := range step.Components {
		component := componentStep.Component
		if componentStep.Action == model.ComponentDelete {
			result[component.Name] = model.ComponentResultSpec{Status: v1alpha2.Deleted, Message: "remote-agent component removed"}
			continue
		}
		action, _ := component.Properties["action"].(string)
		switch strings.ToLower(action) {
		case "", "status", "log":
			message, err := p.statusJSON()
			if err != nil {
				return p.componentFailure(result, component.Name, err)
			}
			result[component.Name] = model.ComponentResultSpec{Status: v1alpha2.Updated, Message: message}
		case "upgrade":
			if err := p.upgrade(ctx, component); err != nil {
				return p.componentFailure(result, component.Name, err)
			}
			message, _ := p.statusJSON()
			result[component.Name] = model.ComponentResultSpec{Status: v1alpha2.Updated, Message: message}
		case "secretrotation":
			if err := p.rotateCredentials(ctx, step.Target, component); err != nil {
				return p.componentFailure(result, component.Name, err)
			}
			message, _ := p.statusJSON()
			result[component.Name] = model.ComponentResultSpec{Status: v1alpha2.Updated, Message: message}
		default:
			return p.componentFailure(result, component.Name, fmt.Errorf("unsupported remote-agent action %q", action))
		}
	}
	return result, nil
}

func (*RemoteAgentProvider) GetValidationRule(context.Context) model.ValidationRule {
	return model.ValidationRule{AllowSidecar: false}
}

func (p *RemoteAgentProvider) status() (map[string]interface{}, error) {
	expiration := ""
	thumbprint := ""
	if p.Config.PublicCertPath != "" {
		var err error
		expiration, thumbprint, err = certificateMetadata(p.Config.PublicCertPath)
		if err != nil {
			return nil, err
		}
	}
	return map[string]interface{}{
		"state":                 "active",
		"version":               p.Config.Version,
		"lastConnected":         time.Now().UTC().Format(time.RFC3339),
		"certificateExpiration": expiration,
		"certificateThumbprint": thumbprint,
		"os":                    runtime.GOOS,
		"architecture":          runtime.GOARCH,
	}, nil
}

func (p *RemoteAgentProvider) statusJSON() (string, error) {
	status, err := p.status()
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(status)
	return string(payload), err
}

func (p *RemoteAgentProvider) componentFailure(result map[string]model.ComponentResultSpec, name string, err error) (map[string]model.ComponentResultSpec, error) {
	result[name] = model.ComponentResultSpec{Status: v1alpha2.UpdateFailed, Message: err.Error()}
	return result, err
}

func (p *RemoteAgentProvider) upgrade(ctx context.Context, component model.ComponentSpec) error {
	desiredVersion, ok := component.Properties["version"].(string)
	if !ok || desiredVersion == "" {
		return fmt.Errorf("upgrade requires a version")
	}
	if desiredVersion == p.Config.Version {
		return nil
	}
	if runtime.GOOS == "windows" {
		return fmt.Errorf("in-place upgrade is not supported on Windows; use the service installer")
	}
	if p.Client == nil || p.Config.BaseURL == "" {
		return fmt.Errorf("upgrade requires an HTTP client and Symphony base URL")
	}
	executable := p.Config.ExecutablePath
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return err
		}
	}
	downloadURL, err := url.JoinPath(p.Config.BaseURL, "files", "remote-agent")
	if err != nil {
		return err
	}
	temporary := executable + ".new"
	if err := downloadToFile(ctx, p.Client, downloadURL, temporary); err != nil {
		return err
	}
	defer os.Remove(temporary)
	expected, _ := component.Properties["sha256"].(string)
	if expected == "" {
		return fmt.Errorf("upgrade requires a sha256 checksum")
	}
	actual, err := fileSHA256(temporary)
	if err != nil {
		return err
	}
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("downloaded binary checksum mismatch")
	}
	if err := os.Chmod(temporary, 0700); err != nil {
		return err
	}
	if err := os.Rename(temporary, executable); err != nil {
		return fmt.Errorf("replace remote-agent executable: %w", err)
	}
	p.Config.Version = desiredVersion
	p.scheduleRestart()
	return nil
}

func (p *RemoteAgentProvider) rotateCredentials(ctx context.Context, targetName string, component model.ComponentSpec) error {
	if p.Client == nil || p.Config.BaseURL == "" {
		return fmt.Errorf("secret rotation requires an HTTP client and Symphony base URL")
	}
	_, thumbprint, err := certificateMetadata(p.Config.PublicCertPath)
	if err != nil {
		return err
	}
	if expected, _ := component.Properties["thumbprint"].(string); expected != "" && strings.EqualFold(expected, thumbprint) {
		return nil
	}
	if targetName == "" {
		targetName = p.Config.TargetName
	}
	endpoint, err := url.JoinPath(p.Config.BaseURL, "targets", "secretrotate", targetName)
	if err != nil {
		return err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	query := parsed.Query()
	query.Set("namespace", p.Config.Namespace)
	parsed.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), nil)
	if err != nil {
		return err
	}
	response, err := p.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return fmt.Errorf("secret rotation returned %s: %s", response.Status, string(body))
	}
	var credentials map[string]string
	if err := json.NewDecoder(response.Body).Decode(&credentials); err != nil {
		return err
	}
	certificate, err := normalizePEM(credentials["public"], "CERTIFICATE")
	if err != nil {
		return fmt.Errorf("decode rotated certificate: %w", err)
	}
	privateKey, err := normalizePEM(credentials["private"], "PRIVATE KEY")
	if err != nil {
		return fmt.Errorf("decode rotated private key: %w", err)
	}
	oldCertificate, err := os.ReadFile(p.Config.PublicCertPath)
	if err != nil {
		return err
	}
	oldPrivateKey, err := os.ReadFile(p.Config.PrivateKeyPath)
	if err != nil {
		return err
	}
	if err := atomicWrite(p.Config.PublicCertPath, certificate, 0600); err != nil {
		return err
	}
	if err := atomicWrite(p.Config.PrivateKeyPath, privateKey, 0600); err != nil {
		_ = atomicWrite(p.Config.PublicCertPath, oldCertificate, 0600)
		_ = atomicWrite(p.Config.PrivateKeyPath, oldPrivateKey, 0600)
		return err
	}
	p.scheduleRestart()
	return nil
}

func (p *RemoteAgentProvider) scheduleRestart() {
	if p.ScheduleRestart != nil {
		p.ScheduleRestart()
	}
}

func certificateMetadata(path string) (string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", "", fmt.Errorf("invalid certificate PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(certificate.Raw)
	return certificate.NotAfter.UTC().Format(time.RFC3339), hex.EncodeToString(digest[:]), nil
}

func downloadToFile(ctx context.Context, client *http.Client, source, destination string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned %s", response.Status)
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, response.Body)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
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

var pemEnvelope = regexp.MustCompile(`(?s)-----BEGIN ([A-Z ]+)-----(.*?)-----END [A-Z ]+-----`)

func normalizePEM(value, fallbackType string) ([]byte, error) {
	match := pemEnvelope.FindStringSubmatch(value)
	blockType := fallbackType
	body := value
	if len(match) == 3 {
		blockType = match[1]
		body = match[2]
	}
	body = strings.Join(strings.Fields(body), "")
	decoded, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: decoded}), nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".remote-agent-credential-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err == nil {
		return nil
	}
	backupPath := path + ".previous"
	_ = os.Remove(backupPath)
	if err := os.Rename(path, backupPath); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Rename(backupPath, path)
		return err
	}
	_ = os.Remove(backupPath)
	return nil
}
