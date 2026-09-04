package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	target "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/docker"
	targethttp "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/http"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/script"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target/win10/sideload"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/eclipse-symphony/symphony/remote-agent/agent"
	httppoller "github.com/eclipse-symphony/symphony/remote-agent/pollers/http"
	mqttpoller "github.com/eclipse-symphony/symphony/remote-agent/pollers/mqtt"
	remoteproviders "github.com/eclipse-symphony/symphony/remote-agent/providers"
	paho "github.com/eclipse/paho.mqtt.golang"
)

var agentVersion = "0.0.0.1"

type SymphonyConfig struct {
	RequestEndpoint  string `json:"requestEndpoint"`
	ResponseEndpoint string `json:"responseEndpoint"`
	BaseURL          string `json:"baseUrl"`
	MQTTBroker       string `json:"mqttBroker"`
	MQTTPort         int    `json:"mqttPort"`
	MQTTUseTLS       *bool  `json:"mqttUseTLS,omitempty"`
	MQTTUsername     string `json:"mqttUsername,omitempty"`
	MQTTPassword     string `json:"mqttPassword,omitempty"`
	TargetName       string `json:"targetName,omitempty"`
	Namespace        string `json:"namespace,omitempty"`
}

type runOptions struct {
	ConfigPath     string
	ClientCertPath string
	ClientKeyPath  string
	LegacyCAPath   string
	ServerCAPath   string
	MQTTCAPath     string
	TargetName     string
	Namespace      string
	TopologyPath   string
	Protocol       string
	UseCertSubject bool
}

func mainLogic(ctx context.Context, arguments []string) error {
	options, err := parseOptions(arguments)
	if err != nil {
		return err
	}
	log := logger.NewLogger("remote-agent")
	config, err := loadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	if config.TargetName != "" && options.TargetName == "remote-target" {
		options.TargetName = config.TargetName
	}
	if config.Namespace != "" && options.Namespace == "default" {
		options.Namespace = config.Namespace
	}

	protocol := strings.ToLower(options.Protocol)
	mqttUsesTLS := config.MQTTUseTLS == nil || *config.MQTTUseTLS
	var certificate *tls.Certificate
	var subject string
	if protocol != "mqtt" || mqttUsesTLS || certificateFilesExist(options.ClientCertPath, options.ClientKeyPath) {
		loadedCertificate, loadedSubject, err := loadClientCertificate(options.ClientCertPath, options.ClientKeyPath)
		if err != nil {
			return err
		}
		certificate = &loadedCertificate
		subject = loadedSubject
	}
	serverCAPath := options.ServerCAPath
	if serverCAPath == "" && protocol != "mqtt" {
		serverCAPath = options.LegacyCAPath
	}
	httpClient, err := newHTTPClient(certificate, serverCAPath, 60*time.Second)
	if err != nil {
		return err
	}
	topology, err := os.ReadFile(options.TopologyPath)
	if err != nil {
		return fmt.Errorf("read topology: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}

	var restartOnce sync.Once
	publicCertPath := ""
	privateKeyPath := ""
	if certificate != nil {
		publicCertPath = options.ClientCertPath
		privateKeyPath = options.ClientKeyPath
	}
	providers, err := composeTargetProviders(topology, remoteproviders.RemoteAgentProviderConfig{
		Version:        agentVersion,
		PublicCertPath: publicCertPath,
		PrivateKeyPath: privateKeyPath,
		BaseURL:        config.BaseURL,
		Namespace:      options.Namespace,
		TargetName:     options.TargetName,
		ExecutablePath: executable,
		StartupArgs:    append([]string(nil), arguments...),
	}, httpClient, func() {
		restartOnce.Do(func() {
			go func() {
				time.Sleep(3 * time.Second)
				if os.Getenv("INVOCATION_ID") == "" && runtime.GOOS != "windows" {
					command := exec.Command(executable, arguments...)
					command.Stdout = os.Stdout
					command.Stderr = os.Stderr
					if err := command.Start(); err != nil {
						log.Errorf("failed to start replacement remote-agent: %v", err)
						return
					}
				}
				os.Exit(3)
			}()
		})
	}, log)
	if err != nil {
		return err
	}
	dispatcher := agent.Agent{Providers: providers, Logger: log, Executions: agent.NewExecutionCache(10_000, 24*time.Hour)}
	collectLogs := func() []string { return logger.CollectRemoteLogs(log) }

	switch protocol {
	case "http", "https":
		if config.RequestEndpoint == "" || config.ResponseEndpoint == "" || config.BaseURL == "" {
			return fmt.Errorf("HTTP mode requires requestEndpoint, responseEndpoint, and baseUrl")
		}
		if err := updateTopologyHTTP(ctx, httpClient, config.BaseURL, options.TargetName, options.Namespace, topology); err != nil {
			return err
		}
		poller := &httppoller.Poller{
			Agent: dispatcher, Client: httpClient, RequestURL: config.RequestEndpoint, ResponseURL: config.ResponseEndpoint,
			Target: options.TargetName, Namespace: options.Namespace, Logger: log, CollectLogs: collectLogs,
		}
		return normalizeContextError(poller.Run(ctx))
	case "mqtt":
		return runMQTT(ctx, config, options, certificate, subject, dispatcher, collectLogs, log, topology)
	default:
		return fmt.Errorf("unsupported protocol %q", options.Protocol)
	}
}

func parseOptions(arguments []string) (runOptions, error) {
	options := runOptions{}
	flags := flag.NewFlagSet("remote-agent", flag.ContinueOnError)
	flags.StringVar(&options.ConfigPath, "config", "config.json", "path to transport configuration")
	flags.StringVar(&options.ClientCertPath, "client-cert", "public.pem", "path to client certificate")
	flags.StringVar(&options.ClientKeyPath, "client-key", "private.pem", "path to client private key")
	flags.StringVar(&options.LegacyCAPath, "ca-cert", "", "legacy shared CA path; prefer protocol-specific CA flags")
	flags.StringVar(&options.ServerCAPath, "server-ca-cert", "", "path to the Symphony HTTPS server CA")
	flags.StringVar(&options.MQTTCAPath, "mqtt-ca-cert", "", "path to the MQTT broker CA")
	flags.StringVar(&options.TargetName, "target-name", "remote-target", "remote target name")
	flags.StringVar(&options.Namespace, "namespace", "default", "target namespace")
	flags.StringVar(&options.TopologyPath, "topology", "topology.json", "path to provider topology")
	flags.StringVar(&options.Protocol, "protocol", "http", "transport protocol: http or mqtt")
	flags.BoolVar(&options.UseCertSubject, "use-cert-subject", false, "use client certificate subject for MQTT topics")
	if err := flags.Parse(arguments); err != nil {
		return runOptions{}, err
	}
	if options.TargetName == "" || options.Namespace == "" {
		return runOptions{}, fmt.Errorf("target name and namespace are required")
	}
	return options, nil
}

func loadConfig(path string) (SymphonyConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SymphonyConfig{}, fmt.Errorf("read config: %w", err)
	}
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	var config SymphonyConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return SymphonyConfig{}, fmt.Errorf("decode config: %w", err)
	}
	return config, nil
}

func loadClientCertificate(certPath, keyPath string) (tls.Certificate, string, error) {
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, "", fmt.Errorf("load client certificate: %w", err)
	}
	if len(certificate.Certificate) == 0 {
		return tls.Certificate{}, "", fmt.Errorf("client certificate chain is empty")
	}
	parsed, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return tls.Certificate{}, "", err
	}
	subject := parsed.Subject.CommonName
	if subject == "" {
		subject = parsed.Subject.String()
	}
	return certificate, subject, nil
}

func newHTTPClient(certificate *tls.Certificate, caPath string, timeout time.Duration) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if certificate != nil {
		tlsConfig.Certificates = []tls.Certificate{*certificate}
	}
	if caPath != "" {
		pool, err := loadCAPool(caPath)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = pool
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}, Timeout: timeout}, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("parse CA certificate %s", path)
	}
	return pool, nil
}

func composeTargetProviders(topologyData []byte, remoteConfig remoteproviders.RemoteAgentProviderConfig, client *http.Client, restart func(), log logger.Logger) (map[string]target.ITargetProvider, error) {
	var topology model.TopologySpec
	if err := json.Unmarshal(topologyData, &topology); err != nil {
		return nil, fmt.Errorf("decode topology: %w", err)
	}
	if len(topology.Bindings) == 0 {
		return nil, fmt.Errorf("topology has no provider bindings")
	}
	result := make(map[string]target.ITargetProvider, len(topology.Bindings))
	for _, binding := range topology.Bindings {
		if binding.Role == "" || binding.Provider == "" {
			return nil, fmt.Errorf("topology binding role and provider are required")
		}
		if _, exists := result[binding.Role]; exists {
			return nil, fmt.Errorf("duplicate topology role %q", binding.Role)
		}
		var provider target.ITargetProvider
		switch binding.Provider {
		case "providers.target.script":
			provider = &script.ScriptProvider{}
		case "providers.target.remote-agent":
			provider = &remoteproviders.RemoteAgentProvider{Config: remoteConfig, Client: client, ScheduleRestart: restart, Logger: log}
		case "providers.target.win10.sideload":
			provider = &sideload.Win10SideLoadProvider{}
		case "providers.target.docker":
			provider = &docker.DockerTargetProvider{}
		case "providers.target.http":
			provider = &targethttp.HttpTargetProvider{}
		default:
			return nil, fmt.Errorf("unsupported target provider %q", binding.Provider)
		}
		if binding.Provider != "providers.target.remote-agent" {
			if err := provider.Init(binding.Config); err != nil {
				return nil, fmt.Errorf("initialize provider %s for role %s: %w", binding.Provider, binding.Role, err)
			}
		}
		result[binding.Role] = provider
	}
	return result, nil
}

func updateTopologyHTTP(ctx context.Context, client *http.Client, baseURL, targetName, namespace string, topology []byte) error {
	endpoint, err := url.JoinPath(baseURL, "targets", "updatetopology", targetName)
	if err != nil {
		return err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	query := parsed.Query()
	query.Set("namespace", namespace)
	parsed.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(topology))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("update topology: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return fmt.Errorf("update topology returned %s: %s", response.Status, string(body))
	}
	return nil
}

func runMQTT(ctx context.Context, config SymphonyConfig, options runOptions, certificate *tls.Certificate, subject string, dispatcher agent.Agent, collectLogs func() []string, log logger.Logger, topology []byte) error {
	if config.MQTTBroker == "" || config.MQTTPort <= 0 {
		return fmt.Errorf("MQTT mode requires mqttBroker and mqttPort")
	}
	useTLS := config.MQTTUseTLS == nil || *config.MQTTUseTLS
	brokerURL, serverName, err := normalizeBrokerURL(config.MQTTBroker, config.MQTTPort, useTLS)
	if err != nil {
		return err
	}
	topicSuffix := strings.ToLower(options.TargetName)
	if options.UseCertSubject && subject != "" {
		topicSuffix = strings.ToLower(subject)
	}

	clientOptions := paho.NewClientOptions().AddBroker(brokerURL).SetClientID(strings.ToLower(options.TargetName))
	clientOptions.SetCleanSession(false)
	clientOptions.SetAutoReconnect(true)
	clientOptions.SetConnectRetry(true)
	clientOptions.SetConnectRetryInterval(5 * time.Second)
	clientOptions.SetMaxReconnectInterval(time.Minute)
	if config.MQTTUsername != "" {
		clientOptions.SetUsername(config.MQTTUsername)
	}
	if config.MQTTPassword != "" {
		clientOptions.SetPassword(config.MQTTPassword)
	}
	if useTLS {
		mqttCAPath := options.MQTTCAPath
		if mqttCAPath == "" {
			mqttCAPath = options.LegacyCAPath
		}
		if mqttCAPath == "" || certificate == nil {
			return fmt.Errorf("TLS MQTT mode requires a client certificate and -mqtt-ca-cert")
		}
		pool, err := loadCAPool(mqttCAPath)
		if err != nil {
			return err
		}
		clientOptions.SetTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{*certificate}, RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12,
		})
	}

	var poller *mqttpoller.Poller
	var initialConnection atomic.Bool
	subscriptionErrors := make(chan error, 1)
	clientOptions.OnConnect = func(client paho.Client) {
		if initialConnection.Swap(true) && poller != nil {
			if err := poller.Subscribe(); err != nil {
				log.Errorf("restore MQTT response subscription: %v", err)
				select {
				case subscriptionErrors <- err:
				default:
				}
			}
		}
	}
	client := paho.NewClient(clientOptions)
	poller = &mqttpoller.Poller{
		Agent: dispatcher, Client: client, RequestTopic: "symphony/request/" + topicSuffix,
		ResponseTopic: "symphony/response/" + topicSuffix, Target: options.TargetName, Namespace: options.Namespace,
		Logger: log, CollectLogs: collectLogs,
	}
	token := client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("connect to MQTT broker: %w", err)
	}
	defer client.Disconnect(1000)
	if err := poller.Subscribe(); err != nil {
		return err
	}
	for {
		if err := poller.UpdateTopology(ctx, topology); err == nil {
			break
		} else {
			log.ErrorfCtx(ctx, "MQTT topology update failed: %v", err)
		}
		timer := time.NewTimer(2 * time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	runErrors := make(chan error, 1)
	go func() { runErrors <- normalizeContextError(poller.Run(runContext)) }()
	select {
	case err := <-subscriptionErrors:
		cancel()
		return fmt.Errorf("restore MQTT response subscription: %w", err)
	case err := <-runErrors:
		return err
	case <-ctx.Done():
		cancel()
		return nil
	}
}

func normalizeBrokerURL(address string, port int, useTLS bool) (string, string, error) {
	scheme := "tcp"
	if useTLS {
		scheme = "tls"
	}
	value := address
	if !strings.Contains(value, "://") {
		value = scheme + "://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", "", err
	}
	host := parsed.Hostname()
	if host == "" {
		return "", "", fmt.Errorf("invalid MQTT broker address %q", address)
	}
	if parsed.Port() == "" {
		parsed.Host = parsed.Host + ":" + strconv.Itoa(port)
	}
	if host == "127.0.0.1" || host == "::1" {
		host = "localhost"
	}
	return parsed.String(), host, nil
}

func normalizeContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func targetNameFromArgs(arguments []string) string {
	for index, argument := range arguments {
		if strings.HasPrefix(argument, "-target-name=") {
			return strings.TrimPrefix(argument, "-target-name=")
		}
		if argument == "-target-name" && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	return "remote-target"
}

func certificateFilesExist(certificatePath, keyPath string) bool {
	_, certificateErr := os.Stat(certificatePath)
	_, keyErr := os.Stat(keyPath)
	return certificateErr == nil && keyErr == nil
}
