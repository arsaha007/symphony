/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	v1alpha2 "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	jwt "github.com/golang-jwt/jwt/v4"
	"github.com/valyala/fasthttp"
	v1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type JWT struct {
	AuthHeader       string                 `json:"authHeader"`
	VerifyKey        string                 `json:"verifyKey"`
	MustHave         []string               `json:"mustHave,omitempty"`
	MustMatch        map[string]interface{} `json:"mustMatch,omitempty"`
	AuthServer       AuthServer             `json:"authServer,omitempty"`
	verifyKey        *rsa.PublicKey
	IgnorePaths      []string          `json:"ignorePaths,omitempty"`
	Roles            []ClaimRoleMap    `json:"roles,omitempty"`
	EnableRBAC       bool              `json:"enableRBAC,omitempty"`
	Policy           map[string]Policy `json:"policy,omitempty"`
	DisableUserCreds bool              `json:"disableUserCreds,omitempty"`
}

// enum string for AuthServer
type AuthServer string

const (
	// AuthServerKuberenetes means we are using kubernetes api server as auth server
	AuthServerKuberenetes AuthServer = "kubernetes"
	SymphonyIssuer        string     = "symphony"
)

var (
	symphonyAPIAddressBase       = os.Getenv("SYMPHONY_API_URL")
	namespace                    = os.Getenv("POD_NAMESPACE")
	apiServiceAccountName        = os.Getenv("SERVICE_ACCOUNT_NAME")
	controllerServiceAccountName = os.Getenv("SYMPHONY_CONTROLLER_SERVICE_ACCOUNT_NAME")
)

func getApiServiceAccountUsername() (string, error) {
	if namespace == "" || apiServiceAccountName == "" {
		return "", v1alpha2.NewCOAError(nil, "Unable to retrieve environment variables for api service account", v1alpha2.InternalError)
	}
	return fmt.Sprintf("system:serviceaccount:%s:%s", namespace, apiServiceAccountName), nil
}

func getControllerServiceAccountUsername() (string, error) {
	if namespace == "" || controllerServiceAccountName == "" {
		return "", v1alpha2.NewCOAError(nil, "Unable to retrieve environment variables for controller service account", v1alpha2.InternalError)
	}
	return fmt.Sprintf("system:serviceaccount:%s:%s", namespace, controllerServiceAccountName), nil
}

type ClaimRoleMap struct {
	Role  string `json:"role"`
	Claim string `json:"claim"`
	Value string `json:"value"`
}
type Policy struct {
	Items map[string]string `json:"items"`
}

func (j JWT) JWT(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if j.IgnorePaths != nil {
			for _, p := range j.IgnorePaths {
				if p == string(ctx.Path()) {
					next(ctx)
					return
				}
			}
		}
		if ctx.IsOptions() {
			next(ctx)
			return
		}
		tokenStr := j.readAuthHeader(ctx)
		if tokenStr == "" {
			if err := authorizeClientCertificate(ctx); err != nil {
				log.Errorf("JWT: client certificate authentication failed: %s", err.Error())
				ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
				return
			}
			next(ctx)
			return
		} else {
			issuer, err := decodeJWTTokenForIssuer(tokenStr)
			if err != nil {
				log.Errorf("JWT: Could not decode issuer from token. %s\n", err.Error())
				ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
				return
			}
			if issuer == SymphonyIssuer {
				if j.DisableUserCreds == true {
					log.Infof("JWT: Token with username plus pwd is not allowed.")
					ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
					return
				}
				log.Debugf("JWT: Validating token with username plus pwd.")
				_, roles, err := j.validateToken(tokenStr)
				if err != nil {
					log.Error("JWT: Validate token with user creds failed. %s\n", err.Error())
					ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
					return
				} else {
					if j.EnableRBAC {
						path := string(ctx.Path())
						method := string(ctx.Method())
						for _, role := range roles {
							if v, ok := j.Policy[role]; ok {
								for key, val := range v.Items {
									if key == "*" || strings.HasPrefix(path, key) {
										if val == "*" || strings.Contains(val, method) {
											next(ctx)
											return
										}
									}
								}
							}
						}
						ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
						return
					}
					next(ctx)
				}
			} else {
				if j.AuthServer == AuthServerKuberenetes {
					log.Debugf("JWT: Validating token with k8s.")
					err := j.validateServiceAccountToken(ctx, tokenStr)
					if err != nil {
						log.Errorf("JWT: Validate token with k8s failed. %s\n", err.Error())
						ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
						return
					}
					next(ctx)
				} else {
					log.Errorf("JWT: Not supported auth server, %s.\n", j.AuthServer)
					ctx.Response.SetStatusCode(fasthttp.StatusUnauthorized)
					return
				}
			}
		}
	}
}

func authorizeClientCertificate(ctx *fasthttp.RequestCtx) error {
	connection, ok := ctx.Conn().(*tls.Conn)
	if !ok {
		return errors.New("bearer token or TLS client certificate is required")
	}
	state := connection.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return errors.New("TLS client certificate is required")
	}
	certificate := state.PeerCertificates[0]
	intermediates := x509.NewCertPool()
	for _, intermediate := range state.PeerCertificates[1:] {
		intermediates.AddCert(intermediate)
	}
	workingCAPath := os.Getenv("CLIENT_CA_FILE")
	bootstrapCAPath := os.Getenv("CLIENT_BOOTSTRAP_CA_FILE")
	if bootstrapCAPath == "" {
		bootstrapCAPath = workingCAPath
	}
	if workingCAPath != "" {
		if err := verifyClientCertificate(certificate, intermediates, workingCAPath); err == nil {
			serviceName := os.Getenv("SYMPHONY_SERVICE_NAME")
			if serviceName != "" && strings.Contains(certificate.Subject.String(), serviceName) {
				return authorizeWorkingRemoteAgent(ctx, certificate, serviceName)
			}
		}
	}
	if bootstrapCAPath == "" {
		return errors.New("client certificate trust is not configured")
	}
	if err := verifyClientCertificate(certificate, intermediates, bootstrapCAPath); err != nil {
		return err
	}
	if !clientSubjectAllowed(certificate) {
		return fmt.Errorf("client certificate subject %q is not allowed", certificate.Subject.String())
	}
	path := string(ctx.Path())
	if strings.Contains(path, "/targets/getcert") || strings.Contains(path, "/files/") {
		return nil
	}
	return errors.New("bootstrap certificate may access only getcert and files endpoints")
}

func authorizeWorkingRemoteAgent(ctx *fasthttp.RequestCtx, certificate *x509.Certificate, serviceName string) error {
	path := string(ctx.Path())
	if strings.Contains(path, "/files/") {
		return nil
	}
	allowed := strings.Contains(path, "/solutionversion/tasks") ||
		strings.Contains(path, "/solutionversion/task/getResult") ||
		strings.Contains(path, "/solution/tasks") ||
		strings.Contains(path, "/solution/task/getResult") ||
		strings.Contains(path, "/targets/updatetopology/") ||
		strings.Contains(path, "/targets/secretrotate/")
	if !allowed {
		return errors.New("working remote-agent certificate is not authorized for this endpoint")
	}
	target := string(ctx.QueryArgs().Peek("target"))
	if target == "" {
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) > 0 {
			target = parts[len(parts)-1]
		}
	}
	namespace := string(ctx.QueryArgs().Peek("namespace"))
	if namespace == "" {
		namespace = "default"
	}
	expectedCommonName := fmt.Sprintf("%s-%s.%s", namespace, target, serviceName)
	if target == "" || certificate.Subject.CommonName != expectedCommonName {
		return fmt.Errorf("client certificate is not authorized for target %q", target)
	}
	return nil
}

func verifyClientCertificate(certificate *x509.Certificate, intermediates *x509.CertPool, caPath string) error {
	data, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return fmt.Errorf("failed to parse client CA file %s", caPath)
	}
	_, err = certificate.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	return err
}

func clientSubjectAllowed(certificate *x509.Certificate) bool {
	configured := strings.Split(os.Getenv("CLIENT_SUBJECTS"), ";")
	for _, candidate := range configured {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if candidate == certificate.Subject.CommonName || candidate == certificate.Subject.String() || strings.Contains(certificate.Subject.String(), candidate) {
			return true
		}
	}
	return false
}
func (j JWT) readAuthHeader(ctx *fasthttp.RequestCtx) string {
	v := ctx.Request.Header.Peek(j.AuthHeader)
	if v != nil {
		tokenStr := string(v)
		token := strings.Split(tokenStr, "Bearer ")
		if len(token) == 2 {
			return strings.TrimSpace(token[1])
		} else {
			return ""
		}
	}
	return ""
}
func (j *JWT) validateToken(tokenStr string) (map[string]interface{}, []string, error) {
	ret := make(map[string]interface{})
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(
		tokenStr,
		claims,
		func(token *jwt.Token) (interface{}, error) {
			if j.verifyKey != nil {
				return j.verifyKey, nil
			} else {
				if strings.HasPrefix(j.VerifyKey, "-----BEGIN PUBLIC KEY-----") {
					verifyKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(j.VerifyKey))
					if err != nil {
						return ret, v1alpha2.NewCOAError(nil, "failed to parse public key", v1alpha2.BadConfig)
					}
					j.verifyKey = verifyKey
					return j.verifyKey, nil
				} else {
					return []byte(j.VerifyKey), nil
				}
			}
		},
	)
	if err != nil {
		return ret, nil, err
	}
	if !token.Valid {
		return ret, nil, errors.New("invalid token")
	}
	for k, v := range claims {
		ret[k] = v
	}
	if len(j.MustHave) > 0 {
		for _, k := range j.MustHave {
			if _, ok := ret[k]; !ok {
				return ret, nil, fmt.Errorf("required claim '%s' is not found", k)
			}
		}
	}
	if len(j.MustMatch) > 0 {
		for k, v := range j.MustMatch {
			if hv, ok := ret[k]; ok {
				if hv != v {
					return ret, nil, fmt.Errorf("claim '%s' doesn't have required value", k)
				}
			} else {
				return ret, nil, fmt.Errorf("required claim '%s' is not found", k)
			}
		}
	}
	var roles []string
	if j.EnableRBAC {
		roles = make([]string, 0)
		for _, m := range j.Roles {
			if v, ok := ret[m.Claim]; ok {
				if m.Value == "*" || v == m.Value {
					roles = append(roles, m.Role)
				}
			}
		}

	}
	return ret, roles, nil
}

func decodeJWTTokenForIssuer(tokenString string) (string, error) {
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return "", err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		issuer, ok := claims["iss"].(string)
		if !ok {
			log.Debugf("The iss claim is not a string")
			return "", errors.New("the iss claim is not a string")
		}
		log.Debugf("Issuer: %s", issuer)
		return issuer, nil
	} else {
		log.Debugf("Invalid token")
		return "", errors.New("invalid token")
	}
}

func (j *JWT) validateServiceAccountToken(ctx *fasthttp.RequestCtx, tokenStr string) error {
	clientset, err := getKubernetesClient()
	if err != nil {
		log.Errorf("JWT: Could not initialize Kubernetes client.\n")
		return v1alpha2.NewCOAError(err, "Could not initialize Kubernetes client", v1alpha2.InternalError)
	}
	tokenReview := &v1.TokenReview{
		Spec: v1.TokenReviewSpec{
			Token: tokenStr,
			Audiences: []string{
				symphonyAPIAddressBase,
			},
		},
	}

	result, err := clientset.AuthenticationV1().TokenReviews().Create(ctx, tokenReview, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("JWT: Token review using kubernetes api server failed. %s\n", err.Error())
		return v1alpha2.NewCOAError(err, "Token review using kubernetes api server failed.", v1alpha2.InternalError)
	}
	if !result.Status.Authenticated {
		log.Errorf("JWT: Validate token with k8s failed. K8s returned not authenticated.\n")
		return v1alpha2.NewCOAError(nil, "Authentication failed.", v1alpha2.Unauthorized)
	} else {
		apiUsername, err := getApiServiceAccountUsername()
		if err != nil {
			return err
		}
		controllerUsername, err := getControllerServiceAccountUsername()
		if err != nil {
			return err
		}
		if result.Status.User.Username != apiUsername && result.Status.User.Username != controllerUsername {
			log.Errorf("JWT: Validate token with k8s failed. K8s returned invalid username, %s\n", result.Status.User.Username)
			return v1alpha2.NewCOAError(nil, "Authentication failed.", v1alpha2.Unauthorized)
		}
	}
	return nil

}
func getKubernetesClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}
