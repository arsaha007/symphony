//go:build windows

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"
)

const servicePrefix = "Symphony-RemoteAgent-"

type windowsService struct {
	arguments []string
}

func (s *windowsService) Execute(_ []string, requests <-chan svc.ChangeRequest, statuses chan<- svc.Status) (bool, uint32) {
	statuses <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- mainLogic(ctx, s.arguments) }()
	statuses <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for {
		select {
		case err := <-done:
			if err != nil {
				log.Printf("remote-agent stopped with error: %v", err)
				return false, 1
			}
			return false, 0
		case request := <-requests:
			switch request.Cmd {
			case svc.Interrogate:
				statuses <- request.CurrentStatus
			case svc.Stop, svc.Shutdown:
				statuses <- svc.Status{State: svc.StopPending}
				cancel()
				select {
				case <-done:
				case <-time.After(15 * time.Second):
				}
				return false, 0
			}
		}
	}
}

func main() {
	arguments := os.Args[1:]
	if len(arguments) > 0 {
		switch arguments[0] {
		case "install", "uninstall", "start", "stop":
			if err := manageService(arguments[0], arguments[1:]); err != nil {
				log.Fatal(err)
			}
			return
		}
	}
	isService, err := svc.IsWindowsService()
	if err != nil {
		log.Fatal(err)
	}
	if isService {
		name := servicePrefix + targetNameFromArgs(arguments)
		if err := svc.Run(name, &windowsService{arguments: arguments}); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := mainLogic(context.Background(), arguments); err != nil {
		log.Fatal(err)
	}
}

func manageService(action string, arguments []string) error {
	manager, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	name := servicePrefix + targetNameFromArgs(arguments)
	if action == "install" {
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		service, err := manager.CreateService(name, executable, mgr.Config{DisplayName: name, Description: "Symphony Remote Agent", StartType: mgr.StartAutomatic}, arguments...)
		if err != nil {
			return err
		}
		defer service.Close()
		if err := service.SetRecoveryActions([]mgr.RecoveryAction{
			{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
			{Type: mgr.ServiceRestart, Delay: 15 * time.Second},
			{Type: mgr.ServiceRestart, Delay: time.Minute},
		}, 86400); err != nil {
			return err
		}
		if err := service.SetRecoveryActionsOnNonCrashFailures(true); err != nil {
			return err
		}
		return eventlog.InstallAsEventCreate(name, eventlog.Error|eventlog.Warning|eventlog.Info)
	}
	service, err := manager.OpenService(name)
	if err != nil {
		return fmt.Errorf("open service %s: %w", name, err)
	}
	defer service.Close()
	switch action {
	case "uninstall":
		if err := service.Delete(); err != nil {
			return err
		}
		return eventlog.Remove(name)
	case "start":
		return service.Start()
	case "stop":
		_, err := service.Control(svc.Stop)
		return err
	default:
		return fmt.Errorf("unsupported service action %q", action)
	}
}
