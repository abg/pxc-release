// This file was generated by counterfeiter
package fakes

import (
	"sync"

	"github.com/cloudfoundry-incubator/galera-healthcheck/monit_client"
	"github.com/pivotal-golang/lager"
)

type FakeMonitClient struct {
	StartServiceBootstrapStub        func() (string, error)
	startServiceBootstrapMutex       sync.RWMutex
	startServiceBootstrapArgsForCall []struct{}
	startServiceBootstrapReturns     struct {
		result1 string
		result2 error
	}
	StartServiceJoinStub        func() (string, error)
	startServiceJoinMutex       sync.RWMutex
	startServiceJoinArgsForCall []struct{}
	startServiceJoinReturns     struct {
		result1 string
		result2 error
	}
	StartServiceSingleNodeStub        func() (string, error)
	startServiceSingleNodeMutex       sync.RWMutex
	startServiceSingleNodeArgsForCall []struct{}
	startServiceSingleNodeReturns     struct {
		result1 string
		result2 error
	}
	StopServiceStub        func() (string, error)
	stopServiceMutex       sync.RWMutex
	stopServiceArgsForCall []struct{}
	stopServiceReturns     struct {
		result1 string
		result2 error
	}
	GetStatusStub        func() (string, error)
	getStatusMutex       sync.RWMutex
	getStatusArgsForCall []struct{}
	getStatusReturns     struct {
		result1 string
		result2 error
	}
	GetLoggerStub        func() lager.Logger
	getLoggerMutex       sync.RWMutex
	getLoggerArgsForCall []struct{}
	getLoggerReturns     struct {
		result1 lager.Logger
	}
}

func (fake *FakeMonitClient) StartServiceBootstrap() (string, error) {
	fake.startServiceBootstrapMutex.Lock()
	fake.startServiceBootstrapArgsForCall = append(fake.startServiceBootstrapArgsForCall, struct{}{})
	fake.startServiceBootstrapMutex.Unlock()
	if fake.StartServiceBootstrapStub != nil {
		return fake.StartServiceBootstrapStub()
	} else {
		return fake.startServiceBootstrapReturns.result1, fake.startServiceBootstrapReturns.result2
	}
}

func (fake *FakeMonitClient) StartServiceBootstrapCallCount() int {
	fake.startServiceBootstrapMutex.RLock()
	defer fake.startServiceBootstrapMutex.RUnlock()
	return len(fake.startServiceBootstrapArgsForCall)
}

func (fake *FakeMonitClient) StartServiceBootstrapReturns(result1 string, result2 error) {
	fake.StartServiceBootstrapStub = nil
	fake.startServiceBootstrapReturns = struct {
		result1 string
		result2 error
	}{result1, result2}
}

func (fake *FakeMonitClient) StartServiceJoin() (string, error) {
	fake.startServiceJoinMutex.Lock()
	fake.startServiceJoinArgsForCall = append(fake.startServiceJoinArgsForCall, struct{}{})
	fake.startServiceJoinMutex.Unlock()
	if fake.StartServiceJoinStub != nil {
		return fake.StartServiceJoinStub()
	} else {
		return fake.startServiceJoinReturns.result1, fake.startServiceJoinReturns.result2
	}
}

func (fake *FakeMonitClient) StartServiceJoinCallCount() int {
	fake.startServiceJoinMutex.RLock()
	defer fake.startServiceJoinMutex.RUnlock()
	return len(fake.startServiceJoinArgsForCall)
}

func (fake *FakeMonitClient) StartServiceJoinReturns(result1 string, result2 error) {
	fake.StartServiceJoinStub = nil
	fake.startServiceJoinReturns = struct {
		result1 string
		result2 error
	}{result1, result2}
}

func (fake *FakeMonitClient) StartServiceSingleNode() (string, error) {
	fake.startServiceSingleNodeMutex.Lock()
	fake.startServiceSingleNodeArgsForCall = append(fake.startServiceSingleNodeArgsForCall, struct{}{})
	fake.startServiceSingleNodeMutex.Unlock()
	if fake.StartServiceSingleNodeStub != nil {
		return fake.StartServiceSingleNodeStub()
	} else {
		return fake.startServiceSingleNodeReturns.result1, fake.startServiceSingleNodeReturns.result2
	}
}

func (fake *FakeMonitClient) StartServiceSingleNodeCallCount() int {
	fake.startServiceSingleNodeMutex.RLock()
	defer fake.startServiceSingleNodeMutex.RUnlock()
	return len(fake.startServiceSingleNodeArgsForCall)
}

func (fake *FakeMonitClient) StartServiceSingleNodeReturns(result1 string, result2 error) {
	fake.StartServiceSingleNodeStub = nil
	fake.startServiceSingleNodeReturns = struct {
		result1 string
		result2 error
	}{result1, result2}
}

func (fake *FakeMonitClient) StopService() (string, error) {
	fake.stopServiceMutex.Lock()
	fake.stopServiceArgsForCall = append(fake.stopServiceArgsForCall, struct{}{})
	fake.stopServiceMutex.Unlock()
	if fake.StopServiceStub != nil {
		return fake.StopServiceStub()
	} else {
		return fake.stopServiceReturns.result1, fake.stopServiceReturns.result2
	}
}

func (fake *FakeMonitClient) StopServiceCallCount() int {
	fake.stopServiceMutex.RLock()
	defer fake.stopServiceMutex.RUnlock()
	return len(fake.stopServiceArgsForCall)
}

func (fake *FakeMonitClient) StopServiceReturns(result1 string, result2 error) {
	fake.StopServiceStub = nil
	fake.stopServiceReturns = struct {
		result1 string
		result2 error
	}{result1, result2}
}

func (fake *FakeMonitClient) GetStatus() (string, error) {
	fake.getStatusMutex.Lock()
	fake.getStatusArgsForCall = append(fake.getStatusArgsForCall, struct{}{})
	fake.getStatusMutex.Unlock()
	if fake.GetStatusStub != nil {
		return fake.GetStatusStub()
	} else {
		return fake.getStatusReturns.result1, fake.getStatusReturns.result2
	}
}

func (fake *FakeMonitClient) GetStatusCallCount() int {
	fake.getStatusMutex.RLock()
	defer fake.getStatusMutex.RUnlock()
	return len(fake.getStatusArgsForCall)
}

func (fake *FakeMonitClient) GetStatusReturns(result1 string, result2 error) {
	fake.GetStatusStub = nil
	fake.getStatusReturns = struct {
		result1 string
		result2 error
	}{result1, result2}
}

func (fake *FakeMonitClient) GetLogger() lager.Logger {
	fake.getLoggerMutex.Lock()
	fake.getLoggerArgsForCall = append(fake.getLoggerArgsForCall, struct{}{})
	fake.getLoggerMutex.Unlock()
	if fake.GetLoggerStub != nil {
		return fake.GetLoggerStub()
	} else {
		return fake.getLoggerReturns.result1
	}
}

func (fake *FakeMonitClient) GetLoggerCallCount() int {
	fake.getLoggerMutex.RLock()
	defer fake.getLoggerMutex.RUnlock()
	return len(fake.getLoggerArgsForCall)
}

func (fake *FakeMonitClient) GetLoggerReturns(result1 lager.Logger) {
	fake.GetLoggerStub = nil
	fake.getLoggerReturns = struct {
		result1 lager.Logger
	}{result1}
}

var _ monit_client.MonitClient = new(FakeMonitClient)
