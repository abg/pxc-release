// Code generated by counterfeiter. DO NOT EDIT.
package upgraderfakes

import (
	sync "sync"

	upgrader "github.com/cloudfoundry/galera-init/upgrader"
)

type FakeUpgrader struct {
	NeedsUpgradeStub        func() (bool, error)
	needsUpgradeMutex       sync.RWMutex
	needsUpgradeArgsForCall []struct {
	}
	needsUpgradeReturns struct {
		result1 bool
		result2 error
	}
	needsUpgradeReturnsOnCall map[int]struct {
		result1 bool
		result2 error
	}
	UpgradeStub        func() error
	upgradeMutex       sync.RWMutex
	upgradeArgsForCall []struct {
	}
	upgradeReturns struct {
		result1 error
	}
	upgradeReturnsOnCall map[int]struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeUpgrader) NeedsUpgrade() (bool, error) {
	fake.needsUpgradeMutex.Lock()
	ret, specificReturn := fake.needsUpgradeReturnsOnCall[len(fake.needsUpgradeArgsForCall)]
	fake.needsUpgradeArgsForCall = append(fake.needsUpgradeArgsForCall, struct {
	}{})
	fake.recordInvocation("NeedsUpgrade", []interface{}{})
	fake.needsUpgradeMutex.Unlock()
	if fake.NeedsUpgradeStub != nil {
		return fake.NeedsUpgradeStub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	fakeReturns := fake.needsUpgradeReturns
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeUpgrader) NeedsUpgradeCallCount() int {
	fake.needsUpgradeMutex.RLock()
	defer fake.needsUpgradeMutex.RUnlock()
	return len(fake.needsUpgradeArgsForCall)
}

func (fake *FakeUpgrader) NeedsUpgradeCalls(stub func() (bool, error)) {
	fake.needsUpgradeMutex.Lock()
	defer fake.needsUpgradeMutex.Unlock()
	fake.NeedsUpgradeStub = stub
}

func (fake *FakeUpgrader) NeedsUpgradeReturns(result1 bool, result2 error) {
	fake.needsUpgradeMutex.Lock()
	defer fake.needsUpgradeMutex.Unlock()
	fake.NeedsUpgradeStub = nil
	fake.needsUpgradeReturns = struct {
		result1 bool
		result2 error
	}{result1, result2}
}

func (fake *FakeUpgrader) NeedsUpgradeReturnsOnCall(i int, result1 bool, result2 error) {
	fake.needsUpgradeMutex.Lock()
	defer fake.needsUpgradeMutex.Unlock()
	fake.NeedsUpgradeStub = nil
	if fake.needsUpgradeReturnsOnCall == nil {
		fake.needsUpgradeReturnsOnCall = make(map[int]struct {
			result1 bool
			result2 error
		})
	}
	fake.needsUpgradeReturnsOnCall[i] = struct {
		result1 bool
		result2 error
	}{result1, result2}
}

func (fake *FakeUpgrader) Upgrade() error {
	fake.upgradeMutex.Lock()
	ret, specificReturn := fake.upgradeReturnsOnCall[len(fake.upgradeArgsForCall)]
	fake.upgradeArgsForCall = append(fake.upgradeArgsForCall, struct {
	}{})
	fake.recordInvocation("Upgrade", []interface{}{})
	fake.upgradeMutex.Unlock()
	if fake.UpgradeStub != nil {
		return fake.UpgradeStub()
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.upgradeReturns
	return fakeReturns.result1
}

func (fake *FakeUpgrader) UpgradeCallCount() int {
	fake.upgradeMutex.RLock()
	defer fake.upgradeMutex.RUnlock()
	return len(fake.upgradeArgsForCall)
}

func (fake *FakeUpgrader) UpgradeCalls(stub func() error) {
	fake.upgradeMutex.Lock()
	defer fake.upgradeMutex.Unlock()
	fake.UpgradeStub = stub
}

func (fake *FakeUpgrader) UpgradeReturns(result1 error) {
	fake.upgradeMutex.Lock()
	defer fake.upgradeMutex.Unlock()
	fake.UpgradeStub = nil
	fake.upgradeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeUpgrader) UpgradeReturnsOnCall(i int, result1 error) {
	fake.upgradeMutex.Lock()
	defer fake.upgradeMutex.Unlock()
	fake.UpgradeStub = nil
	if fake.upgradeReturnsOnCall == nil {
		fake.upgradeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.upgradeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeUpgrader) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.needsUpgradeMutex.RLock()
	defer fake.needsUpgradeMutex.RUnlock()
	fake.upgradeMutex.RLock()
	defer fake.upgradeMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeUpgrader) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ upgrader.Upgrader = new(FakeUpgrader)
