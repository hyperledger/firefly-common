// Code generated by mockery v2.26.1. DO NOT EDIT.

package authmocks

import (
	context "context"

	config "github.com/hyperledger/firefly-common/pkg/config"

	fftypes "github.com/hyperledger/firefly-common/pkg/fftypes"

	mock "github.com/stretchr/testify/mock"
)

// Plugin is an autogenerated mock type for the Plugin type
type Plugin struct {
	mock.Mock
}

// Authorize provides a mock function with given fields: ctx, req
func (_m *Plugin) Authorize(ctx context.Context, req *fftypes.AuthReq) error {
	ret := _m.Called(ctx, req)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *fftypes.AuthReq) error); ok {
		r0 = rf(ctx, req)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Init provides a mock function with given fields: ctx, name, _a2
func (_m *Plugin) Init(ctx context.Context, name string, _a2 config.Section) error {
	ret := _m.Called(ctx, name, _a2)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, config.Section) error); ok {
		r0 = rf(ctx, name, _a2)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// InitConfig provides a mock function with given fields: _a0
func (_m *Plugin) InitConfig(_a0 config.Section) {
	_m.Called(_a0)
}

// Name provides a mock function with given fields:
func (_m *Plugin) Name() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type mockConstructorTestingTNewPlugin interface {
	mock.TestingT
	Cleanup(func())
}

// NewPlugin creates a new instance of Plugin. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewPlugin(t mockConstructorTestingTNewPlugin) *Plugin {
	mock := &Plugin{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
