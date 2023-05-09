// Code generated by mockery v2.26.1. DO NOT EDIT.

package dbmigratemocks

import (
	io "io"

	database "github.com/golang-migrate/migrate/v4/database"

	mock "github.com/stretchr/testify/mock"
)

// Driver is an autogenerated mock type for the Driver type
type Driver struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Driver) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Drop provides a mock function with given fields:
func (_m *Driver) Drop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Lock provides a mock function with given fields:
func (_m *Driver) Lock() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Open provides a mock function with given fields: url
func (_m *Driver) Open(url string) (database.Driver, error) {
	ret := _m.Called(url)

	var r0 database.Driver
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (database.Driver, error)); ok {
		return rf(url)
	}
	if rf, ok := ret.Get(0).(func(string) database.Driver); ok {
		r0 = rf(url)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(database.Driver)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(url)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Run provides a mock function with given fields: migration
func (_m *Driver) Run(migration io.Reader) error {
	ret := _m.Called(migration)

	var r0 error
	if rf, ok := ret.Get(0).(func(io.Reader) error); ok {
		r0 = rf(migration)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetVersion provides a mock function with given fields: version, dirty
func (_m *Driver) SetVersion(version int, dirty bool) error {
	ret := _m.Called(version, dirty)

	var r0 error
	if rf, ok := ret.Get(0).(func(int, bool) error); ok {
		r0 = rf(version, dirty)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Unlock provides a mock function with given fields:
func (_m *Driver) Unlock() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Version provides a mock function with given fields:
func (_m *Driver) Version() (int, bool, error) {
	ret := _m.Called()

	var r0 int
	var r1 bool
	var r2 error
	if rf, ok := ret.Get(0).(func() (int, bool, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func() bool); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(bool)
	}

	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

type mockConstructorTestingTNewDriver interface {
	mock.TestingT
	Cleanup(func())
}

// NewDriver creates a new instance of Driver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewDriver(t mockConstructorTestingTNewDriver) *Driver {
	mock := &Driver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
