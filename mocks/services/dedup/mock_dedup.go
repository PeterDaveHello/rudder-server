// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/services/dedup (interfaces: DedupI)

// Package mock_dedup is a generated GoMock package.
package mock_dedup

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockDedupI is a mock of DedupI interface.
type MockDedupI struct {
	ctrl     *gomock.Controller
	recorder *MockDedupIMockRecorder
}

// MockDedupIMockRecorder is the mock recorder for MockDedupI.
type MockDedupIMockRecorder struct {
	mock *MockDedupI
}

// NewMockDedupI creates a new mock instance.
func NewMockDedupI(ctrl *gomock.Controller) *MockDedupI {
	mock := &MockDedupI{ctrl: ctrl}
	mock.recorder = &MockDedupIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDedupI) EXPECT() *MockDedupIMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockDedupI) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockDedupIMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDedupI)(nil).Close))
}

// FindDuplicates mocks base method.
func (m *MockDedupI) FindDuplicates(arg0 []string, arg1 map[string]struct{}) []int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindDuplicates", arg0, arg1)
	ret0, _ := ret[0].([]int)
	return ret0
}

// FindDuplicates indicates an expected call of FindDuplicates.
func (mr *MockDedupIMockRecorder) FindDuplicates(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindDuplicates", reflect.TypeOf((*MockDedupI)(nil).FindDuplicates), arg0, arg1)
}

// MarkProcessed mocks base method.
func (m *MockDedupI) MarkProcessed(arg0 []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkProcessed", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkProcessed indicates an expected call of MarkProcessed.
func (mr *MockDedupIMockRecorder) MarkProcessed(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkProcessed", reflect.TypeOf((*MockDedupI)(nil).MarkProcessed), arg0)
}

// PrintHistogram mocks base method.
func (m *MockDedupI) PrintHistogram() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "PrintHistogram")
}

// PrintHistogram indicates an expected call of PrintHistogram.
func (mr *MockDedupIMockRecorder) PrintHistogram() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PrintHistogram", reflect.TypeOf((*MockDedupI)(nil).PrintHistogram))
}
