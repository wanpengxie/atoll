package devicebus

import (
	"context"
	"errors"
	"testing"
)

type noopDeviceTransport struct{}

func (noopDeviceTransport) ReadFrame(context.Context) (DeviceFrame, error) {
	return DeviceFrame{}, errors.New("unused")
}

func (noopDeviceTransport) WriteFrame(context.Context, DeviceFrame) error { return nil }
func (noopDeviceTransport) Close() error                                  { return nil }

func TestUnregisterConnectionCompareAndDelete(t *testing.T) {
	svc := NewService(nil, Config{})
	session := Session{ID: "session-1"}
	oldConn := NewConnection(session, noopDeviceTransport{})
	newConn := NewConnection(session, noopDeviceTransport{})

	svc.registerConnection(session.ID, oldConn)
	svc.registerConnection(session.ID, newConn)

	if svc.unregisterConnection(session.ID, oldConn) {
		t.Fatal("old connection unregister removed current connection")
	}
	if got := svc.sessions[session.ID]; got != newConn {
		t.Fatalf("current connection = %p want %p", got, newConn)
	}
	if !svc.unregisterConnection(session.ID, newConn) {
		t.Fatal("current connection unregister did not remove entry")
	}
	if got := svc.sessions[session.ID]; got != nil {
		t.Fatalf("session still registered: %p", got)
	}
}
