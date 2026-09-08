package channelmember

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestProjectionRefreshesOnlyOnAttachmentAndFencesStaleNotifications(t *testing.T) {
	hub := NewHub()
	pair := Pair{Host: "h", Body: "a"}
	p := &manifestProjection{err: ErrUnreachable}
	releaseWatch, err := hub.WatchHandleBinding(pair, func(b HandleBinding) { p.refresh(context.Background(), b) })
	if err != nil {
		t.Fatal(err)
	}
	defer releaseWatch()
	var reads atomic.Int32
	first, err := hub.AttachHandle(pair, func(context.Context, Request) (Response, error) {
		reads.Add(1)
		return Response{Payload: []byte(`{"words":{"old":{"description":"old"}}}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		words, err := p.read()
		if err != nil || words["old"].Description != "old" {
			t.Fatalf("projection=%v %v", words, err)
		}
	}
	if reads.Load() != 1 {
		t.Fatalf("describe calls=%d", reads.Load())
	}
	second, err := hub.AttachHandle(pair, func(context.Context, Request) (Response, error) {
		reads.Add(1)
		return Response{Payload: []byte(`{"words":{"new":{"description":"new"}}}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first()
	words, err := p.read()
	if err != nil || words["new"].Description != "new" || len(words) != 1 {
		t.Fatalf("replacement=%v %v", words, err)
	}
	second()
	if _, err := p.read(); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("detach=%v", err)
	}
	if reads.Load() != 2 {
		t.Fatalf("describe calls=%d", reads.Load())
	}
}

func TestSlowDescribeCannotRestoreDetachedProjection(t *testing.T) {
	p := &manifestProjection{err: ErrUnreachable}
	entered := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.refresh(context.Background(), HandleBinding{Generation: 7, Call: func(context.Context, Request) (Response, error) {
			close(entered)
			<-finish
			return Response{Payload: []byte(`{"words":{"stale":{}}}`)}, nil
		}})
	}()
	<-entered
	p.refresh(context.Background(), HandleBinding{Generation: 7})
	close(finish)
	<-done
	if words, err := p.read(); len(words) != 0 || !errors.Is(err, ErrUnreachable) {
		t.Fatalf("late describe resurrected words: %v %v", words, err)
	}
}
