package cdv_mq

import (
	"log/slog"
)

type Message[T any] struct {
	Payload T
}

type Subscription[T any] struct {
	name string
	ch   chan Message[T]
}

type Mq[T any] struct {
	subscribersByTopic map[string][]*Subscription[T]
	subscribers        []*Subscription[T]
	log                *slog.Logger
}

func NewMq[T any](log *slog.Logger) *Mq[T] {
	return &Mq[T]{
		subscribers:        make([]*Subscription[T], 4),
		subscribersByTopic: make(map[string][]*Subscription[T], 16),
		log:                log,
	}
}

func (m *Mq[T]) AddSubscription(name string, ch chan Message[T], topics ...string) {
	subscription := &Subscription[T]{
		name: name,
		ch:   ch,
	}
	m.subscribers = append(m.subscribers, subscription)

	for _, topic := range topics {
		if m.subscribersByTopic[topic] == nil {
			m.subscribersByTopic[topic] = make([]*Subscription[T], 4)
		}
		m.subscribersByTopic[topic] = append(m.subscribersByTopic[topic], subscription)
	}
}

func (m *Mq[T]) SendMessage(topic string, payload T) {
	msg := Message[T]{Payload: payload}
	for _, sub := range m.subscribersByTopic[topic] {
		select {
		case sub.ch <- msg:
			// message sent
		default:
			// Channel is full, log it and continue without blocking
			m.log.Warn("Warning: Could not send message to channel, as it's full", "subName", sub.name)
		}
	}
}
