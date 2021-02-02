package sec

import (
	"context"
)

// EventType ...
type EventType int

const (
	// EventTypeNewSaga ...
	EventTypeNewSaga EventType = 1
)

// LogSequence ...
type LogSequence uint64

// SagaType ...
type SagaType int

// RequestType ...
type RequestType int

// RequestOutput ...
type RequestOutput struct {
}

// OnRequest ...
type OnRequest func(ctx context.Context, sequence LogSequence, root interface{}, deps []interface{}) RequestOutput

// SagaDecoder ...
type SagaDecoder func(data string) interface{}

// RequestDecoder ...
type RequestDecoder func(data string) interface{}

// RequestRegistry ...
type RequestRegistry struct {
	Type         RequestType
	Dependencies []RequestType
	OnRequest    OnRequest
	Decoder      RequestDecoder
}

// Event ...
type Event struct {
	Type     EventType
	SagaType SagaType
	Content  interface{}
	Data     string
}

// LogEntry ...
type LogEntry struct {
	Sequence LogSequence `db:"sequence"`
	Type     EventType   `db:"type"`
	SagaType SagaType    `db:"saga_type"`
	Data     string      `db:"data"`
}

type sagaRegistryEntry struct {
	decoder  SagaDecoder
	requests []RequestRegistry
}

type waitingRequest struct {
	dependencyCompletedCount int
}

type sagaState struct {
	sagaType        SagaType
	content         interface{}
	activeRequests  map[RequestType]struct{}
	waitingRequests map[RequestType]waitingRequest
}

// Coordinator ...
type Coordinator struct {
	registry     map[SagaType]sagaRegistryEntry
	sagaStates   map[LogSequence]*sagaState
	lastSequence LogSequence
}

type runLoopInput struct {
	eventChan chan Event
}

type startRequest struct {
	sequence    LogSequence
	sagaType    SagaType
	requestType RequestType
	rootData    interface{}
}

type runLoopOutput struct {
	saveLogEntries []LogEntry
	startRequests  []startRequest
}

// NewCoordinator ...
func NewCoordinator() *Coordinator {
	return &Coordinator{
		registry:   map[SagaType]sagaRegistryEntry{},
		sagaStates: map[LogSequence]*sagaState{},
	}
}

// Register ...
func (c *Coordinator) Register(
	sagaType SagaType, sagaDecoder SagaDecoder, registryList []RequestRegistry,
) {
	c.registry[sagaType] = sagaRegistryEntry{
		decoder:  sagaDecoder,
		requests: registryList,
	}
}

func (c *Coordinator) runLoop(ctx context.Context, input runLoopInput) (runLoopOutput, error) {
	select {
	case event := <-input.eventChan:
		c.lastSequence++
		c.sagaStates[c.lastSequence] = &sagaState{
			sagaType: event.SagaType,
			content:  event.Content,
			activeRequests: map[RequestType]struct{}{
				1: {}, // TODO
			},
			waitingRequests: map[RequestType]waitingRequest{
				2: {
					dependencyCompletedCount: 0,
				}, // TODO
			},
		}

		return runLoopOutput{
			saveLogEntries: []LogEntry{
				{
					Sequence: c.lastSequence,
					Type:     EventTypeNewSaga,
					SagaType: event.SagaType,
					Data:     event.Data,
				},
			},
			startRequests: []startRequest{
				{
					sequence:    c.lastSequence,
					sagaType:    event.SagaType,
					requestType: 1, // TODO
					rootData:    event.Content,
				},
			},
		}, nil

	case <-ctx.Done():
		return runLoopOutput{}, nil
	}
}
