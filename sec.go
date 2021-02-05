package sec

import (
	"context"
)

// EventType ...
type EventType int

const (
	// EventTypeNewSaga ...
	EventTypeNewSaga EventType = 1
	// EventTypeRequestCompleted ...
	EventTypeRequestCompleted EventType = 2
	// EventTypeRequestPreconditionFailed is an error that should not be retry
	EventTypeRequestPreconditionFailed EventType = 3
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

type registeredRequest struct {
	dependencies []RequestType
	depended     []RequestType
	onRequest    OnRequest
	decoder      RequestDecoder
}

// Event ...
type Event struct {
	Type         EventType
	SagaType     SagaType
	RootSequence LogSequence
	RequestType  RequestType
	Content      interface{}
	Error        error
	Data         string
}

// LogEntry ...
type LogEntry struct {
	Sequence     LogSequence `db:"sequence"`
	RootSequence LogSequence `db:"root_sequence"`
	Type         EventType   `db:"type"`
	SagaType     SagaType    `db:"saga_type"`
	RequestType  RequestType `db:"request_type"`
	Data         string      `db:"data"`
}

type sagaRegistryEntry struct {
	decoder         SagaDecoder
	requests        map[RequestType]registeredRequest
	allRequestTypes []RequestType
}

type waitingRequest struct {
	dependencyCompletedCount int
}

type completedRequest struct {
	response interface{}
}

type failedRequest struct {
	err error
}

type sagaState struct {
	sagaType     SagaType
	compensating bool
	content      interface{}

	activeRequests    map[RequestType]struct{}
	waitingRequests   map[RequestType]waitingRequest
	completedRequests map[RequestType]completedRequest

	failedRequests             map[RequestType]failedRequest
	activeCompensatingRequests map[RequestType]struct{}
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
	rootSequence       LogSequence
	sagaType           SagaType
	requestType        RequestType
	rootContent        interface{}
	dependentResponses []interface{}
}

type startCompensatingRequest struct {
	rootSequence LogSequence
	sagaType     SagaType
	requestType  RequestType
}

type runLoopOutput struct {
	saveLogEntries            []LogEntry
	startRequests             []startRequest
	startCompensatingRequests []startCompensatingRequest
}

// NewCoordinator ...
func NewCoordinator() *Coordinator {
	return &Coordinator{
		registry:   map[SagaType]sagaRegistryEntry{},
		sagaStates: map[LogSequence]*sagaState{},
	}
}

// Register updates *registryList*
func (c *Coordinator) Register(
	sagaType SagaType, sagaDecoder SagaDecoder, registryList []RequestRegistry,
) {
	requests := make(map[RequestType]registeredRequest)
	depended := make(map[RequestType][]RequestType)
	allRequestTypes := make([]RequestType, 0, len(registryList))

	for _, entry := range registryList {
		allRequestTypes = append(allRequestTypes, entry.Type)

		requests[entry.Type] = registeredRequest{
			dependencies: entry.Dependencies,
			onRequest:    entry.OnRequest,
			decoder:      entry.Decoder,
		}

		for _, dep := range entry.Dependencies {
			depended[dep] = append(depended[dep], entry.Type)
		}
	}

	for requestType := range requests {
		registered := requests[requestType]
		registered.depended = depended[requestType]
		requests[requestType] = registered
	}

	c.registry[sagaType] = sagaRegistryEntry{
		decoder:         sagaDecoder,
		requests:        requests,
		allRequestTypes: allRequestTypes,
	}
}

func (c *Coordinator) handleNewSaga(event Event, output runLoopOutput) runLoopOutput {
	startRequests := output.startRequests

	activeRequests := make(map[RequestType]struct{})
	waitingRequests := make(map[RequestType]waitingRequest)
	for reqType, req := range c.registry[event.SagaType].requests {
		if len(req.dependencies) == 0 {
			activeRequests[reqType] = struct{}{}
			startRequests = append(startRequests, startRequest{
				rootSequence: c.lastSequence,
				sagaType:     event.SagaType,
				requestType:  reqType,
				rootContent:  event.Content,
			})
		} else {
			waitingRequests[reqType] = waitingRequest{
				dependencyCompletedCount: 0,
			}
		}
	}

	c.sagaStates[c.lastSequence] = &sagaState{
		sagaType:        event.SagaType,
		content:         event.Content,
		activeRequests:  activeRequests,
		waitingRequests: waitingRequests,
	}

	saveLogEntries := append(output.saveLogEntries, LogEntry{
		Sequence: c.lastSequence,
		Type:     EventTypeNewSaga,
		SagaType: event.SagaType,
		Data:     event.Data,
	})

	output.startRequests = startRequests
	output.saveLogEntries = saveLogEntries
	return output
}

func (c *Coordinator) handleRequestCompleted(event Event, output runLoopOutput) runLoopOutput {
	startRequests := output.startRequests

	state := c.sagaStates[event.RootSequence]

	sagaConfig := c.registry[state.sagaType]
	requestConfig := sagaConfig.requests[event.RequestType]
	delete(state.activeRequests, event.RequestType)

	if state.completedRequests == nil {
		state.completedRequests = map[RequestType]completedRequest{}
	}
	state.completedRequests[event.RequestType] = completedRequest{
		response: event.Content,
	}

	for _, dependedRequest := range requestConfig.depended {
		value := state.waitingRequests[dependedRequest]
		value.dependencyCompletedCount++
		state.waitingRequests[dependedRequest] = value

		dependedReqConfig := sagaConfig.requests[dependedRequest]
		if value.dependencyCompletedCount == len(dependedReqConfig.dependencies) {
			delete(state.waitingRequests, dependedRequest)
			state.activeRequests[dependedRequest] = struct{}{}

			dependentResponses := make([]interface{}, 0, len(dependedReqConfig.dependencies))
			for _, dependency := range dependedReqConfig.dependencies {
				dependentResponses = append(dependentResponses, state.completedRequests[dependency].response)
			}

			startRequests = append(startRequests, startRequest{
				rootSequence:       event.RootSequence,
				sagaType:           event.SagaType,
				requestType:        dependedRequest,
				rootContent:        state.content,
				dependentResponses: dependentResponses,
			})
		}
	}

	if len(state.activeRequests) == 0 {
		delete(c.sagaStates, event.RootSequence)
	}

	saveLogEntries := append(output.saveLogEntries, LogEntry{
		Sequence:     c.lastSequence,
		RootSequence: event.RootSequence,
		Type:         EventTypeRequestCompleted,
		SagaType:     event.SagaType,
		RequestType:  event.RequestType,
		Data:         event.Data,
	})

	output.saveLogEntries = saveLogEntries
	output.startRequests = startRequests
	return output
}

func (c *Coordinator) handleRequestPreconditionFailed(event Event, output runLoopOutput) runLoopOutput {
	saveLogEntries := output.saveLogEntries

	saveLogEntries = append(saveLogEntries, LogEntry{
		Sequence:     c.lastSequence,
		RootSequence: event.RootSequence,
		Type:         EventTypeRequestPreconditionFailed,
		SagaType:     event.SagaType,
		RequestType:  event.RequestType,
		Data:         event.Data,
	})

	state := c.sagaStates[event.RootSequence]
	sagaConfig := c.registry[event.SagaType]

	state.compensating = true
	delete(state.activeRequests, event.RequestType)
	if state.failedRequests == nil {
		state.failedRequests = map[RequestType]failedRequest{}
	}
	state.failedRequests[event.RequestType] = failedRequest{
		err: event.Error,
	}

	startCompensatingRequests := output.startCompensatingRequests

	for _, requestType := range sagaConfig.allRequestTypes {
		_, existed := state.completedRequests[requestType]
		if !existed {
			continue
		}

		if state.activeCompensatingRequests == nil {
			state.activeCompensatingRequests = map[RequestType]struct{}{}
		}
		state.activeCompensatingRequests[requestType] = struct{}{}
		delete(state.completedRequests, requestType)

		startCompensatingRequests = append(startCompensatingRequests, startCompensatingRequest{
			rootSequence: event.RootSequence,
			sagaType:     event.SagaType,
			requestType:  requestType,
		})
	}

	output.saveLogEntries = saveLogEntries
	output.startCompensatingRequests = startCompensatingRequests
	return output
}

func (c *Coordinator) handleEvent(event Event, output runLoopOutput) runLoopOutput {
	c.lastSequence++

	if event.Type == EventTypeNewSaga {
		return c.handleNewSaga(event, output)
	}
	if event.Type == EventTypeRequestCompleted {
		return c.handleRequestCompleted(event, output)
	}

	return c.handleRequestPreconditionFailed(event, output)
}

func (c *Coordinator) runLoop(ctx context.Context, input runLoopInput) (runLoopOutput, error) {
	select {
	case event := <-input.eventChan:
		var output runLoopOutput
		output = c.handleEvent(event, output)

	BatchLoop:
		for {
			select {
			case e := <-input.eventChan:
				output = c.handleEvent(e, output)
			default:
				break BatchLoop
			}
		}
		return output, nil

	case <-ctx.Done():
		return runLoopOutput{}, nil
	}
}
