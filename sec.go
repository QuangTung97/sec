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
	// EventTypeCompensatingRequestCompleted ...
	EventTypeCompensatingRequestCompleted EventType = 4
	// EventTypeSaveCheckpoint ...
	EventTypeSaveCheckpoint EventType = 5
)

// LogSequence ...
type LogSequence uint64

// NullLogSequence ...
type NullLogSequence struct {
	Valid    bool
	Sequence LogSequence
}

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
	ReplyChan    chan<- SagaResult
}

// SagaResponse ...
type SagaResponse struct {
	RequestType RequestType
	Response    interface{}
}

// SagaError ...
type SagaError struct {
	RequestType RequestType
	Error       error
}

// SagaResult ...
type SagaResult struct {
	Failed    bool
	Responses []SagaResponse
	Errors    []SagaError
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

type waitingCompensatingRequest struct {
	dependedFinishedCount int
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
	replyChan    chan<- SagaResult

	activeRequests    map[RequestType]struct{}
	waitingRequests   map[RequestType]waitingRequest
	completedRequests map[RequestType]completedRequest

	failedRequests                map[RequestType]failedRequest
	waitingCompensatingRequests   map[RequestType]waitingCompensatingRequest
	activeCompensatingRequests    map[RequestType]struct{}
	completedCompensatingRequests map[RequestType]struct{}

	sequenceListIndex SequenceMinListIndex
}

// CoordinatorConfig for configuring Coordinator
type CoordinatorConfig struct {
	BatchSize int
}

// Coordinator ...
type Coordinator struct {
	config CoordinatorConfig

	registry     map[SagaType]sagaRegistryEntry
	sagaStates   map[LogSequence]*sagaState
	lastSequence LogSequence
	sequenceList *SequenceMinList
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

type replyAction struct {
	replyChan chan<- SagaResult
	result    SagaResult
}

type runLoopOutput struct {
	saveLogEntries            []LogEntry
	startRequests             []startRequest
	startCompensatingRequests []startCompensatingRequest
	replyList                 []replyAction
	saveCheckpoint            NullLogSequence
}

// NewCoordinator ...
func NewCoordinator(conf CoordinatorConfig) *Coordinator {
	return &Coordinator{
		config: conf,

		registry:     map[SagaType]sagaRegistryEntry{},
		sagaStates:   map[LogSequence]*sagaState{},
		sequenceList: NewSequenceMinList(1000),
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
	sagaConfig := c.registry[event.SagaType]

	for _, reqType := range sagaConfig.allRequestTypes {
		req := sagaConfig.requests[reqType]

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

	sequenceListIndex := c.sequenceList.Put(c.lastSequence)

	c.sagaStates[c.lastSequence] = &sagaState{
		sagaType:          event.SagaType,
		content:           event.Content,
		replyChan:         event.ReplyChan,
		activeRequests:    activeRequests,
		waitingRequests:   waitingRequests,
		sequenceListIndex: sequenceListIndex,
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

	if !state.compensating {
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
	}

	startCompensatingRequests := output.startCompensatingRequests
	if state.compensating {
		startCompensatingRequests = append(startCompensatingRequests, startCompensatingRequest{
			rootSequence: event.RootSequence,
			sagaType:     event.SagaType,
			requestType:  event.RequestType,
		})
	}

	replyList := output.replyList
	if !state.compensating && len(state.activeRequests) == 0 {
		delete(c.sagaStates, event.RootSequence)
		c.sequenceList.Delete(state.sequenceListIndex)

		responses := make([]SagaResponse, 0, len(sagaConfig.allRequestTypes))
		for _, requestType := range sagaConfig.allRequestTypes {
			responses = append(responses, SagaResponse{
				RequestType: requestType,
				Response:    state.completedRequests[requestType].response,
			})
		}

		replyList = append(replyList, replyAction{
			replyChan: state.replyChan,
			result: SagaResult{
				Failed:    false,
				Responses: responses,
			},
		})
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
	output.startCompensatingRequests = startCompensatingRequests
	output.replyList = replyList
	return output
}

func finished(state *sagaState, requestType RequestType) bool {
	_, existed := state.failedRequests[requestType]
	if existed {
		return true
	}

	_, existed = state.completedCompensatingRequests[requestType]
	if existed {
		return true
	}

	return false
}

func updateWaitingCompensatingRequests(
	state *sagaState, sagaConfig sagaRegistryEntry, event Event,
	startCompensatingRequests []startCompensatingRequest,
) []startCompensatingRequest {
	requestConfig := sagaConfig.requests[event.RequestType]
	for _, dependentRequest := range requestConfig.dependencies {
		dependentRequestConfig := sagaConfig.requests[dependentRequest]

		var dependedFinishedCount int
		waiting, existed := state.waitingCompensatingRequests[dependentRequest]
		if existed {
			dependedFinishedCount = waiting.dependedFinishedCount - 1
		} else {
			dependedFinishedCount = len(dependentRequestConfig.depended)
			for _, dependedRequest := range dependentRequestConfig.depended {
				if finished(state, dependedRequest) {
					dependedFinishedCount--
				}
			}
		}

		if dependedFinishedCount > 0 {
			state.waitingCompensatingRequests[dependentRequest] = waitingCompensatingRequest{
				dependedFinishedCount: dependedFinishedCount,
			}
		} else {
			delete(state.waitingCompensatingRequests, dependentRequest)
			state.activeCompensatingRequests[dependentRequest] = struct{}{}

			startCompensatingRequests = append(startCompensatingRequests, startCompensatingRequest{
				rootSequence: event.RootSequence,
				sagaType:     event.SagaType,
				requestType:  dependentRequest,
			})
		}
	}

	return startCompensatingRequests
}

func (c *Coordinator) checkSagaRollbackCompleted(
	state *sagaState, rootSequence LogSequence,
	sagaConfig sagaRegistryEntry, replyList []replyAction,
) []replyAction {
	if len(state.activeCompensatingRequests) == 0 && len(state.waitingCompensatingRequests) == 0 {
		delete(c.sagaStates, rootSequence)
		c.sequenceList.Delete(state.sequenceListIndex)

		errors := make([]SagaError, 0, len(state.failedRequests))
		for _, requestType := range sagaConfig.allRequestTypes {
			failed, existed := state.failedRequests[requestType]
			if existed {
				errors = append(errors, SagaError{
					RequestType: requestType,
					Error:       failed.err,
				})
			}
		}

		replyList = append(replyList, replyAction{
			replyChan: state.replyChan,
			result: SagaResult{
				Failed: true,
				Errors: errors,
			},
		})
	}

	return replyList
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

	if state.waitingCompensatingRequests == nil {
		state.waitingCompensatingRequests = map[RequestType]waitingCompensatingRequest{}
	}
	if state.activeCompensatingRequests == nil {
		state.activeCompensatingRequests = map[RequestType]struct{}{}
	}

	startCompensatingRequests := updateWaitingCompensatingRequests(
		state, sagaConfig, event, output.startCompensatingRequests)

	replyList := c.checkSagaRollbackCompleted(state, event.RootSequence, sagaConfig, output.replyList)

	output.saveLogEntries = saveLogEntries
	output.startCompensatingRequests = startCompensatingRequests
	output.replyList = replyList
	return output
}

func (c *Coordinator) handleCompensatingRequestCompleted(event Event, output runLoopOutput) runLoopOutput {
	saveLogEntries := output.saveLogEntries

	saveLogEntries = append(saveLogEntries, LogEntry{
		Sequence:     c.lastSequence,
		RootSequence: event.RootSequence,
		Type:         EventTypeCompensatingRequestCompleted,
		SagaType:     event.SagaType,
		RequestType:  event.RequestType,
	})

	state := c.sagaStates[event.RootSequence]
	sagaConfig := c.registry[event.SagaType]

	if state.completedCompensatingRequests == nil {
		state.completedCompensatingRequests = map[RequestType]struct{}{}
	}

	delete(state.activeCompensatingRequests, event.RequestType)
	state.completedCompensatingRequests[event.RequestType] = struct{}{}

	startCompensatingRequests := updateWaitingCompensatingRequests(
		state, sagaConfig, event, output.startCompensatingRequests)

	replyList := c.checkSagaRollbackCompleted(state, event.RootSequence, sagaConfig, output.replyList)

	output.saveLogEntries = saveLogEntries
	output.startCompensatingRequests = startCompensatingRequests
	output.replyList = replyList
	return output
}

func (c *Coordinator) getCheckpoint() LogSequence {
	if c.sequenceList.Len() > 0 {
		return c.sequenceList.MinSequence() - 1
	}
	return c.lastSequence
}

func (c *Coordinator) handleSaveCheckpoint(output runLoopOutput) runLoopOutput {
	c.lastSequence--

	output.saveCheckpoint = NullLogSequence{
		Valid:    true,
		Sequence: c.getCheckpoint(),
	}
	return output
}

func (c *Coordinator) handleEvent(event Event, output runLoopOutput) runLoopOutput {
	c.lastSequence++

	switch event.Type {
	case EventTypeNewSaga:
		return c.handleNewSaga(event, output)

	case EventTypeRequestCompleted:
		return c.handleRequestCompleted(event, output)

	case EventTypeRequestPreconditionFailed:
		return c.handleRequestPreconditionFailed(event, output)

	case EventTypeCompensatingRequestCompleted:
		return c.handleCompensatingRequestCompleted(event, output)

	case EventTypeSaveCheckpoint:
		return c.handleSaveCheckpoint(output)

	default:
		panic("invalid event type")
	}
}

func (c *Coordinator) runLoop(ctx context.Context, input runLoopInput) (runLoopOutput, error) {
	select {
	case event := <-input.eventChan:
		var output runLoopOutput
		output = c.handleEvent(event, output)

	BatchLoop:
		for eventCount := 1; eventCount < c.config.BatchSize; eventCount++ {
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
