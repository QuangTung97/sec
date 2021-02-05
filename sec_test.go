package sec

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

// . Coordinator
// 	+ Event
// 		* New Saga
// 		* Completed
//  + Context

func TestCoordinator_RunLoop_Context(t *testing.T) {
	c := NewCoordinator()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	output, err := c.runLoop(ctx, runLoopInput{})
	assert.Equal(t, nil, err)
	assert.Equal(t, runLoopOutput{}, output)
}

type testSaga struct {
	num int
}

type testResponseContent struct {
	value int
}

const testSagaType SagaType = 1

const testRequestFirst RequestType = 1
const testRequestSecond RequestType = 2

func testSagaDecoder(string) interface{} {
	return nil
}

func TestCoordinator_Register(t *testing.T) {
	c := NewCoordinator()
	sagaDecoder := testSagaDecoder

	c.Register(testSagaType, sagaDecoder,
		[]RequestRegistry{
			{
				Type:         testRequestFirst,
				Dependencies: nil,
				OnRequest: func(ctx context.Context, sequence LogSequence, root interface{}, deps []interface{}) RequestOutput {
					return RequestOutput{}
				},
				Decoder: func(data string) interface{} {
					return nil
				},
			},
			{
				Type:         testRequestSecond,
				Dependencies: []RequestType{testRequestFirst},
				OnRequest: func(ctx context.Context, sequence LogSequence, root interface{}, deps []interface{}) RequestOutput {
					return RequestOutput{}
				},
				Decoder: func(data string) interface{} {
					return nil
				},
			},
		})

	assert.Equal(t, 1, len(c.registry))

	entry, existed := c.registry[testSagaType]
	assert.True(t, existed)
	assert.Equal(t, 2, len(entry.requests))

	request1 := entry.requests[testRequestFirst]
	assert.Equal(t, request1.dependencies, []RequestType(nil))
	assert.Equal(t, request1.depended, []RequestType{testRequestSecond})

	request2 := entry.requests[testRequestSecond]
	assert.Equal(t, request2.dependencies, []RequestType{testRequestFirst})
	assert.Equal(t, request2.depended, []RequestType(nil))

	assert.Equal(t, []RequestType{testRequestFirst, testRequestSecond}, entry.allRequestTypes)
}

func TestCoordinator_RunLoop_Events(t *testing.T) {
	table := []struct {
		name   string
		events []Event

		lastSequenceBefore LogSequence
		lastSequenceAfter  LogSequence

		sagaStatesBefore map[LogSequence]*sagaState
		sagaStatesAfter  map[LogSequence]*sagaState

		output runLoopOutput
	}{
		{
			name: "new-saga",
			events: []Event{
				{
					Type:     EventTypeNewSaga,
					SagaType: testSagaType,
					Content: testSaga{
						num: 100,
					},
					Data: "num: 100",
				},
			},
			lastSequenceBefore: 10,
			lastSequenceAfter:  11,
			sagaStatesBefore:   map[LogSequence]*sagaState{},
			sagaStatesAfter: map[LogSequence]*sagaState{
				11: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
					waitingRequests: map[RequestType]waitingRequest{
						testRequestSecond: {
							dependencyCompletedCount: 0,
						},
					},
				},
			},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence: 11,
						Type:     EventTypeNewSaga,
						SagaType: testSagaType,
						Data:     "num: 100",
					},
				},
				startRequests: []startRequest{
					{
						rootSequence: 11,
						sagaType:     testSagaType,
						requestType:  testRequestFirst,
						rootContent: testSaga{
							num: 100,
						},
					},
				},
			},
		},
		{
			name: "request-completed",
			events: []Event{
				{
					Type:         EventTypeRequestCompleted,
					SagaType:     testSagaType,
					RootSequence: 18,
					RequestType:  testRequestFirst,
					Content: testResponseContent{
						value: 120,
					},
					Data: "response.value: 120",
				},
			},
			lastSequenceBefore: 20,
			lastSequenceAfter:  21,
			sagaStatesBefore: map[LogSequence]*sagaState{
				18: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
					waitingRequests: map[RequestType]waitingRequest{
						testRequestSecond: {
							dependencyCompletedCount: 0,
						},
					},
				},
			},
			sagaStatesAfter: map[LogSequence]*sagaState{
				18: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestSecond: {},
					},
					waitingRequests: map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{
						testRequestFirst: {
							response: testResponseContent{
								value: 120,
							},
						},
					},
				},
			},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence:     21,
						RootSequence: 18,
						Type:         EventTypeRequestCompleted,
						SagaType:     testSagaType,
						RequestType:  testRequestFirst,
						Data:         "response.value: 120",
					},
				},
				startRequests: []startRequest{
					{
						rootSequence: 18,
						sagaType:     testSagaType,
						requestType:  testRequestSecond,
						rootContent: testSaga{
							num: 100,
						},
						dependentResponses: []interface{}{
							testResponseContent{
								value: 120,
							},
						},
					},
				},
			},
		},
		{
			name: "two-new-sagas",
			events: []Event{
				{
					Type:     EventTypeNewSaga,
					SagaType: testSagaType,
					Content: testSaga{
						num: 100,
					},
					Data: "num: 100",
				},
				{
					Type:     EventTypeNewSaga,
					SagaType: testSagaType,
					Content: testSaga{
						num: 200,
					},
					Data: "num: 200",
				},
			},
			lastSequenceBefore: 10,
			lastSequenceAfter:  12,
			sagaStatesBefore:   map[LogSequence]*sagaState{},
			sagaStatesAfter: map[LogSequence]*sagaState{
				11: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
					waitingRequests: map[RequestType]waitingRequest{
						testRequestSecond: {
							dependencyCompletedCount: 0,
						},
					},
				},
				12: {
					sagaType: testSagaType,
					content: testSaga{
						num: 200,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
					waitingRequests: map[RequestType]waitingRequest{
						testRequestSecond: {
							dependencyCompletedCount: 0,
						},
					},
				},
			},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence: 11,
						Type:     EventTypeNewSaga,
						SagaType: testSagaType,
						Data:     "num: 100",
					},
					{
						Sequence: 12,
						Type:     EventTypeNewSaga,
						SagaType: testSagaType,
						Data:     "num: 200",
					},
				},
				startRequests: []startRequest{
					{
						rootSequence: 11,
						sagaType:     testSagaType,
						requestType:  testRequestFirst,
						rootContent: testSaga{
							num: 100,
						},
					},
					{
						rootSequence: 12,
						sagaType:     testSagaType,
						requestType:  testRequestFirst,
						rootContent: testSaga{
							num: 200,
						},
					},
				},
			},
		},
		{
			name: "new-saga-and-request-completed",
			events: []Event{
				{
					Type:     EventTypeNewSaga,
					SagaType: testSagaType,
					Content: testSaga{
						num: 100,
					},
					Data: "num: 100",
				},
				{
					Type:         EventTypeRequestCompleted,
					SagaType:     testSagaType,
					RootSequence: 18,
					RequestType:  testRequestFirst,
					Content: testResponseContent{
						value: 120,
					},
					Data: "response.value: 120",
				},
			},
			lastSequenceBefore: 20,
			lastSequenceAfter:  22,
			sagaStatesBefore: map[LogSequence]*sagaState{
				18: {
					sagaType: testSagaType,
					content: testSaga{
						num: 300,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
					waitingRequests: map[RequestType]waitingRequest{
						testRequestSecond: {
							dependencyCompletedCount: 0,
						},
					},
				},
			},
			sagaStatesAfter: map[LogSequence]*sagaState{
				18: {
					sagaType: testSagaType,
					content: testSaga{
						num: 300,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestSecond: {},
					},
					waitingRequests: map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{
						testRequestFirst: {
							response: testResponseContent{
								value: 120,
							},
						},
					},
				},
				21: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
					waitingRequests: map[RequestType]waitingRequest{
						testRequestSecond: {
							dependencyCompletedCount: 0,
						},
					},
				},
			},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence: 21,
						Type:     EventTypeNewSaga,
						SagaType: testSagaType,
						Data:     "num: 100",
					},
					{
						Sequence:     22,
						RootSequence: 18,
						Type:         EventTypeRequestCompleted,
						SagaType:     testSagaType,
						RequestType:  testRequestFirst,
						Data:         "response.value: 120",
					},
				},
				startRequests: []startRequest{
					{
						rootSequence: 21,
						sagaType:     testSagaType,
						requestType:  testRequestFirst,
						rootContent: testSaga{
							num: 100,
						},
					},
					{
						rootSequence: 18,
						sagaType:     testSagaType,
						requestType:  testRequestSecond,
						rootContent: testSaga{
							num: 300,
						},
						dependentResponses: []interface{}{
							testResponseContent{
								value: 120,
							},
						},
					},
				},
			},
		},
		{
			name: "saga-finished",
			events: []Event{
				{
					Type:         EventTypeRequestCompleted,
					SagaType:     testSagaType,
					RootSequence: 18,
					RequestType:  testRequestSecond,
					Content: testResponseContent{
						value: 250,
					},
					Data: "response.value: 250",
				},
			},
			lastSequenceBefore: 30,
			lastSequenceAfter:  31,
			sagaStatesBefore: map[LogSequence]*sagaState{
				18: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestSecond: {},
					},
					waitingRequests: map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{
						testRequestFirst: {
							response: testResponseContent{
								value: 120,
							},
						},
					},
				},
			},
			sagaStatesAfter: map[LogSequence]*sagaState{},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence:     31,
						RootSequence: 18,
						Type:         EventTypeRequestCompleted,
						SagaType:     testSagaType,
						RequestType:  testRequestSecond,
						Data:         "response.value: 250",
					},
				},
			},
		},
		{
			name: "request-precondition-failed",
			events: []Event{
				{
					Type:         EventTypeRequestPreconditionFailed,
					SagaType:     testSagaType,
					RootSequence: 18,
					RequestType:  testRequestSecond,
					Error:        errors.New("precondition-failed"),
					Data:         "response.error: precondition-failed",
				},
			},
			lastSequenceBefore: 30,
			lastSequenceAfter:  31,
			sagaStatesBefore: map[LogSequence]*sagaState{
				18: {
					sagaType: testSagaType,
					content: testSaga{
						num: 100,
					},
					activeRequests: map[RequestType]struct{}{
						testRequestSecond: {},
					},
					waitingRequests: map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{
						testRequestFirst: {
							response: testResponseContent{
								value: 120,
							},
						},
					},
				},
			},
			sagaStatesAfter: map[LogSequence]*sagaState{
				18: {
					sagaType:     testSagaType,
					compensating: true,
					content: testSaga{
						num: 100,
					},
					activeRequests:    map[RequestType]struct{}{},
					waitingRequests:   map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{},
					failedRequests: map[RequestType]failedRequest{
						testRequestSecond: {
							err: errors.New("precondition-failed"),
						},
					},
					activeCompensatingRequests: map[RequestType]struct{}{
						testRequestFirst: {},
					},
				},
			},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence:     31,
						RootSequence: 18,
						Type:         EventTypeRequestPreconditionFailed,
						SagaType:     testSagaType,
						RequestType:  testRequestSecond,
						Data:         "response.error: precondition-failed",
					},
				},
				startCompensatingRequests: []startCompensatingRequest{
					{
						rootSequence: 18,
						sagaType:     testSagaType,
						requestType:  testRequestFirst,
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			c := NewCoordinator()
			c.Register(testSagaType, testSagaDecoder, []RequestRegistry{
				{
					Type:         testRequestFirst,
					Dependencies: nil,
				},
				{
					Type:         testRequestSecond,
					Dependencies: []RequestType{testRequestFirst},
				},
			})

			c.sagaStates = e.sagaStatesBefore
			c.lastSequence = e.lastSequenceBefore

			ctx := context.Background()

			eventChan := make(chan Event, len(e.events))
			for _, event := range e.events {
				eventChan <- event
			}

			input := runLoopInput{
				eventChan: eventChan,
			}

			output, err := c.runLoop(ctx, input)
			assert.Equal(t, nil, err)
			assert.Equal(t, e.output, output)
			assert.Equal(t, e.sagaStatesAfter, c.sagaStates)
			assert.Equal(t, e.lastSequenceAfter, c.lastSequence)
			assert.Equal(t, 0, len(eventChan))
		})
	}
}
