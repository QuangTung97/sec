package sec

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCoordinator_RunLoop_Context(t *testing.T) {
	c := NewCoordinator(CoordinatorConfig{})

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
	c := NewCoordinator(CoordinatorConfig{})
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
	replyChan := make(chan SagaResult)

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
					replyChan: replyChan,
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
				replyList: []replyAction{
					{
						replyChan: replyChan,
						result: SagaResult{
							Failed: false,
							Responses: []SagaResponse{
								{
									RequestType: testRequestFirst,
									Response: testResponseContent{
										value: 120,
									},
								},
								{
									RequestType: testRequestSecond,
									Response: testResponseContent{
										value: 250,
									},
								},
							},
						},
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
					activeRequests:  map[RequestType]struct{}{},
					waitingRequests: map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{
						testRequestFirst: {
							response: testResponseContent{
								value: 120,
							},
						},
					},
					failedRequests: map[RequestType]failedRequest{
						testRequestSecond: {
							err: errors.New("precondition-failed"),
						},
					},
					waitingCompensatingRequests: map[RequestType]waitingCompensatingRequest{},
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
		{
			name: "completed-compensating",
			events: []Event{
				{
					Type:         EventTypeCompensatingRequestCompleted,
					SagaType:     testSagaType,
					RootSequence: 18,
					RequestType:  testRequestFirst,
				},
			},
			lastSequenceBefore: 30,
			lastSequenceAfter:  31,
			sagaStatesBefore: map[LogSequence]*sagaState{
				18: {
					sagaType:     testSagaType,
					compensating: true,
					replyChan:    replyChan,
					content: testSaga{
						num: 100,
					},
					activeRequests:  map[RequestType]struct{}{},
					waitingRequests: map[RequestType]waitingRequest{},
					completedRequests: map[RequestType]completedRequest{
						testRequestFirst: {
							response: "test.request1.content.123",
						},
					},
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
			sagaStatesAfter: map[LogSequence]*sagaState{},
			output: runLoopOutput{
				saveLogEntries: []LogEntry{
					{
						Sequence:     31,
						RootSequence: 18,
						Type:         EventTypeCompensatingRequestCompleted,
						SagaType:     testSagaType,
						RequestType:  testRequestFirst,
					},
				},
				replyList: []replyAction{
					{
						replyChan: replyChan,
						result: SagaResult{
							Failed: true,
							Errors: []SagaError{
								{
									RequestType: testRequestSecond,
									Error:       errors.New("precondition-failed"),
								},
							},
						},
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			c := NewCoordinator(CoordinatorConfig{
				BatchSize: 100,
			})
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

func TestRunLoop_ComplexSagas(t *testing.T) {
	const sagaTypeA SagaType = 1
	const sagaARequestA RequestType = 1
	const sagaARequestB RequestType = 2
	const sagaARequestC RequestType = 3
	const sagaARequestD RequestType = 4
	const sagaARequestE RequestType = 5

	const sagaTypeB SagaType = 2
	const sagaBRequestA RequestType = 6
	const sagaBRequestB RequestType = 7
	const sagaBRequestC RequestType = 8
	const sagaBRequestD RequestType = 9

	type Step struct {
		events []Event
		output runLoopOutput
	}

	replyChan := make(chan SagaResult)

	table := []struct {
		name         string
		lastSequence LogSequence
		steps        []Step
	}{
		{
			name:         "single-saga-success",
			lastSequence: 30,
			steps: []Step{
				{
					events: []Event{
						{
							Type:      EventTypeNewSaga,
							SagaType:  sagaTypeA,
							Content:   "saga.A.content.100",
							Data:      "saga.A.data.100",
							ReplyChan: replyChan,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence: 31,
								Type:     EventTypeNewSaga,
								Data:     "saga.A.data.100",
								SagaType: sagaTypeA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
								rootContent:  "saga.A.content.100",
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
								rootContent:  "saga.A.content.100",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
							RootSequence: 31,
							Content:      "saga.A.req.B.content.120",
							Data:         "saga.A.req.B.data.120",
						},
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
							RootSequence: 31,
							Content:      "saga.A.req.A.content.140",
							Data:         "saga.A.req.A.data.140",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     32,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.B.data.120",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
							{
								Sequence:     33,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.A.data.140",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestC,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.A.content.140",
									"saga.A.req.B.content.120",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestC,
							RootSequence: 31,
							Content:      "saga.A.req.C.content.160",
							Data:         "saga.A.req.C.data.160",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     34,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.C.data.160",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestC,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestD,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.C.content.160",
								},
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestE,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.C.content.160",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestD,
							RootSequence: 31,
							Content:      "saga.A.req.D.content.180",
							Data:         "saga.A.req.D.data.180",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     35,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.D.data.180",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestD,
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestE,
							RootSequence: 31,
							Content:      "saga.A.req.E.content.199",
							Data:         "saga.A.req.E.data.199",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     36,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.E.data.199",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestE,
							},
						},
						replyList: []replyAction{
							{
								replyChan: replyChan,
								result: SagaResult{
									Failed: false,
									Responses: []SagaResponse{
										{
											RequestType: sagaARequestA,
											Response:    "saga.A.req.A.content.140",
										},
										{
											RequestType: sagaARequestB,
											Response:    "saga.A.req.B.content.120",
										},
										{
											RequestType: sagaARequestC,
											Response:    "saga.A.req.C.content.160",
										},
										{
											RequestType: sagaARequestD,
											Response:    "saga.A.req.D.content.180",
										},
										{
											RequestType: sagaARequestE,
											Response:    "saga.A.req.E.content.199",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "single-saga-a-b-c-success-d-failed-e-success",
			lastSequence: 30,
			steps: []Step{
				{
					events: []Event{
						{
							Type:      EventTypeNewSaga,
							SagaType:  sagaTypeA,
							Content:   "saga.A.content.100",
							Data:      "saga.A.data.100",
							ReplyChan: replyChan,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence: 31,
								Type:     EventTypeNewSaga,
								Data:     "saga.A.data.100",
								SagaType: sagaTypeA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
								rootContent:  "saga.A.content.100",
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
								rootContent:  "saga.A.content.100",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
							RootSequence: 31,
							Content:      "saga.A.req.B.content.120",
							Data:         "saga.A.req.B.data.120",
						},
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
							RootSequence: 31,
							Content:      "saga.A.req.A.content.140",
							Data:         "saga.A.req.A.data.140",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     32,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.B.data.120",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
							{
								Sequence:     33,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.A.data.140",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestC,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.A.content.140",
									"saga.A.req.B.content.120",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestC,
							RootSequence: 31,
							Content:      "saga.A.req.C.content.160",
							Data:         "saga.A.req.C.data.160",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     34,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.C.data.160",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestC,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestD,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.C.content.160",
								},
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestE,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.C.content.160",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestPreconditionFailed,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestD,
							RootSequence: 31,
							Error:        errors.New("saga.A.req.D.error"),
							Data:         "saga.A.req.D.data.error",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     35,
								Type:         EventTypeRequestPreconditionFailed,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestD,
								RootSequence: 31,
								Data:         "saga.A.req.D.data.error",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestE,
							RootSequence: 31,
							Content:      "saga.A.req.E.content.201",
							Data:         "saga.A.req.E.data.201",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     36,
								Type:         EventTypeRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestE,
								RootSequence: 31,
								Data:         "saga.A.req.E.data.201",
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestE,
							},
						},
					},
				},
				{
					events: []Event{
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestE,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     37,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestE,
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestC,
							},
						},
					},
				},
				{
					events: []Event{
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestC,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     38,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestC,
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
							},
						},
					},
				},
				{
					events: []Event{
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
						},
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     39,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
							{
								Sequence:     40,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
						},
						replyList: []replyAction{
							{
								replyChan: replyChan,
								result: SagaResult{
									Failed: true,
									Errors: []SagaError{
										{
											RequestType: sagaARequestD,
											Error:       errors.New("saga.A.req.D.error"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "single-saga-a-b-c-success-d-e-failed",
			lastSequence: 30,
			steps: []Step{
				{
					events: []Event{
						{
							Type:      EventTypeNewSaga,
							SagaType:  sagaTypeA,
							Content:   "saga.A.content.100",
							Data:      "saga.A.data.100",
							ReplyChan: replyChan,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence: 31,
								Type:     EventTypeNewSaga,
								Data:     "saga.A.data.100",
								SagaType: sagaTypeA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
								rootContent:  "saga.A.content.100",
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
								rootContent:  "saga.A.content.100",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
							RootSequence: 31,
							Content:      "saga.A.req.B.content.120",
							Data:         "saga.A.req.B.data.120",
						},
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
							RootSequence: 31,
							Content:      "saga.A.req.A.content.140",
							Data:         "saga.A.req.A.data.140",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     32,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.B.data.120",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
							{
								Sequence:     33,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.A.data.140",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestC,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.A.content.140",
									"saga.A.req.B.content.120",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestC,
							RootSequence: 31,
							Content:      "saga.A.req.C.content.160",
							Data:         "saga.A.req.C.data.160",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     34,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.C.data.160",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestC,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestD,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.C.content.160",
								},
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestE,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.C.content.160",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestPreconditionFailed,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestD,
							RootSequence: 31,
							Error:        errors.New("saga.A.req.D.error"),
							Data:         "saga.A.req.D.data.error",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     35,
								Type:         EventTypeRequestPreconditionFailed,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestD,
								RootSequence: 31,
								Data:         "saga.A.req.D.data.error",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestPreconditionFailed,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestE,
							RootSequence: 31,
							Error:        errors.New("saga.A.req.E.error"),
							Data:         "saga.A.req.E.data.error",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     36,
								Type:         EventTypeRequestPreconditionFailed,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestE,
								RootSequence: 31,
								Data:         "saga.A.req.E.data.error",
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestC,
							},
						},
					},
				},
				{
					events: []Event{
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestC,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     37,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestC,
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
							},
						},
					},
				},
				{
					events: []Event{
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
						},
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     38,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
							{
								Sequence:     39,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
						},
						replyList: []replyAction{
							{
								replyChan: replyChan,
								result: SagaResult{
									Failed: true,
									Errors: []SagaError{
										{
											RequestType: sagaARequestD,
											Error:       errors.New("saga.A.req.D.error"),
										},
										{
											RequestType: sagaARequestE,
											Error:       errors.New("saga.A.req.E.error"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "single-saga-a-b-success-c-failed",
			lastSequence: 30,
			steps: []Step{
				{
					events: []Event{
						{
							Type:      EventTypeNewSaga,
							SagaType:  sagaTypeA,
							Content:   "saga.A.content.100",
							Data:      "saga.A.data.100",
							ReplyChan: replyChan,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence: 31,
								Type:     EventTypeNewSaga,
								Data:     "saga.A.data.100",
								SagaType: sagaTypeA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
								rootContent:  "saga.A.content.100",
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
								rootContent:  "saga.A.content.100",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
							RootSequence: 31,
							Content:      "saga.A.req.B.content.120",
							Data:         "saga.A.req.B.data.120",
						},
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
							RootSequence: 31,
							Content:      "saga.A.req.A.content.140",
							Data:         "saga.A.req.A.data.140",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     32,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.B.data.120",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
							{
								Sequence:     33,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.A.req.A.data.140",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestC,
								rootContent:  "saga.A.content.100",
								dependentResponses: []interface{}{
									"saga.A.req.A.content.140",
									"saga.A.req.B.content.120",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestPreconditionFailed,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestC,
							RootSequence: 31,
							Error:        errors.New("saga.A.req.C.error"),
							Data:         "saga.A.req.C.data.error",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     34,
								Type:         EventTypeRequestPreconditionFailed,
								RootSequence: 31,
								Data:         "saga.A.req.C.data.error",
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestC,
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestA,
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeA,
								requestType:  sagaARequestB,
							},
						},
					},
				},
				{
					events: []Event{
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestA,
						},
						{
							RootSequence: 31,
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeA,
							RequestType:  sagaARequestB,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     35,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestA,
							},
							{
								Sequence:     36,
								RootSequence: 31,
								Type:         EventTypeCompensatingRequestCompleted,
								SagaType:     sagaTypeA,
								RequestType:  sagaARequestB,
							},
						},
						replyList: []replyAction{
							{
								replyChan: replyChan,
								result: SagaResult{
									Failed: true,
									Errors: []SagaError{
										{
											RequestType: sagaARequestC,
											Error:       errors.New("saga.A.req.C.error"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "single-saga-a-failed",
			lastSequence: 30,
			steps: []Step{
				{
					events: []Event{
						{
							Type:      EventTypeNewSaga,
							SagaType:  sagaTypeB,
							Content:   "saga.B.content.200",
							Data:      "saga.B.data.200",
							ReplyChan: replyChan,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence: 31,
								Type:     EventTypeNewSaga,
								Data:     "saga.B.data.200",
								SagaType: sagaTypeB,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeB,
								requestType:  sagaBRequestA,
								rootContent:  "saga.B.content.200",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestPreconditionFailed,
							SagaType:     sagaTypeB,
							RequestType:  sagaBRequestA,
							RootSequence: 31,
							Error:        errors.New("saga.B.req.A.error"),
							Data:         "saga.B.req.A.data.error",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     32,
								Type:         EventTypeRequestPreconditionFailed,
								RootSequence: 31,
								Data:         "saga.B.req.A.data.error",
								SagaType:     sagaTypeB,
								RequestType:  sagaBRequestA,
							},
						},
						replyList: []replyAction{
							{
								replyChan: replyChan,
								result: SagaResult{
									Failed: true,
									Errors: []SagaError{
										{
											RequestType: sagaBRequestA,
											Error:       errors.New("saga.B.req.A.error"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "single-saga-a-success-b-failed",
			lastSequence: 30,
			steps: []Step{
				{
					events: []Event{
						{
							Type:      EventTypeNewSaga,
							SagaType:  sagaTypeB,
							Content:   "saga.B.content.200",
							Data:      "saga.B.data.200",
							ReplyChan: replyChan,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence: 31,
								Type:     EventTypeNewSaga,
								Data:     "saga.B.data.200",
								SagaType: sagaTypeB,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeB,
								requestType:  sagaBRequestA,
								rootContent:  "saga.B.content.200",
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeB,
							RequestType:  sagaBRequestA,
							RootSequence: 31,
							Content:      "saga.B.req.A.content.222",
							Data:         "saga.B.req.A.data.222",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     32,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.B.req.A.data.222",
								SagaType:     sagaTypeB,
								RequestType:  sagaBRequestA,
							},
						},
						startRequests: []startRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeB,
								requestType:  sagaBRequestB,
								rootContent:  "saga.B.content.200",
								dependentResponses: []interface{}{
									"saga.B.req.A.content.222",
								},
							},
							{
								rootSequence: 31,
								sagaType:     sagaTypeB,
								requestType:  sagaBRequestC,
								rootContent:  "saga.B.content.200",
								dependentResponses: []interface{}{
									"saga.B.req.A.content.222",
								},
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeRequestPreconditionFailed,
							SagaType:     sagaTypeB,
							RequestType:  sagaBRequestB,
							RootSequence: 31,
							Error:        errors.New("saga.B.req.B.error"),
							Data:         "saga.B.req.B.data.error",
						},
						{
							Type:         EventTypeRequestCompleted,
							SagaType:     sagaTypeB,
							RequestType:  sagaBRequestC,
							RootSequence: 31,
							Content:      "saga.B.req.C.content.330",
							Data:         "saga.B.req.C.data.330",
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     33,
								Type:         EventTypeRequestPreconditionFailed,
								RootSequence: 31,
								Data:         "saga.B.req.B.data.error",
								SagaType:     sagaTypeB,
								RequestType:  sagaBRequestB,
							},
							{
								Sequence:     34,
								Type:         EventTypeRequestCompleted,
								RootSequence: 31,
								Data:         "saga.B.req.C.data.330",
								SagaType:     sagaTypeB,
								RequestType:  sagaBRequestC,
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeB,
								requestType:  sagaBRequestC,
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeB,
							RequestType:  sagaBRequestC,
							RootSequence: 31,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     35,
								Type:         EventTypeCompensatingRequestCompleted,
								RootSequence: 31,
								SagaType:     sagaTypeB,
								RequestType:  sagaBRequestC,
							},
						},
						startCompensatingRequests: []startCompensatingRequest{
							{
								rootSequence: 31,
								sagaType:     sagaTypeB,
								requestType:  sagaBRequestA,
							},
						},
					},
				},
				{
					events: []Event{
						{
							Type:         EventTypeCompensatingRequestCompleted,
							SagaType:     sagaTypeB,
							RequestType:  sagaBRequestA,
							RootSequence: 31,
						},
					},
					output: runLoopOutput{
						saveLogEntries: []LogEntry{
							{
								Sequence:     36,
								Type:         EventTypeCompensatingRequestCompleted,
								RootSequence: 31,
								SagaType:     sagaTypeB,
								RequestType:  sagaBRequestA,
							},
						},
						replyList: []replyAction{
							{
								replyChan: replyChan,
								result: SagaResult{
									Failed: true,
									Errors: []SagaError{
										{
											RequestType: sagaBRequestB,
											Error:       errors.New("saga.B.req.B.error"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()

			c := NewCoordinator(CoordinatorConfig{
				BatchSize: 1000,
			})
			c.Register(sagaTypeA, nil, []RequestRegistry{
				{
					Type: sagaARequestA,
				},
				{
					Type: sagaARequestB,
				},
				{
					Type:         sagaARequestC,
					Dependencies: []RequestType{sagaARequestA, sagaARequestB},
				},
				{
					Type:         sagaARequestD,
					Dependencies: []RequestType{sagaARequestC},
				},
				{
					Type:         sagaARequestE,
					Dependencies: []RequestType{sagaARequestC},
				},
			})
			c.Register(sagaTypeB, nil, []RequestRegistry{
				{
					Type: sagaBRequestA,
				},
				{
					Type:         sagaBRequestB,
					Dependencies: []RequestType{sagaBRequestA},
				},
				{
					Type:         sagaBRequestC,
					Dependencies: []RequestType{sagaBRequestA},
				},
				{
					Type:         sagaBRequestD,
					Dependencies: []RequestType{sagaBRequestC},
				},
			})

			c.lastSequence = e.lastSequence

			eventChan := make(chan Event, 1000)
			for i, step := range e.steps {
				for _, event := range step.events {
					eventChan <- event
				}

				output, err := c.runLoop(ctx, runLoopInput{
					eventChan: eventChan,
				})
				assert.Nil(t, err)
				assert.Equal(t, 0, len(eventChan), "at step", i)
				assert.Equal(t, step.output, output, "at step", i)
			}
			assert.Equal(t, 0, len(c.sagaStates))
		})
	}
}

func TestRunLoop_LimitBatchSize(t *testing.T) {
	const sagaType SagaType = 1
	const sagaRequestA RequestType = 1
	const sagaRequestB RequestType = 2
	const sagaRequestC RequestType = 3

	const lastSequence LogSequence = 30

	conf := CoordinatorConfig{
		BatchSize: 3,
	}

	c := NewCoordinator(conf)
	c.Register(sagaType, nil, []RequestRegistry{
		{
			Type: sagaRequestA,
		},
		{
			Type:         sagaRequestB,
			Dependencies: []RequestType{sagaRequestA},
		},
		{
			Type:         sagaRequestC,
			Dependencies: []RequestType{sagaRequestA},
		},
	})
	c.lastSequence = lastSequence

	eventChan := make(chan Event, 10)
	events := []Event{
		{
			Type:     EventTypeNewSaga,
			SagaType: sagaType,
			Content:  "content.1",
			Data:     "data.1",
		},
		{
			Type:     EventTypeNewSaga,
			SagaType: sagaType,
			Content:  "content.2",
			Data:     "data.2",
		},
		{
			Type:     EventTypeNewSaga,
			SagaType: sagaType,
			Content:  "content.3",
			Data:     "data.3",
		},
		{
			Type:     EventTypeNewSaga,
			SagaType: sagaType,
			Content:  "content.4",
			Data:     "data.4",
		},
	}

	for _, event := range events {
		eventChan <- event
	}

	_, err := c.runLoop(context.Background(), runLoopInput{
		eventChan: eventChan,
	})
	assert.Nil(t, err)

	assert.Equal(t, 1, len(eventChan))
}

func TestRecover(t *testing.T) {

}
