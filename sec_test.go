package sec

import (
	"context"
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

const testSagaType SagaType = 1

const testRequestFirst RequestType = 1
const testRequestSecond RequestType = 2

func testSagaDecoder(data string) interface{} {
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
		})

	assert.Equal(t, 1, len(c.registry))
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
						sequence:    11,
						sagaType:    testSagaType,
						requestType: testRequestFirst,
						rootData: testSaga{
							num: 100,
						},
					},
				},
			},
		},
		{
			name: "new-saga-with-existing",
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
			lastSequenceBefore: 20,
			lastSequenceAfter:  21,
			sagaStatesBefore: map[LogSequence]*sagaState{
				20: {
					sagaType: testSagaType,
					content: testSaga{
						num: 220,
					},
				},
			},
			sagaStatesAfter: map[LogSequence]*sagaState{
				20: {
					sagaType: testSagaType,
					content: testSaga{
						num: 220,
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
				},
				startRequests: []startRequest{
					{
						sequence:    21,
						sagaType:    testSagaType,
						requestType: testRequestFirst,
						rootData: testSaga{
							num: 100,
						},
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			c := NewCoordinator()
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
		})
	}
}
