package sec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSequenceMinList(t *testing.T) {
	assert.Equal(t, SequenceMinListIndex(0xffffffff), rootMinListIndex)

	list := NewSequenceMinList(5)
	assert.Equal(t, 0, list.Len())

	index := list.Put(LogSequence(30))
	assert.Equal(t, SequenceMinListIndex(0), index)
	assert.Equal(t, 1, list.Len())

	index = list.Put(LogSequence(32))
	assert.Equal(t, SequenceMinListIndex(1), index)
	assert.Equal(t, 2, list.Len())

	index = list.Put(LogSequence(34))
	assert.Equal(t, SequenceMinListIndex(2), index)
	assert.Equal(t, 3, list.Len())

	assert.Equal(t, LogSequence(30), list.MinSequence())

	list.Delete(0)
	assert.Equal(t, 2, list.Len())
	assert.Equal(t, LogSequence(32), list.MinSequence())

	list.Delete(1)
	assert.Equal(t, 1, list.Len())
	assert.Equal(t, LogSequence(34), list.MinSequence())

	index = list.Put(LogSequence(36))
	assert.Equal(t, SequenceMinListIndex(1), index)
	assert.Equal(t, 2, list.Len())

	index = list.Put(LogSequence(38))
	assert.Equal(t, SequenceMinListIndex(0), index)
	assert.Equal(t, 3, list.Len())

	assert.Equal(t, LogSequence(34), list.MinSequence())

	list.Delete(1)
	assert.Equal(t, 2, list.Len())
	assert.Equal(t, LogSequence(34), list.MinSequence())

	list.Delete(0)
	assert.Equal(t, 1, list.Len())
	assert.Equal(t, LogSequence(34), list.MinSequence())

	index = list.Put(LogSequence(40))
	assert.Equal(t, SequenceMinListIndex(0), index)
	assert.Equal(t, 2, list.Len())

	index = list.Put(LogSequence(42))
	assert.Equal(t, SequenceMinListIndex(1), index)
	assert.Equal(t, 3, list.Len())
}

func TestSequenceMinList2(t *testing.T) {
	list := NewSequenceMinList(5)

	var sequences []LogSequence
	assert.Equal(t, sequences, list.AllSequences())

	list.Put(20)
	list.Put(21)
	list.Put(22)
	list.Put(23)
	list.Put(24)

	sequences = []LogSequence{20, 21, 22, 23, 24}
	assert.Equal(t, sequences, list.AllSequences())

	list.Delete(1)
	assert.Equal(t, LogSequence(20), list.MinSequence())
	list.Delete(0)
	assert.Equal(t, LogSequence(22), list.MinSequence())

	list.Delete(4)
	list.Delete(3)
	list.Put(25)
	assert.Equal(t, LogSequence(22), list.MinSequence())
	list.Delete(2)
	assert.Equal(t, LogSequence(25), list.MinSequence())
}
