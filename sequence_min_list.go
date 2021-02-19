package sec

// SequenceMinListIndex for deleting from list
type SequenceMinListIndex uint32

type sequenceMinEntry struct {
	sequence LogSequence
	next     SequenceMinListIndex
	prev     SequenceMinListIndex
}

const rootMinListIndex SequenceMinListIndex = 0xffffffff

// SequenceMinList for storing processing sagas
type SequenceMinList struct {
	data   []sequenceMinEntry
	next   SequenceMinListIndex
	prev   SequenceMinListIndex
	free   SequenceMinListIndex
	length int
}

// NewSequenceMinList creates new sequence list
func NewSequenceMinList(capacity int) *SequenceMinList {
	return &SequenceMinList{
		data:   make([]sequenceMinEntry, 0, capacity),
		next:   rootMinListIndex,
		prev:   rootMinListIndex,
		free:   rootMinListIndex,
		length: 0,
	}
}

// Len returns number of elements in the list
func (l *SequenceMinList) Len() int {
	return l.length
}

// Put add sequence to the list
func (l *SequenceMinList) Put(sequence LogSequence) SequenceMinListIndex {
	var index SequenceMinListIndex
	if l.free == rootMinListIndex {
		index = SequenceMinListIndex(len(l.data))
		l.data = append(l.data, sequenceMinEntry{
			sequence: sequence,
			next:     rootMinListIndex,
			prev:     l.prev,
		})
	} else {
		index = l.free
		l.free = l.data[l.free].next
		l.data[index] = sequenceMinEntry{
			sequence: sequence,
			next:     rootMinListIndex,
			prev:     l.prev,
		}
	}

	if l.prev == rootMinListIndex {
		l.next = index
	} else {
		l.data[l.prev].next = index
	}

	l.prev = index
	l.length++

	return index
}

// MinSequence returns the min sequence
func (l *SequenceMinList) MinSequence() LogSequence {
	return l.data[l.next].sequence
}

// Delete the entry at index
func (l *SequenceMinList) Delete(index SequenceMinListIndex) {
	entry := l.data[index]
	if entry.prev == rootMinListIndex {
		l.next = entry.next
	} else {
		l.data[entry.prev].next = entry.next
	}

	if entry.next == rootMinListIndex {
		l.prev = entry.prev
	} else {
		l.data[entry.next].prev = entry.prev
	}

	l.data[index].next = l.free
	l.free = index
	l.length--
}

// AllSequences returns all sequences from min to max
func (l *SequenceMinList) AllSequences() []LogSequence {
	if l.length == 0 {
		return nil
	}

	result := make([]LogSequence, 0, l.length)
	p := l.next
	for p != rootMinListIndex {
		entry := l.data[p]
		result = append(result, entry.sequence)
		p = entry.next
	}
	return result
}
