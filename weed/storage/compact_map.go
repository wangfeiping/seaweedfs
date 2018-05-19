package storage

import (
	"strconv"
	"sync"
)

type NeedleValue struct {
	Key    Key
	Offset uint32 `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 `comment:"Size of the data portion"`
}

const (
	batch = 100000
)

type Key uint64

func (k Key) String() string {
	return strconv.FormatUint(uint64(k), 10)
}

type CompactSection struct {
	sync.RWMutex
	values   []NeedleValue
	overflow map[Key]NeedleValue
	start    Key
	end      Key
	counter  int
}

func NewCompactSection(start Key) *CompactSection {
	return &CompactSection{
		values:   make([]NeedleValue, batch),
		overflow: make(map[Key]NeedleValue),
		start:    start,
	}
}

//return old entry size
func (cs *CompactSection) Set(key Key, offset, size uint32) (oldOffset, oldSize uint32) {
	cs.Lock()
	if key > cs.end {
		cs.end = key
	}
	if i := cs.binarySearchValues(key); i >= 0 {
		oldOffset, oldSize = cs.values[i].Offset, cs.values[i].Size
		//println("key", key, "old size", ret)
		cs.values[i].Offset, cs.values[i].Size = offset, size
	} else {
		needOverflow := cs.counter >= batch
		needOverflow = needOverflow || cs.counter > 0 && cs.values[cs.counter-1].Key > key
		if needOverflow {
			//println("start", cs.start, "counter", cs.counter, "key", key)
			if oldValue, found := cs.overflow[key]; found {
				oldOffset, oldSize = oldValue.Offset, oldValue.Size
			}
			cs.overflow[key] = NeedleValue{Key: key, Offset: offset, Size: size}
		} else {
			p := &cs.values[cs.counter]
			p.Key, p.Offset, p.Size = key, offset, size
			//println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
			cs.counter++
		}
	}
	cs.Unlock()
	return
}

//return old entry size
func (cs *CompactSection) Delete(key Key) uint32 {
	cs.Lock()
	ret := uint32(0)
	if i := cs.binarySearchValues(key); i >= 0 {
		if cs.values[i].Size > 0 {
			ret = cs.values[i].Size
			cs.values[i].Size = 0
		}
	}
	if v, found := cs.overflow[key]; found {
		delete(cs.overflow, key)
		ret = v.Size
	}
	cs.Unlock()
	return ret
}
func (cs *CompactSection) Get(key Key) (*NeedleValue, bool) {
	cs.RLock()
	if v, ok := cs.overflow[key]; ok {
		cs.RUnlock()
		return &v, true
	}
	if i := cs.binarySearchValues(key); i >= 0 {
		cs.RUnlock()
		return &cs.values[i], true
	}
	cs.RUnlock()
	return nil, false
}
func (cs *CompactSection) binarySearchValues(key Key) int {
	l, h := 0, cs.counter-1
	if h >= 0 && cs.values[h].Key < key {
		return -2
	}
	//println("looking for key", key)
	for l <= h {
		m := (l + h) / 2
		//println("mid", m, "key", cs.values[m].Key, cs.values[m].Offset, cs.values[m].Size)
		if cs.values[m].Key < key {
			l = m + 1
		} else if key < cs.values[m].Key {
			h = m - 1
		} else {
			//println("found", m)
			return m
		}
	}
	return -1
}

//This map assumes mostly inserting increasing keys
type CompactMap struct {
	list []*CompactSection
}

func NewCompactMap() CompactMap {
	return CompactMap{}
}

func (cm *CompactMap) Set(key Key, offset, size uint32) (oldOffset, oldSize uint32) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		//println(x, "creating", len(cm.list), "section, starting", key)
		cm.list = append(cm.list, NewCompactSection(key))
		x = len(cm.list) - 1
		//keep compact section sorted by start
		for x > 0 {
			if cm.list[x-1].start > cm.list[x].start {
				cm.list[x-1], cm.list[x] = cm.list[x], cm.list[x-1]
				x = x - 1
			} else {
				break
			}
		}
	}
	return cm.list[x].Set(key, offset, size)
}
func (cm *CompactMap) Delete(key Key) uint32 {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return uint32(0)
	}
	return cm.list[x].Delete(key)
}
func (cm *CompactMap) Get(key Key) (*NeedleValue, bool) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return nil, false
	}
	return cm.list[x].Get(key)
}
func (cm *CompactMap) binarySearchCompactSection(key Key) int {
	l, h := 0, len(cm.list)-1
	if h < 0 {
		return -5
	}
	if cm.list[h].start <= key {
		if cm.list[h].counter < batch || key <= cm.list[h].end {
			return h
		}
		return -4
	}
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}
	return -3
}

// Visit visits all entries or stop if any error when visiting
func (cm *CompactMap) Visit(visit func(NeedleValue) error) error {
	for _, cs := range cm.list {
		cs.RLock()
		for _, v := range cs.overflow {
			if err := visit(v); err != nil {
				cs.RUnlock()
				return err
			}
		}
		for _, v := range cs.values {
			if _, found := cs.overflow[v.Key]; !found {
				if err := visit(v); err != nil {
					cs.RUnlock()
					return err
				}
			}
		}
		cs.RUnlock()
	}
	return nil
}
