package topology

import (
	"errors"
	"io/ioutil"
	"math/rand"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Topology struct {
	NodeImpl

	collectionMap *util.ConcurrentReadMap

	pulse int64

	volumeSizeLimit uint64

	Sequence sequence.Sequencer

	chanFullVolumes chan storage.VolumeInfo

	Configuration *Configuration

	RaftServer raft.Server
}

func NewTopology(id string, confFile string, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int) (*Topology, error) {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.collectionMap = util.NewConcurrentReadMap()
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit

	t.Sequence = seq

	t.chanFullVolumes = make(chan storage.VolumeInfo)

	err := t.loadConfiguration(confFile)

	return t, err
}

func (t *Topology) IsLeader() bool {
	if leader, e := t.Leader(); e == nil {
		return leader == t.RaftServer.Name()
	}
	return false
}

func (t *Topology) Leader() (string, error) {
	l := ""
	if t.RaftServer != nil {
		l = t.RaftServer.Leader()
	} else {
		return "", errors.New("Raft Server not ready yet!")
	}

	if l == "" {
		// We are a single node cluster, we are the leader
		return t.RaftServer.Name(), errors.New("Raft Server not initialized!")
	}

	return l, nil
}

func (t *Topology) loadConfiguration(configurationFile string) error {
	b, e := ioutil.ReadFile(configurationFile)
	if e == nil {
		t.Configuration, e = NewConfiguration(b)
		return e
	}
	glog.V(0).Infoln("Using default configurations.")
	return nil
}

func (t *Topology) Lookup(collection string, vid storage.VolumeId) []*DataNode {
	//maybe an issue if lots of collections?
	if collection == "" {
		for _, c := range t.collectionMap.Items() {
			if list := c.(*Collection).Lookup(vid); list != nil {
				return list
			}
		}
	} else {
		if c, ok := t.collectionMap.Find(collection); ok {
			return c.(*Collection).Lookup(vid)
		}
	}
	return nil
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	next := vid.Next()
	go t.RaftServer.Do(NewMaxVolumeIdCommand(next))
	return next
}

func (t *Topology) HasWritableVolume(option *VolumeGrowOption) bool {
	vl := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl)
	return vl.GetActiveVolumeCount(option) > 0
}

func (t *Topology) PickForWrite(count uint64, option *VolumeGrowOption) (string, uint64, *DataNode, error) {
	vid, count, datanodes, err := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl).PickForWrite(count, option)
	if err != nil || datanodes.Length() == 0 {
		return "", 0, nil, errors.New("No writable volumes available!")
	}
	fileId, count := t.Sequence.NextFileId(count)
	return storage.NewFileId(*vid, fileId, rand.Uint32()).String(), count, datanodes.Head(), nil
}

func (t *Topology) GetVolumeLayout(collectionName string, rp *storage.ReplicaPlacement, ttl *storage.TTL) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		return NewCollection(collectionName, t.volumeSizeLimit)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl)
}

func (t *Topology) FindCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Find(collectionName)
	return c.(*Collection), hasCollection
}

func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

func (t *Topology) RegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).RegisterVolume(&v, dn)
}
func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info:%+v", v)
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).UnRegisterVolume(&v, dn)
}

func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	dc := NewDataCenter(dcName)
	t.LinkChildNode(dc)
	return dc
}
