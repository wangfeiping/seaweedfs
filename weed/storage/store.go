package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

type MasterNodes struct {
	nodes          []string
	leader         string
	possibleLeader string
}

func (mn *MasterNodes) String() string {
	return fmt.Sprintf("nodes:%v, leader:%s", mn.nodes, mn.leader)
}

func NewMasterNodes(bootstrapNode string) (mn *MasterNodes) {
	mn = &MasterNodes{nodes: []string{bootstrapNode}, leader: ""}
	return
}
func (mn *MasterNodes) Reset() {
	if mn.leader != "" {
		mn.leader = ""
		glog.V(0).Infof("Resetting master nodes: %v", mn)
	}
}
func (mn *MasterNodes) SetPossibleLeader(possibleLeader string) {
	// TODO try to check this leader first
	mn.possibleLeader = possibleLeader
}
func (mn *MasterNodes) FindMaster() (leader string, err error) {
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.leader == "" {
		for _, m := range mn.nodes {
			glog.V(4).Infof("Listing masters on %s", m)
			if leader, masters, e := operation.ListMasters(m); e == nil {
				if leader != "" {
					mn.nodes = append(masters, m)
					mn.leader = leader
					glog.V(2).Infof("current master nodes is %v", mn)
					break
				}
			} else {
				glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
			}
		}
	}
	if mn.leader == "" {
		return "", errors.New("No master node available!")
	}
	return mn.leader, nil
}

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	Ip              string
	Port            int
	PublicUrl       string
	Locations       []*DiskLocation
	dataCenter      string //optional informaton, overwriting master setting if exists
	rack            string //optional information, overwriting master setting if exists
	connected       bool
	VolumeSizeLimit uint64 //read from the master
	Client          pb.Seaweed_SendHeartbeatClient
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, PublicUrl:%s, dataCenter:%s, rack:%s, connected:%v, volumeSizeLimit:%d", s.Ip, s.Port, s.PublicUrl, s.dataCenter, s.rack, s.connected, s.VolumeSizeLimit)
	return
}

func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int, needleMapKind NeedleMapType) (s *Store) {
	s = &Store{Port: port, Ip: ip, PublicUrl: publicUrl}
	s.Locations = make([]*DiskLocation, 0)
	for i := 0; i < len(dirnames); i++ {
		location := NewDiskLocation(dirnames[i], maxVolumeCounts[i])
		location.loadExistingVolumes(needleMapKind)
		s.Locations = append(s.Locations, location)
	}
	return
}
func (s *Store) AddVolume(volumeListString string, collection string, needleMapKind NeedleMapType, replicaPlacement string, ttlString string, preallocate int64) error {
	rt, e := NewReplicaPlacementFromString(replicaPlacement)
	if e != nil {
		return e
	}
	ttl, e := ReadTTL(ttlString)
	if e != nil {
		return e
	}
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := NewVolumeId(id_string)
			if err != nil {
				return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", id_string)
			}
			e = s.addVolume(VolumeId(id), collection, needleMapKind, rt, ttl, preallocate)
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				return fmt.Errorf("Volume Start Id %s is not a valid unsigned integer!", pair[0])
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				return fmt.Errorf("Volume End Id %s is not a valid unsigned integer!", pair[1])
			}
			for id := start; id <= end; id++ {
				if err := s.addVolume(VolumeId(id), collection, needleMapKind, rt, ttl, preallocate); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *Store) DeleteCollection(collection string) (e error) {
	for _, location := range s.Locations {
		e = location.DeleteCollectionFromDiskLocation(collection)
		if e != nil {
			return
		}
	}
	return
}

func (s *Store) findVolume(vid VolumeId) *Volume {
	for _, location := range s.Locations {
		if v, found := location.FindVolume(vid); found {
			return v
		}
	}
	return nil
}
func (s *Store) findFreeLocation() (ret *DiskLocation) {
	max := 0
	for _, location := range s.Locations {
		currentFreeCount := location.MaxVolumeCount - location.VolumesLen()
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}
func (s *Store) addVolume(vid VolumeId, collection string, needleMapKind NeedleMapType, replicaPlacement *ReplicaPlacement, ttl *TTL, preallocate int64) error {
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}
	if location := s.findFreeLocation(); location != nil {
		glog.V(0).Infof("In dir %s adds volume:%v collection:%s replicaPlacement:%v ttl:%v",
			location.Directory, vid, collection, replicaPlacement, ttl)
		if volume, err := NewVolume(location.Directory, collection, vid, needleMapKind, replicaPlacement, ttl, preallocate); err == nil {
			location.SetVolume(vid, volume)
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Store) Status() []*VolumeInfo {
	var stats []*VolumeInfo
	for _, location := range s.Locations {
		location.RLock()
		for k, v := range location.volumes {
			s := &VolumeInfo{
				Id:               VolumeId(k),
				Size:             v.ContentSize(),
				Collection:       v.Collection,
				ReplicaPlacement: v.ReplicaPlacement,
				Version:          v.Version(),
				FileCount:        v.nm.FileCount(),
				DeleteCount:      v.nm.DeletedCount(),
				DeletedByteCount: v.nm.DeletedSize(),
				ReadOnly:         v.readOnly,
				Ttl:              v.Ttl}
			stats = append(stats, s)
		}
		location.RUnlock()
	}
	sortVolumeInfos(stats)
	return stats
}

func (s *Store) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *Store) SetRack(rack string) {
	s.rack = rack
}

func (s *Store) CollectHeartbeat() *pb.Heartbeat {
	var volumeMessages []*pb.VolumeInformationMessage
	maxVolumeCount := 0
	var maxFileKey uint64
	for _, location := range s.Locations {
		maxVolumeCount = maxVolumeCount + location.MaxVolumeCount
		location.Lock()
		for k, v := range location.volumes {
			if maxFileKey < v.nm.MaxFileKey() {
				maxFileKey = v.nm.MaxFileKey()
			}
			if !v.expired(s.VolumeSizeLimit) {
				volumeMessage := &pb.VolumeInformationMessage{
					Id:               uint32(k),
					Size:             uint64(v.Size()),
					Collection:       v.Collection,
					FileCount:        uint64(v.nm.FileCount()),
					DeleteCount:      uint64(v.nm.DeletedCount()),
					DeletedByteCount: v.nm.DeletedSize(),
					ReadOnly:         v.readOnly,
					ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
					Version:          uint32(v.Version()),
					Ttl:              v.Ttl.ToUint32(),
				}
				volumeMessages = append(volumeMessages, volumeMessage)
			} else {
				if v.exiredLongEnough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
					location.deleteVolumeById(v.Id)
					glog.V(0).Infoln("volume", v.Id, "is deleted.")
				} else {
					glog.V(0).Infoln("volume", v.Id, "is expired.")
				}
			}
		}
		location.Unlock()
	}

	return &pb.Heartbeat{
		Ip:             s.Ip,
		Port:           uint32(s.Port),
		PublicUrl:      s.PublicUrl,
		MaxVolumeCount: uint32(maxVolumeCount),
		MaxFileKey:     maxFileKey,
		DataCenter:     s.dataCenter,
		Rack:           s.rack,
		Volumes:        volumeMessages,
	}

}
func (s *Store) Close() {
	for _, location := range s.Locations {
		location.Close()
	}
}
func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) {
	if v := s.findVolume(i); v != nil {
		if v.readOnly {
			err = fmt.Errorf("Volume %d is read only", i)
			return
		}
		// TODO: count needle size ahead
		if MaxPossibleVolumeSize >= v.ContentSize()+uint64(size) {
			size, err = v.writeNeedle(n)
		} else {
			err = fmt.Errorf("Volume Size Limit %d Exceeded! Current size is %d", s.VolumeSizeLimit, v.ContentSize())
		}
		if s.VolumeSizeLimit < v.ContentSize()+3*uint64(size) {
			glog.V(0).Infoln("volume", i, "size", v.ContentSize(), "will exceed limit", s.VolumeSizeLimit)
			if s.Client != nil {
				if e := s.Client.Send(s.CollectHeartbeat()); e != nil {
					glog.V(0).Infoln("error when reporting size:", e)
				}
			}
		}
		return
	}
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("Volume %d not found!", i)
	return
}
func (s *Store) Delete(i VolumeId, n *Needle) (uint32, error) {
	if v := s.findVolume(i); v != nil && !v.readOnly {
		return v.deleteNeedle(n)
	}
	return 0, nil
}
func (s *Store) ReadVolumeNeedle(i VolumeId, n *Needle) (int, error) {
	if v := s.findVolume(i); v != nil {
		return v.readNeedle(n)
	}
	return 0, fmt.Errorf("Volume %v not found!", i)
}
func (s *Store) GetVolume(i VolumeId) *Volume {
	return s.findVolume(i)
}

func (s *Store) HasVolume(i VolumeId) bool {
	v := s.findVolume(i)
	return v != nil
}
