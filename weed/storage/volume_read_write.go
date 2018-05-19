package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

// isFileUnchanged checks whether this needle to write is same as last one.
// It requires serialized access in the same volume.
func (v *Volume) isFileUnchanged(n *Needle) bool {
	if v.Ttl.String() != "" {
		return false
	}
	nv, ok := v.nm.Get(n.Id)
	if ok && nv.Offset > 0 {
		oldNeedle := new(Needle)
		err := oldNeedle.ReadData(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file %v", err)
			return false
		}
		defer oldNeedle.ReleaseMemory()
		if oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize
			return true
		}
	}
	return false
}

// Destroy removes everything related to this volume
func (v *Volume) Destroy() (err error) {
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.Close()
	err = os.Remove(v.dataFile.Name())
	if err != nil {
		return
	}
	err = v.nm.Destroy()
	return
}

// AppendBlob append a blob to end of the data file, used in replication
func (v *Volume) AppendBlob(b []byte) (offset int64, err error) {
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	} else if offset != int64(v.dataFileSize) {
		glog.V(0).Infof("dataFileSize %d != actual data file size: %d", v.dataFileSize, offset)
	}
	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		} else if offset != int64(v.dataFileSize) {
			glog.V(0).Infof("dataFileSize %d != actual data file size: %d", v.dataFileSize, offset)
		}
	}
	_, err = v.dataFile.Write(b)
	v.dataFileSize += int64(len(b))
	return
}

func (v *Volume) writeNeedle(n *Needle) (size uint32, err error) {
	glog.V(4).Infof("writing needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.isFileUnchanged(n) {
		size = n.DataSize
		glog.V(4).Infof("needle is unchanged!")
		return
	}
	var offset, actualSize int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	} else if offset != int64(v.dataFileSize) {
		glog.V(0).Infof("dataFileSize %d != actual data file size: %d", v.dataFileSize, offset)
	}

	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		}
	}

	if size, actualSize, err = n.Append(v.dataFile, v.Version()); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("%s\ncannot truncate %s: %v", err, v.dataFile.Name(), e)
		}
		return
	}
	v.dataFileSize += actualSize

	nv, ok := v.nm.Get(n.Id)
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		if err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
		}
	}
	if v.lastModifiedTime < n.LastModified {
		v.lastModifiedTime = n.LastModified
	}
	return
}

func (v *Volume) deleteNeedle(n *Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.dataFile.Name())
	}
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok && nv.Size != TombstoneFileSize {
		size := nv.Size
		// println("adding tombstone", n.Id, "at offset", v.dataFileSize)
		if err := v.nm.Delete(n.Id, uint32(v.dataFileSize/NeedlePaddingSize)); err != nil {
			return size, err
		}
		if offset, err := v.dataFile.Seek(0, 2); err != nil {
			return size, err
		} else if offset != int64(v.dataFileSize) {
			glog.V(0).Infof("dataFileSize %d != actual data file size: %d, deleteMarker: %d", v.dataFileSize, offset, getActualSize(0))
		}
		n.Data = nil
		_, actualSize, err := n.Append(v.dataFile, v.Version())
		v.dataFileSize += actualSize
		return size, err
	}
	return 0, nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *Needle) (int, error) {
	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset == 0 {
		return -1, errors.New("Not Found")
	}
	if nv.Size == TombstoneFileSize {
		return -1, errors.New("Already Deleted")
	}
	err := n.ReadData(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
	if err != nil {
		return 0, err
	}
	bytesRead := len(n.Data)
	if !n.HasTtl() {
		return bytesRead, nil
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return bytesRead, nil
	}
	if !n.HasLastModifiedDate() {
		return bytesRead, nil
	}
	if uint64(time.Now().Unix()) < n.LastModified+uint64(ttlMinutes*60) {
		return bytesRead, nil
	}
	n.ReleaseMemory()
	return -1, errors.New("Not Found")
}

func ScanVolumeFile(dirname string, collection string, id VolumeId,
	needleMapKind NeedleMapType,
	visitSuperBlock func(SuperBlock) error,
	readNeedleBody bool,
	visitNeedle func(n *Needle, offset int64) error) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind); err != nil {
		return fmt.Errorf("Failed to load volume %d: %v", id, err)
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("Failed to process volume %d super block: %v", id, err)
	}

	version := v.Version()

	offset := int64(SuperBlockSize)
	n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %v", e)
		return
	}
	for n != nil {
		if readNeedleBody {
			if err = n.ReadNeedleBody(v.dataFile, version, offset+int64(NeedleHeaderSize), rest); err != nil {
				glog.V(0).Infof("cannot read needle body: %v", err)
				//err = fmt.Errorf("cannot read needle body: %v", err)
				//return
			}
			if n.DataSize >= n.Size {
				// this should come from a bug reported on #87 and #93
				// fixed in v0.69
				// remove this whole "if" clause later, long after 0.69
				oldRest, oldSize := rest, n.Size
				padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
				n.Size = 0
				rest = n.Size + NeedleChecksumSize + padding
				if rest%NeedlePaddingSize != 0 {
					rest += (NeedlePaddingSize - rest%NeedlePaddingSize)
				}
				glog.V(4).Infof("Adjusting n.Size %d=>0 rest:%d=>%d %+v", oldSize, oldRest, rest, n)
			}
		}
		if err = visitNeedle(n, offset); err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
		}
		offset += int64(NeedleHeaderSize) + int64(rest)
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, rest, err = ReadNeedleHeader(v.dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header: %v", err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}

	return
}
