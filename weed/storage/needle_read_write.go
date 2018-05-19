package storage

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	FlagGzip                = 0x01
	FlagHasName             = 0x02
	FlagHasMime             = 0x04
	FlagHasLastModifiedDate = 0x08
	FlagHasTtl              = 0x10
	FlagHasPairs            = 0x20
	FlagIsChunkManifest     = 0x80
	LastModifiedBytesLength = 5
	TtlBytesLength          = 2
)

func (n *Needle) DiskSize() int64 {
	return getActualSize(n.Size)
}

func (n *Needle) Append(w io.Writer, version Version) (size uint32, actualSize int64, err error) {
	if s, ok := w.(io.Seeker); ok {
		if end, e := s.Seek(0, 1); e == nil {
			defer func(s io.Seeker, off int64) {
				if err != nil {
					if _, e = s.Seek(off, 0); e != nil {
						glog.V(0).Infof("Failed to seek %s back to %d with error: %v", w, off, e)
					}
				}
			}(s, end)
		} else {
			err = fmt.Errorf("Cannot Read Current Volume Position: %v", e)
			return
		}
	}
	switch version {
	case Version1:
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.Size = uint32(len(n.Data))
		size = n.Size
		util.Uint32toBytes(header[12:16], n.Size)
		if _, err = w.Write(header); err != nil {
			return
		}
		if _, err = w.Write(n.Data); err != nil {
			return
		}
		actualSize = NeedleHeaderSize + int64(n.Size)
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0 : NeedleChecksumSize+padding])
		return
	case Version2:
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.DataSize, n.NameSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Name)), uint8(len(n.Mime))
		if n.DataSize > 0 {
			n.Size = 4 + n.DataSize + 1
			if n.HasName() {
				n.Size = n.Size + 1 + uint32(n.NameSize)
			}
			if n.HasMime() {
				n.Size = n.Size + 1 + uint32(n.MimeSize)
			}
			if n.HasLastModifiedDate() {
				n.Size = n.Size + LastModifiedBytesLength
			}
			if n.HasTtl() {
				n.Size = n.Size + TtlBytesLength
			}
			if n.HasPairs() {
				n.Size += 2 + uint32(n.PairsSize)
			}
		} else {
			n.Size = 0
		}
		size = n.DataSize
		util.Uint32toBytes(header[12:16], n.Size)
		if _, err = w.Write(header); err != nil {
			return
		}
		if n.DataSize > 0 {
			util.Uint32toBytes(header[0:4], n.DataSize)
			if _, err = w.Write(header[0:4]); err != nil {
				return
			}
			if _, err = w.Write(n.Data); err != nil {
				return
			}
			util.Uint8toBytes(header[0:1], n.Flags)
			if _, err = w.Write(header[0:1]); err != nil {
				return
			}
			if n.HasName() {
				util.Uint8toBytes(header[0:1], n.NameSize)
				if _, err = w.Write(header[0:1]); err != nil {
					return
				}
				if _, err = w.Write(n.Name); err != nil {
					return
				}
			}
			if n.HasMime() {
				util.Uint8toBytes(header[0:1], n.MimeSize)
				if _, err = w.Write(header[0:1]); err != nil {
					return
				}
				if _, err = w.Write(n.Mime); err != nil {
					return
				}
			}
			if n.HasLastModifiedDate() {
				util.Uint64toBytes(header[0:8], n.LastModified)
				if _, err = w.Write(header[8-LastModifiedBytesLength : 8]); err != nil {
					return
				}
			}
			if n.HasTtl() && n.Ttl != nil {
				n.Ttl.ToBytes(header[0:TtlBytesLength])
				if _, err = w.Write(header[0:TtlBytesLength]); err != nil {
					return
				}
			}
			if n.HasPairs() {
				util.Uint16toBytes(header[0:2], n.PairsSize)
				if _, err = w.Write(header[0:2]); err != nil {
					return
				}
				if _, err = w.Write(n.Pairs); err != nil {
					return
				}
			}
		}
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0 : NeedleChecksumSize+padding])

		return n.DataSize, getActualSize(n.Size), err
	}
	return 0, 0, fmt.Errorf("Unsupported Version! (%d)", version)
}

func ReadNeedleBlob(r *os.File, offset int64, size uint32) (dataSlice []byte, block *Block, err error) {
	return getBytesForFileBlock(r, offset, int(getActualSize(size)))
}

func (n *Needle) ReadData(r *os.File, offset int64, size uint32, version Version) (err error) {
	bytes, block, err := ReadNeedleBlob(r, offset, size)
	if err != nil {
		return err
	}
	n.rawBlock = block
	n.ParseNeedleHeader(bytes)
	if n.Size != size {
		return fmt.Errorf("File Entry Not Found. Needle %d Memory %d", n.Size, size)
	}
	switch version {
	case Version1:
		n.Data = bytes[NeedleHeaderSize : NeedleHeaderSize+size]
	case Version2:
		n.readNeedleDataVersion2(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
	}
	if size == 0 {
		return nil
	}
	checksum := util.BytesToUint32(bytes[NeedleHeaderSize+size : NeedleHeaderSize+size+NeedleChecksumSize])
	newChecksum := NewCRC(n.Data)
	if checksum != newChecksum.Value() {
		return errors.New("CRC error! Data On Disk Corrupted")
	}
	n.Checksum = newChecksum
	return nil
}
func (n *Needle) ParseNeedleHeader(bytes []byte) {
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:NeedleHeaderSize])
}
func (n *Needle) readNeedleDataVersion2(bytes []byte) {
	index, lenBytes := 0, len(bytes)
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		if int(n.DataSize)+index > lenBytes {
			// this if clause is due to bug #87 and #93, fixed in v0.69
			// remove this clause later
			return
		}
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	if index < lenBytes && n.HasTtl() {
		n.Ttl = LoadTTLFromBytes(bytes[index : index+TtlBytesLength])
		index = index + TtlBytesLength
	}
	if index < lenBytes && n.HasPairs() {
		n.PairsSize = util.BytesToUint16(bytes[index : index+2])
		index += 2
		end := index + int(n.PairsSize)
		n.Pairs = bytes[index:end]
		index = end
	}
}

func ReadNeedleHeader(r *os.File, version Version, offset int64) (n *Needle, bodyLength uint32, err error) {
	n = new(Needle)
	if version == Version1 || version == Version2 {
		bytes := make([]byte, NeedleHeaderSize)
		var count int
		count, err = r.ReadAt(bytes, offset)
		if count <= 0 || err != nil {
			return nil, 0, err
		}
		n.ParseNeedleHeader(bytes)
		padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
		bodyLength = n.Size + NeedleChecksumSize + padding
	}
	return
}

//n should be a needle already read the header
//the input stream will read until next file entry
func (n *Needle) ReadNeedleBody(r *os.File, version Version, offset int64, bodyLength uint32) (err error) {
	if bodyLength <= 0 {
		return nil
	}
	switch version {
	case Version1:
		bytes := make([]byte, bodyLength)
		if _, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		n.Data = bytes[:n.Size]
		n.Checksum = NewCRC(n.Data)
	case Version2:
		bytes := make([]byte, bodyLength)
		if _, err = r.ReadAt(bytes, offset); err != nil {
			return
		}
		n.readNeedleDataVersion2(bytes[0:n.Size])
		n.Checksum = NewCRC(n.Data)
	default:
		err = fmt.Errorf("Unsupported Version! (%d)", version)
	}
	return
}

func (n *Needle) IsGzipped() bool {
	return n.Flags&FlagGzip > 0
}
func (n *Needle) SetGzipped() {
	n.Flags = n.Flags | FlagGzip
}
func (n *Needle) HasName() bool {
	return n.Flags&FlagHasName > 0
}
func (n *Needle) SetHasName() {
	n.Flags = n.Flags | FlagHasName
}
func (n *Needle) HasMime() bool {
	return n.Flags&FlagHasMime > 0
}
func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | FlagHasMime
}
func (n *Needle) HasLastModifiedDate() bool {
	return n.Flags&FlagHasLastModifiedDate > 0
}
func (n *Needle) SetHasLastModifiedDate() {
	n.Flags = n.Flags | FlagHasLastModifiedDate
}
func (n *Needle) HasTtl() bool {
	return n.Flags&FlagHasTtl > 0
}
func (n *Needle) SetHasTtl() {
	n.Flags = n.Flags | FlagHasTtl
}

func (n *Needle) IsChunkedManifest() bool {
	return n.Flags&FlagIsChunkManifest > 0
}

func (n *Needle) SetIsChunkManifest() {
	n.Flags = n.Flags | FlagIsChunkManifest
}

func (n *Needle) HasPairs() bool {
	return n.Flags&FlagHasPairs != 0
}

func (n *Needle) SetHasPairs() {
	n.Flags = n.Flags | FlagHasPairs
}
