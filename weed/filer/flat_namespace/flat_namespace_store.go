package flat_namespace

type FlatNamespaceStore interface {
	Put(fullFileName string, fid string, ttl string) (err error)
	Get(fullFileName string) (fid string, err error)
	Delete(fullFileName string) (err error)
}

