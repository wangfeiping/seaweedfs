package weed_server

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

/*
Move a folder or a file, with 4 Use cases:
	mv fromDir toNewDir
	mv fromDir toOldDir
	mv fromFile toDir
	mv fromFile toFile

Wildcard is not supported.

*/
func (fs *FilerServer) moveHandler(w http.ResponseWriter, r *http.Request) {
	from := r.FormValue("from")
	to := r.FormValue("to")
	err := fs.filer.Move(from, to)
	if err != nil {
		glog.V(4).Infoln("moving", from, "->", to, err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (fs *FilerServer) registerHandler(w http.ResponseWriter, r *http.Request) {
	path := r.FormValue("path")
	fileId := r.FormValue("fileId")
	err := fs.filer.CreateFile(path, fileId, "")
	if err != nil {
		glog.V(4).Infof("register %s to %s error: %v", fileId, path, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

