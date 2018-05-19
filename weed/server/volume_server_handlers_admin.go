package weed_server

import (
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = vs.store.Status()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	preallocate := int64(0)
	if r.FormValue("preallocate") != "" {
		preallocate, err = strconv.ParseInt(r.FormValue("preallocate"), 10, 64)
		if err != nil {
			glog.V(0).Infoln("ignoring invalid int64 value for preallocate = %v", r.FormValue("preallocate"))
		}
	}
	err = vs.store.AddVolume(
		r.FormValue("volume"),
		r.FormValue("collection"),
		vs.needleMapKind,
		r.FormValue("replication"),
		r.FormValue("ttl"),
		preallocate,
	)
	if err == nil {
		writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	}
	glog.V(2).Infoln("assign volume = %s, collection = %s , replication = %s, error = %v",
		r.FormValue("volume"), r.FormValue("collection"), r.FormValue("replication"), err)
}

func (vs *VolumeServer) deleteCollectionHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.DeleteCollection(r.FormValue("collection"))
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	glog.V(2).Infof("deleting collection = %s, error = %v", r.FormValue("collection"), err)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	var ds []*stats.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	m["DiskStatuses"] = ds
	writeJsonQuiet(w, r, http.StatusOK, m)
}
