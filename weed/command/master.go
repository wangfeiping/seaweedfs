package command

import (
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func init() {
	cmdMaster.Run = runMaster // break init cycle
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
	mport                   = cmdMaster.Flag.Int("port", 9333, "http listen port")
	masterIp                = cmdMaster.Flag.String("ip", "localhost", "master <ip>|<server> address")
	masterBindIp            = cmdMaster.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	metaFolder              = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterPeers             = cmdMaster.Flag.String("peers", "", "other master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094")
	volumeSizeLimitMB       = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	volumePreallocate       = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	mpulse                  = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	confFile                = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "Deprecating! xml configuration file")
	defaultReplicaPlacement = cmdMaster.Flag.String("defaultReplication", "000", "Default replication type if not specified.")
	// mTimeout                = cmdMaster.Flag.Int("idleTimeout", 30, "connection idle seconds")
	mMaxCpu               = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold      = cmdMaster.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterWhiteListOption = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	masterSecureKey       = cmdMaster.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	masterCpuProfile      = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")

	masterWhiteList []string
)

func runMaster(cmd *Command, args []string) bool {
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mMaxCpu)
	if *masterCpuProfile != "" {
		f, err := os.Create(*masterCpuProfile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		OnInterrupt(func() {
			pprof.StopCPUProfile()
		})
	}
	if err := util.TestFolderWritable(*metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *metaFolder, err)
	}
	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, *mport, *metaFolder,
		*volumeSizeLimitMB, *volumePreallocate,
		*mpulse, *confFile, *defaultReplicaPlacement, *garbageThreshold,
		masterWhiteList, *masterSecureKey,
	)

	listeningAddress := *masterBindIp + ":" + strconv.Itoa(*mport)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		myMasterAddress := *masterIp + ":" + strconv.Itoa(*mport)
		var peers []string
		if *masterPeers != "" {
			peers = strings.Split(*masterPeers, ",")
		}
		raftServer := weed_server.NewRaftServer(r, peers, myMasterAddress, *metaFolder, ms.Topo, *mpulse)
		ms.SetRaftServer(raftServer)
	}()

	// start grpc and http server
	m := cmux.New(listener)

	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	pb.RegisterSeaweedServer(grpcS, ms)
	reflection.Register(grpcS)

	httpS := &http.Server{Handler: r}

	go grpcS.Serve(grpcL)
	go httpS.Serve(httpL)

	if err := m.Serve(); err != nil {
		glog.Fatalf("master server failed to serve: %v", err)
	}

	return true
}
