package redis_store

import (
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	redis "gopkg.in/redis.v5"
)

type RedisClusterStore struct {
	Client *redis.ClusterClient
}

func NewRedisClusterStore(redisServer string, password string, database int) *RedisClusterStore {
	hostPorts := strings.Split(redisServer, ",")
	if len(hostPorts) < 2 {
		return nil
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: hostPorts,
	})
	return &RedisClusterStore{Client: client}
}

func (s *RedisClusterStore) Get(fullFileName string) (fid string, err error) {
	fid, err = s.Client.Get(fullFileName).Result()
	if err == redis.Nil {
		err = filer.ErrNotFound
	}
	return fid, err
}

func (s *RedisClusterStore) Put(fullFileName string, fid string) (err error) {
	//	_, err = s.Client.Set(fullFileName, fid, 0).Result()
	//	if err == redis.Nil {
	//		err = nil
	//	}
	//	return err
	pipe := s.Client.TxPipeline()
	if err = pipe.Set(fullFileName, fid, 0).Err(); err != nil {
		e := pipe.Discard()
		if e != nil {
			glog.Errorf("redis txpipeline discard error! file = %s fid = %s : %v", fullFileName, fid, e)
		}
	} else if err = pipe.Set(fid, fullFileName, 0).Err(); err != nil {
		e := pipe.Discard()
		if e != nil {
			glog.Errorf("redis txpipeline discard error! fid = %s file = %s : %v", fullFileName, fid, e)
		}
	}
	_, e := pipe.Exec()
	if e != nil {
		glog.Errorf("redis txpipeline exec error! %s %s : %v", fullFileName, fid, e)
	}
	glog.Infof("redis tx put %s %s", fullFileName, fid)
	return
}

// Currently the fid is not returned
func (s *RedisClusterStore) Delete(fullFileName string) (err error) {
	//	_, err = s.Client.Del(fullFileName).Result()
	//	if err == redis.Nil {
	//		err = nil
	//	}
	//	return err
	var fid string
	fid, err = s.Get(fullFileName)
	if err != nil {
		return
	}
	pipe := s.Client.TxPipeline()
	if err = pipe.Del(fullFileName).Err(); err != nil {
		e := pipe.Discard()
		if e != nil {
			glog.Fatalf("redis txpipeline delete discard error! file = %s fid = %s : %v", fullFileName, fid, e)
		}
	} else if err = pipe.Del(fid).Err(); err != nil {
		e := pipe.Discard()
		if e != nil {
			glog.Fatalf("redis txpipeline delete discard error! fid = %s file = %s : %v", fullFileName, fid, e)
		}
	}
	_, e := pipe.Exec()
	if e != nil {
		glog.Fatalf("redis txpipeline delete exec error! %s %s : %v", fullFileName, fid, e)
	}
	glog.Infof("redis tx delete %s %s", fullFileName, fid)
	return
}

func (s *RedisClusterStore) Close() {
	if s.Client != nil {
		s.Client.Close()
	}
}
