package redis_store

import (
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	redis "gopkg.in/redis.v2"
)

var shadowTag string = ",shadow://"

type RedisShadowStore struct {
	Client       *redis.Client
	ShadowClient *redis.Client
}

func NewRedisShadowStore(hostPort string, password string, database int) *RedisShadowStore {
	if !strings.Contains(hostPort, shadowTag) {
		return nil
	}
	addrs := strings.Split(hostPort, shadowTag)
	l := len(addrs)
	if l < 2 {
		return nil
	}
	clientHostPort := addrs[0]
	shadowHostPort := addrs[1]
	glog.Infof("client redis.server: %s", clientHostPort)
	glog.Infof("shadow redis.server: %s", shadowHostPort)
	client := redis.NewTCPClient(&redis.Options{
		Addr:     clientHostPort,
		Password: password,
		DB:       int64(database),
	})
	shadowClient := redis.NewTCPClient(&redis.Options{
		Addr:     shadowHostPort,
		Password: password,
		DB:       int64(database),
	})
	return &RedisShadowStore{
		Client:       client,
		ShadowClient: shadowClient,
	}
}

func (s *RedisShadowStore) Get(fullFileName string) (fid string, err error) {
	fid, err = s.Client.Get(fullFileName).Result()
	glog.Infof("redis client path %s get fid %s", fullFileName, fid)
	if err != nil { // == redis.Nil {
		fid, err = s.ShadowClient.Get(fullFileName).Result()
		glog.Infof("redis shadow path %s get fid %s", fullFileName, fid)
		if err == redis.Nil {
			err = filer.ErrNotFound
		}
	}
	return fid, err
}

func (s *RedisShadowStore) Put(fullFileName string, fid string, ttl string) (err error) {
	_, err = s.Client.Set(fullFileName, fid).Result()
	if err == redis.Nil || err == nil {
		_, err = s.Client.Set(fid, fullFileName).Result()
	}
	if err == redis.Nil {
		err = nil
	}
	return err
}

// Currently the fid is not returned
func (s *RedisShadowStore) Delete(fullFileName string) (err error) {
	_, err = s.Client.Del(fullFileName).Result()
	_, err2 := s.ShadowClient.Del(fullFileName).Result()
	if err == redis.Nil {
		err = nil
	}
	if err == nil && err2 != redis.Nil {
		err = err2
	}
	return err
}

func (s *RedisShadowStore) Close() {
	if s.Client != nil {
		s.Client.Close()
	}
	if s.ShadowClient != nil {
		s.ShadowClient.Close()
	}
}

