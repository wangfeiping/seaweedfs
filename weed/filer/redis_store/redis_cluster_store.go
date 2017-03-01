package redis_store

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
//        "fmt"

	redis "gopkg.in/redis.v5"
)

type RedisClusterStore struct {
	Client *redis.ClusterClient
}

func NewRedisClusterStore(hostPorts []string, password string, database int) *RedisClusterStore {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     hostPorts,
	})
	return &RedisClusterStore{Client: client}
}

func (s *RedisClusterStore) Get(fullFileName string) (fid string, err error) {
	fid, err = s.Client.Get(fullFileName).Result()
	if err == redis.Nil {
		err = filer.ErrNotFound
	}
	return fid, err

//      pong, err := s.Client.Ping().Result()
//	fmt.Println(pong, err)
//      val, err := s.Client.Get("key").Result()
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("key", val)
//        return "", nil
}
func (s *RedisClusterStore) Put(fullFileName string, fid string) (err error) {
	_, err = s.Client.Set(fullFileName, fid, 0).Result()
	if err == redis.Nil {
		err = nil
	}
	return err
}

// Currently the fid is not returned
func (s *RedisClusterStore) Delete(fullFileName string) (err error) {
	_, err = s.Client.Del(fullFileName).Result()
	if err == redis.Nil {
		err = nil
	}
	return err
}

func (s *RedisClusterStore) Close() {
	if s.Client != nil {
		s.Client.Close()
	}
}
