package rocketdb_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ostafen/rocketdb"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	host = "localhost"
	port = 6379
)

func prepareDB() *rocketdb.DB {
	return rocketdb.New(redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: "",
		DB:       0,
	}))
}

func TestReadOnlyTxShouldGiveErrorOnSet(t *testing.T) {
	db := prepareDB()
	defer db.Close()

	err := db.View(func(tx *rocketdb.Tx) error {
		return tx.Set("key", "value")
	})
	require.Equal(t, err, rocketdb.ErrReadOnlyTx)
}

func TestAtomicIncr(t *testing.T) {
	db := prepareDB()
	defer db.Close()

	count := "count"

	err := db.Update(func(tx *rocketdb.Tx) error {
		return tx.Set(count, string(intToByte(0)))
	})
	require.NoError(t, err)

	n := 100
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			var err error = rocketdb.ErrConflict
			for err == rocketdb.ErrConflict {
				err = db.Update(func(tx *rocketdb.Tx) error {
					val, err := tx.Get(count)
					if err != nil {
						return err
					}
					new := bytesToInt([]byte(val)) + 1
					return tx.Set(count, string(intToByte(new)))
				})
				if err != nil {
					sleep := rand.Intn(100)
					time.Sleep(time.Millisecond * time.Duration(sleep))
				}
			}
			require.NoError(t, err)

			wg.Done()
		}(i)
	}
	wg.Wait()
}

func intToByte(i int) []byte {
	return binary.BigEndian.AppendUint32([]byte{}, uint32(i))
}

func bytesToInt(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}
