package rocketdb

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type DB struct {
	cli *redis.Client
}

func New(cli *redis.Client) *DB {
	return &DB{
		cli: cli,
	}
}

const (
	txMarkerKey = "__txMarker"
)

type Tx struct {
	cli         *redis.Client
	ctx         context.Context
	cancelTx    context.CancelFunc
	readOnly    bool
	watchedKeys map[string]struct{}
	updatedKeys map[string]string
	cmdCh       chan any
	markerTid   uint64
}

func getTidKey(key string) string {
	return key + ":tid"
}

var script = fmt.Sprintf(`
	local tid = redis.call("INCR", "%s")
	for i, key in ipairs(KEYS) do
		if ARGV[i] == '' then
			redis.call("DEL", key)
		else
			redis.call("SET", key, ARGV[i])
		end

		redis.call("SET", key .. "%s", tid)
	end
`, txMarkerKey, ":tid")

func (tx *Tx) tryCommit(rtx *redis.Tx) error {
	keys := make([]string, 0, len(tx.updatedKeys))
	values := make([]any, 0, len(tx.updatedKeys))
	for k, v := range tx.updatedKeys {
		keys = append(keys, k)
		values = append(values, v)
	}

	_, err := rtx.TxPipelined(tx.ctx, func(p redis.Pipeliner) error {
		cmd := p.Eval(tx.ctx, script, keys, values...)
		return cmd.Err()
	})
	if err == redis.Nil {
		return nil
	}
	return err
}

func (db *DB) Begin(ctx context.Context, update bool) (*Tx, error) {
	marker, err := db.cli.Get(ctx, txMarkerKey).Uint64()
	if err == redis.Nil {
		marker = 0
	} else if err != nil {
		return nil, err
	}

	txCtx, cancel := context.WithCancel(ctx)
	tx := &Tx{
		ctx:         txCtx,
		cancelTx:    cancel,
		cli:         db.cli,
		readOnly:    !update,
		markerTid:   marker,
		cmdCh:       make(chan any, 1),
		watchedKeys: make(map[string]struct{}),
	}

	if update {
		tx.updatedKeys = make(map[string]string)
	}

	go tx.runTx()
	return tx, nil
}

func (db *DB) runTx(txFunc func(tx *Tx) error, update bool) error {
	tx, err := db.Begin(context.Background(), update)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := txFunc(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (tx *Tx) Rollback() error {
	tx.cancelTx()
	return nil
}

func (db *DB) View(txFunc func(tx *Tx) error) error {
	return db.runTx(txFunc, false)
}

func (db *DB) Update(txFunc func(tx *Tx) error) error {
	return db.runTx(txFunc, true)
}

func (db *DB) Close() error {
	return db.cli.Close()
}

type cmd struct {
	errCh chan error
}

type getCmd struct {
	cmd
	key     string
	replyCh chan string
}

type commitCmd struct {
	cmd
}

var (
	ErrConflict   = errors.New("key conflict")
	ErrReadOnlyTx = errors.New("transaction is not writable")
)

func get(ctx context.Context, rtx *redis.Tx, key string) (string, uint64) {
	values := rtx.MGet(ctx, key, getTidKey(key)).Val()
	value := values[0].(string)
	tid, _ := strconv.ParseUint(values[1].(string), 10, 64)
	return value, tid
}

func (tx *Tx) runTx() {
	tx.cli.Watch(tx.ctx, func(rtx *redis.Tx) error {
		for {
			select {
			case <-tx.ctx.Done():
				return nil

			case cmd := <-tx.cmdCh:
				switch c := cmd.(type) {
				case *getCmd:
					if _, watched := tx.watchedKeys[c.key]; !watched {
						if err := rtx.Watch(tx.ctx, c.key); err.Err() != nil {
							c.errCh <- err.Err()
							continue
						}
						tx.watchedKeys[c.key] = struct{}{}
					}

					value, tid := get(tx.ctx, rtx, c.key)
					if tid > tx.markerTid {
						c.errCh <- ErrConflict
					}
					c.replyCh <- value

				case *commitCmd:
					err := tx.tryCommit(rtx)
					if err == redis.TxFailedErr {
						err = ErrConflict
					}
					c.errCh <- err
				}
			}
		}
	})
}

func (tx *Tx) Get(key string) (string, error) {
	if value, has := tx.updatedKeys[key]; has {
		return value, nil
	}

	replyCh := make(chan string, 1)
	errCh := make(chan error, 1)

	tx.cmdCh <- &getCmd{
		key: key,
		cmd: cmd{
			errCh: errCh,
		},
		replyCh: replyCh,
	}

	select {
	case reply := <-replyCh:
		return reply, nil

	case err := <-errCh:
		return "", err
	}
}

func (tx *Tx) set(key, value string) error {
	if tx.readOnly {
		return ErrReadOnlyTx
	}
	tx.updatedKeys[key] = value
	return nil
}

func (tx *Tx) Set(key, value string) error {
	return tx.set(key, value)
}

func (tx *Tx) Remove(key string) error {
	tx.set(key, "")
	return nil
}

func (tx *Tx) Commit() error {
	errCh := make(chan error, 1)

	tx.cmdCh <- &commitCmd{
		cmd: cmd{
			errCh: errCh,
		},
	}

	return <-errCh
}
