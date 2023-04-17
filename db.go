package rocketdb

import (
	"context"

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

func (db *DB) View(txFunc func(tx *Tx) error) error {
	return db.runTx(txFunc, false)
}

func (db *DB) Update(txFunc func(tx *Tx) error) error {
	return db.runTx(txFunc, true)
}

func (db *DB) Close() error {
	return db.cli.Close()
}
