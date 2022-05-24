package tiwatch

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/c4pt0r/log"
	_ "github.com/go-sql-driver/mysql"
)

var (
	PollDuration time.Duration = time.Second
)

// TiWatch, a PoC implementation of Etcd's important APIs: Watch, Get, Set
// The core idea is:
// 1. TiDB is a scalable database with **SQL** semantics.
// 2. TiDB supports secondary indexes. very fast lookup.
// 3. TiDB supports transactions, with pessimistic concurrency control
// 4. TiDB's pessimistic concurrency control is based on MVCC and pessimistic lock is in-memory
// 5. TiDB's lock is row-level, totally scalable.
// 6. TiDB uses multi-raft architecture to achieve strong consistency and auto-failover
type TiWatch struct {
	dsn string
	db  *sql.DB
	ns  string

	watchers map[string]chan string
	versions map[string]int64
}

type OpType int

const (
	TypeDelete OpType = iota
	TypeUpdate
)

type Op struct {
	Type OpType
	Key  string
	Val  string
}

func New(dsn string, namespace string) *TiWatch {
	return &TiWatch{
		dsn:      dsn,
		ns:       namespace,
		watchers: make(map[string]chan string),
		versions: make(map[string]int64),
	}
}

func genTableName(ns string) string {
	return "tiwatch_" + ns
}

func (b *TiWatch) Init() error {
	var err error
	b.db, err = sql.Open("mysql", b.dsn)
	if err != nil {
		return err
	}
	return b.createTables()
}

func (b *TiWatch) createTables() error {
	_, err := b.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			k VARCHAR(255) NOT NULL,
			v VARCHAR(255) NOT NULL,
			version BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (k)
		)
	`, genTableName(b.ns)))
	if err != nil {
		return err
	}
	return nil
}

func (b *TiWatch) Close() error {
	return b.db.Close()
}

func (b *TiWatch) Get(key string) (string, bool, error) {
	var value string
	err := b.db.QueryRow(fmt.Sprintf(`
		SELECT 
			v
		FROM 
			%s
		WHERE
			k = ?
		ORDER BY version DESC
		LIMIT 1
	`, genTableName(b.ns)), key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	return value, true, nil
}

func (b *TiWatch) Delete(key string) error {
	txn, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	_, err = txn.Exec(fmt.Sprintf(`
		SELECT 
			k 
		FROM
			%s
		WHERE k = ?
		FOR UPDATE
	`, genTableName(b.ns)), key)
	if err != nil {
		return err
	}
	_, err = txn.Exec(fmt.Sprintf(`
		DELETE FROM
			%s
		WHERE k = ?
	`, genTableName(b.ns)), key)
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (b *TiWatch) Set(key string, value string) error {
	txn, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	_, err = txn.Exec(fmt.Sprintf(`
		SELECT 
			k 
		FROM
			%s
		WHERE k = ?
		FOR UPDATE
	`, genTableName(b.ns)), key)
	if err != nil {
		return err
	}
	// if using INSERT here instead of UPSERT, we can keep change history feed
	_, err = txn.Exec(fmt.Sprintf(`
		INSERT INTO 
			%s (k, v, version)
		VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE
			v = VALUES(v),
			version = version + 1
	`, genTableName(b.ns)), key, value, 0)

	if err != nil {
		return err
	}
	return txn.Commit()
}

func (b *TiWatch) getMaxVersion(key string) (int64, error) {
	var version int64
	err := b.db.QueryRow(fmt.Sprintf(`
		SELECT
			IFNULL(MAX(version), 0)
		FROM
			%s
		WHERE k = ?
	`, genTableName(b.ns)), key).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (b *TiWatch) Watch(key string) <-chan Op {
	ch := make(chan Op)
	go func() {
		for {
			var err error
			// get local version
			version, ok := b.versions[key]
			if !ok {
				version, err = b.getMaxVersion(key)
				if err != nil {
					if err == sql.ErrNoRows {
						b.Set(key, "")
					} else {
						log.Error(err)
					}
				}
				b.versions[key] = version
			}
			// get remote version
			remoteVersion, err := b.getMaxVersion(key)
			if err != nil {
				log.Error(err)
				continue
			}
			// if remote version is greater than local version, we need to update local version
			// someone else must delete the key
			if remoteVersion == 0 && version > 0 {
				ch <- Op{
					Type: TypeDelete,
					Key:  key,
				}
				b.versions[key] = 0
				continue
			}
			// if remote version is greater than local version, get value
			if remoteVersion > version {
				value, _, err := b.Get(key)
				if err != nil {
					log.Error(err)
					continue
				}
				ch <- Op{Type: TypeUpdate, Key: key, Val: value}
				b.versions[key] = remoteVersion
			} else {
				// if remote version is less than or equal to local version, sleep
				time.Sleep(PollDuration)
			}

		}
	}()
	return ch
}
