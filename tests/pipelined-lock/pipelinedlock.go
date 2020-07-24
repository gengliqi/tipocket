package pipelinedlock

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"

	"github.com/pingcap/tipocket/pkg/cluster/types"
	"github.com/pingcap/tipocket/pkg/core"
	"github.com/pingcap/tipocket/util"
)

var (
	insertConcurrency = 100
	insertBatchSize   = 100
	maxTransfer       = 100
	retryLimit        = 200
)

const stmtCreate = `
CREATE TABLE IF NOT EXISTS pipelinedlock (
  value BIGINT,
);
TRUNCATE TABLE pipelinedlock;
`

type Config struct {
	Interval    time.Duration `toml:"interval"`
	Concurrency int           `toml:"concurrency"`
}

// CaseCreator creates ledgerClient
type CaseCreator struct {
	Cfg *Config
}

func (l CaseCreator) Create(node types.ClientNode) core.Client {
	return &plClient{
		Config: l.Cfg,
	}
}

type plClient struct {
	*Config
	wg           sync.WaitGroup
	stop         int32
	db           *sql.DB
	lock         sync.Locker
	count        int64
	success      int64
	shortCount   int64
	shortSuccess int64
}

func (c *plClient) SetUp(ctx context.Context, _ []types.Node, clientNodes []types.ClientNode, idx int) error {
	if idx != 0 {
		return nil
	}

	var err error
	node := clientNodes[idx]
	dsn := fmt.Sprintf("root@tcp(%s:%d)/test", node.IP, node.Port)

	log.Infof("start to init...")
	db, err := util.OpenDB(dsn, 1)
	if err != nil {
		log.Fatalf("[plClient] create db client error %v", err)
	}
	_, err = db.Exec(fmt.Sprintf("set @@global.tidb_txn_mode = 'pessimistic';"))
	if err != nil {
		log.Fatalf("[plClient] set txn_mode failed: %v", err)
	}
	time.Sleep(5 * time.Second)
	c.db, err = util.OpenDB(dsn, c.Concurrency)
	if err != nil {
		return err
	}
	defer func() {
		log.Infof("init end...")
	}()
	if _, err := c.db.Exec(stmtCreate); err != nil {
		log.Fatalf("execute statement %s error %v", stmtCreate, err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.Interval):
				c.lock.Lock()
				log.Infof("success rate: %v, interval success rate: %v", 1.0*c.success/c.count, c.Interval, 1.0*c.shortSuccess/c.shortCount)
				c.shortCount = 0
				c.shortSuccess = 0
				c.lock.Unlock()
			}
		}
	}()

	return nil
}

func (c *plClient) TearDown(ctx context.Context, nodes []types.ClientNode, idx int) error {
	return nil
}

func (c *plClient) Invoke(ctx context.Context, node types.ClientNode, r interface{}) core.UnknownResponse {
	panic("implement me")
}

func (c *plClient) NextRequest() interface{} {
	panic("implement me")
}

func (c *plClient) DumpState(ctx context.Context) (interface{}, error) {
	panic("implement me")
}

func (c *plClient) Start(ctx context.Context, cfg interface{}, clientNodes []types.ClientNode) error {
	log.Infof("start to test...")
	defer func() {
		log.Infof("test end...")
	}()
	var wg sync.WaitGroup
	for i := 0; i < c.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := c.Execute(c.db); err != nil {
					log.Errorf("exec failed %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// Execute is run case
func (c *plClient) Execute(db *sql.DB) error {
	if atomic.LoadInt32(&c.stop) != 0 {
		return errors.New("ledgerClient stopped")
	}

	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	query := "update pipelinedlock set value=value+1"
	if _, err := tx.Exec(query); err != nil {
		return errors.Trace(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := tx.Commit(); err != nil {
		c.lock.Lock()
		c.count++
		c.shortCount++
		c.lock.Unlock()
	} else {
		c.lock.Lock()
		c.count++
		c.success++
		c.shortCount++
		c.shortSuccess++
		c.lock.Unlock()
	}

	return nil
}
