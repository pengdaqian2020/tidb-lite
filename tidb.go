// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tidblite

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/printer"
	"go.uber.org/zap"
)

var (
	// singleton instance
	tidbServer *TiDBServer
	tidbConfig *config.Config
	isClosed   bool
	mu         sync.Mutex
)

const (
	defaultRetryTime = 10
)

// TiDBServer ...
type TiDBServer struct {
	Cfg     *config.Config
	Svr     *server.Server
	Storage kv.Storage
	Dom     *domain.Domain

	closeGracefully bool
	ConnOpts        string
}

// NewTiDBServer returns a new TiDBServer
func NewTiDBServer(options *Options) (*TiDBServer, error) {
	mu.Lock()
	defer mu.Unlock()

	if tidbServer != nil && !isClosed {
		return nil, errors.New("already had one tidb server")
	}

	isClosed = false

	tidbConfig = config.NewConfig()
	tidbConfig.Store = "mocktikv"
	tidbConfig.Path = options.DataDir
	tidbConfig.Port = uint(options.Port)
	tidbConfig.Socket = options.Socket
	if err := tidbConfig.Valid(); err != nil {
		return nil, errors.Annotatef(err, "invalid config")
	}

	tidbServer = &TiDBServer{
		Cfg:      tidbConfig,
		ConnOpts: options.ConnOpts,
	}

	if err := tidbServer.registerStores(); err != nil {
		return nil, err
	}
	if err := tidbServer.setGlobalVars(); err != nil {
		return nil, err
	}
	if err := tidbServer.setupLog(); err != nil {
		return nil, err
	}
	tidbServer.printInfo()
	if err := tidbServer.createStoreAndDomain(); err != nil {
		return nil, err
	}
	if err := tidbServer.createServer(); err != nil {
		return nil, err
	}

	go func() {
		if err := tidbServer.runServer(); err != nil {
			log.Error("tidb lite run server failed", zap.Error(err))
		}
		tidbServer.cleanup(tidbServer.closeGracefully)
	}()

	return tidbServer, nil
}

// GetTiDBServer returns the tidb server if it is not nil
func GetTiDBServer() (*TiDBServer, error) {
	mu.Lock()
	defer mu.Unlock()

	if tidbServer == nil {
		return nil, errors.New("tidb server not exists")
	}

	if isClosed {
		return nil, errors.New("tidb server is not running")
	}

	return tidbServer, nil
}

// CreateConn creates a database connection.
func (t *TiDBServer) CreateConn() (*sql.DB, error) {
	var dbDSN string
	if t.Cfg.Port != 0 {
		dbDSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s", "root", "", "127.0.0.1", t.Cfg.Port, t.ConnOpts)
	} else {
		dbDSN = fmt.Sprintf("%s:%s@unix(%s)/?%s", "root", "", t.Cfg.Socket, t.ConnOpts)
	}

	var (
		dbConn *sql.DB
		err    error
	)
	for i := 0; i < defaultRetryTime; i++ {
		dbConn, err = sql.Open("mysql", dbDSN)
		if err == nil {
			return dbConn, nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return dbConn, err
}

// Close closes TiDB Server.
func (t *TiDBServer) Close() {
	t.serverShutdown(false)
}

// CloseGracefully closes TiDB server gracefully.
func (t *TiDBServer) CloseGracefully() {
	t.serverShutdown(true)
}

func (t *TiDBServer) printInfo() {
	// Make sure the TiDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	printer.PrintTiDBInfo()
	log.SetLevel(level)
}

func (t *TiDBServer) registerStores() error {
	kvstore.Register("tikv", tikv.Driver{})

	tikv.NewGCHandlerFunc = gcworker.NewGCWorker
	kvstore.Register("mocktikv", mockstore.MockDriver{})

	return nil
}

func (t *TiDBServer) createServer() error {
	driver := server.NewTiDBDriver(t.Storage)
	var err error
	t.Svr, err = server.NewServer(t.Cfg, driver)
	if err != nil {
		// Both domain and Storage have started, so we have to clean them before exiting.
		t.closeDomainAndStorage()
		return err
	}

	go t.Dom.ExpensiveQueryHandle().SetSessionManager(t.Svr).Run()
	return nil
}

func (t *TiDBServer) runServer() error {
	defer func() {
		if err := recover(); err != nil {
			log.Error("tidb lite run server failed", zap.Reflect("error", err))
		}
	}()

	return t.Svr.Run()
}

func (t *TiDBServer) createStoreAndDomain() error {
	fullPath := fmt.Sprintf("%s://%s", t.Cfg.Store, t.Cfg.Path)
	var err error
	t.Storage, err = kvstore.New(fullPath)
	if err != nil {
		return err
	}
	// Bootstrap a session to load information schema.
	t.Dom, err = session.BootstrapSession(t.Storage)
	if err != nil {
		if err1 := t.Storage.Close(); err1 != nil {
			log.Error("close tidb lite's Storage failed", zap.Error(err1))
		}
		return err
	}
	return nil
}

func (t *TiDBServer) setGlobalVars() error {
	ddlLeaseDuration := parseDuration(t.Cfg.Lease)
	session.SetSchemaLease(ddlLeaseDuration)
	runtime.GOMAXPROCS(int(t.Cfg.Performance.MaxProcs))
	statsLeaseDuration := parseDuration(t.Cfg.Performance.StatsLease)
	session.SetStatsLease(statsLeaseDuration)
	bindinfo.Lease = parseDuration(t.Cfg.Performance.BindInfoLease)
	domain.RunAutoAnalyze = t.Cfg.Performance.RunAutoAnalyze
	statistics.FeedbackProbability.Store(t.Cfg.Performance.FeedbackProbability)
	handle.MaxQueryFeedbackCount.Store(int64(t.Cfg.Performance.QueryFeedbackLimit))
	statistics.RatioOfPseudoEstimate.Store(t.Cfg.Performance.PseudoEstimateRatio)
	ddl.RunWorker = t.Cfg.RunDDL
	if t.Cfg.SplitTable {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	}
	plannercore.AllowCartesianProduct.Store(t.Cfg.Performance.CrossJoin)
	privileges.SkipWithGrant = t.Cfg.Security.SkipGrantTable

	priority := mysql.Str2Priority(t.Cfg.Performance.ForcePriority)
	variable.ForcePriority = int32(priority)
	variable.SysVars[variable.TiDBForcePriority].Value = mysql.Priority2Str[priority]

	variable.SysVars[variable.TIDBMemQuotaQuery].Value = strconv.FormatInt(t.Cfg.MemQuotaQuery, 10)
	variable.SysVars["lower_case_table_names"].Value = strconv.Itoa(t.Cfg.LowerCaseTableNames)
	variable.SysVars[variable.LogBin].Value = variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)

	variable.SysVars[variable.Port].Value = fmt.Sprintf("%d", t.Cfg.Port)
	variable.SysVars[variable.Socket].Value = t.Cfg.Socket
	variable.SysVars[variable.DataDir].Value = t.Cfg.Path
	variable.SysVars[variable.TiDBSlowQueryFile].Value = t.Cfg.Log.SlowQueryFile

	// For CI environment we default enable prepare-plan-cache.
	plannercore.SetPreparedPlanCache(config.CheckTableBeforeDrop || t.Cfg.PreparedPlanCache.Enabled)
	if plannercore.PreparedPlanCacheEnabled() {
		plannercore.PreparedPlanCacheCapacity = t.Cfg.PreparedPlanCache.Capacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = t.Cfg.PreparedPlanCache.MemoryGuardRatio
		if plannercore.PreparedPlanCacheMemoryGuardRatio < 0.0 || plannercore.PreparedPlanCacheMemoryGuardRatio > 1.0 {
			plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		}
		plannercore.PreparedPlanCacheMaxMemory.Store(t.Cfg.Performance.MaxMemory)
		total, err := memory.MemTotal()
		if err != nil {
			return err
		}
		if plannercore.PreparedPlanCacheMaxMemory.Load() > total || plannercore.PreparedPlanCacheMaxMemory.Load() <= 0 {
			plannercore.PreparedPlanCacheMaxMemory.Store(total)
		}
	}

	tikv.CommitMaxBackoff = int(parseDuration(t.Cfg.TiKVClient.CommitTimeout).Seconds() * 1000)
	tikv.RegionCacheTTLSec = int64(t.Cfg.TiKVClient.RegionCacheTTL)

	return nil
}

func (t *TiDBServer) serverShutdown(isgraceful bool) {
	mu.Lock()
	defer mu.Unlock()

	t.closeGracefully = isgraceful
	t.Svr.Close()
	isClosed = true
}

func (t *TiDBServer) closeDomainAndStorage() {
	atomic.StoreUint32(&tikv.ShuttingDown, 1)
	t.Dom.Close()
	if err := t.Storage.Close(); err != nil {
		log.Error("close tidb lite's Storage failed", zap.Error(err))
	}
}

func (t *TiDBServer) cleanup(graceful bool) {
	if t.closeGracefully {
		t.Svr.GracefulDown(context.Background(), nil)
	} else {
		t.Svr.TryGracefulDown()
	}

	t.closeDomainAndStorage()
}

func (t *TiDBServer) setupLog() error {
	if err := logutil.InitZapLogger(t.Cfg.Log.ToLogConfig()); err != nil {
		return err
	}

	if err := logutil.InitLogger(t.Cfg.Log.ToLogConfig()); err != nil {
		return err
	}

	return nil
}

/*
 * SetDBInfoMetaAndReload is used to store the correct dbInfo and tableInfo into
 * TiDB-lite meta layer directly. Cause the dbInfo and tableInfo is extracted from ddl history
 * job, so it's correctness is guaranteed.
 */

func (t *TiDBServer) SetDBInfoMetaAndReload(newDBs []*model.DBInfo) error {
	err := kv.RunInNewTxn(t.Storage, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		originDBs, err1 := t.ListDatabases()
		if err1 != nil {
			return errors.Trace(err1)
		}
		// delete the origin db with same ID and name.
		deleteDBIfExist := func(newDB *model.DBInfo) error {
			for _, originDB := range originDBs {
				if originDB.ID == newDB.ID {
					if err1 = t.DropDatabase(originDB.ID); err1 != nil {
						return errors.Trace(err1)
					}
				}
			}
			return nil
		}

		// store meta in kv Storage.
		for _, newDB := range newDBs {
			if err1 = deleteDBIfExist(newDB); err1 != nil {
				return errors.Trace(err1)
			}
			// create database.
			if err1 = t.CreateDatabase(newDB); err1 != nil {
				return errors.Trace(err1)
			}
			// create table.
			for _, newTable := range newDB.Tables {
				// like create table do, it should rebase to AutoIncID-1.
				autoID := newTable.AutoIncID
				if autoID > 1 {
					autoID = autoID - 1
				}
				if err1 = t.CreateTableAndSetAutoID(newDB.ID, newTable, autoID); err1 != nil {
					return errors.Trace(err1)
				}
			}
		}
		/*
		 * update schema version here, when it exceed 100, domain reload will fetch all tables from meta directly
		 * rather than applying schemaDiff one by one.
		 */
		for i := 0; i <= 105; i++ {
			_, err := t.GenSchemaVersion()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return t.Dom.Reload()
}

func (t *TiDBServer) GetStorage() kv.Storage {
	return t.Storage
}
