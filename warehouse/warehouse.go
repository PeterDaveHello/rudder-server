package warehouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/archive"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type App struct {
	app                app.App
	conf               *config.Config
	logger             logger.Logger
	stats              stats.Stats
	bcConfig           backendconfig.BackendConfig
	db                 *sqlmw.DB
	notifier           *pgnotifier.PGNotifier
	tenantManager      *multitenant.Manager
	controlPlaneClient *controlplane.Client
	bcManager          *backendConfigManager
	encodingFactory    *encoding.Factory
	jobsManager        *jobs.AsyncJobWh

	lastProcessedMarkerMap     map[string]int64
	lastProcessedMarkerMapLock sync.RWMutex
	triggerUploadsMap          map[string]bool // `whType:sourceID:destinationID` -> boolean value representing if an upload was triggered or not
	triggerUploadsMapLock      sync.RWMutex

	appName string

	config struct {
		host     string
		user     string
		password string
		database string
		sslMode  string
		port     int

		warehouseMode              string
		runningMode                string
		uploadFreqInS              int64
		shouldForceSetLowerVersion bool
		dbQueryTimeout             time.Duration
		maxOpenConnections         int

		configBackendURL string
		region           string
	}
}

func NewApp(
	app app.App,
	conf *config.Config,
	log logger.Logger,
	stats stats.Stats,
) *App {
	a := &App{
		app:                    app,
		conf:                   conf,
		logger:                 log,
		stats:                  stats,
		lastProcessedMarkerMap: map[string]int64{},
		triggerUploadsMap:      map[string]bool{},
	}

	a.conf.RegisterInt64ConfigVariable(1800, &a.config.uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	a.conf.RegisterDurationConfigVariable(5, &a.config.dbQueryTimeout, true, time.Minute, []string{"Warehouse.dbHandleTimeout", "Warehouse.dbHanndleTimeoutInMin"}...)

	a.config.host = conf.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	a.config.user = conf.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	a.config.password = conf.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu")
	a.config.database = conf.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	a.config.sslMode = conf.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	a.config.port = conf.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	a.config.warehouseMode = conf.GetString("Warehouse.mode", "embedded")
	a.config.runningMode = conf.GetString("Warehouse.runningMode", "")
	a.config.shouldForceSetLowerVersion = conf.GetBool("SQLMigrator.forceSetLowerVersion", true)
	a.config.maxOpenConnections = conf.GetInt("Warehouse.maxOpenConnections", 20)
	a.config.configBackendURL = conf.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	a.config.region = conf.GetString("region", "")

	a.appName = misc.DefaultString("rudder-server").OnError(os.Hostname())

	return a
}

var (
	application                app.App
	dbHandle                   *sql.DB
	wrappedDBHandle            *sqlmw.DB
	notifier                   pgnotifier.PGNotifier
	tenantManager              *multitenant.Manager
	controlPlaneClient         *controlplane.Client
	uploadFreqInS              int64
	lastProcessedMarkerMap     map[string]int64
	lastProcessedMarkerMapLock sync.RWMutex
	bcManager                  *backendConfigManager
	triggerUploadsMap          map[string]bool // `whType:sourceID:destinationID` -> boolean value representing if an upload was triggered or not
	triggerUploadsMapLock      sync.RWMutex
	pkgLogger                  logger.Logger
	asyncWh                    *jobs.AsyncJobWh
)

var defaultUploadPriority = 100

const (
	DegradedMode        = "degraded"
	triggerUploadQPName = "triggerUpload"
)

type (
	WorkerIdentifierT string
	JobID             int64
)

func Init4() {
	pkgLogger = logger.NewLogger().Child("warehouse")
}

func getUploadFreqInS(syncFrequency string) int64 {
	freqInMin, err := strconv.ParseInt(syncFrequency, 10, 64)
	if err != nil {
		return uploadFreqInS
	}
	return freqInMin * 60
}

func uploadFrequencyExceeded(warehouse model.Warehouse, syncFrequency string) bool {
	freqInS := getUploadFreqInS(syncFrequency)
	lastProcessedMarkerMapLock.RLock()
	defer lastProcessedMarkerMapLock.RUnlock()
	if lastExecTime, ok := lastProcessedMarkerMap[warehouse.Identifier]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastProcessedMarker(warehouse model.Warehouse, lastProcessedTime time.Time) {
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	lastProcessedMarkerMap[warehouse.Identifier] = lastProcessedTime.Unix()
}

func getBucketFolder(batchID, tableName string) string {
	return fmt.Sprintf(`%v-%v`, batchID, tableName)
}

// Gets the config from config backend and extracts enabled write keys
func (a *App) monitorDestRouters(ctx context.Context) error {
	dstToWhRouter := make(map[string]*router)

	for configData := range tenantManager.WatchConfig(ctx) {
		if err := a.onConfigDataEvent(ctx, configData, dstToWhRouter); err != nil {
			return fmt.Errorf("on config data event: %w", err)
		}
	}

	g, _ := errgroup.WithContext(ctx)
	for _, router := range dstToWhRouter {
		router := router
		g.Go(router.Shutdown)
	}
	return g.Wait()
}

func (a *App) onConfigDataEvent(ctx context.Context, configMap map[string]backendconfig.ConfigT, dstToWhRouter map[string]*router) error {
	enabledDestinations := make(map[string]bool)

	for _, wConfig := range configMap {
		for _, source := range wConfig.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true

				if !slices.Contains(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
					continue
				}

				router, ok := dstToWhRouter[destination.DestinationDefinition.Name]
				if ok {
					a.logger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
					router.Enable()
					continue
				}

				a.logger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)

				router, err := newRouter(
					ctx,
					a.app,
					destination.DestinationDefinition.Name,
					a.conf,
					a.logger.Child("router"),
					a.stats,
					a.db,
					a.notifier,
					a.tenantManager,
					a.controlPlaneClient,
					a.bcManager,
					a.encodingFactory,
				)
				if err != nil {
					return fmt.Errorf("setup warehouse %q: %w", destination.DestinationDefinition.Name, err)
				}
				dstToWhRouter[destination.DestinationDefinition.Name] = router
			}
		}
	}

	for _, key := range lo.Keys(dstToWhRouter) {
		if _, ok := enabledDestinations[key]; !ok {
			if wh, ok := dstToWhRouter[key]; ok {
				a.logger.Info("Disabling a existing warehouse destination: ", key)

				wh.Disable()
			}
		}
	}

	return nil
}

func isUploadTriggered(wh model.Warehouse) bool {
	triggerUploadsMapLock.RLock()
	defer triggerUploadsMapLock.RUnlock()
	return triggerUploadsMap[wh.Identifier]
}

func triggerUpload(wh model.Warehouse) {
	triggerUploadsMapLock.Lock()
	defer triggerUploadsMapLock.Unlock()
	pkgLogger.Infof("[WH]: Upload triggered for warehouse '%s'", wh.Identifier)
	triggerUploadsMap[wh.Identifier] = true
}

func clearTriggeredUpload(wh model.Warehouse) {
	triggerUploadsMapLock.Lock()
	defer triggerUploadsMapLock.Unlock()
	delete(triggerUploadsMap, wh.Identifier)
}

// Setup prepares the database connection for warehouse service.
// Also verifies database compatibility and creates the required tables.
func (a *App) Setup(ctx context.Context) error {
	if !db.IsNormalMode() {
		return nil
	}
	if err := a.setupDB(ctx, a.connectionString()); err != nil {
		return fmt.Errorf("cannot setup warehouse db: %w", err)
	}
	return nil
}

func (a *App) connectionString() string {
	if !a.checkForWarehouseEnvVars() {
		return misc.GetConnectionString()
	}

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s application_name=%s",
		a.config.host,
		a.config.port,
		a.config.user,
		a.config.password,
		a.config.database,
		a.config.sslMode,
		a.appName,
	)
}

// checkForWarehouseEnvVars checks if the required database environment variables are set
func (a *App) checkForWarehouseEnvVars() bool {
	return a.conf.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

func (a *App) setupDB(ctx context.Context, dsn string) error {
	database, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("could not open: %w", err)
	}
	database.SetMaxOpenConns(a.config.maxOpenConnections)

	isCompatible, err := validators.IsPostgresCompatible(ctx, database)
	if err != nil {
		return fmt.Errorf("could not check compatibility: %w", err)
	} else if !isCompatible {
		return errors.New("warehouse Service needs postgres version >= 10. Exiting")
	}

	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping: %w", err)
	}

	a.db = sqlmw.New(
		database,
		sqlmw.WithLogger(a.logger.Child("db")),
		sqlmw.WithQueryTimeout(a.config.dbQueryTimeout),
		sqlmw.WithStats(a.stats),
	)

	err = a.setupTables()
	if err != nil {
		return fmt.Errorf("could not setup tables: %w", err)
	}

	return nil
}

func (a *App) setupTables() error {
	m := &migrator.Migrator{
		Handle:                     a.db.DB,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: a.config.shouldForceSetLowerVersion,
	}

	operation := func() error {
		return m.Migrate("warehouse")
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)

	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		a.logger.Warnf("retrying warehouse database migration in %s: %v", t, err)
	})
	if err != nil {
		return fmt.Errorf("could not migrate: %w", err)
	}

	return nil
}

// Start starts the warehouse service
func (a *App) Start(ctx context.Context, app app.App) error {
	if a.db == nil && !isStandAloneSlave(a.config.warehouseMode) {
		return errors.New("warehouse service cannot start, database connection is not setup")
	}

	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone(a.config.warehouseMode) && !db.IsNormalMode() {
		a.logger.Infof("Skipping start of warehouse service...")
		return nil
	}

	a.logger.Infof("WH: Starting Warehouse service...")
	psqlInfo := getConnectionString()

	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Fatal(r)
			panic(r)
		}
	}()

	g, gCtx := errgroup.WithContext(ctx)

	tenantManager = multitenant.New(config.Default, backendconfig.DefaultBackendConfig)

	g.Go(func() error {
		tenantManager.Run(gCtx)
		return nil
	})

	bcManager = newBackendConfigManager(
		config.Default, wrappedDBHandle, tenantManager,
		pkgLogger.Child("wh_bc_manager"),
	)
	g.Go(func() error {
		bcManager.Start(gCtx)
		return nil
	})

	RegisterAdmin(bcManager, pkgLogger)

	runningMode := config.GetString("Warehouse.runningMode", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degraded mode...")
		if isMaster(mode) {
			err := InitWarehouseAPI(dbHandle, bcManager, pkgLogger.Child("upload_api"))
			if err != nil {
				pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
				return err
			}
		}

		api := NewApi(
			mode, config.Default, pkgLogger, stats.Default,
			backendconfig.DefaultBackendConfig, wrappedDBHandle, nil, tenantManager,
			bcManager, nil,
		)
		return api.Start(ctx)
	}
	var err error
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		return fmt.Errorf("cannot setup pgnotifier: %w", err)
	}

	// Setting up reporting client only if standalone master or embedded connecting to different DB for warehouse
	// A different DB for warehouse is used when:
	// 1. MultiTenant (uses RDS)
	// 2. rudderstack-postgresql-warehouse pod in Hosted and Enterprise
	if (isStandAlone(mode) && isMaster(mode)) || (misc.GetConnectionString() != psqlInfo) {
		reporting := application.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			reporting.AddClient(gCtx, types.Config{ConnInfo: psqlInfo, ClientName: types.WarehouseReportingClient})
			return nil
		}))
	}

	if isStandAlone(mode) && isMaster(mode) {
		// Report warehouse features
		g.Go(func() error {
			backendconfig.DefaultBackendConfig.WaitForConfig(gCtx)

			c := controlplane.NewClient(
				backendconfig.GetConfigBackendURL(),
				backendconfig.DefaultBackendConfig.Identity(),
			)

			err := c.SendFeatures(gCtx, info.WarehouseComponent.Name, info.WarehouseComponent.Features)
			if err != nil {
				pkgLogger.Errorf("error sending warehouse features: %v", err)
			}

			// We don't want to exit if we fail to send features
			return nil
		})
	}

	if isSlave(mode) {
		pkgLogger.Infof("WH: Starting warehouse slave...")
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			cm := newConstraintsManager(config.Default)
			ef := encoding.NewFactory(config.Default)

			slave := newSlave(config.Default, pkgLogger, stats.Default, &notifier, bcManager, cm, ef)
			return slave.setupSlave(gCtx)
		}))
	}

	if isMaster(mode) {
		pkgLogger.Infof("[WH]: Starting warehouse master...")

		backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

		region := config.GetString("region", "")

		controlPlaneClient = controlplane.NewClient(
			backendconfig.GetConfigBackendURL(),
			backendconfig.DefaultBackendConfig.Identity(),
			controlplane.WithRegion(region),
		)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return notifier.ClearJobs(gCtx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return monitorDestRouters(gCtx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			archive.CronArchiver(gCtx, archive.New(
				config.Default,
				pkgLogger,
				stats.Default,
				dbHandle,
				filemanager.New,
				tenantManager,
			))
			return nil
		}))

		err := InitWarehouseAPI(dbHandle, bcManager, pkgLogger.Child("upload_api"))
		if err != nil {
			pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
			return err
		}
		asyncWh = jobs.InitWarehouseJobsAPI(gCtx, dbHandle, &notifier)
		jobs.WithConfig(asyncWh, config.Default)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return asyncWh.InitAsyncJobRunner()
		}))
	}

	g.Go(func() error {
		api := NewApi(
			mode, config.Default, pkgLogger, stats.Default,
			backendconfig.DefaultBackendConfig, wrappedDBHandle, &notifier, tenantManager,
			bcManager, asyncWh,
		)
		return api.Start(gCtx)
	})

	return g.Wait()
}
