package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-chi/chi/v5"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
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
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/api"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type App struct {
	application app.App
	conf        *config.Config
	logger      logger.Logger
	stats       stats.Stats

	dbHandle        *sql.DB
	wrappedDBHandle *sqlquerywrapper.DB

	notifier           pgnotifier.PGNotifier
	tenantManager      *multitenant.Manager
	controlPlaneClient *controlplane.Client
	bcConfig           backendconfig.BackendConfig

	bcManager *backendConfigManager
	asyncJob  *jobs.AsyncJobWh

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

		webPort int

		noOfSlaveWorkerRoutines int
		uploadFreqInS           int64

		warehouseMode                       string
		maxStagingFileReadBufferCapacityInK int

		tableCountQueryTimeout     time.Duration
		runningMode                string
		shouldForceSetLowerVersion bool

		dbHandleTimeout time.Duration

		configBackendURL string
		region           string

		maxOpenConnections int
	}
}

func NewApp(
	ctx context.Context,
	app app.App,
	conf *config.Config,
	log logger.Logger,
	stats stats.Stats,
) *App {
	a := &App{}

	a.application = app
	a.conf = conf
	a.logger = log
	a.stats = stats
	a.triggerUploadsMap = map[string]bool{}

	a.conf.RegisterIntConfigVariable(4, &a.config.noOfSlaveWorkerRoutines, true, 1, "Warehouse.noOfSlaveWorkerRoutines")
	a.conf.RegisterInt64ConfigVariable(1800, &a.config.uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	a.conf.RegisterIntConfigVariable(10240, &a.config.maxStagingFileReadBufferCapacityInK, true, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	a.conf.RegisterDurationConfigVariable(30, &a.config.tableCountQueryTimeout, true, time.Second, []string{"Warehouse.tableCountQueryTimeout", "Warehouse.tableCountQueryTimeoutInS"}...)
	a.conf.RegisterDurationConfigVariable(5, &a.config.dbHandleTimeout, true, time.Minute, []string{"Warehouse.dbHandleTimeout", "Warehouse.dbHanndleTimeoutInMin"}...)

	a.config.host = conf.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	a.config.user = conf.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	a.config.database = conf.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	a.config.port = conf.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	a.config.password = conf.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	a.config.sslMode = conf.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	a.config.maxOpenConnections = conf.GetInt("Warehouse.maxOpenConnections", 20)
	a.config.runningMode = conf.GetString("Warehouse.runningMode", "")
	a.config.configBackendURL = conf.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	a.config.region = conf.GetString("region", "")
	a.config.webPort = conf.GetInt("Warehouse.webPort", 8082)
	a.config.warehouseMode = conf.GetString("Warehouse.mode", "embedded")
	a.config.shouldForceSetLowerVersion = conf.GetBool("SQLMigrator.forceSetLowerVersion", true)

	a.appName = misc.DefaultString("rudder-server").OnError(os.Hostname())

	return a
}

var (
	application                         app.App
	wrappedDBHandle                     *sqlquerywrapper.DB
	notifier                            pgnotifier.PGNotifier
	tenantManager                       *multitenant.Manager
	noOfSlaveWorkerRoutines             int
	uploadFreqInS                       int64
	lastProcessedMarkerMap              map[string]int64
	lastProcessedMarkerExp              = expvar.NewMap("lastProcessedMarkerMap")
	lastProcessedMarkerMapLock          sync.RWMutex
	maxStagingFileReadBufferCapacityInK int
	bcManager                           *backendConfigManager
	pkgLogger                           logger.Logger
)

const (
	DegradedMode        = "degraded"
	triggerUploadQPName = "triggerUpload"
)

type (
	WorkerIdentifierT string
	JobID             int64
)

func Init4() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse")
}

func loadConfig() {
	// Port where WH is running
	config.RegisterIntConfigVariable(4, &noOfSlaveWorkerRoutines, true, 1, "Warehouse.noOfSlaveWorkerRoutines")
	config.RegisterInt64ConfigVariable(1800, &uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	lastProcessedMarkerMap = map[string]int64{}

	config.RegisterIntConfigVariable(10240, &maxStagingFileReadBufferCapacityInK, true, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
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
	lastProcessedMarkerExp.Set(warehouse.Identifier, lastProcessedTime)
}

func getBucketFolder(batchID, tableName string) string {
	return fmt.Sprintf(`%v-%v`, batchID, tableName)
}

// Gets the config from config backend and extracts enabled write keys
func (a *App) monitorDestRouters(ctx context.Context) error {
	dstToWhRouter := make(map[string]*router)

	ch := a.tenantManager.WatchConfig(ctx)
	for configData := range ch {
		err := a.onConfigDataEvent(ctx, configData, dstToWhRouter)
		if err != nil {
			return err
		}
	}

	g, _ := errgroup.WithContext(context.Background())
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
				if slices.Contains(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
					router, ok := dstToWhRouter[destination.DestinationDefinition.Name]
					if !ok {
						a.logger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)
						router, err := newRouter(
							ctx,
							destination.DestinationDefinition.Name,
							a.conf,
							a.logger,
							a.stats,
							a.wrappedDBHandle,
							a.notifier,
							a.tenantManager,
							a.controlPlaneClient,
							a.bcManager,
						)
						if err != nil {
							return fmt.Errorf("setup warehouse %q: %w", destination.DestinationDefinition.Name, err)
						}
						dstToWhRouter[destination.DestinationDefinition.Name] = router
					} else {
						a.logger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
						router.Enable()
					}
				}
			}
		}
	}

	keys := lo.Keys(dstToWhRouter)
	for _, key := range keys {
		if _, ok := enabledDestinations[key]; !ok {
			if wh, ok := dstToWhRouter[key]; ok {
				a.logger.Info("Disabling a existing warehouse destination: ", key)
				wh.Disable()
			}
		}
	}

	return nil
}

func (a *App) setupTables() error {
	m := &migrator.Migrator{
		Handle:                     a.dbHandle,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: a.config.shouldForceSetLowerVersion,
	}

	operation := func() error {
		return m.Migrate("warehouse")
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		a.logger.Warnf("Failed to setup WH db tables: %v, retrying after %v", err, t)
	})
	if err != nil {
		return fmt.Errorf("could not run warehouse database migrations: %w", err)
	}
	return nil
}

func (a *App) pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	ctx := r.Context()
	// read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	// unmarshall body
	var pendingEventsReq warehouseutils.PendingEventsRequest
	err = json.Unmarshal(body, &pendingEventsReq)
	if err != nil {
		a.logger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	sourceID, taskRunID := pendingEventsReq.SourceID, pendingEventsReq.TaskRunID
	// return error if source id is empty
	if sourceID == "" || taskRunID == "" {
		a.logger.Errorf("empty source_id or task_run_id in the pending events request")
		http.Error(w, "empty source_id or task_run_id", http.StatusBadRequest)
		return
	}

	workspaceID, err := a.tenantManager.SourceToWorkspace(ctx, sourceID)
	if err != nil {
		a.logger.Errorf("[WH]: Error checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if a.tenantManager.DegradedWorkspace(workspaceID) {
		a.logger.Infof("[WH]: Workspace (id: %q) is degraded: %v", workspaceID, err)
		http.Error(w, "workspace is in degraded mode", http.StatusServiceUnavailable)
		return
	}

	pendingEvents := false
	var (
		pendingStagingFileCount int64
		pendingUploadCount      int64
	)

	// check whether there are any pending staging files or uploads for the given source id
	// get pending staging files
	pendingStagingFileCount, err = repo.NewStagingFiles(a.wrappedDBHandle).CountPendingForSource(ctx, sourceID)
	if err != nil {
		err := fmt.Errorf("error getting pending staging file count : %v", err)
		a.logger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filters := []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: taskRunID},
		{Key: "status", NotEquals: true, Value: model.ExportedData},
		{Key: "status", NotEquals: true, Value: model.Aborted},
	}

	pendingUploadCount, err = repo.NewUploads(a.wrappedDBHandle).Count(ctx, filters...)

	if err != nil {
		a.logger.Errorf("getting pending uploads count", "error", err)
		http.Error(w, fmt.Sprintf(
			"getting pending uploads count: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	filters = []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: pendingEventsReq.TaskRunID},
		{Key: "status", Value: "aborted"},
	}

	abortedUploadCount, err := repo.NewUploads(a.wrappedDBHandle).Count(ctx, filters...)
	if err != nil {
		a.logger.Errorf("getting aborted uploads count", "error", err.Error())
		http.Error(w, fmt.Sprintf("getting aborted uploads count: %s", err), http.StatusInternalServerError)
		return
	}

	// if there are any pending staging files or uploads, set pending events as true
	if (pendingStagingFileCount + pendingUploadCount) > int64(0) {
		pendingEvents = true
	}

	// read `triggerUpload` queryParam
	var triggerPendingUpload bool
	triggerUploadQP := r.URL.Query().Get(triggerUploadQPName)
	if triggerUploadQP != "" {
		triggerPendingUpload, _ = strconv.ParseBool(triggerUploadQP)
	}

	// trigger upload if there are pending events and triggerPendingUpload is true
	if pendingEvents && triggerPendingUpload {
		a.logger.Infof("[WH]: Triggering upload for all wh destinations connected to source '%s'", sourceID)

		wh := bcManager.WarehousesBySourceID(sourceID)

		// return error if no such destinations found
		if len(wh) == 0 {
			err := fmt.Errorf("no warehouse destinations found for source id '%s'", sourceID)
			a.logger.Errorf("[WH]: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, warehouse := range wh {
			a.triggerUpload(warehouse)
		}
	}

	// create and write response
	res := warehouseutils.PendingEventsResponse{
		PendingEvents:            pendingEvents,
		PendingStagingFilesCount: pendingStagingFileCount,
		PendingUploadCount:       pendingUploadCount,
		AbortedEvents:            abortedUploadCount > 0,
	}

	resBody, err := json.Marshal(res)
	if err != nil {
		err := fmt.Errorf("failed to marshall pending events response : %v", err)
		a.logger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func (a *App) triggerUploadHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	a.logger.LogRequest(r)

	ctx := r.Context()

	// read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	// unmarshall body
	var triggerUploadReq warehouseutils.TriggerUploadRequest
	err = json.Unmarshal(body, &triggerUploadReq)
	if err != nil {
		a.logger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	workspaceID, err := tenantManager.SourceToWorkspace(ctx, triggerUploadReq.SourceID)
	if err != nil {
		a.logger.Errorf("[WH]: Error checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if tenantManager.DegradedWorkspace(workspaceID) {
		a.logger.Infof("[WH]: Workspace (id: %q) is degraded: %v", workspaceID, err)
		http.Error(w, "workspace is in degraded mode", http.StatusServiceUnavailable)
		return
	}

	err = a.TriggerUploadHandler(triggerUploadReq.SourceID, triggerUploadReq.DestinationID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *App) TriggerUploadHandler(sourceID, destID string) error {
	// return error if source id and dest id is empty
	if sourceID == "" && destID == "" {
		err := fmt.Errorf("empty source and destination id")
		a.logger.Errorf("[WH]: trigger upload : %v", err)
		return err
	}

	var wh []model.Warehouse
	if sourceID != "" && destID == "" {
		wh = bcManager.WarehousesBySourceID(sourceID)
	}
	if destID != "" {
		wh = bcManager.WarehousesByDestID(destID)
	}

	// return error if no such destinations found
	if len(wh) == 0 {
		err := fmt.Errorf("no warehouse destinations found for source id '%s'", sourceID)
		a.logger.Errorf("[WH]: %v", err)
		return err
	}

	// iterate over each wh destination and trigger upload
	for _, warehouse := range wh {
		a.triggerUpload(warehouse)
	}
	return nil
}

func (a *App) fetchTablesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}

	var connectionsTableRequest warehouseutils.FetchTablesRequest
	err = json.Unmarshal(body, &connectionsTableRequest)
	if err != nil {
		a.logger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	schemaRepo := repo.NewWHSchemas(a.wrappedDBHandle)
	tables, err := schemaRepo.GetTablesForConnection(ctx, connectionsTableRequest.Connections)
	if err != nil {
		a.logger.Errorf("[WH]: Error fetching tables: %v", err)
		http.Error(w, "can't fetch tables from schemas repo", http.StatusInternalServerError)
		return
	}
	resBody, err := json.Marshal(warehouseutils.FetchTablesResponse{
		ConnectionsTables: tables,
	})
	if err != nil {
		err := fmt.Errorf("failed to marshall tables to response : %v", err)
		a.logger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func (a *App) isUploadTriggered(wh model.Warehouse) bool {
	a.triggerUploadsMapLock.RLock()
	defer a.triggerUploadsMapLock.RUnlock()
	return a.triggerUploadsMap[wh.Identifier]
}

func (a *App) triggerUpload(wh model.Warehouse) {
	a.triggerUploadsMapLock.Lock()
	defer a.triggerUploadsMapLock.Unlock()
	a.logger.Infof("[WH]: Upload triggered for warehouse '%s'", wh.Identifier)
	a.triggerUploadsMap[wh.Identifier] = true
}

func (a *App) clearTriggeredUpload(wh model.Warehouse) {
	a.triggerUploadsMapLock.Lock()
	defer a.triggerUploadsMapLock.Unlock()
	delete(a.triggerUploadsMap, wh.Identifier)
}

func (a *App) healthHandler(w http.ResponseWriter, _ *http.Request) {
	var dbService, pgNotifierService string

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if a.config.runningMode != DegradedMode {
		if !a.checkPGHealth(ctx, notifier.GetDBHandle()) {
			http.Error(w, "Cannot connect to pgNotifierService", http.StatusInternalServerError)
			return
		}
		pgNotifierService = "UP"
	}

	if a.isMaster() {
		if !a.checkPGHealth(ctx, a.dbHandle) {
			http.Error(w, "Cannot connect to dbService", http.StatusInternalServerError)
			return
		}
		dbService = "UP"
	}

	healthVal := fmt.Sprintf(`
		{
			"server": "UP",
			"db": %q,
			"pgNotifier": %q,
			"acceptingEvents": "TRUE",
			"warehouseMode": %q,
			"goroutines": "%d"
		}
	`,
		dbService,
		pgNotifierService,
		strings.ToUpper(a.config.warehouseMode),
		runtime.NumGoroutine(),
	)

	_, _ = w.Write([]byte(healthVal))
}

func (a *App) checkPGHealth(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}

	healthCheckMsg := "Rudder Warehouse DB Health Check"
	msg := ""

	err := db.QueryRowContext(ctx, `SELECT '`+healthCheckMsg+`'::text as message;`).Scan(&msg)
	if err != nil {
		return false
	}

	return healthCheckMsg == msg
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

func (a *App) startWebHandler(ctx context.Context) error {
	srvMux := chi.NewRouter()

	// do not register same endpoint when running embedded in rudder backend
	if a.isStandAlone() {
		srvMux.Get("/health", a.healthHandler)
	}
	if a.config.runningMode != DegradedMode {
		if a.isMaster() {
			a.logger.Infof("WH: Warehouse master service waiting for BackendConfig before starting on %d", a.config.webPort)
			a.bcConfig.WaitForConfig(ctx)

			srvMux.Handle("/v1/process", (&api.WarehouseAPI{
				Logger:      a.logger,
				Stats:       a.stats,
				Repo:        repo.NewStagingFiles(a.wrappedDBHandle),
				Multitenant: a.tenantManager,
			}).Handler())

			// triggers upload only when there are pending events and triggerUpload is sent for a sourceId
			srvMux.Post("/v1/warehouse/pending-events", a.pendingEventsHandler)
			// triggers uploads for a source
			srvMux.Post("/v1/warehouse/trigger-upload", a.triggerUploadHandler)

			// Warehouse Async Job end-points
			srvMux.Post("/v1/warehouse/jobs", a.asyncJob.AddWarehouseJobHandler)          // FIXME: add degraded mode
			srvMux.Get("/v1/warehouse/jobs/status", a.asyncJob.StatusWarehouseJobHandler) // FIXME: add degraded mode

			// fetch schema info
			// TODO: Remove this endpoint once sources change is released
			srvMux.Get("/v1/warehouse/fetch-tables", a.fetchTablesHandler)
			srvMux.Get("/internal/v1/warehouse/fetch-tables", a.fetchTablesHandler)

			pkgLogger.Infof("WH: Starting warehouse master service in %d", a.config.webPort)
		} else {
			pkgLogger.Infof("WH: Starting warehouse slave service in %d", a.config.webPort)
		}
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.config.webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadHeaderTimeout: 3 * time.Second,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}

// checkForWarehouseEnvVars Checks if all the required Env Variables for Warehouse are present
func (a *App) checkForWarehouseEnvVars() bool {
	return a.conf.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

// isStandAlone checks if gateway is running or not
func (a *App) isStandAlone() bool {
	switch a.config.warehouseMode {
	case config.EmbeddedMode, config.EmbeddedMasterMode:
		return false
	default:
		return true
	}
}

func (a *App) isMaster() bool {
	switch a.config.warehouseMode {
	case config.MasterMode, config.MasterSlaveMode, config.EmbeddedMode, config.EmbeddedMasterMode:
		return true
	default:
		return false
	}
}

func (a *App) isSlave() bool {
	switch a.config.warehouseMode {
	case config.SlaveMode, config.MasterSlaveMode, config.EmbeddedMode:
		return true
	default:
		return false
	}
}

func (a *App) isStandAloneSlave() bool {
	return a.config.warehouseMode == config.SlaveMode
}

// Setup prepares the database connection for warehouse service, verifies database compatibility and creates the required tables
func (a *App) Setup(ctx context.Context) error {
	if !db.IsNormalMode() {
		return nil
	}

	if err := a.setupDB(ctx, a.connectionString()); err != nil {
		return fmt.Errorf("cannot setup warehouse db: %w", err)
	}
	return nil
}

func (a *App) setupDB(ctx context.Context, connInfo string) error {
	var err error
	var db *sql.DB

	if db, err = sql.Open("postgres", connInfo); err != nil {
		return fmt.Errorf("could not open WH db: %w", err)
	}

	db.SetMaxOpenConns(config.GetInt("Warehouse.maxOpenConnections", 20))

	isDBCompatible, err := validators.IsPostgresCompatible(ctx, db)
	if err != nil {
		return fmt.Errorf("could not check WH db compatibility: %w", err)
	}
	if !isDBCompatible {
		err := errors.New("rudder Warehouse Service needs postgres version >= 10. Exiting")
		a.logger.Error(err)
		return err
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping WH db: %w", err)
	}

	a.wrappedDBHandle = sqlquerywrapper.New(
		a.dbHandle,
		sqlquerywrapper.WithLogger(a.logger.Child("dbHandle")),
		sqlquerywrapper.WithQueryTimeout(a.config.dbHandleTimeout),
	)

	return a.setupTables()
}

// Start starts the warehouse service
func (a *App) Start(ctx context.Context, app app.App) error {
	if a.dbHandle == nil && !a.isStandAloneSlave() {
		return errors.New("warehouse service cannot start, database connection is not setup")
	}

	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !a.isStandAlone() && !db.IsNormalMode() {
		a.logger.Infof("Skipping start of warehouse service...")
		return nil
	}

	a.logger.Infof("WH: Starting Warehouse service...")

	psqlInfo := a.connectionString()

	defer func() {
		if r := recover(); r != nil {
			a.logger.Fatal(r)
			panic(r)
		}
	}()

	g, gCtx := errgroup.WithContext(ctx)

	a.tenantManager = &multitenant.Manager{
		BackendConfig: a.bcConfig,
	}
	g.Go(func() error {
		a.tenantManager.Run(gCtx)
		return nil
	})

	a.bcManager = newBackendConfigManager(
		a.conf, a.wrappedDBHandle, a.tenantManager,
		a.logger.Child("wh_bc_manager"),
	)
	g.Go(func() error {
		a.bcManager.Start(gCtx)
		return nil
	})

	RegisterAdmin(a.bcManager, a.logger)

	runningMode := config.GetString("Warehouse.runningMode", "")
	if runningMode == DegradedMode {
		a.logger.Infof("WH: Running warehouse service in degraded mode...")
		if a.isMaster() {
			err := InitWarehouseAPI(a.dbHandle, bcManager, a.logger.Child("upload_api"))
			if err != nil {
				a.logger.Errorf("WH: Failed to start warehouse api: %v", err)
				return err
			}
		}
		return a.startWebHandler(ctx)
	}
	var err error
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		return fmt.Errorf("cannot setup pgnotifier: %w", err)
	}

	// Setting up reporting client
	// only if standalone or embedded connecting to diff DB for warehouse
	if (a.isStandAlone() && a.isMaster()) || (misc.GetConnectionString() != psqlInfo) {
		reporting := a.application.Features().Reporting.Setup(a.bcConfig)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			reporting.AddClient(gCtx, types.Config{ConnInfo: psqlInfo, ClientName: types.WarehouseReportingClient})
			return nil
		}))
	}

	if a.isStandAlone() && a.isMaster() {
		// Report warehouse features
		g.Go(func() error {
			a.bcConfig.WaitForConfig(gCtx)

			c := controlplane.NewClient(
				a.config.configBackendURL,
				a.bcConfig.Identity(),
			)

			err := c.SendFeatures(gCtx, info.WarehouseComponent.Name, info.WarehouseComponent.Features)
			if err != nil {
				a.logger.Errorf("error sending warehouse features: %v", err)
			}

			// We don't want to exit if we fail to send features
			return nil
		})
	}

	if a.isSlave() {
		a.logger.Infof("WH: Starting warehouse slave...")
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return setupSlave(gCtx)
		}))
	}

	if a.isMaster() {
		a.logger.Infof("[WH]: Starting warehouse master...")

		a.bcConfig.WaitForConfig(ctx)

		a.controlPlaneClient = controlplane.NewClient(
			a.config.configBackendURL,
			a.bcConfig.Identity(),
			controlplane.WithRegion(a.config.region),
		)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return a.notifier.ClearJobs(gCtx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return a.monitorDestRouters(ctx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			archive.CronArchiver(gCtx, &archive.Archiver{
				DB:          a.dbHandle,
				Stats:       a.stats,
				Logger:      a.logger.Child("archiver"),
				FileManager: filemanager.New,
				Multitenant: a.tenantManager,
			})
			return nil
		}))

		err := InitWarehouseAPI(a.dbHandle, a.bcManager, a.logger.Child("upload_api"))
		if err != nil {
			a.logger.Errorf("WH: Failed to start warehouse api: %v", err)
			return err
		}

		a.asyncJob = jobs.InitWarehouseJobsAPI(gCtx, a.dbHandle, &a.notifier)
		jobs.WithConfig(a.asyncJob, a.conf)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return a.asyncJob.InitAsyncJobRunner()
		}))
	}

	g.Go(func() error {
		return a.startWebHandler(gCtx)
	})

	return g.Wait()
}
