package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/app"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/identity"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const (
	GeneratingStagingFileFailedState = "generating_staging_file_failed"
	GeneratedStagingFileState        = "generated_staging_file"
	FetchingRemoteSchemaFailed       = "fetching_remote_schema_failed"
	InternalProcessingFailed         = "internal_processing_failed"
)

const (
	cloudSourceCategory          = "cloud"
	singerProtocolSourceCategory = "singer-protocol"
)

var stateTransitions map[string]*uploadState

type uploadState struct {
	inProgress string
	failed     string
	completed  string
	nextState  *uploadState
}

type tableNameT string

type UploadJobFactory struct {
	app                  app.App
	dbHandle             *sqlquerywrapper.DB
	destinationValidator validations.DestinationValidator
	loadFile             *loadfiles.LoadFileGenerator
	recovery             *service.Recovery
	pgNotifier           *pgnotifier.PGNotifier
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats
	encodingFactory      *encoding.Factory
}

type UploadJob struct {
	app                  app.App
	ctx                  context.Context
	dbHandle             *sqlmiddleware.DB
	destinationValidator validations.DestinationValidator
	loadfile             *loadfiles.LoadFileGenerator
	tableUploadsRepo     *repo.TableUploads
	recovery             *service.Recovery
	whManager            manager.Manager
	pgNotifier           *pgnotifier.PGNotifier
	schemaHandle         *Schema
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats
	loadFileGenStartTime time.Time

	upload           model.Upload
	warehouse        model.Warehouse
	stagingFiles     []*model.StagingFile
	stagingFileIDs   []int64
	schemaLock       sync.Mutex
	uploadLock       sync.Mutex
	alertSender      alerta.AlertSender
	now              func() time.Time
	hasSchemaChanged bool

	pendingTableUploads      []model.PendingTableUpload
	pendingTableUploadsRepo  pendingTableUploadsRepo
	pendingTableUploadsOnce  sync.Once
	pendingTableUploadsError error

	config struct {
		refreshPartitionBatchSize                        int
		retryTimeWindow                                  time.Duration
		minRetryAttempts                                 int
		disableAlter                                     bool
		minUploadBackoff                                 time.Duration
		maxUploadBackoff                                 time.Duration
		alwaysRegenerateAllLoadFiles                     bool
		reportingEnabled                                 bool
		generateTableLoadCountMetrics                    bool
		disableGenerateTableLoadCountMetricsWorkspaceIDs []string
		maxParallelLoadsWorkspaceIDs                     map[string]interface{}
		columnsBatchSize                                 int
		longRunningUploadStatThresholdInMin              time.Duration
		tableCountQueryTimeout                           time.Duration
	}

	errorHandler    ErrorHandler
	encodingFactory *encoding.Factory

	stats struct {
		uploadTime                         stats.Measurement
		userTablesLoadTime                 stats.Measurement
		identityTablesLoadTime             stats.Measurement
		otherTablesLoadTime                stats.Measurement
		loadFileGenerationTime             stats.Measurement
		tablesAdded                        stats.Measurement
		columnsAdded                       stats.Measurement
		uploadFailed                       stats.Measurement
		totalRowsSynced                    stats.Measurement
		numStagedEvents                    stats.Measurement
		uploadSuccess                      stats.Measurement
		stagingLoadFileEventsCountMismatch stats.Measurement
	}
}

type UploadColumn struct {
	Column string
	Value  interface{}
}

type pendingTableUploadsRepo interface {
	PendingTableUploads(ctx context.Context, namespace string, uploadID int64, destID string) ([]model.PendingTableUpload, error)
}

const (
	UploadStatusField          = "status"
	UploadStartLoadFileIDField = "start_load_file_id"
	UploadEndLoadFileIDField   = "end_load_file_id"
	UploadUpdatedAtField       = "updated_at"
	UploadTimingsField         = "timings"
	UploadSchemaField          = "schema"
	UploadLastExecAtField      = "last_exec_at"
	UploadInProgress           = "in_progress"
)

var (
	alwaysMarkExported                               = []string{warehouseutils.DiscardsTable}
	warehousesToAlwaysRegenerateAllLoadFilesOnResume = []string{warehouseutils.SNOWFLAKE, warehouseutils.BQ}
)

func init() {
	initializeStateMachine()
}

func (f *UploadJobFactory) NewUploadJob(ctx context.Context, dto *model.UploadJob, whManager manager.Manager) *UploadJob {
	uj := &UploadJob{
		ctx:                  ctx,
		app:                  f.app,
		dbHandle:             f.dbHandle,
		loadfile:             f.loadFile,
		recovery:             f.recovery,
		pgNotifier:           f.pgNotifier,
		whManager:            whManager,
		destinationValidator: f.destinationValidator,
		conf:                 f.conf,
		logger:               f.logger,
		statsFactory:         f.statsFactory,
		tableUploadsRepo:     repo.NewTableUploads(f.dbHandle),
		schemaHandle: NewSchema(
			f.dbHandle,
			dto.Warehouse,
			config.Default,
		),

		upload:         dto.Upload,
		warehouse:      dto.Warehouse,
		stagingFiles:   dto.StagingFiles,
		stagingFileIDs: repo.StagingFileIDs(dto.StagingFiles),

		pendingTableUploadsRepo: repo.NewUploads(f.dbHandle),
		pendingTableUploads:     []model.PendingTableUpload{},

		alertSender: alerta.NewClient(
			f.conf.GetString("ALERTA_URL", "https://alerta.rudderstack.com/api/"),
		),
		now: timeutil.Now,

		errorHandler:    ErrorHandler{whManager},
		encodingFactory: f.encodingFactory,
	}

	uj.config.refreshPartitionBatchSize = f.conf.GetInt("Warehouse.refreshPartitionBatchSize", 100)
	uj.config.minRetryAttempts = f.conf.GetInt("Warehouse.minRetryAttempts", 3)
	uj.config.disableAlter = f.conf.GetBool("Warehouse.disableAlter", false)
	uj.config.alwaysRegenerateAllLoadFiles = f.conf.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", true)
	uj.config.reportingEnabled = f.conf.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	uj.config.generateTableLoadCountMetrics = f.conf.GetBool("Warehouse.generateTableLoadCountMetrics", true)
	uj.config.disableGenerateTableLoadCountMetricsWorkspaceIDs = f.conf.GetStringSlice("Warehouse.disableGenerateTableLoadCountMetricsWorkspaceIDs", nil)
	uj.config.columnsBatchSize = f.conf.GetInt(fmt.Sprintf("Warehouse.%s.columnsBatchSize", warehouseutils.WHDestNameMap[uj.upload.DestinationType]), 100)
	uj.config.maxParallelLoadsWorkspaceIDs = f.conf.GetStringMap(fmt.Sprintf("Warehouse.%s.maxParallelLoadsWorkspaceIDs", warehouseutils.WHDestNameMap[uj.upload.DestinationType]), nil)

	if f.conf.IsSet("Warehouse.tableCountQueryTimeout") {
		uj.config.tableCountQueryTimeout = f.conf.GetDuration("Warehouse.tableCountQueryTimeout", 30, time.Second)
	} else {
		uj.config.tableCountQueryTimeout = f.conf.GetDuration("Warehouse.tableCountQueryTimeoutInS", 30, time.Second)
	}
	if f.conf.IsSet("Warehouse.longRunningUploadStatThreshold") {
		uj.config.longRunningUploadStatThresholdInMin = f.conf.GetDuration("Warehouse.longRunningUploadStatThreshold", 120, time.Minute)
	} else {
		uj.config.longRunningUploadStatThresholdInMin = f.conf.GetDuration("Warehouse.longRunningUploadStatThresholdInMin", 120, time.Minute)
	}
	if f.conf.IsSet("Warehouse.minUploadBackoff") {
		uj.config.minUploadBackoff = f.conf.GetDuration("Warehouse.minUploadBackoff", 60, time.Second)
	} else {
		uj.config.minUploadBackoff = f.conf.GetDuration("Warehouse.minUploadBackoffInS", 60, time.Second)
	}
	if f.conf.IsSet("Warehouse.maxUploadBackoff") {
		uj.config.maxUploadBackoff = f.conf.GetDuration("Warehouse.maxUploadBackoff", 1800, time.Second)
	} else {
		uj.config.maxUploadBackoff = f.conf.GetDuration("Warehouse.maxUploadBackoffInS", 1800, time.Second)
	}
	if f.conf.IsSet("Warehouse.retryTimeWindow") {
		uj.config.retryTimeWindow = f.conf.GetDuration("Warehouse.retryTimeWindow", 180, time.Minute)
	} else {
		uj.config.retryTimeWindow = f.conf.GetDuration("Warehouse.retryTimeWindowInMins", 180, time.Minute)
	}

	uj.stats.uploadTime = uj.timerStat("upload_time")
	uj.stats.userTablesLoadTime = uj.timerStat("user_tables_load_time")
	uj.stats.identityTablesLoadTime = uj.timerStat("identity_tables_load_time")
	uj.stats.otherTablesLoadTime = uj.timerStat("other_tables_load_time")
	uj.stats.loadFileGenerationTime = uj.timerStat("load_file_generation_time")
	uj.stats.tablesAdded = uj.counterStat("tables_added")
	uj.stats.columnsAdded = uj.counterStat("columns_added")
	uj.stats.uploadFailed = uj.counterStat("warehouse_failed_uploads")
	uj.stats.totalRowsSynced = uj.counterStat("total_rows_synced")
	uj.stats.numStagedEvents = uj.counterStat("num_staged_events")
	uj.stats.uploadSuccess = uj.counterStat("upload_success")
	uj.stats.stagingLoadFileEventsCountMismatch = uj.guageStat("warehouse_staging_load_file_events_count_mismatched")

	return uj
}

func (job *UploadJob) identifiesTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentifiesTable)
}

func (job *UploadJob) usersTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.UsersTable)
}

func (job *UploadJob) identityMergeRulesTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable)
}

func (job *UploadJob) identityMappingsTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMappingsTable)
}

func (job *UploadJob) syncRemoteSchema() (bool, error) {
	if err := job.schemaHandle.fetchSchemaFromLocal(job.ctx); err != nil {
		return false, fmt.Errorf("fetching schema from local: %w", err)
	}
	if err := job.schemaHandle.fetchSchemaFromWarehouse(job.ctx, job.whManager); err != nil {
		return false, fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	schemaChanged := job.schemaHandle.hasSchemaChanged()
	if schemaChanged {
		job.logger.Infow("schema changed",
			logfield.SourceID, job.warehouse.Source.ID,
			logfield.SourceType, job.warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, job.warehouse.Destination.ID,
			logfield.DestinationType, job.warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, job.warehouse.WorkspaceID,
			logfield.Namespace, job.warehouse.Namespace,
		)

		if err := job.schemaHandle.updateLocalSchema(job.ctx, job.upload.ID, job.schemaHandle.schemaInWarehouse); err != nil {
			return false, fmt.Errorf("updating local schema: %w", err)
		}
	}

	return schemaChanged, nil
}

func (job *UploadJob) run() (err error) {
	start := job.now()
	ch := job.trackLongRunningUpload()
	defer func() {
		_ = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadInProgress, Value: false}}})

		job.stats.uploadTime.Since(start)
		ch <- struct{}{}
	}()

	job.uploadLock.Lock()
	defer job.uploadLock.Unlock()
	_ = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadLastExecAtField, Value: job.now()}, {Column: UploadInProgress, Value: true}}})

	if len(job.stagingFiles) == 0 {
		err := fmt.Errorf("no staging files found")
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	whManager := job.whManager
	whManager.SetConnectionTimeout(warehouseutils.GetConnectionTimeout(
		job.warehouse.Type, job.warehouse.Destination.ID,
	))
	err = whManager.Setup(job.ctx, job.warehouse, job)
	if err != nil {
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}
	defer whManager.Cleanup(job.ctx)

	if err = job.recovery.Recover(job.ctx, whManager, job.warehouse); err != nil {
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	hasSchemaChanged, err := job.syncRemoteSchema()
	if err != nil {
		_, _ = job.setUploadError(err, FetchingRemoteSchemaFailed)
		return err
	}
	if hasSchemaChanged {
		job.logger.Infof("[WH] Remote schema changed for Warehouse: %s", job.warehouse.Identifier)
	}

	job.schemaHandle.uploadSchema = job.upload.UploadSchema

	var (
		newStatus       string
		nextUploadState *uploadState
	)

	// do not set nextUploadState if hasSchemaChanged to make it start from 1st step again
	if !hasSchemaChanged {
		nextUploadState = getNextUploadState(job.upload.Status)
	}
	if nextUploadState == nil {
		nextUploadState = stateTransitions[model.GeneratedUploadSchema]
	}

	for {
		stateStartTime := job.now()
		err = nil

		_ = job.setUploadStatus(UploadStatusOpts{Status: nextUploadState.inProgress})
		job.logger.Debugf("[WH] Upload: %d, Current state: %s", job.upload.ID, nextUploadState.inProgress)

		targetStatus := nextUploadState.completed

		switch targetStatus {
		case model.GeneratedUploadSchema:
		case model.CreatedTableUploads:
		case model.GeneratedLoadFiles:
		case model.UpdatedTableUploadsCounts:
		case model.CreatedRemoteSchema:
		case model.ExportedData:
		default:
			// If unknown state, start again
			newStatus = model.Waiting
		}

		if err != nil {
			state, err := job.setUploadError(err, newStatus)
			if err == nil && state == model.Aborted {
				job.generateUploadAbortedMetrics()
			}
			break
		}

		job.logger.Debugf("[WH] Upload: %d, Next state: %s", job.upload.ID, newStatus)

		uploadStatusOpts := UploadStatusOpts{Status: newStatus}
		if newStatus == model.ExportedData {

			rowCount, _ := repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(job.ctx, job.upload)

			reportingMetric := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        job.upload.SourceID,
					DestinationID:   job.upload.DestinationID,
					SourceTaskRunID: job.upload.SourceTaskRunID,
					SourceJobID:     job.upload.SourceJobID,
					SourceJobRunID:  job.upload.SourceJobRunID,
				},
				PUDetails: types.PUDetails{
					InPU:       types.BATCH_ROUTER,
					PU:         types.WAREHOUSE,
					TerminalPU: true,
				},
				StatusDetail: &types.StatusDetail{
					Status:      jobsdb.Succeeded.State,
					StatusCode:  200,
					Count:       rowCount,
					SampleEvent: []byte("{}"),
				},
			}
			uploadStatusOpts.ReportingMetric = reportingMetric
		}
		_ = job.setUploadStatus(uploadStatusOpts)

		// record metric for time taken by the current state
		job.timerStat(nextUploadState.inProgress).SendTiming(time.Since(stateStartTime))

		if newStatus == model.ExportedData {
			break
		}

		nextUploadState = getNextUploadState(newStatus)
	}

	if newStatus != model.ExportedData {
		return fmt.Errorf("upload Job failed: %w", err)
	}

	return nil
}

func (job *UploadJob) trackLongRunningUpload() chan struct{} {
	ch := make(chan struct{}, 1)
	rruntime.GoForWarehouse(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(job.config.longRunningUploadStatThresholdInMin):
			job.logger.Infof("[WH]: Registering stat for long running upload: %d, dest: %s", job.upload.ID, job.warehouse.Identifier)

			job.statsFactory.NewTaggedStat(
				"warehouse.long_running_upload",
				stats.CountType,
				stats.Tags{
					"workspaceId": job.warehouse.WorkspaceID,
					"destID":      job.warehouse.Destination.ID,
				},
			).Count(1)
		}
	})
	return ch
}

func (job *UploadJob) resolveIdentities(populateHistoricIdentities bool) (err error) {
	idr := identity.New(
		job.warehouse,
		job.dbHandle,
		job,
		job.upload.ID,
		job.whManager,
		downloader.NewDownloader(&job.warehouse, job, 8),
		job.encodingFactory,
	)
	if populateHistoricIdentities {
		return idr.ResolveHistoricIdentities(job.ctx)
	}
	return idr.Resolve(job.ctx)
}

// getNewTimings appends current status with current time to timings column
// e.g. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (job *UploadJob) getNewTimings(status string) ([]byte, model.Timings) {
	timings, err := repo.NewUploads(job.dbHandle).UploadTimings(job.ctx, job.upload.ID)
	if err != nil {
		job.logger.Error("error getting timing, scrapping them", err)
	}
	timing := map[string]time.Time{status: job.now()}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		panic(err)
	}
	return marshalledTimings, timings
}

func (job *UploadJob) getUploadFirstAttemptTime() (timing time.Time) {
	var firstTiming sql.NullString
	sqlStatement := fmt.Sprintf(`
		SELECT
		  timings -> 0 as firstTimingObj
		FROM
		  %s
		WHERE
		  id = %d;
`,
		warehouseutils.WarehouseUploadsTable,
		job.upload.ID,
	)
	err := job.dbHandle.QueryRowContext(job.ctx, sqlStatement).Scan(&firstTiming)
	if err != nil {
		return
	}
	_, timing = warehouseutils.TimingFromJSONString(firstTiming)
	return timing
}

type UploadStatusOpts struct {
	Status           string
	AdditionalFields []UploadColumn
	ReportingMetric  types.PUReportedMetric
}

func (job *UploadJob) setUploadStatus(statusOpts UploadStatusOpts) (err error) {
	job.logger.Debugf("[WH]: Setting status of %s for wh_upload:%v", statusOpts.Status, job.upload.ID)
	// TODO: fetch upload model instead of just timings
	marshalledTimings, timings := job.getNewTimings(statusOpts.Status)
	opts := []UploadColumn{
		{Column: UploadStatusField, Value: statusOpts.Status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: job.now()},
	}

	job.upload.Status = statusOpts.Status
	job.upload.Timings = timings

	additionalFields := make([]UploadColumn, 0, len(statusOpts.AdditionalFields)+len(opts))
	additionalFields = append(additionalFields, statusOpts.AdditionalFields...)
	additionalFields = append(additionalFields, opts...)

	uploadColumnOpts := UploadColumnsOpts{Fields: additionalFields}

	if statusOpts.ReportingMetric != (types.PUReportedMetric{}) {
		txn, err := job.dbHandle.BeginTx(job.ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		uploadColumnOpts.Txn = txn
		err = job.setUploadColumns(uploadColumnOpts)
		if err != nil {
			return err
		}
		if job.config.reportingEnabled {
			job.app.Features().Reporting.GetReportingInstance().Report(
				[]*types.PUReportedMetric{&statusOpts.ReportingMetric},
				txn.GetTx(),
			)
		}
		return txn.Commit()
	}
	return job.setUploadColumns(uploadColumnOpts)
}

type UploadColumnsOpts struct {
	Fields []UploadColumn
	Txn    *sqlmiddleware.Tx
}

// SetUploadColumns sets any column values passed as args in UploadColumn format for WarehouseUploadsTable
func (job *UploadJob) setUploadColumns(opts UploadColumnsOpts) error {
	var columns string
	values := []interface{}{job.upload.ID}
	// setting values using syntax $n since Exec can correctly format time.Time strings
	for idx, f := range opts.Fields {
		// start with $2 as $1 is upload.ID
		columns += fmt.Sprintf(`%s=$%d`, f.Column, idx+2)
		if idx < len(opts.Fields)-1 {
			columns += ","
		}
		values = append(values, f.Value)
	}
	sqlStatement := fmt.Sprintf(`
		UPDATE
		  %s
		SET
		  %s
		WHERE
		  id = $1;
`,
		warehouseutils.WarehouseUploadsTable,
		columns,
	)

	var querier interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}
	if opts.Txn != nil {
		querier = opts.Txn
	} else {
		querier = job.dbHandle
	}
	_, err := querier.ExecContext(job.ctx, sqlStatement, values...)
	return err
}

func (job *UploadJob) triggerUploadNow() (err error) {
	job.uploadLock.Lock()
	defer job.uploadLock.Unlock()
	newJobState := model.Waiting

	metadata := repo.ExtractUploadMetadata(job.upload)

	metadata.NextRetryTime = job.now().Add(-time.Hour * 1)
	metadata.Retried = true
	metadata.Priority = 50

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	uploadColumns := []UploadColumn{
		{Column: "status", Value: newJobState},
		{Column: "metadata", Value: metadataJSON},
		{Column: "updated_at", Value: job.now()},
	}

	txn, err := job.dbHandle.BeginTx(job.ctx, &sql.TxOptions{})
	if err != nil {
		panic(err)
	}
	err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn})
	if err != nil {
		panic(err)
	}
	err = txn.Commit()

	job.upload.Status = newJobState
	return err
}

// extractAndUpdateUploadErrorsByState extracts and augment errors in format
// { "internal_processing_failed": { "errors": ["account-locked", "account-locked"] }}
// from a particular upload.
func extractAndUpdateUploadErrorsByState(message json.RawMessage, state string, statusError error) (map[string]map[string]interface{}, error) {
	var uploadErrors map[string]map[string]interface{}
	err := json.Unmarshal(message, &uploadErrors)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal error into upload errors: %v", err)
	}

	if uploadErrors == nil {
		uploadErrors = make(map[string]map[string]interface{})
	}

	if _, ok := uploadErrors[state]; !ok {
		uploadErrors[state] = make(map[string]interface{})
	}
	errorByState := uploadErrors[state]

	// increment attempts for errored stage
	if attempt, ok := errorByState["attempt"]; ok {
		errorByState["attempt"] = int(attempt.(float64)) + 1
	} else {
		errorByState["attempt"] = 1
	}

	// append errors for errored stage
	if errList, ok := errorByState["errors"]; ok {
		errorByState["errors"] = append(errList.([]interface{}), statusError.Error())
	} else {
		errorByState["errors"] = []string{statusError.Error()}
	}

	return uploadErrors, nil
}

func (job *UploadJob) setUploadError(statusError error, state string) (string, error) {
	var (
		errorTags                  = job.errorHandler.MatchErrorMappings(statusError)
		destCredentialsValidations *bool
	)

	defer func() {
		job.logger.Warnw("upload error",
			logfield.UploadJobID, job.upload.ID,
			logfield.UploadStatus, state,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Namespace, job.upload.Namespace,
			logfield.Error, statusError,
			logfield.UseRudderStorage, job.upload.UseRudderStorage,
			logfield.Priority, job.upload.Priority,
			logfield.Retried, job.upload.Retried,
			logfield.Attempt, job.upload.Attempts,
			logfield.LoadFileType, job.upload.LoadFileType,
			logfield.ErrorMapping, errorTags.Value,
			logfield.DestinationCredsValid, destCredentialsValidations,
		)
	}()

	job.counterStat(fmt.Sprintf("error_%s", state)).Count(1)
	upload := job.upload

	err := job.setUploadStatus(UploadStatusOpts{Status: state})
	if err != nil {
		return "", fmt.Errorf("unable to set upload's job: %d status: %w", job.upload.ID, err)
	}

	uploadErrors, err := extractAndUpdateUploadErrorsByState(job.upload.Error, state, statusError)
	if err != nil {
		return "", fmt.Errorf("unable to handle upload errors in job: %d by state: %s, err: %v",
			job.upload.ID,
			state,
			err)
	}

	// Reset the state as aborted if max retries
	// exceeded.
	uploadErrorAttempts := uploadErrors[state]["attempt"].(int)

	if job.Aborted(uploadErrorAttempts, job.getUploadFirstAttemptTime()) {
		state = model.Aborted
	}

	metadata := repo.ExtractUploadMetadata(job.upload)

	metadata.NextRetryTime = job.now().Add(job.durationBeforeNextAttempt(upload.Attempts + 1))
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	serializedErr, _ := json.Marshal(&uploadErrors)
	serializedErr = warehouseutils.SanitizeJSON(serializedErr)

	uploadColumns := []UploadColumn{
		{Column: "status", Value: state},
		{Column: "metadata", Value: metadataJSON},
		{Column: "error", Value: serializedErr},
		{Column: "updated_at", Value: job.now()},
	}

	txn, err := job.dbHandle.BeginTx(job.ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("unable to start transaction: %w", err)
	}
	if err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn}); err != nil {
		return "", fmt.Errorf("unable to change upload columns: %w", err)
	}
	inputCount, _ := repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(job.ctx, upload)
	outputCount, _ := job.tableUploadsRepo.TotalExportedEvents(job.ctx, job.upload.ID, []string{
		warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.DiscardsTable),
	})

	failCount := inputCount - outputCount
	reportingStatus := jobsdb.Failed.State
	if state == model.Aborted {
		reportingStatus = jobsdb.Aborted.State
	}
	reportingMetrics := []*types.PUReportedMetric{{
		ConnectionDetails: types.ConnectionDetails{
			SourceID:        job.upload.SourceID,
			DestinationID:   job.upload.DestinationID,
			SourceTaskRunID: job.upload.SourceTaskRunID,
			SourceJobID:     job.upload.SourceJobID,
			SourceJobRunID:  job.upload.SourceJobRunID,
		},
		PUDetails: types.PUDetails{
			InPU:       types.BATCH_ROUTER,
			PU:         types.WAREHOUSE,
			TerminalPU: true,
		},
		StatusDetail: &types.StatusDetail{
			Status:         reportingStatus,
			StatusCode:     400, // TODO: Change this to error specific code
			Count:          failCount,
			SampleEvent:    []byte("{}"),
			SampleResponse: string(serializedErr),
		},
	}}
	if outputCount > 0 {
		reportingMetrics = append(reportingMetrics, &types.PUReportedMetric{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:        job.upload.SourceID,
				DestinationID:   job.upload.DestinationID,
				SourceTaskRunID: job.upload.SourceTaskRunID,
				SourceJobID:     job.upload.SourceJobID,
				SourceJobRunID:  job.upload.SourceJobRunID,
			},
			PUDetails: types.PUDetails{
				InPU:       types.BATCH_ROUTER,
				PU:         types.WAREHOUSE,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Status:         jobsdb.Succeeded.State,
				StatusCode:     400, // TODO: Change this to error specific code
				Count:          failCount,
				SampleEvent:    []byte("{}"),
				SampleResponse: string(serializedErr),
			},
		})
	}
	if job.config.reportingEnabled {
		job.app.Features().Reporting.GetReportingInstance().Report(reportingMetrics, txn.GetTx())
	}
	err = txn.Commit()

	job.upload.Status = state
	job.upload.Error = serializedErr

	job.stats.uploadFailed.Count(1)

	// On aborted state, validate credentials to allow
	// us to differentiate between user caused abort vs platform issue.
	if state == model.Aborted {
		// base tag to be sent as stat

		tags := []warehouseutils.Tag{errorTags}

		valid, err := job.validateDestinationCredentials()
		if err == nil {
			tags = append(tags, warehouseutils.Tag{Name: "destination_creds_valid", Value: strconv.FormatBool(valid)})
			destCredentialsValidations = &valid
		}

		job.counterStat("upload_aborted", tags...).Count(1)
	}

	return state, err
}

// Aborted returns true if the job has been aborted
func (job *UploadJob) Aborted(attempts int, startTime time.Time) bool {
	// Defensive check to prevent garbage startTime
	if startTime.IsZero() {
		return false
	}

	return attempts > job.config.minRetryAttempts && job.now().Sub(startTime) > job.config.retryTimeWindow
}

func (job *UploadJob) durationBeforeNextAttempt(attempt int64) time.Duration { // Add state(retryable/non-retryable) as an argument to decide backoff etc.)
	var d time.Duration
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = job.config.minUploadBackoff
	b.MaxInterval = job.config.maxUploadBackoff
	b.MaxElapsedTime = 0
	b.Multiplier = 2
	b.RandomizationFactor = 0
	b.Reset()
	for index := int64(0); index < attempt; index++ {
		d = b.NextBackOff()
	}
	return d
}

func (job *UploadJob) validateDestinationCredentials() (bool, error) {
	if job.destinationValidator == nil {
		return false, errors.New("failed to validate as destinationValidator is not set")
	}
	response := job.destinationValidator.Validate(job.ctx, &job.warehouse.Destination)
	return response.Success, nil
}

func (job *UploadJob) DTO() *model.UploadJob {
	return &model.UploadJob{
		Warehouse:    job.warehouse,
		Upload:       job.upload,
		StagingFiles: job.stagingFiles,
	}
}

/*
 * State Machine for upload job lifecycle
 */

func getNextUploadState(dbStatus string) *uploadState {
	for _, uploadState := range stateTransitions {
		if dbStatus == uploadState.inProgress || dbStatus == uploadState.failed {
			return uploadState
		}
		if dbStatus == uploadState.completed {
			return uploadState.nextState
		}
	}
	return nil
}

func getInProgressState(state string) string {
	uploadState, ok := stateTransitions[state]
	if !ok {
		panic(fmt.Errorf("invalid Upload state: %s", state))
	}
	return uploadState.inProgress
}

func initializeStateMachine() {
	stateTransitions = make(map[string]*uploadState)

	waitingState := &uploadState{
		completed: model.Waiting,
	}
	stateTransitions[model.Waiting] = waitingState

	generateUploadSchemaState := &uploadState{
		inProgress: "generating_upload_schema",
		failed:     "generating_upload_schema_failed",
		completed:  model.GeneratedUploadSchema,
	}
	stateTransitions[model.GeneratedUploadSchema] = generateUploadSchemaState

	createTableUploadsState := &uploadState{
		inProgress: "creating_table_uploads",
		failed:     "creating_table_uploads_failed",
		completed:  model.CreatedTableUploads,
	}
	stateTransitions[model.CreatedTableUploads] = createTableUploadsState

	generateLoadFilesState := &uploadState{
		inProgress: "generating_load_files",
		failed:     "generating_load_files_failed",
		completed:  model.GeneratedLoadFiles,
	}
	stateTransitions[model.GeneratedLoadFiles] = generateLoadFilesState

	updateTableUploadCountsState := &uploadState{
		inProgress: "updating_table_uploads_counts",
		failed:     "updating_table_uploads_counts_failed",
		completed:  model.UpdatedTableUploadsCounts,
	}
	stateTransitions[model.UpdatedTableUploadsCounts] = updateTableUploadCountsState

	createRemoteSchemaState := &uploadState{
		inProgress: "creating_remote_schema",
		failed:     "creating_remote_schema_failed",
		completed:  model.CreatedRemoteSchema,
	}
	stateTransitions[model.CreatedRemoteSchema] = createRemoteSchemaState

	exportDataState := &uploadState{
		inProgress: "exporting_data",
		failed:     "exporting_data_failed",
		completed:  model.ExportedData,
	}
	stateTransitions[model.ExportedData] = exportDataState

	abortState := &uploadState{
		completed: model.Aborted,
	}
	stateTransitions[model.Aborted] = abortState

	waitingState.nextState = generateUploadSchemaState
	generateUploadSchemaState.nextState = createTableUploadsState
	createTableUploadsState.nextState = generateLoadFilesState
	generateLoadFilesState.nextState = updateTableUploadCountsState
	updateTableUploadCountsState.nextState = createRemoteSchemaState
	createRemoteSchemaState.nextState = exportDataState
	exportDataState.nextState = nil
	abortState.nextState = nil
}
