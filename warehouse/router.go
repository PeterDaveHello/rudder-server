package warehouse

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const defaultUploadPriority = 100

type Router struct {
	destType   string
	warehouses []model.Warehouse

	dbHandle          *sqlquerywrapper.DB
	warehouseDBHandle *DB
	stagingRepo       *repo.StagingFiles
	uploadRepo        *repo.Uploads
	whSchemaRepo      *repo.WHSchema

	notifier             pgnotifier.PGNotifier
	isEnabled            atomic.Bool
	configSubscriberLock sync.RWMutex

	workerChannelMap     map[string]chan *UploadJob
	workerChannelMapLock sync.RWMutex

	inProgressMap     map[WorkerIdentifierT][]JobID
	inProgressMapLock sync.RWMutex

	activeWorkerCount    atomic.Int32
	workspaceBySourceIDs map[string]string
	stats                stats.Stats
	uploadJobFactory     UploadJobFactory
	now                  func() time.Time
	nowSQL               string
	logger               logger.Logger
	conf                 *config.Config

	backgroundCancel context.CancelFunc
	backgroundGroup  errgroup.Group
	backgroundWait   func() error

	tenantManager *multitenant.Manager
	bcManager     *backendConfigManager

	config struct {
		noOfWorkers                       int
		maxConcurrentUploadJobs           int
		allowMultipleSourcesForJobsPickup bool
		enableJitterForSyncs              bool
		maxParallelJobCreation            int
		waitForWorkerSleep                time.Duration
		uploadAllocatorSleep              time.Duration
		mainLoopSleep                     time.Duration
		stagingFilesBatchSize             int
		uploadStatusTrackFrequency        time.Duration
		warehouseSyncFreqIgnore           bool
		shouldPopulateHistoricIdentities  bool
	}
}

func NewRouter(
	ctx context.Context,
	destType string,
	conf *config.Config,
	logger logger.Logger,
	stats stats.Stats,
	db *sqlquerywrapper.DB,
	pgNotifier pgnotifier.PGNotifier,
	tenantManager *multitenant.Manager,
	controlPlaneClient *controlplane.Client,
	bcManager *backendConfigManager,
) (*Router, error) {
	r := &Router{}

	r.conf = conf
	r.stats = stats

	r.logger = logger.Child(destType)
	r.logger.Infof("WH: Warehouse Router started: %s", destType)

	r.dbHandle = db
	// We now have access to the warehouseDBHandle through
	// which we will be running the db calls.
	r.warehouseDBHandle = NewWarehouseDB(db)
	r.stagingRepo = repo.NewStagingFiles(db)
	r.uploadRepo = repo.NewUploads(db)
	r.whSchemaRepo = repo.NewWHSchemas(db)

	r.notifier = pgNotifier
	r.tenantManager = tenantManager
	r.bcManager = bcManager
	r.destType = destType

	if err := r.resetInProgressJobs(ctx); err != nil {
		return nil, err
	}

	r.Enable()
	r.inProgressMap = make(map[WorkerIdentifierT][]JobID)

	r.uploadJobFactory = UploadJobFactory{
		stats:                r.stats,
		dbHandle:             r.dbHandle,
		pgNotifier:           &r.notifier,
		destinationValidator: validations.NewDestinationValidator(),
		loadFile: &loadfiles.LoadFileGenerator{
			Logger:             r.logger.Child("loadfile"),
			Notifier:           &pgNotifier,
			StageRepo:          repo.NewStagingFiles(db),
			LoadRepo:           repo.NewLoadFiles(db),
			ControlPlaneClient: controlPlaneClient,
		},
		recovery: service.NewRecovery(destType, repo.NewUploads(db)),
	}
	loadfiles.WithConfig(r.uploadJobFactory.loadFile, r.conf)

	whName := warehouseutils.WHDestNameMap[destType]

	r.conf.RegisterIntConfigVariable(8, &r.config.noOfWorkers, true, 1, fmt.Sprintf(`Warehouse.%v.noOfWorkers`, whName), "Warehouse.noOfWorkers")
	r.conf.RegisterIntConfigVariable(1, &r.config.maxConcurrentUploadJobs, false, 1, fmt.Sprintf(`Warehouse.%v.maxConcurrentUploadJobs`, whName))
	r.conf.RegisterIntConfigVariable(8, &r.config.maxParallelJobCreation, true, 1, "Warehouse.maxParallelJobCreation")
	r.conf.RegisterDurationConfigVariable(5, &r.config.waitForWorkerSleep, false, time.Second, []string{"Warehouse.waitForWorkerSleep", "Warehouse.waitForWorkerSleepInS"}...)
	r.conf.RegisterDurationConfigVariable(5, &r.config.uploadAllocatorSleep, false, time.Second, []string{"Warehouse.uploadAllocatorSleep", "Warehouse.uploadAllocatorSleepInS"}...)
	r.conf.RegisterDurationConfigVariable(5, &r.config.mainLoopSleep, true, time.Second, []string{"Warehouse.mainLoopSleep", "Warehouse.mainLoopSleepInS"}...)
	r.conf.RegisterIntConfigVariable(960, &r.config.stagingFilesBatchSize, true, 1, "Warehouse.stagingFilesBatchSize")
	r.conf.RegisterDurationConfigVariable(30, &r.config.uploadStatusTrackFrequency, false, time.Minute, []string{"Warehouse.uploadStatusTrackFrequency", "Warehouse.uploadStatusTrackFrequencyInMin"}...)
	r.conf.RegisterBoolConfigVariable(false, &r.config.allowMultipleSourcesForJobsPickup, false, fmt.Sprintf(`Warehouse.%v.allowMultipleSourcesForJobsPickup`, whName))
	r.conf.RegisterBoolConfigVariable(false, &r.config.enableJitterForSyncs, true, "Warehouse.enableJitterForSyncs")
	r.conf.RegisterBoolConfigVariable(false, &r.config.warehouseSyncFreqIgnore, true, "Warehouse.warehouseSyncFreqIgnore")
	r.conf.RegisterBoolConfigVariable(false, &r.config.shouldPopulateHistoricIdentities, false, "Warehouse.populateHistoricIdentities")

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	r.backgroundCancel = cancel
	r.backgroundWait = g.Wait

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.backendConfigSubscriber(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.runUploadJobAllocator(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.mainLoop(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		return r.CronTracker(ctx)
	}))

	return r, nil
}

func (r *Router) resetInProgressJobs(ctx context.Context) error {
	sqlStatement := fmt.Sprintf(`
		UPDATE
		  %s
		SET
		  in_progress = %t
		WHERE
		  destination_type = '%s'
		  AND in_progress = %t;
`,
		warehouseutils.WarehouseUploadsTable,
		false,
		r.destType,
		true,
	)
	_, err := r.dbHandle.ExecContext(ctx, sqlStatement)
	return err
}

// Backend Config subscriber subscribes to backend-config and gets all the configurations that includes all sources, destinations and their latest values.
func (r *Router) backendConfigSubscriber(ctx context.Context) {
	for warehouses := range r.bcManager.Subscribe(ctx) {
		r.logger.Info(`Received updated workspace config`)

		warehouses = lo.Filter(warehouses, func(warehouse model.Warehouse, _ int) bool {
			return warehouse.Destination.DestinationDefinition.Name == r.destType
		})

		for _, warehouse := range warehouses {
			if warehouseutils.IDResolutionEnabled() && slices.Contains(warehouseutils.IdentityEnabledWarehouses, r.destType) {
				r.setupIdentityTables(ctx, warehouse)
				if r.config.shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
					// non-blocking populate historic identities
					r.populateHistoricIdentities(ctx, warehouse)
				}
			}
		}

		r.configSubscriberLock.Lock()
		r.warehouses = warehouses
		if r.workspaceBySourceIDs == nil {
			r.workspaceBySourceIDs = make(map[string]string)
		}
		for _, warehouse := range warehouses {
			r.workspaceBySourceIDs[warehouse.Source.ID] = warehouse.WorkspaceID
		}
		r.configSubscriberLock.Unlock()

		r.workerChannelMapLock.Lock()
		if r.workerChannelMap == nil {
			r.workerChannelMap = make(map[string]chan *UploadJob)
		}
		for _, warehouse := range warehouses {
			if warehouse.Destination.DestinationDefinition.Name != r.destType {
				continue
			}

			workerName := r.workerIdentifier(warehouse)
			// spawn one worker for each unique destID_namespace
			// check this commit to https://github.com/rudderlabs/rudder-server/pull/476/commits/fbfddf167aa9fc63485fe006d34e6881f5019667
			// to avoid creating goroutine for disabled sources/destinations
			if _, ok := r.workerChannelMap[workerName]; !ok {
				workerChan := r.initWorker()
				r.workerChannelMap[workerName] = workerChan
			}
		}
		r.workerChannelMapLock.Unlock()
	}
}

// workerIdentifier get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func (r *Router) workerIdentifier(warehouse model.Warehouse) (identifier string) {
	if r.config.allowMultipleSourcesForJobsPickup {
		return warehouse.Source.ID + "_" + warehouse.Destination.ID + "_" + warehouse.Namespace
	}
	return warehouse.Destination.ID + "_" + warehouse.Namespace
}

func (r *Router) initWorker() chan *UploadJob {
	workerChan := make(chan *UploadJob, 1000)
	for i := 0; i < r.config.maxConcurrentUploadJobs; i++ {
		r.backgroundGroup.Go(func() error {
			for uploadJob := range workerChan {
				r.incrementActiveWorkers()
				err := r.handleUploadJob(uploadJob)
				if err != nil {
					r.logger.Errorf("[WH] Failed in handle Upload jobs for worker: %+w", err)
				}
				r.removeDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
				r.decrementActiveWorkers()
			}
			return nil
		})
	}
	return workerChan
}

func (*Router) handleUploadJob(uploadJob *UploadJob) error {
	// Process the upload job
	return uploadJob.run()
}

func (r *Router) incrementActiveWorkers() {
	r.activeWorkerCount.Add(1)
}

func (r *Router) decrementActiveWorkers() {
	r.activeWorkerCount.Add(-1)
}

func (r *Router) getActiveWorkerCount() int {
	return int(r.activeWorkerCount.Load())
}

func (r *Router) setDestInProgress(warehouse model.Warehouse, jobID int64) {
	identifier := r.workerIdentifier(warehouse)
	r.inProgressMapLock.Lock()
	defer r.inProgressMapLock.Unlock()
	r.inProgressMap[WorkerIdentifierT(identifier)] = append(r.inProgressMap[WorkerIdentifierT(identifier)], JobID(jobID))
}

func (r *Router) removeDestInProgress(warehouse model.Warehouse, jobID int64) {
	r.inProgressMapLock.Lock()
	defer r.inProgressMapLock.Unlock()
	identifier := r.workerIdentifier(warehouse)
	if idx, inProgress := r.checkInProgressMap(jobID, identifier); inProgress {
		r.inProgressMap[WorkerIdentifierT(identifier)] = removeFromJobsIDT(r.inProgressMap[WorkerIdentifierT(identifier)], idx)
	}
}

func removeFromJobsIDT(slice []JobID, idx int) []JobID {
	return append(slice[:idx], slice[idx+1:]...)
}

func (r *Router) isUploadJobInProgress(warehouse model.Warehouse, jobID int64) (int, bool) {
	identifier := r.workerIdentifier(warehouse)
	r.inProgressMapLock.RLock()
	defer r.inProgressMapLock.RUnlock()
	return r.checkInProgressMap(jobID, identifier)
}

func (r *Router) getInProgressNamespaces() []string {
	r.inProgressMapLock.RLock()
	defer r.inProgressMapLock.RUnlock()
	var identifiers []string
	for k, v := range r.inProgressMap {
		if len(v) >= r.config.maxConcurrentUploadJobs {
			identifiers = append(identifiers, string(k))
		}
	}
	return identifiers
}

func (r *Router) checkInProgressMap(jobID int64, identifier string) (int, bool) {
	for idx, id := range r.inProgressMap[WorkerIdentifierT(identifier)] {
		if jobID == int64(id) {
			return idx, true
		}
	}
	return 0, false
}

func (r *Router) runUploadJobAllocator(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-r.bcManager.initialConfigFetched:
			r.logger.Debugf("Initial config fetched in runUploadJobAllocator for %s", r.destType)
		}

		availableWorkers := r.config.noOfWorkers - r.getActiveWorkerCount()
		if availableWorkers < 1 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(r.config.waitForWorkerSleep):
			}
			continue
		}

		inProgressNamespaces := r.getInProgressNamespaces()
		r.logger.Debugf(`Current inProgress namespace identifiers for %s: %v`, r.destType, inProgressNamespaces)

		uploadJobsToProcess, err := r.uploadsToProcess(ctx, availableWorkers, inProgressNamespaces)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) ||
				strings.Contains(err.Error(), "pq: canceling statement due to user request") {
				break loop
			} else {
				r.logger.Errorf(`Error executing uploadsToProcess: %v`, err)
				panic(err)
			}
		}

		for _, uploadJob := range uploadJobsToProcess {
			r.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
		}

		for _, uploadJob := range uploadJobsToProcess {
			workerName := r.workerIdentifier(uploadJob.warehouse)
			r.workerChannelMapLock.RLock()
			r.workerChannelMap[workerName] <- uploadJob
			r.workerChannelMapLock.RUnlock()
		}

		select {
		case <-ctx.Done():
			break loop
		case <-time.After(r.config.uploadAllocatorSleep):
		}
	}

	r.workerChannelMapLock.RLock()
	for _, workerChannel := range r.workerChannelMap {
		close(workerChannel)
	}
	r.workerChannelMapLock.RUnlock()
}

func (r *Router) uploadsToProcess(ctx context.Context, availableWorkers int, skipIdentifiers []string) ([]*UploadJob, error) {
	uploads, err := r.uploadRepo.GetToProcess(ctx, r.destType, availableWorkers, repo.ProcessOptions{
		SkipIdentifiers:                   skipIdentifiers,
		SkipWorkspaces:                    r.tenantManager.DegradedWorkspaces(),
		AllowMultipleSourcesForJobsPickup: r.config.allowMultipleSourcesForJobsPickup,
	})
	if err != nil {
		return nil, err
	}

	var uploadJobs []*UploadJob
	for _, upload := range uploads {
		r.configSubscriberLock.RLock()
		if upload.WorkspaceID == "" {
			var ok bool
			upload.WorkspaceID, ok = r.workspaceBySourceIDs[upload.SourceID]
			if !ok {
				r.logger.Warnf("could not find workspace id for source id: %s", upload.SourceID)
			}
		}

		warehouse, found := lo.Find(r.warehouses, func(w model.Warehouse) bool {
			return w.Source.ID == upload.SourceID && w.Destination.ID == upload.DestinationID
		})
		r.configSubscriberLock.RUnlock()

		upload.UseRudderStorage = warehouse.GetBoolDestinationConfig("useRudderStorage")

		if !found {
			uploadJob := r.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
				Upload: upload,
			}, nil)
			err := fmt.Errorf("unable to find source : %s or destination : %s, both or the connection between them", upload.SourceID, upload.DestinationID)
			_, _ = uploadJob.setUploadError(err, model.Aborted)
			r.logger.Errorf("%v", err)
			continue
		}

		stagingFilesList, err := r.stagingRepo.GetForUpload(ctx, upload)
		if err != nil {
			return nil, err
		}

		whManager, err := manager.New(r.destType, r.conf, r.logger, r.stats)
		if err != nil {
			return nil, err
		}
		uploadJob := r.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFilesList,
		}, whManager)

		uploadJobs = append(uploadJobs, uploadJob)
	}

	jobsStats, err := r.uploadRepo.UploadJobsStats(ctx, r.destType, repo.ProcessOptions{
		SkipIdentifiers: skipIdentifiers,
		SkipWorkspaces:  r.tenantManager.DegradedWorkspaces(),
	})
	if err != nil {
		return nil, fmt.Errorf("processing stats: %w", err)
	}

	r.processingStats(availableWorkers, jobsStats)

	return uploadJobs, nil
}

func (r *Router) processingStats(availableWorkers int, jobStats model.UploadJobsStats) {
	// Get pending jobs
	pendingJobsStat := r.stats.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, stats.Tags{
		"destType": r.destType,
	})
	pendingJobsStat.Gauge(int(jobStats.PendingJobs))

	availableWorkersStat := r.stats.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, stats.Tags{
		"destType": r.destType,
	})
	availableWorkersStat.Gauge(availableWorkers)

	pickupLagStat := r.stats.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, stats.Tags{
		"destType": r.destType,
	})
	pickupLagStat.SendTiming(jobStats.PickupLag)

	pickupWaitTimeStat := r.stats.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, stats.Tags{
		"destType": r.destType,
	})
	pickupWaitTimeStat.SendTiming(jobStats.PickupWaitTime)
}

func (r *Router) mainLoop(ctx context.Context) {
	for {
		if !r.isEnabled.Load() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.config.mainLoopSleep):
			}
			continue
		}

		jobCreationChan := make(chan struct{}, r.config.maxParallelJobCreation)
		r.configSubscriberLock.RLock()
		wg := sync.WaitGroup{}
		wg.Add(len(r.warehouses))

		r.stats.NewTaggedStat("wh_scheduler.warehouse_length", stats.GaugeType, stats.Tags{
			warehouseutils.DestinationType: r.destType,
		}).Gauge(len(r.warehouses)) // Correlation between number of warehouses and scheduling time.
		whTotalSchedulingStats := r.stats.NewTaggedStat("wh_scheduler.total_scheduling_time", stats.TimerType, stats.Tags{
			warehouseutils.DestinationType: r.destType,
		})
		whTotalSchedulingStart := time.Now()

		for _, warehouse := range r.warehouses {
			w := warehouse
			rruntime.GoForWarehouse(func() {
				jobCreationChan <- struct{}{}
				defer func() {
					wg.Done()
					<-jobCreationChan
				}()

				r.logger.Debugf("[WH] Processing Jobs for warehouse: %s", w.Identifier)
				err := r.createJobs(ctx, w)
				if err != nil {
					r.logger.Errorf("[WH] Failed to process warehouse Jobs: %v", err)
				}
			})
		}
		r.configSubscriberLock.RUnlock()
		wg.Wait()

		whTotalSchedulingStats.Since(whTotalSchedulingStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.config.mainLoopSleep):
		}
	}
}

func (r *Router) createJobs(ctx context.Context, warehouse model.Warehouse) (err error) {
	if ok, err := r.canCreateUpload(warehouse); !ok {
		r.stats.NewTaggedStat("wh_scheduler.upload_sync_skipped", stats.CountType, stats.Tags{
			"workspaceId":   warehouse.WorkspaceID,
			"destinationID": warehouse.Destination.ID,
			"destType":      warehouse.Destination.DestinationDefinition.Name,
			"reason":        err.Error(),
		}).Count(1)
		r.logger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded: %v", warehouse.Identifier, err)
		return nil
	}

	priority := defaultUploadPriority
	uploadID, uploadStatus, uploadPriority := r.getLatestUploadStatus(ctx, &warehouse)
	if uploadStatus == model.Waiting {
		// If it is present do nothing else delete it
		if _, inProgress := r.isUploadJobInProgress(warehouse, uploadID); !inProgress {
			err := r.uploadRepo.DeleteWaiting(ctx, uploadID)
			if err != nil {
				r.logger.Error(err, "uploadID", uploadID, "warehouse", warehouse.Identifier)
			}
			priority = uploadPriority // copy the priority from the latest upload job.
		}
	}

	stagingFilesFetchStat := r.stats.NewTaggedStat("wh_scheduler.pending_staging_files", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	stagingFilesFetchStart := time.Now()
	stagingFilesList, err := r.stagingRepo.Pending(ctx, warehouse.Source.ID, warehouse.Destination.ID)
	if err != nil {
		return fmt.Errorf("pending staging files for %q: %w", warehouse.Identifier, err)
	}
	stagingFilesFetchStat.Since(stagingFilesFetchStart)

	if len(stagingFilesList) == 0 {
		r.logger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobCreationStat := r.stats.NewTaggedStat("wh_scheduler.create_upload_jobs", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	defer uploadJobCreationStat.RecordDuration()()

	uploadStartAfter := r.uploadStartAfterTime()
	err = r.createUploadJobsFromStagingFiles(ctx, warehouse, stagingFilesList, priority, uploadStartAfter)
	if err != nil {
		return err
	}
	setLastProcessedMarker(warehouse, uploadStartAfter)

	return nil
}

func (r *Router) getLatestUploadStatus(ctx context.Context, warehouse *model.Warehouse) (int64, string, int) {
	uploadID, status, priority, err := r.warehouseDBHandle.GetLatestUploadStatus(
		ctx,
		warehouse.Source.ID,
		warehouse.Destination.ID)
	if err != nil {
		r.logger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
	}

	return uploadID, status, priority
}

func (r *Router) uploadStartAfterTime() time.Time {
	if r.config.enableJitterForSyncs {
		return timeutil.Now().Add(time.Duration(rand.Intn(15)) * time.Second)
	}
	return time.Now()
}

func (r *Router) createUploadJobsFromStagingFiles(ctx context.Context, warehouse model.Warehouse, stagingFiles []*model.StagingFile, priority int, uploadStartAfter time.Time) error {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// E.g. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	uploadTriggered := isUploadTriggered(warehouse)
	if uploadTriggered {
		priority = 50
	}

	batches := service.StageFileBatching(stagingFiles, r.config.stagingFilesBatchSize)
	for _, batch := range batches {
		upload := model.Upload{
			SourceID:        warehouse.Source.ID,
			Namespace:       warehouse.Namespace,
			WorkspaceID:     warehouse.WorkspaceID,
			DestinationID:   warehouse.Destination.ID,
			DestinationType: r.destType,
			Status:          model.Waiting,

			LoadFileType:  warehouseutils.GetLoadFileType(r.destType),
			NextRetryTime: uploadStartAfter,
			Priority:      priority,

			// The following will be populated by staging files:
			// FirstEventAt:     0,
			// LastEventAt:      0,
			// UseRudderStorage: false,
			// SourceTaskRunID:  "",
			// SourceJobID:      "",
			// SourceJobRunID:   "",
		}

		_, err := r.uploadRepo.CreateWithStagingFiles(ctx, upload, batch)
		if err != nil {
			return fmt.Errorf("creating upload: %w", err)
		}
	}

	// reset upload trigger if the upload was triggered
	if uploadTriggered {
		clearTriggeredUpload(warehouse)
	}

	return nil
}

// Enable enables a router :)
func (r *Router) Enable() {
	r.isEnabled.Store(true)
}

// Disable disables a router:)
func (r *Router) Disable() {
	r.isEnabled.Store(false)
}

func (r *Router) Shutdown() error {
	r.backgroundCancel()
	return r.backgroundWait()
}
