package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/lib/pq"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/archiver/tablearchiver"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	archiveUploadRelatedRecords bool
	uploadsArchivalTimeInDays   int
	archiverTickerTime          time.Duration
)

func Init() {
	loadConfigArchiver()
}

func loadConfigArchiver() {
	config.RegisterBoolConfigVariable(true, &archiveUploadRelatedRecords, true, "Warehouse.archiveUploadRelatedRecords")
	config.RegisterIntConfigVariable(5, &uploadsArchivalTimeInDays, true, 1, "Warehouse.uploadsArchivalTimeInDays")
	config.RegisterDurationConfigVariable(360, &archiverTickerTime, true, time.Minute, []string{"Warehouse.archiverTickerTime", "Warehouse.archiverTickerTimeInMin"}...) // default 6 hours
}

type backupRecordsArgs struct {
	tableName      string
	tableFilterSQL string
	sourceID       string
	destID         string
	uploadID       int64
}

type uploadRecord struct {
	sourceID           string
	destID             string
	uploadID           int64
	startStagingFileId int64
	endStagingFileId   int64
	startLoadFileID    int64
	endLoadFileID      int64
	uploadMetadata     json.RawMessage
	workspaceID        string
}

type Archiver struct {
	DB          *sql.DB
	Stats       stats.Stats
	Logger      lf.Logger
	FileManager filemanager.Factory
	Multitenant *multitenant.Manager
}

func (a *Archiver) backupRecords(ctx context.Context, args backupRecordsArgs) (backupLocation string, err error) {
	logFields := []lf.KeyValue{
		lf.UploadID(args.uploadID),
		lf.SourceID(args.sourceID),
		lf.DestinationID(args.destID),
		lf.TableName(args.tableName),
	}
	a.Logger.Infow("Starting backupRecords", logFields...)

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		a.Logger.Errorw("Failed to create tmp DIR", logFields...)
		return
	}
	backupPathDirName := "/" + misc.RudderArchives + "/"
	pathPrefix := strcase.ToKebab(warehouseutils.WarehouseStagingFilesTable)

	path := fmt.Sprintf(`%v%v.%v.%v.%v.%v.json.gz`,
		tmpDirPath+backupPathDirName,
		pathPrefix,
		args.sourceID,
		args.destID,
		args.uploadID,
		timeutil.Now().Unix(),
	)
	defer misc.RemoveFilePaths(path)

	fManager, err := a.FileManager(&filemanager.Settings{
		Provider: config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanagerutil.GetProviderConfigForBackupsFromEnv(ctx, config.Default),
	})
	if err != nil {
		err = fmt.Errorf("error in creating a file manager for:%s. Error: %w",
			config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3"), err,
		)
		return
	}

	tmpl := fmt.Sprintf(`
		SELECT
		  json_agg(dump_table)
		FROM
		  (
			SELECT
			  *
			FROM
			  %[1]s
			WHERE
			  %[2]s
			ORDER BY
			  id ASC
			LIMIT
			  %[3]s offset %[4]s
		  ) AS dump_table`,
		args.tableName,
		args.tableFilterSQL,
		tablearchiver.PaginationAction,
		tablearchiver.OffsetAction,
	)
	tableJSONArchiver := tablearchiver.TableJSONArchiver{
		DbHandle:      a.DB,
		Pagination:    config.GetInt("Warehouse.Archiver.backupRowsBatchSize", 100),
		QueryTemplate: tmpl,
		OutputPath:    path,
		FileManager:   fManager,
	}

	backupLocation, err = tableJSONArchiver.Do()
	a.Logger.Infow("Completed backupRecords", logFields...)

	return
}

func (a *Archiver) deleteFilesInStorage(ctx context.Context, locations []string) error {
	fManager, err := a.FileManager(&filemanager.Settings{
		Provider: warehouseutils.S3,
		Config:   misc.GetRudderObjectStorageConfig(""),
	})
	if err != nil {
		err = fmt.Errorf("cannot create file manager for Rudder Storage: %w", err)
		return err
	}

	err = fManager.Delete(ctx, locations)
	if err != nil {
		a.Logger.Errorw("Error in deleting objects in Rudder S3", lf.Error(err))
	}
	return err
}

func (*Archiver) usedRudderStorage(metadata []byte) bool {
	return gjson.GetBytes(metadata, "use_rudder_storage").Bool()
}

func (a *Archiver) Do(ctx context.Context) error {
	a.Logger.Infow("Started archiving for warehouse")

	sqlStatement := fmt.Sprintf(`
		SELECT
		  id,
		  source_id,
		  destination_id,
		  start_staging_file_id,
		  end_staging_file_id,
		  start_load_file_id,
		  end_load_file_id,
		  metadata,
		  workspace_id
		FROM
		  %s
		WHERE
		  (
			(
			  metadata ->> 'archivedStagingAndLoadFiles'
			):: bool IS DISTINCT
			FROM
			  TRUE
		  )
		  AND created_at < NOW() - $1::interval
		  AND status = $2
		  AND NOT workspace_id = ANY ( $3 )
		LIMIT
		  10000;`,
		pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable),
	)

	// empty workspace id should be excluded as a safety measure
	skipWorkspaceIDs := []string{""}
	skipWorkspaceIDs = append(skipWorkspaceIDs, a.Multitenant.DegradedWorkspaces()...)

	rows, err := a.DB.QueryContext(ctx, sqlStatement,
		fmt.Sprintf("%d DAY", uploadsArchivalTimeInDays),
		model.ExportedData,
		pq.Array(skipWorkspaceIDs),
	)
	defer func() {
		if err != nil {
			a.Logger.Errorw("Error occurred while archiving for warehouse uploads with error", lf.Error(err))
			a.Stats.NewStat("warehouse.archiver.archiveFailed", stats.CountType).Increment()
		}
	}()
	if err == sql.ErrNoRows {
		a.Logger.Debugw("No uploads found for archival", lf.Query(sqlStatement))
		return nil
	}
	if err != nil {
		return fmt.Errorf("querying wh_uploads for archival: %w", err)
	}

	var uploadsToArchive []*uploadRecord
	for rows.Next() {
		var u uploadRecord
		err := rows.Scan(
			&u.uploadID,
			&u.sourceID,
			&u.destID,
			&u.startStagingFileId,
			&u.endStagingFileId,
			&u.startLoadFileID,
			&u.endLoadFileID,
			&u.uploadMetadata,
			&u.workspaceID,
		)
		if err != nil {
			a.Logger.Errorw("Error scanning wh_upload for archival. Error: %", lf.Error(err))
			continue
		}
		uploadsToArchive = append(uploadsToArchive, &u)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scanning wh_uploads rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing rows after scanning wh_uploads for archival: %w", err)
	}

	var archivedUploads int
	for _, u := range uploadsToArchive {
		txn, err := a.DB.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			a.Logger.Errorw("Error creating txn in archiveUploadFiles", lf.Error(err))
			continue
		}

		hasUsedRudderStorage := a.usedRudderStorage(u.uploadMetadata)

		// archive staging files
		stagingFileIDs, stagingFileLocations, err := a.getStagingFilesData(ctx, txn, u)
		if err != nil {
			a.Logger.Errorw("Error getting staging files data", lf.UploadID(u.uploadID), lf.Error(err))
			_ = txn.Rollback()
			continue
		}

		var storedStagingFilesLocation string
		if len(stagingFileIDs) > 0 {
			if !hasUsedRudderStorage {
				filterSQL := fmt.Sprintf(`id IN (%v)`, misc.IntArrayToString(stagingFileIDs, ","))
				storedStagingFilesLocation, err = a.backupRecords(ctx, backupRecordsArgs{
					tableName:      warehouseutils.WarehouseStagingFilesTable,
					sourceID:       u.sourceID,
					destID:         u.destID,
					tableFilterSQL: filterSQL,
					uploadID:       u.uploadID,
				})
				if err != nil {
					a.Logger.Errorw("Error backing up staging files", lf.UploadID(u.uploadID), lf.Error(err))
					_ = txn.Rollback()
					continue
				}
			} else {
				a.Logger.Debugw("Object storage not configured to archive upload related staging file records."+
					"Deleting the ones that need to be archived", lf.UploadID(u.uploadID))

				err = a.deleteFilesInStorage(ctx, stagingFileLocations)
				if err != nil {
					a.Logger.Errorw("Error deleting staging files from Rudder S3",
						lf.UploadID(u.uploadID), lf.Error(err))
					_ = txn.Rollback()
					continue
				}
			}

			// delete staging file records
			stmt := fmt.Sprintf(`
				DELETE FROM %s
				WHERE id = ANY($1);`,
				pq.QuoteIdentifier(warehouseutils.WarehouseStagingFilesTable),
			)
			_, err = txn.ExecContext(ctx, stmt, pq.Array(stagingFileIDs))
			if err != nil {
				a.Logger.Errorw("Error running txn in archiveUploadFiles",
					lf.Query(stmt), lf.Error(err))
				_ = txn.Rollback()
				continue
			}

			// delete load file records
			if err := a.deleteLoadFileRecords(ctx, txn, stagingFileIDs, hasUsedRudderStorage); err != nil {
				a.Logger.Errorw("Error while deleting load file records",
					lf.UploadID(u.uploadID), lf.Error(err))
				_ = txn.Rollback()
				continue
			}
		}

		// update upload metadata
		u.uploadMetadata, _ = sjson.SetBytes(u.uploadMetadata, "archivedStagingAndLoadFiles", true)
		stmt := fmt.Sprintf(`
			UPDATE %s
			SET metadata = $1
			WHERE id = $2;`,
			pq.QuoteIdentifier(warehouseutils.WarehouseUploadsTable),
		)
		_, err = txn.ExecContext(ctx, stmt, u.uploadMetadata, u.uploadID)
		if err != nil {
			a.Logger.Errorw("Error running txn while archiving upload files", lf.Query(stmt), lf.Error(err))
			_ = txn.Rollback()
			continue
		}

		if err = txn.Commit(); err != nil {
			a.Logger.Errorw("Error committing txn while archiving upload files", lf.Error(err))
			_ = txn.Rollback()
			continue
		}

		archivedUploads++
		if storedStagingFilesLocation != "" {
			a.Logger.Debugw("Archived upload at location",
				lf.UploadID(u.uploadID), lf.Location(storedStagingFilesLocation),
			)
		}

		a.Stats.NewTaggedStat("warehouse.archiver.numArchivedUploads", stats.CountType, stats.Tags{
			"destination": u.destID,
			"source":      u.sourceID,
		}).Increment()
	}

	a.Logger.Infow("Successfully archived uploads", lf.Generic("numArchivedUploads")(archivedUploads))

	return nil
}

func (a *Archiver) getStagingFilesData(ctx context.Context, txn *sql.Tx, u *uploadRecord) ([]int64, []string, error) {
	stmt := fmt.Sprintf(`
		SELECT
		  id,
		  location
		FROM
		  %s
		WHERE
		  source_id = $1
		  AND destination_id = $2
		  AND id >= $3
		  and id <= $4;`,
		pq.QuoteIdentifier(warehouseutils.WarehouseStagingFilesTable),
	)

	stagingFileRows, err := txn.QueryContext(ctx, stmt,
		u.sourceID,
		u.destID,
		u.startStagingFileId,
		u.endStagingFileId,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot query staging files data: %w", err)
	}
	defer func() { _ = stagingFileRows.Close() }()

	var (
		stagingFileIDs       []int64
		stagingFileLocations []string
	)
	for stagingFileRows.Next() {
		var (
			stagingFileID       int64
			stagingFileLocation string
		)
		err = stagingFileRows.Scan(
			&stagingFileID,
			&stagingFileLocation,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("scanning staging file id: %w", err)
		}
		stagingFileIDs = append(stagingFileIDs, stagingFileID)
		stagingFileLocations = append(stagingFileLocations, stagingFileLocation)
	}
	if err = stagingFileRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterating staging file ids: %w", err)
	}

	return stagingFileIDs, stagingFileLocations, nil
}

func (a *Archiver) deleteLoadFileRecords(
	ctx context.Context, txn *sql.Tx, stagingFileIDs []int64, hasUsedRudderStorage bool,
) error {
	stmt := fmt.Sprintf(`
		DELETE FROM %s
		WHERE staging_file_id = ANY($1) RETURNING location;`,
		pq.QuoteIdentifier(warehouseutils.WarehouseLoadFilesTable),
	)
	loadLocationRows, err := txn.QueryContext(ctx, stmt, pq.Array(stagingFileIDs))
	if err != nil {
		return fmt.Errorf("cannot delete load files with staging_file_id = %+v: %w", stagingFileIDs, err)
	}

	defer func() { _ = loadLocationRows.Close() }()

	if !hasUsedRudderStorage {
		return nil // no need to delete files in rudder storage
	}

	var loadLocations []string
	for loadLocationRows.Next() {
		var loc string
		if err = loadLocationRows.Scan(&loc); err != nil {
			return fmt.Errorf("cannot scan load file location: %w", err)
		}

		u, err := url.Parse(loc)
		if err != nil {
			return fmt.Errorf("cannot parse load file location %q: %w", loc, err)
		}

		loadLocations = append(loadLocations, u.Path[1:])
	}
	if err = loadLocationRows.Err(); err != nil {
		return fmt.Errorf("iterating load file locations: %w", err)
	}

	err = a.deleteFilesInStorage(ctx, loadLocations)
	if err != nil {
		return fmt.Errorf("error deleting files in storage: %w", err)
	}

	return nil
}
