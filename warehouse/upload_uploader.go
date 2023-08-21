package warehouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (job *UploadJob) GetLoadFilesMetadata(ctx context.Context, options warehouseutils.GetLoadFilesOptions) (loadFiles []warehouseutils.LoadFile) {
	var tableFilterSQL string
	if options.Table != "" {
		tableFilterSQL = fmt.Sprintf(` AND table_name='%s'`, options.Table)
	}

	var limitSQL string
	if options.Limit != 0 {
		limitSQL = fmt.Sprintf(`LIMIT %d`, options.Limit)
	}

	sqlStatement := fmt.Sprintf(`
		WITH row_numbered_load_files as (
		  SELECT
			location,
			metadata,
			row_number() OVER (
			  PARTITION BY staging_file_id,
			  table_name
			  ORDER BY
				id DESC
			) AS row_number
		  FROM
			%[1]s
		  WHERE
			staging_file_id IN (%[2]v) %[3]s
		)
		SELECT
		  location,
		  metadata
		FROM
		  row_numbered_load_files
		WHERE
		  row_number = 1
		%[4]s;
`,
		warehouseutils.WarehouseLoadFilesTable,
		misc.IntArrayToString(job.stagingFileIDs, ","),
		tableFilterSQL,
		limitSQL,
	)

	job.logger.Debugf(`Fetching loadFileLocations: %v`, sqlStatement)
	rows, err := job.dbHandle.QueryContext(ctx, sqlStatement)
	if err != nil {
		panic(fmt.Errorf("query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var location string
		var metadata json.RawMessage
		err := rows.Scan(&location, &metadata)
		if err != nil {
			panic(fmt.Errorf("failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		loadFiles = append(loadFiles, warehouseutils.LoadFile{
			Location: location,
			Metadata: metadata,
		})
	}
	if err = rows.Err(); err != nil {
		panic(fmt.Errorf("iterate query results: %s\nwith Error : %w", sqlStatement, err))
	}
	return
}

func (job *UploadJob) GetSampleLoadFileLocation(ctx context.Context, tableName string) (location string, err error) {
	locations := job.GetLoadFilesMetadata(ctx, warehouseutils.GetLoadFilesOptions{Table: tableName, Limit: 1})
	if len(locations) == 0 {
		return "", fmt.Errorf(`no load file found for table:%s`, tableName)
	}
	return locations[0].Location, nil
}

func (job *UploadJob) GetSchemaInWarehouse() (schema model.Schema) {
	if job.schemaHandle == nil {
		return
	}
	return job.schemaHandle.schemaInWarehouse
}

func (job *UploadJob) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	return job.schemaHandle.schemaInWarehouse[tableName]
}

func (job *UploadJob) GetTableSchemaInUpload(tableName string) model.TableSchema {
	return job.schemaHandle.uploadSchema[tableName]
}

func (job *UploadJob) GetSingleLoadFile(ctx context.Context, tableName string) (warehouseutils.LoadFile, error) {
	var (
		tableUpload model.TableUpload
		err         error
	)

	if tableUpload, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, tableName); err != nil {
		return warehouseutils.LoadFile{}, fmt.Errorf("get single load file: %w", err)
	}

	return warehouseutils.LoadFile{Location: tableUpload.Location}, err
}

func (job *UploadJob) ShouldOnDedupUseNewRecord() bool {
	category := job.warehouse.Source.SourceDefinition.Category
	return category == singerProtocolSourceCategory || category == cloudSourceCategory
}

func (job *UploadJob) UseRudderStorage() bool {
	return job.upload.UseRudderStorage
}

func (job *UploadJob) GetLoadFileGenStartTIme() time.Time {
	if !job.loadFileGenStartTime.IsZero() {
		return job.loadFileGenStartTime
	}
	return model.GetLoadFileGenTime(job.upload.Timings)
}

func (job *UploadJob) GetLoadFileType() string {
	return job.upload.LoadFileType
}

func (job *UploadJob) GetFirstLastEvent() (time.Time, time.Time) {
	return job.upload.FirstEventAt, job.upload.LastEventAt
}

func (job *UploadJob) GetLocalSchema(ctx context.Context) (model.Schema, error) {
	return job.schemaHandle.getLocalSchema(ctx)
}

func (job *UploadJob) UpdateLocalSchema(ctx context.Context, schema model.Schema) error {
	return job.schemaHandle.updateLocalSchema(ctx, job.upload.ID, schema)
}
