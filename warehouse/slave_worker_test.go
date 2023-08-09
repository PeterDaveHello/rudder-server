package warehouse

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSlaveWorker(t *testing.T) {
	misc.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	minioResource, err := destination.SetupMINIO(pool, t)
	require.NoError(t, err)

	ctx := context.Background()

	destConf := map[string]interface{}{
		"bucketName":       minioResource.BucketName,
		"accessKeyID":      minioResource.AccessKey,
		"accessKey":        minioResource.AccessKey,
		"secretAccessKey":  minioResource.SecretKey,
		"endPoint":         minioResource.Endpoint,
		"forcePathStyle":   true,
		"s3ForcePathStyle": true,
		"disableSSL":       true,
		"region":           minioResource.SiteRegion,
		"enableSSE":        false,
		"bucketProvider":   "MINIO",
	}

	uploadFile := func(t *testing.T, destConf map[string]interface{}, filePath string) string {
		f, err := os.Open(filePath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: "MINIO",
			Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
				Provider: "MINIO",
				Config:   destConf,
			}),
		})

		uf, err := fm.Upload(ctx, f)
		require.NoError(t, err)

		return uf.ObjectName
	}

	t.Run("Upload Job", func(t *testing.T) {
		uploadJobLocation := uploadFile(t, destConf, "testdata/upload-job.staging.json.gz")

		stagingFile, err := os.Open("testdata/upload-job.staging.json.gz")
		require.NoError(t, err)

		reader, err := gzip.NewReader(stagingFile)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		scanner := bufio.NewScanner(reader)
		schemaMap := make(model.Schema)

		type event struct {
			Metadata struct {
				Table   string            `json:"table"`
				Columns map[string]string `json:"columns"`
			}
		}

		stagingEvents := make([]event, 0)

		for scanner.Scan() {
			lineBytes := scanner.Bytes()

			var stagingEvent event
			err := json.Unmarshal(lineBytes, &stagingEvent)
			require.NoError(t, err)

			stagingEvents = append(stagingEvents, stagingEvent)
		}

		for _, event := range stagingEvents {
			tableName := event.Metadata.Table

			if _, ok := schemaMap[tableName]; !ok {
				schemaMap[tableName] = make(model.TableSchema)
			}
			for columnName, columnType := range event.Metadata.Columns {
				if _, ok := schemaMap[tableName][columnName]; !ok {
					schemaMap[tableName][columnName] = columnType
				}
			}
		}

		t.Run("success", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			workerIdx := 1

			slaveWorker := newSlaveWorker(
				config.Default,
				logger.NOP,
				stats.Default,
				notifier,
				newBackendConfigManager(config.Default, nil, tenantManager, logger.NOP),
				newConstraintsManager(config.Default),
				workerIdx,
			)

			p := payload{
				UploadID:                     1,
				StagingFileID:                1,
				StagingFileLocation:          uploadJobLocation,
				UploadSchema:                 schemaMap,
				WorkspaceID:                  "test_workspace_id",
				SourceID:                     "test_source_id",
				SourceName:                   "test_source_name",
				DestinationID:                "test_destination_id",
				DestinationName:              "test_destination_name",
				DestinationType:              "test_destination_type",
				DestinationNamespace:         "test_destination_namespace",
				DestinationRevisionID:        uuid.New().String(),
				StagingDestinationRevisionID: uuid.New().String(),
				DestinationConfig:            destConf,
				StagingDestinationConfig:     map[string]interface{}{},
				UseRudderStorage:             false,
				StagingUseRudderStorage:      false,
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claim := pgnotifier.Claim{
				ID:        1,
				BatchID:   uuid.New().String(),
				Payload:   payloadJson,
				Status:    "waiting",
				Workspace: "test_workspace",
				Attempt:   0,
				JobType:   "upload",
			}

			go func() {
				slaveWorker.processClaimedUploadJob(ctx, claim, workerIdx)
			}()

			response := <-publishCh
			require.NoError(t, response.Err)

			var uploadPayload payload
			err = json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.BatchID)
			require.Equal(t, uploadPayload.UploadID, p.UploadID)
			require.Equal(t, uploadPayload.StagingFileID, p.StagingFileID)
			require.Equal(t, uploadPayload.StagingFileLocation, p.StagingFileLocation)
			require.Equal(t, uploadPayload.UploadSchema, p.UploadSchema)
			require.Equal(t, uploadPayload.WorkspaceID, p.WorkspaceID)
			require.Equal(t, uploadPayload.SourceID, p.SourceID)
			require.Equal(t, uploadPayload.SourceName, p.SourceName)
			require.Equal(t, uploadPayload.DestinationID, p.DestinationID)
			require.Equal(t, uploadPayload.DestinationName, p.DestinationName)
			require.Equal(t, uploadPayload.DestinationType, p.DestinationType)
			require.Equal(t, uploadPayload.DestinationNamespace, p.DestinationNamespace)
			require.Equal(t, uploadPayload.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, uploadPayload.StagingDestinationRevisionID, p.StagingDestinationRevisionID)
			require.Equal(t, uploadPayload.DestinationConfig, p.DestinationConfig)
			require.Equal(t, uploadPayload.StagingDestinationConfig, p.StagingDestinationConfig)
			require.Equal(t, uploadPayload.UseRudderStorage, p.UseRudderStorage)
			require.Equal(t, uploadPayload.StagingUseRudderStorage, p.StagingUseRudderStorage)
			require.Equal(t, uploadPayload.UniqueLoadGenID, p.UniqueLoadGenID)
			require.Equal(t, uploadPayload.RudderStoragePrefix, p.RudderStoragePrefix)
			require.Equal(t, uploadPayload.LoadFileType, p.LoadFileType)

			require.Len(t, uploadPayload.Output, 8)
			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 4)
				require.Equal(t, output.StagingFileID, p.StagingFileID)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)
			}
		})

		t.Run("clickhouse bool", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			workerIdx := 1

			slaveWorker := newSlaveWorker(
				config.Default,
				logger.NOP,
				stats.Default,
				notifier,
				newBackendConfigManager(config.Default, nil, tenantManager, logger.NOP),
				newConstraintsManager(config.Default),
				workerIdx,
			)

			p := payload{
				UploadID:                     1,
				StagingFileID:                1,
				StagingFileLocation:          uploadJobLocation,
				UploadSchema:                 schemaMap,
				WorkspaceID:                  "test_workspace_id",
				SourceID:                     "test_source_id",
				SourceName:                   "test_source_name",
				DestinationID:                "test_destination_id",
				DestinationName:              "test_destination_name",
				DestinationType:              warehouseutils.CLICKHOUSE,
				DestinationNamespace:         "test_destination_namespace",
				DestinationRevisionID:        uuid.New().String(),
				StagingDestinationRevisionID: uuid.New().String(),
				DestinationConfig:            destConf,
				StagingDestinationConfig:     map[string]interface{}{},
				UseRudderStorage:             false,
				StagingUseRudderStorage:      false,
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claim := pgnotifier.Claim{
				ID:        1,
				BatchID:   uuid.New().String(),
				Payload:   payloadJson,
				Status:    "waiting",
				Workspace: "test_workspace",
				Attempt:   0,
				JobType:   "upload",
			}

			go func() {
				slaveWorker.processClaimedUploadJob(ctx, claim, workerIdx)
			}()

			response := <-publishCh
			require.NoError(t, response.Err)

			var uploadPayload payload
			err = json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.BatchID)
			require.Equal(t, uploadPayload.UploadID, p.UploadID)
			require.Equal(t, uploadPayload.StagingFileID, p.StagingFileID)
			require.Equal(t, uploadPayload.StagingFileLocation, p.StagingFileLocation)

			require.Len(t, uploadPayload.Output, 8)
			for _, output := range uploadPayload.Output {
				require.Equal(t, output.TotalRows, 4)
				require.Equal(t, output.StagingFileID, p.StagingFileID)
				require.Equal(t, output.DestinationRevisionID, p.DestinationRevisionID)
				require.Equal(t, output.UseRudderStorage, p.StagingUseRudderStorage)

				fm, err := filemanager.New(&filemanager.Settings{
					Provider: "MINIO",
					Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
						Provider: "MINIO",
						Config:   destConf,
					}),
				})
				require.NoError(t, err)

				tmpFile, err := os.CreateTemp("", "load.csv")
				require.NoError(t, err)
				t.Cleanup(func() {
					os.Remove(tmpFile.Name())
				})

				objKey, err := fm.GetObjectNameFromLocation(output.Location)
				require.NoError(t, err)

				err = fm.Download(ctx, tmpFile, objKey)
				require.NoError(t, err)

				tmpFile, err = os.Open(tmpFile.Name())
				defer func() { _ = tmpFile.Close() }()

				gzReader, err := gzip.NewReader(tmpFile)
				require.NoError(t, err)

				reader := csv.NewReader(gzReader)
				reader.Comma = ','

				sortedColMap := p.sortedColumnMapForAllTables()

				for i := 0; i < output.TotalRows; i++ {
					row, err := reader.Read()
					require.NoError(t, err)

					sortedCols := sortedColMap[output.TableName]

					_, boolColIdx, _ := lo.FindIndexOf(sortedCols, func(item string) bool {
						return item == "test_boolean"
					})
					_, boolArrayColIdx, _ := lo.FindIndexOf(sortedCols, func(item string) bool {
						return item == "test_boolean_array"
					})
					require.Equal(t, row[boolColIdx], "1")
					require.Equal(t, row[boolArrayColIdx], "[1,0]")
				}
			}
		})

		t.Run("schema limit exceeded", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			workerIdx := 1

			slaveWorker := newSlaveWorker(
				config.Default,
				logger.NOP,
				stats.Default,
				notifier,
				newBackendConfigManager(config.Default, nil, tenantManager, logger.NOP),
				newConstraintsManager(config.Default),
				workerIdx,
			)

			columnCountLimitMap = map[string]int{
				warehouseutils.S3Datalake: 10,
			}

			p := payload{
				UploadID:                     1,
				StagingFileID:                1,
				StagingFileLocation:          uploadJobLocation,
				UploadSchema:                 schemaMap,
				WorkspaceID:                  "test_workspace_id",
				SourceID:                     "test_source_id",
				SourceName:                   "test_source_name",
				DestinationID:                "test_destination_id",
				DestinationName:              "test_destination_name",
				DestinationType:              warehouseutils.S3Datalake,
				DestinationNamespace:         "test_destination_namespace",
				DestinationRevisionID:        uuid.New().String(),
				StagingDestinationRevisionID: uuid.New().String(),
				DestinationConfig:            destConf,
				StagingDestinationConfig:     map[string]interface{}{},
				UseRudderStorage:             false,
				StagingUseRudderStorage:      false,
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claim := pgnotifier.Claim{
				ID:        1,
				BatchID:   uuid.New().String(),
				Payload:   payloadJson,
				Status:    "waiting",
				Workspace: "test_workspace",
				Attempt:   0,
				JobType:   "upload",
			}

			go func() {
				slaveWorker.processClaimedUploadJob(ctx, claim, workerIdx)
			}()

			response := <-publishCh
			require.EqualError(t, response.Err, "staging file schema limit exceeded for stagingFileID: 1, actualCount: 21")
		})

		t.Run("discards", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			workerIdx := 1

			slaveWorker := newSlaveWorker(
				config.Default,
				logger.NOP,
				stats.Default,
				notifier,
				newBackendConfigManager(config.Default, nil, tenantManager, logger.NOP),
				newConstraintsManager(config.Default),
				workerIdx,
			)

			p := payload{
				UploadID:            1,
				StagingFileID:       1,
				StagingFileLocation: uploadJobLocation,
				UploadSchema: map[string]model.TableSchema{
					"tracks": map[string]string{
						"id":                 "int",
						"user_id":            "int",
						"uuid_ts":            "timestamp",
						"received_at":        "timestamp",
						"original_timestamp": "timestamp",
						"timestamp":          "timestamp",
						"sent_at":            "timestamp",
						"event":              "string",
						"event_text":         "string",
					},
				},
				WorkspaceID:                  "test_workspace_id",
				SourceID:                     "test_source_id",
				SourceName:                   "test_source_name",
				DestinationID:                "test_destination_id",
				DestinationName:              "test_destination_name",
				DestinationType:              "test_destination_type",
				DestinationNamespace:         "test_destination_namespace",
				DestinationRevisionID:        uuid.New().String(),
				StagingDestinationRevisionID: uuid.New().String(),
				DestinationConfig:            destConf,
				StagingDestinationConfig:     map[string]interface{}{},
				UseRudderStorage:             false,
				StagingUseRudderStorage:      false,
				UniqueLoadGenID:              uuid.New().String(),
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				LoadFileType:                 "csv",
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claim := pgnotifier.Claim{
				ID:        1,
				BatchID:   uuid.New().String(),
				Payload:   payloadJson,
				Status:    "waiting",
				Workspace: "test_workspace",
				Attempt:   0,
				JobType:   "upload",
			}

			go func() {
				slaveWorker.processClaimedUploadJob(ctx, claim, workerIdx)
			}()

			response := <-publishCh
			require.NoError(t, response.Err)

			var uploadPayload payload
			err = json.Unmarshal(response.Payload, &uploadPayload)
			require.NoError(t, err)
			require.Equal(t, uploadPayload.BatchID, claim.BatchID)
			require.Equal(t, uploadPayload.UploadID, p.UploadID)
			require.Equal(t, uploadPayload.StagingFileID, p.StagingFileID)
			require.Equal(t, uploadPayload.StagingFileLocation, p.StagingFileLocation)
			require.Equal(t, uploadPayload.UploadSchema, p.UploadSchema)
			require.Equal(t, uploadPayload.WorkspaceID, p.WorkspaceID)
			require.Equal(t, uploadPayload.SourceID, p.SourceID)
			require.Equal(t, uploadPayload.SourceName, p.SourceName)
			require.Equal(t, uploadPayload.DestinationID, p.DestinationID)
			require.Equal(t, uploadPayload.DestinationName, p.DestinationName)
			require.Equal(t, uploadPayload.DestinationType, p.DestinationType)
			require.Equal(t, uploadPayload.DestinationNamespace, p.DestinationNamespace)
			require.Equal(t, uploadPayload.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, uploadPayload.StagingDestinationRevisionID, p.StagingDestinationRevisionID)
			require.Equal(t, uploadPayload.DestinationConfig, p.DestinationConfig)
			require.Equal(t, uploadPayload.StagingDestinationConfig, p.StagingDestinationConfig)
			require.Equal(t, uploadPayload.UseRudderStorage, p.UseRudderStorage)
			require.Equal(t, uploadPayload.StagingUseRudderStorage, p.StagingUseRudderStorage)
			require.Equal(t, uploadPayload.UniqueLoadGenID, p.UniqueLoadGenID)
			require.Equal(t, uploadPayload.RudderStoragePrefix, p.RudderStoragePrefix)
			require.Equal(t, uploadPayload.LoadFileType, p.LoadFileType)

			require.Len(t, uploadPayload.Output, 9)

			discardsOutput, ok := lo.Find(uploadPayload.Output, func(o uploadResult) bool {
				return o.TableName == warehouseutils.DiscardsTable
			})
			require.True(t, ok)
			require.Equal(t, discardsOutput.TotalRows, 24)
			require.Equal(t, discardsOutput.StagingFileID, p.StagingFileID)
			require.Equal(t, discardsOutput.DestinationRevisionID, p.DestinationRevisionID)
			require.Equal(t, discardsOutput.UseRudderStorage, p.StagingUseRudderStorage)
		})
	})

	t.Run("async job", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		_, err = pgResource.DB.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS test_namespace;")
		require.NoError(t, err)
		_, err = pgResource.DB.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_namespace.test_table_name (id int, user_id int, uuid_ts timestamp, received_at timestamp, original_timestamp timestamp, timestamp timestamp, sent_at timestamp, event text, event_text text, context_sources_job_run_id text, context_sources_task_run_id text, context_source_id text, context_destination_id text);")
		require.NoError(t, err)

		workspaceID := "test_workspace_id"
		sourceID := "test_source_id"
		destinationID := "test_destination_id"
		workerIdx := 1

		ctrl := gomock.NewController(t)
		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					workspaceID: {
						WorkspaceID: workspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      sourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      destinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											Name: warehouseutils.POSTGRES,
										},
										Config: map[string]interface{}{
											"host":             pgResource.Host,
											"database":         pgResource.Database,
											"user":             pgResource.User,
											"password":         pgResource.Password,
											"port":             pgResource.Port,
											"sslMode":          "disable",
											"namespace":        "test_namespace",
											"useSSL":           false,
											"syncFrequency":    "30",
											"useRudderStorage": false,
											"bucketName":       minioResource.BucketName,
											"accessKeyID":      minioResource.AccessKey,
											"accessKey":        minioResource.AccessKey,
											"secretAccessKey":  minioResource.SecretKey,
											"endPoint":         minioResource.Endpoint,
											"forcePathStyle":   true,
											"s3ForcePathStyle": true,
											"disableSSL":       true,
											"region":           minioResource.SiteRegion,
											"enableSSE":        false,
											"bucketProvider":   "MINIO",
										},
									},
								},
							},
						},
					},
				},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		tenantManager := &multitenant.Manager{
			BackendConfig: mockBackendConfig,
		}
		bcm := newBackendConfigManager(config.Default, nil, tenantManager, logger.NOP)
		go func() {
			bcm.Start(ctx)
		}()

		<-bcm.initialConfigFetched

		t.Run("success", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			c := config.New()
			c.Set("Warehouse.postgres.enableDeleteByJobs", true)

			slaveWorker := newSlaveWorker(
				c,
				logger.NOP,
				stats.Default,
				notifier,
				bcm,
				newConstraintsManager(config.Default),
				workerIdx,
			)

			p := jobs.AsyncJobPayload{
				Id:            "1",
				SourceID:      sourceID,
				DestinationID: destinationID,
				TableName:     "test_table_name",
				WorkspaceID:   workspaceID,
				AsyncJobType:  "deletebyjobrunid",
				MetaData:      []byte(`{"job_run_id": "1", "task_run_id": "1", "start_time": "2020-01-01T00:00:00Z"}`),
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claim := pgnotifier.Claim{
				ID:        1,
				BatchID:   uuid.New().String(),
				Payload:   payloadJson,
				Status:    "waiting",
				Workspace: "test_workspace",
				Attempt:   0,
				JobType:   "async_job",
			}

			go func() {
				slaveWorker.processClaimedAsyncJob(ctx, claim)
			}()

			response := <-publishCh
			require.NoError(t, response.Err)

			var asyncResult asyncJobRunResult
			err = json.Unmarshal(response.Payload, &asyncResult)
			require.NoError(t, err)

			require.Equal(t, "1", asyncResult.ID)
			require.True(t, asyncResult.Result)
		})

		t.Run("invalid job type", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			c := config.New()
			c.Set("Warehouse.postgres.enableDeleteByJobs", true)

			slaveWorker := newSlaveWorker(
				c,
				logger.NOP,
				stats.Default,
				notifier,
				bcm,
				newConstraintsManager(config.Default),
				workerIdx,
			)

			p := jobs.AsyncJobPayload{
				Id:            "1",
				SourceID:      sourceID,
				DestinationID: destinationID,
				TableName:     "test_table_name",
				WorkspaceID:   workspaceID,
				AsyncJobType:  "invalid_job_type",
				MetaData:      []byte(`{"job_run_id": "1", "task_run_id": "1", "start_time": "2020-01-01T00:00:00Z"}`),
			}

			payloadJson, err := json.Marshal(p)
			require.NoError(t, err)

			claim := pgnotifier.Claim{
				ID:        1,
				BatchID:   uuid.New().String(),
				Payload:   payloadJson,
				Status:    "waiting",
				Workspace: "test_workspace",
				Attempt:   0,
				JobType:   "async_job",
			}

			go func() {
				slaveWorker.processClaimedAsyncJob(ctx, claim)
			}()

			response := <-publishCh
			require.EqualError(t, response.Err, "invalid AsyncJobType")
		})

		t.Run("invalid configuration", func(t *testing.T) {
			publishCh := make(chan *pgnotifier.ClaimResponse)
			defer close(publishCh)

			notifier := &mockSlaveNotifier{
				publishCh: publishCh,
			}

			c := config.New()
			c.Set("Warehouse.postgres.enableDeleteByJobs", true)

			slaveWorker := newSlaveWorker(
				c,
				logger.NOP,
				stats.Default,
				notifier,
				bcm,
				newConstraintsManager(config.Default),
				workerIdx,
			)

			testCases := []struct {
				name          string
				sourceID      string
				destinationID string
				expectedError error
			}{
				{
					name:          "invalid parameters",
					expectedError: errors.New("invalid Parameters"),
				},
				{
					name:          "invalid source id",
					sourceID:      "invalid_source_id",
					destinationID: destinationID,
					expectedError: errors.New("invalid Source Id"),
				},
				{
					name:          "invalid destination id",
					sourceID:      sourceID,
					destinationID: "invalid_destination_id",
					expectedError: errors.New("invalid Destination Id"),
				},
			}

			for _, tc := range testCases {
				tc := tc

				t.Run(tc.name, func(t *testing.T) {
					p := jobs.AsyncJobPayload{
						Id:            "1",
						SourceID:      tc.sourceID,
						DestinationID: tc.destinationID,
						TableName:     "test_table_name",
						WorkspaceID:   workspaceID,
						AsyncJobType:  "deletebyjobrunid",
						MetaData:      []byte(`{"job_run_id": "1", "task_run_id": "1", "start_time": "2020-01-01T00:00:00Z"}`),
					}

					payloadJson, err := json.Marshal(p)
					require.NoError(t, err)

					claim := pgnotifier.Claim{
						ID:        1,
						BatchID:   uuid.New().String(),
						Payload:   payloadJson,
						Status:    "waiting",
						Workspace: "test_workspace",
						Attempt:   0,
						JobType:   "async_job",
					}

					go func() {
						slaveWorker.processClaimedAsyncJob(ctx, claim)
					}()

					response := <-publishCh
					require.EqualError(t, response.Err, tc.expectedError.Error())
				})
			}
		})
	})
}
