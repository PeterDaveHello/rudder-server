package warehouse

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSlaveJobPayload(t *testing.T) {
	t.Run("pickup staging configuration", func(t *testing.T) {
		inputs := []struct {
			job      *payload
			expected bool
		}{
			{
				job:      &payload{},
				expected: false,
			},
			{
				job: &payload{
					StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
					DestinationRevisionID:        "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				},
				expected: false,
			},
			{
				job: &payload{
					StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
					DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
				},
				expected: false,
			},
			{
				job: &payload{
					StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
					DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
					StagingDestinationConfig:     map[string]string{},
				},
				expected: true,
			},
		}
		for _, input := range inputs {
			got := input.job.pickupStagingConfiguration()
			require.Equal(t, got, input.expected)
		}
	})

	t.Run("sorted columns map", func(t *testing.T) {
		p := &payload{}
		p.UploadSchema = make(model.Schema)
		p.UploadSchema["a"] = model.TableSchema{
			"0": "0",
			"1": "1",
		}
		p.UploadSchema["b"] = model.TableSchema{
			"2": "2",
			"3": "3",
		}

		require.Equal(t, p.sortedColumnMapForAllTables(), map[string][]string{
			"a": {"0", "1"},
			"b": {"2", "3"},
		})
	})
}

type mockLoadFileWriter struct {
	file *os.File
	data []string
}

func (m *mockLoadFileWriter) WriteGZ(s string) error {
	m.data = append(m.data, strings.Trim(s, "\n"))
	return nil
}

func (m *mockLoadFileWriter) Write(p []byte) (int, error) {
	m.data = append(m.data, string(p))
	return len(p), nil
}

func (m *mockLoadFileWriter) WriteRow(r []interface{}) error {
	return errors.New("not implemented")
}

func (*mockLoadFileWriter) Close() error {
	return nil
}

func (m *mockLoadFileWriter) GetLoadFile() *os.File {
	return m.file
}

func TestSlaveJob(t *testing.T) {
	misc.Init()

	var (
		provider         = "MINIO"
		workspaceID      = "test-workspace-id"
		destinationID    = "test-destination-id"
		destinationName  = "test-destination-name"
		sourceID         = "test-source-id"
		sourceName       = "test-source-name"
		destType         = "POSTGRES"
		worker           = 7
		prefix           = warehouseutils.DatalakeTimeWindowFormat
		namespace        = "test-namespace"
		loadObjectFolder = "test-load-object-folder"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("download staging file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "staging.dump")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, os.Remove(f.Name())) })

		_, err = f.WriteString("RudderStack")
		require.NoError(t, err)
		require.NoError(t, f.Close())

		minioResource, err := destination.SetupMINIO(pool, t)
		require.NoError(t, err)

		conf := map[string]interface{}{
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
			"bucketProvider":   provider,
		}

		p := payload{
			WorkspaceID:     workspaceID,
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationName: destinationName,
			DestinationType: destType,
		}

		fm, err := p.fileManager(conf, false)
		require.NoError(t, err)

		ctx := context.Background()

		uf, err := fm.Upload(ctx, f)
		require.NoError(t, err)

		now := time.Now()

		t.Run("download", func(t *testing.T) {
			jr := newJobRun(p, config.Default, logger.NOP, stats.Default)
			jr.job.StagingFileLocation = uf.ObjectName
			jr.job.DestinationConfig = conf
			jr.now = func() time.Time {
				return now
			}

			defer jr.cleanup()

			jr.stagingFilePath, err = jr.getStagingFilePath(1)
			require.NoError(t, err)
			require.Contains(t, jr.stagingFilePath, "rudder-warehouse-json-uploads-tmp/_1/POSTGRES_test-destination-id/staging.dump")

			err = jr.downloadStagingFile(ctx)
			require.NoError(t, err)
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			jr := newJobRun(p, config.Default, logger.NOP, stats.Default)
			jr.job.StagingFileLocation = uf.ObjectName
			jr.job.DestinationConfig = conf
			jr.now = func() time.Time {
				return now
			}

			defer jr.cleanup()

			jr.stagingFilePath, err = jr.getStagingFilePath(1)
			require.NoError(t, err)
			require.Contains(t, jr.stagingFilePath, "rudder-warehouse-json-uploads-tmp/_1/POSTGRES_test-destination-id/staging.dump")

			err = jr.downloadStagingFile(ctx)
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("download twice succeeded", func(t *testing.T) {
			p := payload{
				WorkspaceID:     workspaceID,
				SourceID:        sourceID,
				DestinationID:   destinationID,
				DestinationName: destinationName,
				DestinationType: destType,
			}

			jr := newJobRun(p, config.Default, logger.NOP, stats.Default)
			jr.job.StagingFileLocation = uf.ObjectName
			jr.job.DestinationRevisionID = uuid.New().String()
			jr.job.DestinationConfig = map[string]interface{}{}
			jr.job.StagingDestinationRevisionID = uuid.New().String()
			jr.job.StagingDestinationConfig = conf
			jr.now = func() time.Time {
				return now
			}

			defer jr.cleanup()

			jr.stagingFilePath, err = jr.getStagingFilePath(1)
			require.NoError(t, err)
			require.Contains(t, jr.stagingFilePath, "rudder-warehouse-json-uploads-tmp/_1/POSTGRES_test-destination-id/staging.dump")

			err = jr.downloadStagingFile(ctx)
			require.NoError(t, err)
		})

		t.Run("download twice failed", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			jr := newJobRun(p, config.Default, logger.NOP, stats.Default)
			jr.job.StagingFileLocation = uf.ObjectName
			jr.job.DestinationConfig = conf
			jr.job.DestinationRevisionID = uuid.New().String()
			jr.job.StagingDestinationRevisionID = uuid.New().String()
			jr.job.StagingDestinationConfig = conf
			jr.now = func() time.Time {
				return now
			}

			defer jr.cleanup()

			jr.stagingFilePath, err = jr.getStagingFilePath(1)
			require.NoError(t, err)
			require.Contains(t, jr.stagingFilePath, "rudder-warehouse-json-uploads-tmp/_1/POSTGRES_test-destination-id/staging.dump")

			err = jr.downloadStagingFile(ctx)
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("writer and reader", func(t *testing.T) {
		const (
			table = "test_table"
			lines = 100
		)

		p := payload{
			WorkspaceID:     workspaceID,
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationName: destinationName,
			DestinationType: destType,
		}

		jr := newJobRun(p, config.Default, logger.NOP, stats.Default)
		jr.outputFileWritersMap = make(map[string]encoding.LoadFileWriter)
		jr.tableEventCountMap = make(map[string]int)
		jr.now = func() time.Time {
			return time.Now()
		}

		defer jr.cleanup()

		t.Run("writer", func(t *testing.T) {
			writer, err := jr.writer(table)
			require.NoError(t, err)

			jr.stagingFilePath = writer.GetLoadFile().Name()

			for i := 0; i < lines; i++ {
				_, err = writer.Write([]byte(fmt.Sprintf("test %d\n", i)))
				require.NoError(t, err)
			}
			require.NoError(t, writer.Close())
		})

		t.Run("reader", func(t *testing.T) {
			jr.stagingFileReader, err = jr.reader()
			require.NoError(t, err)

			scanner := bufio.NewScanner(jr.stagingFileReader)
			scanner.Split(bufio.ScanLines)

			for i := 0; i < lines; i++ {
				require.True(t, scanner.Scan())
				require.Equal(t, fmt.Sprintf("test %d", i), scanner.Text())
			}
		})
	})

	t.Run("discards", func(t *testing.T) {
		discardWriter := &mockLoadFileWriter{}

		p := payload{
			DestinationType: warehouseutils.RS,
			LoadFileType:    warehouseutils.LoadFileTypeCsv,
		}

		now := time.Date(2020, 4, 27, 20, 0, 0, 0, time.UTC)

		jr := newJobRun(p, config.Default, logger.NOP, stats.Default)
		jr.uuidTS = now
		jr.now = func() time.Time {
			return now
		}

		for _, column := range []string{"test_discard_column", "uuid_ts", "loaded_at"} {
			err = jr.handleDiscardTypes("test_table", "loaded_at", column,
				map[string]interface{}{
					"id":          "test_id",
					"received_at": now,
				},
				&constraintsViolation{},
				discardWriter,
			)
			require.NoError(t, err)
		}

		err = jr.handleDiscardTypes("test_table", "loaded_at", "test_constrains",
			map[string]interface{}{},
			&constraintsViolation{
				isViolated:         true,
				violatedIdentifier: "test_violated_identifier",
			},
			discardWriter,
		)
		require.NoError(t, err)
		require.Equal(t, discardWriter.data, []string{
			"loaded_at,test_discard_column,2020-04-27 20:00:00 +0000 UTC,test_id,test_table,2020-04-27T20:00:00.000Z",
			"loaded_at,uuid_ts,2020-04-27 20:00:00 +0000 UTC,test_id,test_table,2020-04-27T20:00:00.000Z",
			"loaded_at,loaded_at,2020-04-27 20:00:00 +0000 UTC,test_id,test_table,2020-04-27T20:00:00.000Z",
			"loaded_at,test_constrains,2020-04-27T20:00:00.000Z,test_violated_identifier,test_table,2020-04-27T20:00:00.000Z",
		})

		t.Log(discardWriter.data)
	})

	t.Run("upload load files", func(t *testing.T) {
		ctxCancel, cancel := context.WithCancel(context.Background())
		cancel()

		testCases := []struct {
			name              string
			destType          string
			ctx               context.Context
			conf              map[string]any
			additionalWriters int
			wantError         error
		}{
			{
				name:              "Parquet file",
				additionalWriters: 9,
				destType:          warehouseutils.S3Datalake,
			},
			{
				name: "Few files",
			},
			{
				name:              "many files",
				additionalWriters: 49,
			},
			{
				name:      "Context cancelled",
				ctx:       ctxCancel,
				wantError: errors.New("uploading load file to object storage: context canceled"),
			},
			{
				name: "Unknown provider",
				conf: map[string]any{
					"bucketProvider": "UNKNOWN",
				},
				wantError: errors.New("creating uploader: service provider not supported: UNKNOWN"),
			},
			{
				name: "Invalid endpoint",
				conf: map[string]any{
					"endPoint": "http://localhost:1234",
				},
				wantError:         errors.New("uploading load file to object storage: uploading load file: Endpoint url cannot have fully qualified paths."),
				additionalWriters: 9,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				minioResource, err := destination.SetupMINIO(pool, t)
				require.NoError(t, err)

				conf := map[string]any{
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
					"bucketProvider":   provider,
				}

				for k, v := range tc.conf {
					conf[k] = v
				}

				f, err := os.CreateTemp(t.TempDir(), "load.dump")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, os.Remove(f.Name())) })

				m := &mockLoadFileWriter{
					file: f,
				}

				writerMap := map[string]encoding.LoadFileWriter{
					"test": m,
				}
				for i := 0; i < tc.additionalWriters; i++ {
					writerMap[fmt.Sprintf("test-%d", i)] = m
				}

				store := memstats.New()
				stagingFileID := int64(1001)

				destType := destType
				if tc.destType != "" {
					destType = tc.destType
				}

				job := payload{
					StagingFileID:            stagingFileID,
					DestinationConfig:        conf,
					UseRudderStorage:         false,
					StagingDestinationConfig: conf,
					StagingUseRudderStorage:  false,
					WorkspaceID:              workspaceID,
					DestinationID:            destinationID,
					DestinationName:          destinationName,
					SourceID:                 sourceID,
					SourceName:               sourceName,
					DestinationType:          destType,
					LoadFilePrefix:           prefix,
					UniqueLoadGenID:          uuid.New().String(),
					DestinationNamespace:     namespace,
				}
				c := config.New()
				c.Set("Warehouse.numLoadFileUploadWorkers", worker)
				c.Set("Warehouse.slaveUploadTimeout", "5m")
				c.Set("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", loadObjectFolder)

				jr := newJobRun(job, c, logger.NOP, store)
				jr.since = func(t time.Time) time.Duration {
					return time.Second
				}
				jr.outputFileWritersMap = writerMap

				ctx := context.Background()
				if tc.ctx != nil {
					ctx = tc.ctx
				}

				loadFile, err := jr.uploadLoadFiles(ctx)
				if tc.wantError != nil {
					require.EqualError(t, err, tc.wantError.Error())
					return
				}

				require.NoError(t, err)
				require.Len(t, loadFile, len(jr.outputFileWritersMap))
				require.EqualValues(t, time.Second*time.Duration(len(jr.outputFileWritersMap)), store.Get("load_file_total_upload_time", jr.buildTags()).LastDuration())
				for i := 0; i < len(jr.outputFileWritersMap); i++ {
					require.EqualValues(t, time.Second, store.Get("load_file_upload_time", jr.buildTags()).LastDuration())
				}

				outputPathRegex := fmt.Sprintf(`http://localhost:%s/testbucket/%s/test.*/%s/.*/load.dump`, minioResource.Port, loadObjectFolder, sourceID)
				if slices.Contains(warehouseutils.TimeWindowDestinations, destType) {
					outputPathRegex = fmt.Sprintf(`http://localhost:%s/testbucket/rudder-datalake/%s/test.*/2006/01/02/15/load.dump`, minioResource.Port, namespace)
				}

				for _, f := range loadFile {
					require.Regexp(t, outputPathRegex, f.Location)
					require.Equal(t, f.StagingFileID, stagingFileID)
				}
			})
		}
	})
}
