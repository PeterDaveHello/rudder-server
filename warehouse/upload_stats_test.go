package warehouse

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/mock_stats"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"

	"github.com/golang/mock/gomock"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestUploadJob_Stats(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	sqlStatement, err := os.ReadFile("testdata/sql/stats_test.sql")
	require.NoError(t, err)

	_, err = pgResource.DB.Exec(string(sqlStatement))
	require.NoError(t, err)

	t.Run("Generate upload success metrics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStats := mock_stats.NewMockStats(ctrl)
		mockMeasurement := mock_stats.NewMockMeasurement(ctrl)

		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(mockMeasurement)
		mockMeasurement.EXPECT().Count(4).Times(2)
		mockMeasurement.EXPECT().Count(1).Times(1)

		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: mockStats,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
				SourceID:           "test-sourceID",
				DestinationID:      "test-destinationID",
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		job.generateUploadSuccessMetrics()
	})

	t.Run("Generate upload aborted metrics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStats := mock_stats.NewMockStats(ctrl)
		mockMeasurement := mock_stats.NewMockMeasurement(ctrl)

		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(mockMeasurement)
		mockMeasurement.EXPECT().Count(4).Times(2)

		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: mockStats,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
				SourceID:           "test-sourceID",
				DestinationID:      "test-destinationID",
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		job.generateUploadAbortedMetrics()
	})

	t.Run("Record table load", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStats := mock_stats.NewMockStats(ctrl)
		mockMeasurement := mock_stats.NewMockMeasurement(ctrl)

		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(mockMeasurement)
		mockMeasurement.EXPECT().Count(4).Times(2)
		mockMeasurement.EXPECT().Since(gomock.Any()).Times(1)

		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: mockStats,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
				WorkspaceID:        "workspaceID",
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		job.recordTableLoad("tracks", 4)
	})

	t.Run("Record load files generation time", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockStats := mock_stats.NewMockStats(ctrl)
		mockMeasurement := mock_stats.NewMockMeasurement(ctrl)

		mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(mockMeasurement)
		mockMeasurement.EXPECT().SendTiming(gomock.Any()).Times(1)

		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: mockStats,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		err = job.recordLoadFileGenerationTimeStat(1, 4)
		require.NoError(t, err)
	})
}

func TestUploadJob_MatchRows(t *testing.T) {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		destinationName = "test-destinationName"
		namespace       = "test-namespace"
		destinationType = "POSTGRES"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	sqlStatement, err := os.ReadFile("testdata/sql/upload_test.sql")
	require.NoError(t, err)

	_, err = pgResource.DB.Exec(string(sqlStatement))
	require.NoError(t, err)

	t.Run("Total rows in load files", func(t *testing.T) {
		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: stats.Default,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				DestinationID:      destinationID,
				SourceID:           sourceID,
				StagingFileStartID: 1,
				StagingFileEndID:   5,
				Namespace:          namespace,
			},
			Warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
			StagingFiles: []*model.StagingFile{
				{ID: 1},
				{ID: 2},
				{ID: 3},
				{ID: 4},
				{ID: 5},
			},
		}, nil)

		count := job.getTotalRowsInLoadFiles(context.Background())
		require.EqualValues(t, 5, count)
	})

	t.Run("Total rows in staging files", func(t *testing.T) {
		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: stats.Default,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				DestinationID:      destinationID,
				SourceID:           sourceID,
				StagingFileStartID: 1,
				StagingFileEndID:   5,
				Namespace:          namespace,
			},
			Warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
			StagingFiles: []*model.StagingFile{
				{ID: 1},
				{ID: 2},
				{ID: 3},
				{ID: 4},
				{ID: 5},
			},
		}, nil)

		count, err := repo.NewStagingFiles(sqlmiddleware.New(pgResource.DB)).TotalEventsForUpload(context.Background(), job.upload)
		require.NoError(t, err)
		require.EqualValues(t, 5, count)
	})

	t.Run("Get uploads timings", func(t *testing.T) {
		ujf := &UploadJobFactory{
			conf:         config.Default,
			logger:       logger.NOP,
			statsFactory: stats.Default,
			dbHandle:     sqlmiddleware.New(pgResource.DB),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				DestinationID:      destinationID,
				SourceID:           sourceID,
				StagingFileStartID: 1,
				StagingFileEndID:   5,
				Namespace:          namespace,
			},
			Warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
			StagingFiles: []*model.StagingFile{
				{ID: 1},
				{ID: 2},
				{ID: 3},
				{ID: 4},
				{ID: 5},
			},
		}, nil)

		exportedData, err := time.Parse(time.RFC3339, "2020-04-21T15:26:34.344356Z")
		require.NoError(t, err)

		exportingData, err := time.Parse(time.RFC3339, "2020-04-21T15:16:19.687716Z")
		require.NoError(t, err)

		timings, err := repo.NewUploads(job.dbHandle).UploadTimings(context.Background(), job.upload.ID)
		require.NoError(t, err)
		require.EqualValues(t, timings, model.Timings{
			{
				"exported_data":  exportedData,
				"exporting_data": exportingData,
			},
		})
	})

	t.Run("Staging files and load files events match", func(t *testing.T) {
		testCases := []struct {
			name        string
			stagingFile []*model.StagingFile
			statsCount  int
		}{
			{
				name: "In case of no mismatch",
				stagingFile: []*model.StagingFile{
					{ID: 1},
					{ID: 2},
					{ID: 3},
					{ID: 4},
					{ID: 5},
				},
			},
			{
				name: "In case of mismatch",
				stagingFile: []*model.StagingFile{
					{ID: 1},
					{ID: 2},
				},
				statsCount: 1,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				mockStats := mock_stats.NewMockStats(ctrl)
				mockMeasurement := mock_stats.NewMockMeasurement(ctrl)
				mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(mockMeasurement)
				mockMeasurement.EXPECT().Gauge(gomock.Any()).Times(tc.statsCount)

				ujf := &UploadJobFactory{
					conf:         config.Default,
					logger:       logger.NOP,
					statsFactory: mockStats,
					dbHandle:     sqlmiddleware.New(pgResource.DB),
				}
				job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
					Upload: model.Upload{
						ID:                 1,
						DestinationID:      destinationID,
						SourceID:           sourceID,
						StagingFileStartID: 1,
						StagingFileEndID:   5,
						Namespace:          namespace,
					},
					Warehouse: model.Warehouse{
						Type: destinationType,
						Destination: backendconfig.DestinationT{
							ID:   destinationID,
							Name: destinationName,
						},
						Source: backendconfig.SourceT{
							ID:   sourceID,
							Name: destinationName,
						},
					},
					StagingFiles: tc.stagingFile,
				}, nil)

				err := job.matchRowsInStagingAndLoadFiles(context.Background())
				require.NoError(t, err)
			})
		}
	})
}
