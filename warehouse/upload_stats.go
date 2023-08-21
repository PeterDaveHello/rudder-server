package warehouse

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const moduleName = "warehouse"

func warehouseTagName(destID, sourceName, destName, sourceID string) string {
	return misc.GetTagName(destID, sourceName, destName, misc.TailTruncateStr(sourceID, 6))
}

func (job *UploadJob) buildTags(extraTags ...warehouseutils.Tag) stats.Tags {
	tags := stats.Tags{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
		"workspaceId": job.upload.WorkspaceID,
		"destID":      job.upload.DestinationID,
		"sourceID":    job.upload.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return tags
}

func (job *UploadJob) timerStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	return job.statsFactory.NewTaggedStat(name, stats.TimerType, job.buildTags(extraTags...))
}

func (job *UploadJob) counterStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	return job.statsFactory.NewTaggedStat(name, stats.CountType, job.buildTags(extraTags...))
}

func (job *UploadJob) guageStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	extraTags = append(extraTags, warehouseutils.Tag{
		Name:  "sourceCategory",
		Value: job.warehouse.Source.SourceDefinition.Category,
	})
	return job.statsFactory.NewTaggedStat(name, stats.GaugeType, job.buildTags(extraTags...))
}

func (job *UploadJob) generateUploadAbortedMetrics() {
	var (
		numUploadedEvents int64
		numStagedEvents   int64
		err               error
	)
	numUploadedEvents, err = job.tableUploadsRepo.TotalExportedEvents(
		job.ctx,
		job.upload.ID,
		[]string{},
	)
	if err != nil {
		job.logger.Warnw("sum of total exported events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	numStagedEvents, err = repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(
		job.ctx,
		job.upload,
	)
	if err != nil {
		job.logger.Warnw("total events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	job.stats.totalRowsSynced.Count(int(numUploadedEvents))
	job.stats.numStagedEvents.Count(int(numStagedEvents))
}

func (job *UploadJob) recordLoadFileGenerationTimeStat(startID, endID int64) (err error) {
	stmt := fmt.Sprintf(`SELECT EXTRACT(EPOCH FROM (f2.created_at - f1.created_at))::integer as delta
		FROM (SELECT created_at FROM %[1]s WHERE id=%[2]d) f1
		CROSS JOIN
		(SELECT created_at FROM %[1]s WHERE id=%[3]d) f2
	`, warehouseutils.WarehouseLoadFilesTable, startID, endID)
	var timeTakenInS time.Duration
	err = job.dbHandle.QueryRowContext(job.ctx, stmt).Scan(&timeTakenInS)
	if err != nil {
		job.logger.Errorf("[WH]: Failed to generate load file generation time stat: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}

	job.stats.loadFileGenerationTime.SendTiming(timeTakenInS * time.Second)
	return nil
}

func persistSSLFileErrorStat(workspaceID, destType, destName, destID, sourceName, sourceID, errTag string) {
	tags := stats.Tags{
		"workspaceId":   workspaceID,
		"module":        moduleName,
		"destType":      destType,
		"warehouseID":   warehouseTagName(destID, sourceName, destName, sourceID),
		"destinationID": destID,
		"errTag":        errTag,
	}
	stats.Default.NewTaggedStat("persist_ssl_file_failure", stats.CountType, tags).Count(1)
}
