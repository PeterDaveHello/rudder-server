package warehouse

import (
	"context"
	"fmt"
)

func updateTableUploads(ctx context.Context, job *UploadJob) (err error) {
	for tableName := range job.upload.UploadSchema {
		if err = job.tableUploadsRepo.PopulateTotalEventsFromStagingFileIDs(
			job.ctx,
			job.upload.ID,
			tableName,
			job.stagingFileIDs,
		); err != nil {
			return fmt.Errorf("populate table uploads total events from staging file: %w", err)
		}
	}
	return nil
}
