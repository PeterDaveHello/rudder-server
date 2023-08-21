package warehouse

import "context"

func createRemoteSchema(ctx context.Context, job *UploadJob) (err error) {
	if len(job.schemaHandle.schemaInWarehouse) != 0 {
		return
	}
	return job.whManager.CreateSchema(ctx)
}
