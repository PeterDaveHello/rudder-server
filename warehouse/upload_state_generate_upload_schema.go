package warehouse

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func generateUploadSchema(ctx context.Context, job *UploadJob) error {
	return job.generateUploadSchema()
}

func (job *UploadJob) generateUploadSchema() error {
	if err := job.schemaHandle.prepareUploadSchema(
		job.ctx,
		job.stagingFiles,
	); err != nil {
		return fmt.Errorf("consolidate staging files schema using warehouse schema: %w", err)
	}

	if err := job.setUploadSchema(job.schemaHandle.uploadSchema); err != nil {
		return fmt.Errorf("set upload schema: %w", err)
	}

	return nil
}

func (job *UploadJob) setUploadSchema(consolidatedSchema model.Schema) error {
	marshalledSchema, err := json.Marshal(consolidatedSchema)
	if err != nil {
		return fmt.Errorf("marshal upload schema: %w", err)
	}

	job.upload.UploadSchema = consolidatedSchema
	return job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadSchemaField, Value: marshalledSchema}}})
}
