package warehouse

import (
	"github.com/samber/lo"

	"golang.org/x/exp/slices"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func createTableUploads(job *UploadJob) (err error) {
	return job.initTableUploads()
}

func (job *UploadJob) initTableUploads() error {
	tables := lo.Map(lo.Keys(job.upload.UploadSchema), func(tableName string, index int) string {
		if job.includeIdentityMappingsTable(tableName) {
			return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMappingsTable)
		}
		return tableName
	})

	return job.tableUploadsRepo.Insert(
		job.ctx,
		job.upload.ID,
		lo.Uniq(tables),
	)
}

func (job *UploadJob) includeIdentityMappingsTable(table string) bool {
	return slices.Contains(warehouseutils.IdentityEnabledWarehouses, job.warehouse.Type) && table == warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable)
}
