package validations

import (
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (m *Manager) stepsToValidate(dest *backendconfig.DestinationT) *model.StepsResponse {
	var (
		destType = dest.DestinationDefinition.Name
		steps    []*model.Step
	)

	steps = []*model.Step{{
		ID:   len(steps) + 1,
		Name: model.VerifyingObjectStorage,
	}}

	switch destType {
	case warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
	case warehouseutils.S3Datalake:
		wh := m.createDummyWarehouse(dest)
		if canUseGlue := schemarepository.UseGlue(&wh); !canUseGlue {
			break
		}

		steps = append(steps,
			&model.Step{
				ID:   len(steps) + 1,
				Name: model.VerifyingCreateSchema,
			},
			&model.Step{
				ID:   len(steps) + 2,
				Name: model.VerifyingCreateAndAlterTable,
			},
			&model.Step{
				ID:   len(steps) + 3,
				Name: model.VerifyingFetchSchema,
			},
		)
	default:
		steps = append(steps,
			&model.Step{
				ID:   len(steps) + 1,
				Name: model.VerifyingConnections,
			},
			&model.Step{
				ID:   len(steps) + 2,
				Name: model.VerifyingCreateSchema,
			},
			&model.Step{
				ID:   len(steps) + 3,
				Name: model.VerifyingCreateAndAlterTable,
			},
			&model.Step{
				ID:   len(steps) + 4,
				Name: model.VerifyingFetchSchema,
			},
			&model.Step{
				ID:   len(steps) + 5,
				Name: model.VerifyingLoadTable,
			},
		)
	}
	return &model.StepsResponse{
		Steps: steps,
	}
}
