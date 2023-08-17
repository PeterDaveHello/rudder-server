package validations

import (
	"context"
	"encoding/json"
	"fmt"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	namespace = "rudderstack_setup_test"
	table     = "setup_test_staging"
)

var (
	tableSchemaMap model.TableSchema
	alterColumnMap model.TableSchema
	payloadMap     map[string]interface{}
)

func init() {
	tableSchemaMap = model.TableSchema{
		"id":  "int",
		"val": "string",
	}
	payloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	alterColumnMap = model.TableSchema{
		"val_alter": "string",
	}
}

type Manager struct {
	conf               *config.Config
	logger             logger.Logger
	fileManagerFactory filemanager.Factory

	config struct {
		objectStorageValidationTimeout time.Duration
		connectionTestingFolder        string
	}
}

func NewManager(conf *config.Config, logger logger.Logger, fileManagerFactory filemanager.Factory) *Manager {
	m := &Manager{
		conf:               conf,
		logger:             logger,
		fileManagerFactory: fileManagerFactory,
	}

	m.config.objectStorageValidationTimeout = conf.GetDuration("Warehouse.Validations.ObjectStorageTimeoutInSec", 15, time.Second)
	m.config.connectionTestingFolder = conf.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)

	return m
}

// Validate the destination by running all the validation steps
func (m *Manager) Validate(ctx context.Context, req *model.ValidationRequest) (json.RawMessage, error) {
	switch req.Path {
	case "validate":
		return json.Marshal(m.validateDestination(ctx, req.Destination, req.Step))
	case "steps":
		return json.Marshal(m.stepsToValidate(req.Destination))
	default:
		return nil, fmt.Errorf("invalid path: %s", req.Path)
	}
}

func (m *Manager) ValidateAllSteps(ctx context.Context, dest *backendconfig.DestinationT) *model.DestinationValidationResponse {
	return m.validateDestination(ctx, dest, "")
}
