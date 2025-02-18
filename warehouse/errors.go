package warehouse

import (
	"fmt"
	"strings"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type InvalidDestinationCredErr struct {
	Base      error
	Operation string
}

func (err InvalidDestinationCredErr) Error() string {
	return fmt.Sprintf("Invalid destination creds, failed for operation: %s with err: \n%s", err.Operation, err.Base.Error())
}

type ErrorHandler struct {
	Manager manager.Manager
}

// MatchErrorMappings matches the error with the error mappings defined in the integrations
// and returns the corresponding joins of the matched error type
// else returns UnknownError
func (e *ErrorHandler) MatchErrorMappings(err error) warehouseutils.Tag {
	if e.Manager == nil || err == nil {
		return warehouseutils.Tag{Name: "error_mapping", Value: string(model.Noop)}
	}

	var (
		errMappings []string
		errString   = err.Error()
	)

	for _, em := range e.Manager.ErrorMappings() {
		if em.Format.MatchString(errString) {
			errMappings = append(errMappings, string(em.Type))
		}
	}

	if len(errMappings) > 0 {
		return warehouseutils.Tag{Name: "error_mapping", Value: strings.Join(errMappings, ",")}
	}
	return warehouseutils.Tag{Name: "error_mapping", Value: string(model.UnknownError)}
}
