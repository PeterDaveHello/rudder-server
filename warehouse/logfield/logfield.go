package logfield

import (
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

//const (
//	UploadJobID                = "uploadJobID"
//	UploadStatus               = "uploadStatus"
//	UseRudderStorage           = "useRudderStorage"
//	SourceType                 = "sourceType"
//	DestinationType            = "destinationType"
//	DestinationRevisionID      = "destinationRevisionID"
//	DestinationValidationsStep = "step"
//	WorkspaceID                = "workspaceID"
//	Namespace                  = "namespace"
//	Schema                     = "schema"
//	ColumnName                 = "columnName"
//	ColumnType                 = "columnType"
//	Priority                   = "priority"
//	Retried                    = "retried"
//	Attempt                    = "attempt"
//	LoadFileType               = "loadFileType"
//	ErrorMapping               = "errorMapping"
//	DestinationCredsValid      = "destinationCredsValid"
//	QueryExecutionTime         = "queryExecutionTime"
//	StagingTableName           = "stagingTableName"
//)

type Fields struct {
	SourceType            string
	DestinationType       string
	DestinationRevisionID string
	WorkspaceID           string
	Namespace             string
	Warehouse             struct {
		UploadJobID      string
		UploadStatus     string
		UseRudderStorage string
	}
	// TODO add more fields here
}

func (f *Fields) keyAndValues() []interface{} {
	var kvs []interface{}
	if f.SourceType != "" {
		kvs = append(kvs, "sourceType", f.SourceType)
	}
	if f.DestinationType != "" {
		kvs = append(kvs, "destinationType", f.DestinationType)
	}
	if f.DestinationRevisionID != "" {
		kvs = append(kvs, "destinationRevisionID", f.DestinationRevisionID)
	}
	if f.WorkspaceID != "" {
		kvs = append(kvs, "workspaceID", f.WorkspaceID)
	}
	if f.Namespace != "" {
		kvs = append(kvs, "namespace", f.Namespace)
	}
	if f.Warehouse.UploadJobID != "" {
		kvs = append(kvs, "uploadJobID", f.Warehouse.UploadJobID)
	}
	if f.Warehouse.UploadStatus != "" {
		kvs = append(kvs, "uploadStatus", f.Warehouse.UploadStatus)
	}
	if f.Warehouse.UseRudderStorage != "" {
		kvs = append(kvs, "useRudderStorage", f.Warehouse.UseRudderStorage)
	}
	return kvs
}

func NewLogger(component string, l logger.Logger) Logger {
	return Logger{component: component, logger: l}
}

type Logger struct {
	component string
	logger    logger.Logger
	once      sync.Once
}

func (l *Logger) init() {
	l.once.Do(func() {
		if l.logger == nil {
			l.logger = logger.NOP
		}
	})
}

func (l *Logger) Infow(msg string, fields Fields) {
	l.init()
	l.logger.Infow(msg, fields)
}

func (l *Logger) Debugw(msg string, fields Fields) {
	l.init()
	l.logger.Debugw(msg, fields)
}

func (l *Logger) Errorw(msg string, fields Fields) {
	l.init()
	l.logger.Errorw(msg, fields)
}

func (l *Logger) Warnw(msg string, fields Fields) {
	l.init()
	l.logger.Warnw(msg, fields)
}

func (l *Logger) Fatalw(msg string, fields Fields) {
	l.init()
	l.logger.Fatalw(msg, fields)
}
