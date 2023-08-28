package logfield

import (
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	UploadJobID                = "uploadJobID"
	UploadStatus               = "uploadStatus"
	UseRudderStorage           = "useRudderStorage"
	SourceType                 = "sourceType"
	DestinationType            = "destinationType"
	DestinationRevisionID      = "destinationRevisionID"
	DestinationValidationsStep = "step"
	WorkspaceID                = "workspaceID"
	Namespace                  = "namespace"
	Schema                     = "schema"
	ColumnName                 = "columnName"
	ColumnType                 = "columnType"
	Priority                   = "priority"
	Retried                    = "retried"
	Attempt                    = "attempt"
	LoadFileType               = "loadFileType"
	ErrorMapping               = "errorMapping"
	DestinationCredsValid      = "destinationCredsValid"
	QueryExecutionTime         = "queryExecutionTime"
	StagingTableName           = "stagingTableName"
)

// TODO finish converting constants to functions to experiment

type Field[T any] func(v T) KeyValue[T]
type ErrField func(v error) KeyValue[error]

type KeyValue[T any] struct {
	k string
	v T
}

func (kv KeyValue[T]) key() string {
	return kv.k
}

func (kv KeyValue[T]) value() interface{} {
	return kv.v
}

type KVInterface interface {
	key() string
	value() interface{}
}

var (
	UploadID      Field[int64]  = func(v int64) KeyValue[int64] { return KeyValue[int64]{"uploadID", v} }
	SourceID      Field[string] = func(v string) KeyValue[string] { return KeyValue[string]{"sourceID", v} }
	DestinationID Field[string] = func(v string) KeyValue[string] { return KeyValue[string]{"destinationID", v} }
	TableName     Field[string] = func(v string) KeyValue[string] { return KeyValue[string]{"tableName", v} }
	Location      Field[string] = func(v string) KeyValue[string] { return KeyValue[string]{"location", v} }
	Query         Field[string] = func(v string) KeyValue[string] { return KeyValue[string]{"query", v} }
	Error         ErrField      = func(v error) KeyValue[error] {
		return KeyValue[error]{"error", v}
	}
)

// Generic should be used only when there is no other option and it doesn't make sense to create a new Field
func Generic[T any](k string) Field[T] {
	return func(v T) KeyValue[T] {
		return KeyValue[T]{k, v}
	}
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

func (l *Logger) Infow(msg string, kvs ...KVInterface) {
	l.init()
	l.logger.Infow(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Debugw(msg string, kvs ...KVInterface) {
	l.init()
	l.logger.Debugw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Errorw(msg string, kvs ...KVInterface) {
	l.init()
	l.logger.Errorw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Warnw(msg string, kvs ...KVInterface) {
	l.init()
	l.logger.Warnw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Fatalw(msg string, kvs ...KVInterface) {
	l.init()
	l.logger.Fatalw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) keyAndValues(kvs []KVInterface) []interface{} {
	length := len(kvs) * 2
	if l.component != "" {
		length += 2
	}
	s := make([]interface{}, 0, length)
	for _, kv := range kvs {
		s = append(s, kv.key(), kv.value())
	}
	if l.component != "" {
		s = append(s, "component", l.component)
	}
	return s
}
