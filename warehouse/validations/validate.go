package validations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type dummyUploader struct {
	dest *backendconfig.DestinationT
}

func (*dummyUploader) GetSchemaInWarehouse() model.Schema { return model.Schema{} }
func (*dummyUploader) GetLocalSchema(context.Context) (model.Schema, error) {
	return model.Schema{}, nil
}
func (*dummyUploader) UpdateLocalSchema(context.Context, model.Schema) error { return nil }
func (*dummyUploader) ShouldOnDedupUseNewRecord() bool                       { return false }
func (*dummyUploader) GetFirstLastEvent() (time.Time, time.Time)             { return time.Time{}, time.Time{} }
func (*dummyUploader) GetLoadFileGenStartTIme() time.Time                    { return time.Time{} }
func (*dummyUploader) GetSampleLoadFileLocation(context.Context, string) (string, error) {
	return "", nil
}

func (*dummyUploader) GetTableSchemaInWarehouse(string) model.TableSchema {
	return model.TableSchema{}
}

func (*dummyUploader) GetTableSchemaInUpload(string) model.TableSchema {
	return model.TableSchema{}
}

func (*dummyUploader) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
	return []warehouseutils.LoadFile{}
}

func (*dummyUploader) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

func (m *dummyUploader) GetLoadFileType() string {
	return warehouseutils.GetLoadFileType(m.dest.DestinationDefinition.Name)
}

func (m *dummyUploader) UseRudderStorage() bool {
	return misc.IsConfiguredToUseRudderObjectStorage(m.dest.Config)
}

func (m *Manager) validateDestination(ctx context.Context, dest *backendconfig.DestinationT, stepToValidate string) *model.DestinationValidationResponse {
	var (
		destID          = dest.ID
		destType        = dest.DestinationDefinition.Name
		stepsToValidate []*model.Step
		err             error
	)

	m.logger.Infow("validate destination configuration",
		logfield.DestinationID, destID,
		logfield.DestinationType, destType,
		logfield.DestinationRevisionID, dest.RevisionID,
		logfield.WorkspaceID, dest.WorkspaceID,
		logfield.DestinationValidationsStep, stepToValidate,
	)

	// check if req has specified a step in query params
	if stepToValidate != "" {
		stepI, err := strconv.Atoi(stepToValidate)
		if err != nil {
			return &model.DestinationValidationResponse{
				Error: fmt.Sprintf("Invalid step: %s", stepToValidate),
			}
		}

		// get validation step
		var vs *model.Step
		for _, s := range m.stepsToValidate(dest).Steps {
			if s.ID == stepI {
				vs = s
				break
			}
		}

		if vs == nil {
			return &model.DestinationValidationResponse{
				Error: fmt.Sprintf("Invalid step: %s", stepToValidate),
			}
		}

		stepsToValidate = append(stepsToValidate, vs)
	} else {
		stepsToValidate = append(stepsToValidate, m.stepsToValidate(dest).Steps...)
	}

	// Iterate over all selected steps and validate
	for _, step := range stepsToValidate {
		switch step.Name {
		case model.VerifyingObjectStorage:
			err = m.validateObjectStorage(ctx, dest)
		case model.VerifyingConnections:
			err = m.validateConnections(ctx, dest)
		case model.VerifyingCreateSchema:
			err = m.validateCreateSchema(ctx, dest)
		case model.VerifyingCreateAndAlterTable:
			err = m.validateCreateAlterTabl(ctx, dest)
		case model.VerifyingFetchSchema:
			err = m.validateFetchSchema(ctx, dest)
		case model.VerifyingLoadTable:
			err = m.validateLoadTable(ctx, dest)
		default:
			err = fmt.Errorf("invalid step: %s", step.Name)
		}

		if err != nil {
			step.Error = err.Error()

			m.logger.Warnw("not able to validate destination configuration",
				logfield.DestinationID, destID,
				logfield.DestinationType, destType,
				logfield.DestinationRevisionID, dest.RevisionID,
				logfield.WorkspaceID, dest.WorkspaceID,
				logfield.DestinationValidationsStep, step.Name,
				logfield.Error, step.Error,
			)
			break
		} else {
			step.Success = true
		}
	}

	res := &model.DestinationValidationResponse{
		Steps:   stepsToValidate,
		Success: err == nil,
	}
	if err != nil {
		res.Error = err.Error()
	}

	return res
}

func (m *Manager) validateObjectStorage(ctx context.Context, dest *backendconfig.DestinationT) error {
	tempPath, err := m.CreateTempLoadFile(dest)
	if err != nil {
		return fmt.Errorf("creating temp load file: %w", err)
	}

	uploadObject, err := m.uploadFile(ctx, dest, tempPath)
	if err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	err = m.downloadFile(ctx, dest, uploadObject.ObjectName)
	if err != nil {
		return fmt.Errorf("download file: %w", err)
	}

	return nil
}

func (m *Manager) uploadFile(ctx context.Context, dest *backendconfig.DestinationT, filePath string) (filemanager.UploadedFile, error) {
	var (
		err        error
		output     filemanager.UploadedFile
		fm         filemanager.FileManager
		uploadFile *os.File

		destinationType = dest.DestinationDefinition.Name
		prefixes        = []string{m.config.connectionTestingFolder, destinationType, warehouseutils.RandHex(), time.Now().Format("01-02-2006")}
	)

	if fm, err = m.createFileManager(dest); err != nil {
		return filemanager.UploadedFile{}, err
	}

	if uploadFile, err = os.Open(filePath); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("opening file: %w", err)
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer func() { _ = uploadFile.Close() }()

	if output, err = fm.Upload(ctx, uploadFile, prefixes...); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("uploading file: %w", err)
	}

	return output, nil
}

func (m *Manager) createFileManager(dest *backendconfig.DestinationT) (filemanager.FileManager, error) {
	var (
		destType = dest.DestinationDefinition.Name
		conf     = dest.Config
		provider = warehouseutils.ObjectStorageType(destType, conf, misc.IsConfiguredToUseRudderObjectStorage(conf))
	)

	fileManager, err := m.fileManagerFactory(&filemanager.Settings{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           conf,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(conf),
			WorkspaceID:      dest.WorkspaceID,
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("creating file manager: %w", err)
	}

	fileManager.SetTimeout(m.config.objectStorageValidationTimeout)

	return fileManager, nil
}

func (m *Manager) downloadFile(ctx context.Context, dest *backendconfig.DestinationT, location string) error {
	var (
		err          error
		fm           filemanager.FileManager
		downloadFile *os.File
		tmpDirPath   string
		filePath     string

		destinationType = dest.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)
	)

	if fm, err = m.createFileManager(dest); err != nil {
		return err
	}

	if tmpDirPath, err = misc.CreateTMPDIR(); err != nil {
		return fmt.Errorf("create tmp dir: %w", err)
	}

	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		m.config.connectionTestingFolder,
		destinationType,
		warehouseutils.RandHex(),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(loadFileType),
	)
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	if downloadFile, err = os.Create(filePath); err != nil {
		return fmt.Errorf("creating file: %w", err)
	}

	// cleanup
	defer misc.RemoveFilePaths(filePath)
	defer func() { _ = downloadFile.Close() }()

	if err = fm.Download(ctx, downloadFile, location); err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}
	return nil
}

func (m *Manager) validateConnections(ctx context.Context, dest *backendconfig.DestinationT) error {
	mgr, err := m.createManager(ctx, dest)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	defer mgr.Cleanup(ctx)

	ctx, cancel := context.WithTimeout(ctx, warehouseutils.TestConnectionTimeout)
	defer cancel()

	return mgr.TestConnection(ctx, m.createDummyWarehouse(dest))
}

func (m *Manager) createManager(ctx context.Context, dest *backendconfig.DestinationT) (manager.WarehouseOperations, error) {
	var (
		destType  = dest.DestinationDefinition.Name
		warehouse = m.createDummyWarehouse(dest)

		operations manager.WarehouseOperations
		err        error
	)

	if operations, err = manager.NewWarehouseOperations(destType, config.Default, m.logger, stats.Default); err != nil {
		return nil, fmt.Errorf("getting manager: %w", err)
	}

	operations.SetConnectionTimeout(warehouseutils.TestConnectionTimeout)

	if err = operations.Setup(ctx, warehouse, &dummyUploader{
		dest: dest,
	}); err != nil {
		return nil, fmt.Errorf("setting up manager: %w", err)
	}

	return operations, nil
}

func (m *Manager) createDummyWarehouse(dest *backendconfig.DestinationT) model.Warehouse {
	var (
		destType  = dest.DestinationDefinition.Name
		namespace = m.configuredNamespaceInDestination(dest)
	)

	return model.Warehouse{
		WorkspaceID: dest.WorkspaceID,
		Destination: *dest,
		Namespace:   namespace,
		Type:        destType,
	}
}

func (m *Manager) configuredNamespaceInDestination(dest *backendconfig.DestinationT) string {
	var (
		destType = dest.DestinationDefinition.Name
		conf     = dest.Config
	)

	if destType == warehouseutils.CLICKHOUSE {
		return conf["database"].(string)
	}

	if conf["namespace"] != nil {
		namespace := conf["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
}

func (m *Manager) validateCreateSchema(ctx context.Context, dest *backendconfig.DestinationT) error {
	mgr, err := m.createManager(ctx, dest)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	defer mgr.Cleanup(ctx)

	return mgr.CreateSchema(ctx)
}

func (m *Manager) validateCreateAlterTabl(ctx context.Context, dest *backendconfig.DestinationT) error {
	mgr, err := m.createManager(ctx, dest)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	defer mgr.Cleanup(ctx)

	table := m.getTable(dest)

	if err := mgr.CreateTable(ctx, table, tableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	defer func() { _ = mgr.DropTable(ctx, table) }()

	for columnName, columnType := range alterColumnMap {
		if err := mgr.AddColumns(ctx, table, []warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}}); err != nil {
			return fmt.Errorf("alter table: %w", err)
		}
	}

	return nil
}

func (m *Manager) getTable(dest *backendconfig.DestinationT) string {
	destType := dest.DestinationDefinition.Name
	conf := dest.Config

	if destType == warehouseutils.DELTALAKE {
		enableExternalLocation, _ := conf["enableExternalLocation"].(bool)
		externalLocation, _ := conf["externalLocation"].(string)
		if enableExternalLocation && externalLocation != "" {
			return m.tableWithUUID()
		}
	}

	return table
}

func (m *Manager) tableWithUUID() string {
	return table + "_" + warehouseutils.RandHex()
}

func (m *Manager) validateFetchSchema(ctx context.Context, dest *backendconfig.DestinationT) error {
	mgr, err := m.createManager(ctx, dest)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	defer mgr.Cleanup(ctx)

	if _, _, err := mgr.FetchSchema(ctx); err != nil {
		return fmt.Errorf("fetch schema: %w", err)
	}
	return nil
}

func (m *Manager) validateLoadTable(ctx context.Context, dest *backendconfig.DestinationT) error {
	mgr, err := m.createManager(ctx, dest)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	defer mgr.Cleanup(ctx)

	table := m.getTable(dest)

	tempPath, err := m.CreateTempLoadFile(dest)
	if err != nil {
		return fmt.Errorf("create temp load file: %w", err)
	}

	uploadOutput, err := m.uploadFile(ctx, dest, tempPath)
	if err != nil {
		return fmt.Errorf("upload file: %w", err)
	}

	if err := mgr.CreateTable(ctx, table, tableSchemaMap); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	defer func() { _ = mgr.DropTable(ctx, table) }()

	err = mgr.LoadTestTable(ctx, uploadOutput.Location, table, payloadMap, warehouseutils.GetLoadFileType(dest.DestinationDefinition.Name))
	if err != nil {
		return fmt.Errorf("load test table: %w", err)
	}

	return nil
}

func (m *Manager) CreateTempLoadFile(dest *backendconfig.DestinationT) (string, error) {
	var (
		tmpDirPath string
		filePath   string
		err        error
		writer     encoding.LoadFileWriter

		destinationType = dest.DestinationDefinition.Name
		loadFileType    = warehouseutils.GetLoadFileType(destinationType)
	)

	if tmpDirPath, err = misc.CreateTMPDIR(); err != nil {
		return "", fmt.Errorf("create tmp dir: %w", err)
	}

	filePath = fmt.Sprintf("%v/%v/%v.%v.%v.%v",
		tmpDirPath,
		m.config.connectionTestingFolder,
		destinationType,
		warehouseutils.RandHex(),
		time.Now().Unix(),
		warehouseutils.GetLoadFileFormat(loadFileType),
	)
	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}

	if loadFileType == warehouseutils.LoadFileTypeParquet {
		writer, err = encoding.CreateParquetWriter(tableSchemaMap, filePath, destinationType)
	} else {
		writer, err = misc.CreateGZ(filePath)
	}
	if err != nil {
		return "", fmt.Errorf("creating writer for file: %s with error: %w", filePath, err)
	}

	eventLoader := encoding.GetNewEventLoader(destinationType, loadFileType, writer)
	for _, column := range []string{"id", "val"} {
		eventLoader.AddColumn(column, tableSchemaMap[column], payloadMap[column])
	}

	if err = eventLoader.Write(); err != nil {
		return "", fmt.Errorf("writing to file: %w", err)
	}

	if err = writer.Close(); err != nil {
		return "", fmt.Errorf("closing writer: %w", err)
	}

	return filePath, nil
}
