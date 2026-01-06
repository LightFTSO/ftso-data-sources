package rpcmanager

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"roselabs.mx/ftso-data-sources/config"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/helpers"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

// -----------------------------------------------------------------------------
// RPC Data Structures
// -----------------------------------------------------------------------------

type DataSourceArgs struct {
	Options datasource.DataSourceOptions
}

type DataSourceReply struct {
	Message string
}

type Asset struct {
	AssetName string
	Category  string
}

type AssetsArgs struct {
	Assets []Asset
}

type RenameAssetArgs struct {
	AssetName string
	NewName   string
	Category  string
}

type AssetReply struct {
	Message string
}

type CurrentAssetsReply struct {
	Assets config.AssetConfig
}

type ShutdownReply struct {
	Message string
}

// -----------------------------------------------------------------------------
// RPC Manager
// -----------------------------------------------------------------------------

type RPCManager struct {
	GlobalConfig  config.ConfigOptions
	DataSources   map[string]datasource.FtsoDataSource
	CurrentAssets config.AssetConfig
	TickerTopic   *tickertopic.TickerTopic

	Mu sync.Mutex
	Wg *sync.WaitGroup
}

// -----------------------------------------------------------------------------
// Data Source Management
// -----------------------------------------------------------------------------

// InitDataSources initializes all data sources defined in the config.
// It is safe to call this concurrently as it acquires the lock.
func (m *RPCManager) InitDataSources() error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	return m.initDataSourcesLocked()
}

// initDataSourcesLocked is the internal logic assuming lock is held.
func (m *RPCManager) initDataSourcesLocked() error {
	enabledDataSources := m.GlobalConfig.Datasources

	if len(enabledDataSources) < 1 {
		if m.GlobalConfig.Env != "development" {
			return errors.New("no data sources defined in configuration")
		}
		slog.Warn("No data sources enabled (Development Mode)")
		return nil
	}

	allSymbols := symbols.GetAllSymbols(
		m.CurrentAssets.Crypto,
		m.CurrentAssets.Commodities,
		m.CurrentAssets.Forex,
		m.CurrentAssets.Stocks,
	)

	// 1. Build Data Sources
	for _, sourceOpt := range enabledDataSources {
		// We pass m.Wg to the datasource. The datasource is responsible for
		// calling Add(1) when it connects and Done() when it disconnects/closes.
		src, err := datasource.BuildDataSource(sourceOpt, allSymbols, m.TickerTopic, m.Wg)
		if err != nil {
			slog.Error("Failed to build data source", "source", sourceOpt.Source, "error", err)
			continue
		}
		m.DataSources[src.GetName()] = src
	}

	// 2. Connect Data Sources
	for _, ds := range m.DataSources {
		// Launch connection in background.
		// Note: The DataSource implementation is responsible for managing its own
		// Goroutine lifecycle and WaitGroup usage.
		go func(d datasource.FtsoDataSource) {
			if err := d.Connect(); err != nil {
				slog.Error("Data source connection failed", "source", d.GetName(), "error", err)
			}
		}(ds)
	}

	return nil
}

func (m *RPCManager) ReloadDataSources(args struct{}, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	slog.Info("Reloading all data sources...")

	// 1. Close existing
	for name, ds := range m.DataSources {
		if err := ds.Close(); err != nil {
			slog.Warn("Error closing data source during reload", "source", name, "error", err)
		}
		delete(m.DataSources, name)
	}

	// 2. Re-initialize
	if err := m.initDataSourcesLocked(); err != nil {
		return err
	}

	reply.Message = "All data sources reloaded successfully"
	return nil
}

func (m *RPCManager) TurnOnDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	ds, exists := m.DataSources[name]
	if !exists {
		return fmt.Errorf("data source '%s' not found", name)
	}

	if ds.IsRunning() {
		reply.Message = "Data source already running"
		return nil
	}

	go func() {
		if err := ds.Connect(); err != nil {
			slog.Error("Failed to turn on data source", "source", name, "error", err)
		}
	}()

	reply.Message = fmt.Sprintf("Data source '%s' turned on", name)
	return nil
}

func (m *RPCManager) TurnOffDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	ds, exists := m.DataSources[name]
	if !exists {
		return fmt.Errorf("data source '%s' not found", name)
	}

	if !ds.IsRunning() {
		reply.Message = "Data source already stopped"
		return nil
	}

	if err := ds.Close(); err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Data source '%s' turned off", name)
	return nil
}

func (m *RPCManager) AddDataSource(args DataSourceArgs, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	name := args.Options.Source
	if _, exists := m.DataSources[name]; exists {
		return errors.New("data source already exists")
	}

	allSymbols := symbols.GetAllSymbols(
		m.CurrentAssets.Crypto,
		m.CurrentAssets.Commodities,
		m.CurrentAssets.Forex,
		m.CurrentAssets.Stocks,
	)

	src, err := datasource.BuildDataSource(args.Options, allSymbols, m.TickerTopic, m.Wg)
	if err != nil {
		return err
	}

	m.DataSources[name] = src

	go func() {
		if err := src.Connect(); err != nil {
			slog.Error("Failed to connect new data source", "source", name, "error", err)
		}
	}()

	reply.Message = fmt.Sprintf("Data source '%s' added and started", name)
	return nil
}

func (m *RPCManager) RemoveDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	ds, exists := m.DataSources[name]
	if !exists {
		return fmt.Errorf("data source '%s' not found", name)
	}

	if err := ds.Close(); err != nil {
		return err
	}

	delete(m.DataSources, name)
	reply.Message = fmt.Sprintf("Data source '%s' removed", name)
	return nil
}

// -----------------------------------------------------------------------------
// Asset Management
// -----------------------------------------------------------------------------

func (m *RPCManager) AddAsset(args AssetsArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	modified := false
	for _, a := range args.Assets {
		assetsPtr, err := m.getAssetSlicePtr(a.Category)
		if err != nil {
			return err
		}

		assetName := strings.ToUpper(a.AssetName)
		if helpers.ItemInSlice(assetName, *assetsPtr) {
			continue // Skip duplicates
		}

		*assetsPtr = append(*assetsPtr, assetName)
		modified = true
	}

	if !modified {
		reply.Message = "No new assets added (duplicates or invalid)"
		return nil
	}

	// Update global config reference
	m.updateAssetConfig()

	// Persist to file
	config.UpdateConfig(m.GlobalConfig)

	// Hot Reload
	if err := m.reloadDataSourcesLocked(); err != nil {
		return err
	}

	reply.Message = "Assets added and data sources reloaded"
	slog.Info("Assets added via RPC", "assets", args.Assets)
	return nil
}

func (m *RPCManager) RemoveAsset(args AssetsArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	modified := false
	for _, a := range args.Assets {
		assetsPtr, err := m.getAssetSlicePtr(a.Category)
		if err != nil {
			return err
		}

		assetName := strings.ToUpper(a.AssetName)
		if !helpers.ItemInSlice(assetName, *assetsPtr) {
			continue
		}

		*assetsPtr = helpers.RemoveFromSlice(*assetsPtr, assetName)
		modified = true
	}

	if !modified {
		reply.Message = "No assets removed (not found)"
		return nil
	}

	m.updateAssetConfig()
	config.UpdateConfig(m.GlobalConfig)

	if err := m.reloadDataSourcesLocked(); err != nil {
		return err
	}

	reply.Message = "Assets removed and data sources reloaded"
	slog.Info("Assets removed via RPC", "assets", args.Assets)
	return nil
}

func (m *RPCManager) RenameAsset(args RenameAssetArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	assetsPtr, err := m.getAssetSlicePtr(args.Category)
	if err != nil {
		return err
	}

	oldName := strings.ToUpper(args.AssetName)
	newName := strings.ToUpper(args.NewName)

	if !helpers.ItemInSlice(oldName, *assetsPtr) {
		return fmt.Errorf("asset '%s' does not exist", oldName)
	}
	if helpers.ItemInSlice(newName, *assetsPtr) {
		return fmt.Errorf("new name '%s' already exists", newName)
	}

	// Perform Rename
	*assetsPtr = helpers.RemoveFromSlice(*assetsPtr, oldName)
	*assetsPtr = append(*assetsPtr, newName)

	m.updateAssetConfig()
	config.UpdateConfig(m.GlobalConfig)

	if err := m.reloadDataSourcesLocked(); err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Renamed '%s' to '%s'", oldName, newName)
	slog.Info("Asset renamed via RPC", "old", oldName, "new", newName)
	return nil
}

func (m *RPCManager) GetAssets(args struct{}, reply *CurrentAssetsReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	reply.Assets = m.CurrentAssets
	return nil
}

// -----------------------------------------------------------------------------
// System
// -----------------------------------------------------------------------------

// Shutdown handles the RPC command to shut down
func (m *RPCManager) Shutdown(args struct{}, reply *ShutdownReply) error {
	// Re-use the CloseAll logic
	m.CloseAll()
	reply.Message = "Shutdown initiated"
	return nil
}

func (m *RPCManager) CloseAll() {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	slog.Info("Stopping all data sources...")
	for name, ds := range m.DataSources {
		// Log at INFO so we know exactly what is happening
		slog.Info("Stopping data source", "source", name)

		if err := ds.Close(); err != nil {
			// Filter out "not running" noise if desired, or just log it
			if err.Error() != "datasource is not running" {
				slog.Warn("Error stopping data source", "source", name, "error", err)
			} else {
				slog.Debug("Data source was already stopped", "source", name)
			}
		}
	}
	// Clear the map after closing
	m.DataSources = make(map[string]datasource.FtsoDataSource)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func (m *RPCManager) updateAssetConfig() {
	m.GlobalConfig.Assets = m.CurrentAssets
}

func (m *RPCManager) getAssetSlicePtr(category string) (*[]string, error) {
	switch strings.ToLower(category) {
	case "crypto":
		return &m.CurrentAssets.Crypto, nil
	case "commodities":
		return &m.CurrentAssets.Commodities, nil
	case "forex":
		return &m.CurrentAssets.Forex, nil
	case "stocks":
		return &m.CurrentAssets.Stocks, nil
	default:
		return nil, fmt.Errorf("unknown asset category: %s", category)
	}
}

// reloadDataSourcesLocked reloads without acquiring the lock again
func (m *RPCManager) reloadDataSourcesLocked() error {
	for name, ds := range m.DataSources {
		_ = ds.Close()
		delete(m.DataSources, name)
	}
	return m.initDataSourcesLocked()
}
