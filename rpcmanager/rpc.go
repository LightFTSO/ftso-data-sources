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

// RPC Arguments and Reply Structures
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

// RPCManager handles RPC operations for data sources and assets
type RPCManager struct {
	GlobalConfig  config.ConfigOptions
	DataSources   map[string]datasource.FtsoDataSource
	CurrentAssets config.AssetConfig
	TickerTopic   *tickertopic.TickerTopic

	Mu sync.Mutex
	Wg sync.WaitGroup
}

// TurnOnDataSource activates an existing data source
func (m *RPCManager) TurnOnDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	ds, exists := m.DataSources[name]
	if !exists {
		return errors.New("data source not found")
	}

	if ds.IsRunning() {
		reply.Message = "Data source already running"
		return nil
	}

	m.Wg.Add(1)
	go func() {
		defer m.Wg.Done()
		if err := ds.Connect(); err != nil {
			slog.Error("Data source encountered an error", "source", name, "error", err)
		}
	}()

	reply.Message = fmt.Sprintf("Data source '%s' turned on successfully", name)
	return nil
}

// TurnOffDataSource deactivates an existing data source
func (m *RPCManager) TurnOffDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	ds, exists := m.DataSources[name]
	if !exists {
		return errors.New("data source not found")
	}

	if !ds.IsRunning() {
		reply.Message = "Data source already stopped"
		return nil
	}

	if err := ds.Close(); err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Data source '%s' turned off successfully", name)
	return nil
}

// AddDataSource creates and starts a new data source
func (m *RPCManager) AddDataSource(args DataSourceArgs, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	if _, exists := m.DataSources[args.Options.Source]; exists {
		return errors.New("data source already exists")
	}

	allSymbols := symbols.GetAllSymbols(
		m.getAssetList().Crypto,
		m.getAssetList().Commodities,
		m.getAssetList().Forex,
		m.getAssetList().Stocks,
	)

	src, err := datasource.BuildDataSource(args.Options, allSymbols, m.TickerTopic, &m.Wg)
	if err != nil {
		return err
	}

	m.DataSources[args.Options.Source] = src

	m.Wg.Add(1)
	go func() {
		defer m.Wg.Done()
		if err := src.Connect(); err != nil {
			slog.Error("Data source encountered an error", "source", args.Options.Source, "error", err)
		}
	}()

	reply.Message = fmt.Sprintf("Data source '%s' added and started successfully", args.Options.Source)
	return nil
}

// RemoveDataSource stops and removes an existing data source
func (m *RPCManager) RemoveDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	ds, exists := m.DataSources[name]
	if !exists {
		return errors.New("data source not found")
	}

	if err := ds.Close(); err != nil {
		return err
	}

	delete(m.DataSources, name)
	reply.Message = "Data source removed successfully"
	return nil
}

// ReloadDataSources refreshes all data sources based on current configuration
func (m *RPCManager) ReloadDataSources(args struct{}, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	// Disconnect all existing data sources
	for name, ds := range m.DataSources {
		if err := ds.Close(); err != nil {
			slog.Error("Error disconnecting data source", "source", name, "error", err)
		}
		delete(m.DataSources, name)
	}

	// Reinitialize data sources
	if err := m.InitDataSources(); err != nil {
		return err
	}

	reply.Message = "All data sources reloaded successfully"
	return nil
}

// InitDataSources initializes data sources from the global configuration
func (m *RPCManager) InitDataSources() error {
	enabledDataSources := m.GlobalConfig.Datasources

	m.Wg.Add(1)
	defer m.Wg.Done()

	if len(enabledDataSources) < 1 {
		if m.GlobalConfig.Env != "development" {
			return errors.New("no data sources defined in configuration")
		}
		slog.Warn("No data sources enabled, where will get the data from?")
	}

	// Create all data sources first
	allSymbols := symbols.GetAllSymbols(
		m.getAssetList().Crypto,
		m.getAssetList().Commodities,
		m.getAssetList().Forex,
		m.getAssetList().Stocks,
	)

	for _, source := range enabledDataSources {
		src, err := datasource.BuildDataSource(source, allSymbols, m.TickerTopic, &m.Wg)
		if err != nil {
			slog.Error("Error creating data source", "source", source.Source, "error", err)
			continue
		}
		m.DataSources[src.GetName()] = src
	}

	// Then connect them all
	for _, source := range enabledDataSources {
		ds, exists := m.DataSources[source.Source]
		if !exists {
			continue
		}

		m.Wg.Add(1)
		go func(ds datasource.FtsoDataSource) {
			defer m.Wg.Done()
			if err := ds.Connect(); err != nil {
				slog.Error("Data source encountered an error", "source", ds.GetName(), "error", err)
			}
		}(ds)
	}

	return nil
}

// AddAsset adds new assets to the configuration
func (m *RPCManager) AddAsset(args AssetsArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	for _, a := range args.Assets {
		currentAssets, err := m.getAssetsByCategory(a.Category)
		if err != nil {
			return err
		}

		assetName := strings.ToUpper(a.AssetName)
		if helpers.ItemInSlice(assetName, currentAssets) {
			return errors.New("asset already exists")
		}

		// Add the asset to the appropriate category
		switch a.Category {
		case "crypto":
			m.CurrentAssets.Crypto = append(currentAssets, assetName)
		case "commodities":
			m.CurrentAssets.Commodities = append(currentAssets, assetName)
		case "forex":
			m.CurrentAssets.Forex = append(currentAssets, assetName)
		case "stocks":
			m.CurrentAssets.Stocks = append(currentAssets, assetName)
		}
	}

	m.GlobalConfig.Assets = m.CurrentAssets

	// Reload data sources to recognize the new assets
	if err := m.reloadDataSourcesLocked(); err != nil {
		return err
	}

	msg := "Assets added successfully"
	slog.Info(msg, "assets", args.Assets)
	reply.Message = msg
	config.UpdateConfig(m.GlobalConfig, true)

	return nil
}

// RemoveAsset removes assets from the configuration
func (m *RPCManager) RemoveAsset(args AssetsArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	for _, a := range args.Assets {
		currentAssets, err := m.getAssetsByCategory(a.Category)
		if err != nil {
			return err
		}

		assetName := strings.ToUpper(a.AssetName)
		if !helpers.ItemInSlice(assetName, currentAssets) {
			return errors.New("asset to remove does not exist")
		}

		// Remove the asset from the appropriate category
		switch a.Category {
		case "crypto":
			m.CurrentAssets.Crypto = helpers.RemoveFromSlice(m.CurrentAssets.Crypto, assetName)
		case "commodities":
			m.CurrentAssets.Commodities = helpers.RemoveFromSlice(m.CurrentAssets.Commodities, assetName)
		case "forex":
			m.CurrentAssets.Forex = helpers.RemoveFromSlice(m.CurrentAssets.Forex, assetName)
		case "stocks":
			m.CurrentAssets.Stocks = helpers.RemoveFromSlice(m.CurrentAssets.Stocks, assetName)
		}
	}

	m.GlobalConfig.Assets = m.CurrentAssets

	// Reload data sources to reflect the asset removal
	if err := m.reloadDataSourcesLocked(); err != nil {
		return err
	}

	msg := "Assets removed successfully"
	slog.Info(msg, "assets", args.Assets)
	reply.Message = msg
	config.UpdateConfig(m.GlobalConfig, true)
	return nil
}

// RenameAsset renames an existing asset
func (m *RPCManager) RenameAsset(args RenameAssetArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Wg.Add(1)
	defer m.Wg.Done()

	currentAssets, err := m.getAssetsByCategory(args.Category)
	if err != nil {
		return err
	}

	oldName := strings.ToUpper(args.AssetName)
	newName := strings.ToUpper(args.NewName)

	if !helpers.ItemInSlice(oldName, currentAssets) {
		return errors.New("asset to rename does not exist")
	}

	if helpers.ItemInSlice(newName, currentAssets) {
		return errors.New("new asset name already exists")
	}

	// Replace old asset with new asset in the appropriate category
	switch args.Category {
	case "crypto":
		m.CurrentAssets.Crypto = helpers.RemoveFromSlice(m.CurrentAssets.Crypto, oldName)
		m.CurrentAssets.Crypto = append(m.CurrentAssets.Crypto, newName)
	case "commodities":
		m.CurrentAssets.Commodities = helpers.RemoveFromSlice(m.CurrentAssets.Commodities, oldName)
		m.CurrentAssets.Commodities = append(m.CurrentAssets.Commodities, newName)
	case "forex":
		m.CurrentAssets.Forex = helpers.RemoveFromSlice(m.CurrentAssets.Forex, oldName)
		m.CurrentAssets.Forex = append(m.CurrentAssets.Forex, newName)
	case "stocks":
		m.CurrentAssets.Stocks = helpers.RemoveFromSlice(m.CurrentAssets.Stocks, oldName)
		m.CurrentAssets.Stocks = append(m.CurrentAssets.Stocks, newName)
	}

	m.GlobalConfig.Assets = m.CurrentAssets

	// Reload data sources to reflect the asset renaming
	if err = m.reloadDataSourcesLocked(); err != nil {
		return err
	}

	msg := fmt.Sprintf("Asset renamed successfully from %s to %s", oldName, newName)

	slog.Info(msg, "category", args.Category)
	reply.Message = msg

	config.UpdateConfig(m.GlobalConfig, true)
	return nil
}

// GetAssets returns the current configured asset list
func (m *RPCManager) GetAssets(args struct{}, reply *CurrentAssetsReply) error {
	m.Wg.Add(1)
	defer m.Wg.Done()
	reply.Assets = m.CurrentAssets
	return nil
}

// Shutdown gracefully closes all data sources
func (m *RPCManager) Shutdown(args struct{}, reply *ShutdownReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	for name, ds := range m.DataSources {
		if err := ds.Close(); err != nil {
			slog.Error("Error disconnecting data source", "source", name, "error", err)
		}
	}
	m.DataSources = make(map[string]datasource.FtsoDataSource)

	reply.Message = "Shutting down..."
	return nil
}

// reloadDataSourcesLocked reloads data sources assuming the mutex is already locked
func (m *RPCManager) reloadDataSourcesLocked() error {
	m.Wg.Add(1)
	defer m.Wg.Done()

	// Disconnect all existing data sources
	for name, ds := range m.DataSources {
		if err := ds.Close(); err != nil {
			slog.Error("Error disconnecting data source", "source", name, "error", err)
		}
	}
	m.DataSources = make(map[string]datasource.FtsoDataSource)

	// Reinitialize data sources
	return m.InitDataSources()
}

// getAssetList returns the current assets configuration
func (m *RPCManager) getAssetList() config.AssetConfig {
	return m.CurrentAssets
}

// getAssetsByCategory returns assets for a specific category
func (m *RPCManager) getAssetsByCategory(category string) ([]string, error) {
	switch category {
	case "crypto":
		return m.CurrentAssets.Crypto, nil
	case "commodities":
		return m.CurrentAssets.Commodities, nil
	case "forex":
		return m.CurrentAssets.Forex, nil
	case "stocks":
		return m.CurrentAssets.Stocks, nil
	default:
		return nil, errors.New("unknown category")
	}
}
