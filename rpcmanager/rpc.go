package rpcmanager

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/config"
	"roselabs.mx/ftso-data-sources/datasource"
	"roselabs.mx/ftso-data-sources/helpers"
	"roselabs.mx/ftso-data-sources/symbols"
)

// RPC Arguments and Reply Structures
type DataSourceArgs struct {
	Options datasource.DataSourceOptions
}

type DataSourceReply struct {
	Message string
}

type NewAssetArgs struct {
	AssetName string
	Category  string
}

// AssetArgs represents arguments for asset RPC methods
type RenameAssetArgs struct {
	AssetName string
	NewName   string // Used for renaming
	Category  string
}

// AssetReply represents the reply from asset RPC methods
type AssetReply struct {
	Message string
}

type RPCManager struct {
	GlobalConfig  config.ConfigOptions
	DataSources   map[string]datasource.FtsoDataSource
	CurrentAssets config.AssetConfig // Set of current assets
	TickerTopic   *broadcast.Broadcaster

	Mu sync.Mutex
	Wg sync.WaitGroup
}

// TurnOnDataSource turns on an existing data source
func (m *RPCManager) TurnOnDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

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
		err := ds.Connect()
		if err != nil {
			log.Printf("Data source %s encountered an error: %v", name, err)
		}
	}()

	reply.Message = fmt.Sprintf("Data source '%s' turned on successfully", name)
	return nil
}

// TurnOffDataSource turns off an existing data source
func (m *RPCManager) TurnOffDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	ds, exists := m.DataSources[name]
	if !exists {
		return errors.New("data source not found")
	}

	if !ds.IsRunning() {
		reply.Message = "Data source already stopped"
		return nil
	}

	err := ds.Close()
	if err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Data source '%s' turned off successfully", name)
	return nil
}

// AddDataSource adds and starts a new data source
func (m *RPCManager) AddDataSource(args DataSourceArgs, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	_, exists := m.DataSources[args.Options.Source]
	if exists {
		return errors.New("data source already exists")
	}

	// Build and connect the new data source
	src, err := datasource.BuilDataSource(args.Options, symbols.GetAllSymbols(
		m.getAssetList().Crypto, m.getAssetList().Commodities, m.getAssetList().Forex, m.getAssetList().Stocks,
	), m.TickerTopic, &m.Wg)
	if err != nil {
		return err
	}

	m.DataSources[args.Options.Source] = src

	m.Wg.Add(1)
	go func() {
		defer m.Wg.Done()
		err := src.Connect()
		if err != nil {
			log.Printf("Data source %s encountered an error: %v", args.Options.Source, err)
		}
	}()

	reply.Message = fmt.Sprintf("Data source '%s' added and started successfully", args.Options.Source)
	return nil
}

// RemoveDataSource stops and removes an existing data source
func (m *RPCManager) RemoveDataSource(name string, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	ds, exists := m.DataSources[name]
	if !exists {
		return errors.New("data source not found")
	}

	err := ds.Close()
	if err != nil {
		return err
	}

	delete(m.DataSources, name)
	reply.Message = "Data source removed successfully"
	return nil
}

// ReloadDataSources reloads all data sources based on the current configuration
func (m *RPCManager) ReloadDataSources(args struct{}, reply *DataSourceReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	// Disconnect all existing data sources
	for name, ds := range m.DataSources {
		err := ds.Close()
		if err != nil {
			log.Printf("Error disconnecting data source %s: %v", name, err)
		}
		delete(m.DataSources, name)
	}

	// Reinitialize data sources
	err := m.InitDataSources()
	if err != nil {
		return err
	}

	reply.Message = "All data sources reloaded successfully"
	return nil
}

// Initialize data sources from the global configuration
func (m *RPCManager) InitDataSources() error {
	allDataSources := datasource.AllDataSources()

	enabledDataSources := m.GlobalConfig.Datasources

	if len(enabledDataSources) < 1 {
		if m.GlobalConfig.Env != "development" {
			return errors.New("no data sources defined in configuration")
		}
		log.Println("Warning: No data sources enabled, where will get the data from?")
	}

	for _, source := range allDataSources {
		src, err := datasource.BuilDataSource(source, symbols.GetAllSymbols(m.getAssetList().Crypto, m.getAssetList().Commodities, m.getAssetList().Forex, m.getAssetList().Stocks), m.TickerTopic, &m.Wg)
		if err != nil {
			log.Printf("Error creating data source %s: %v", source.Source, err)
			continue
		}

		m.DataSources[src.GetName()] = src
	}

	for _, source := range enabledDataSources {
		m.Wg.Add(1)
		go func(ds datasource.FtsoDataSource) {
			defer m.Wg.Done()
			err := ds.Connect()
			if err != nil {
				log.Printf("Data source %s encountered an error: %v", ds.GetName(), err)
			}
		}(m.DataSources[source.Source])
	}

	return nil
}

// AddAsset adds a new base asset
func (m *RPCManager) AddAsset(args NewAssetArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	var currentAssets []string

	switch args.Category {
	case "crypto":
		currentAssets = m.CurrentAssets.Crypto
	case "commodities":
		currentAssets = m.CurrentAssets.Commodities
	case "forex":
		currentAssets = m.CurrentAssets.Forex
	case "stocks":
		currentAssets = m.CurrentAssets.Stocks
	default:
		return errors.New("unknown category")
	}

	assetName := strings.ToUpper(args.AssetName)
	if exists := helpers.ItemInSlice(assetName, currentAssets); exists {
		return errors.New("asset already exists")
	}

	// Add the asset
	switch args.Category {
	case "crypto":
		m.CurrentAssets.Crypto = append(m.CurrentAssets.Crypto, assetName)
	case "commodities":
		m.CurrentAssets.Commodities = append(m.CurrentAssets.Commodities, assetName)
	case "forex":
		m.CurrentAssets.Forex = append(m.CurrentAssets.Forex, assetName)
	case "stocks":
		m.CurrentAssets.Stocks = append(m.CurrentAssets.Stocks, assetName)
	}

	// Reload data sources to recognize the new asset
	err := m.reloadDataSourcesLocked()
	if err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Asset %s added successfully", assetName)
	return nil
}

// RemoveAsset removes an existing base asset
func (m *RPCManager) RemoveAsset(args NewAssetArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	var currentAssets []string

	switch args.Category {
	case "crypto":
		currentAssets = m.CurrentAssets.Crypto
	case "commodities":
		currentAssets = m.CurrentAssets.Commodities
	case "forex":
		currentAssets = m.CurrentAssets.Forex
	case "stocks":
		currentAssets = m.CurrentAssets.Stocks
	default:
		return errors.New("unknown category")
	}

	assetName := strings.ToUpper(args.AssetName)
	if exists := helpers.ItemInSlice(assetName, currentAssets); exists {
		return errors.New("asset already exists")
	}

	// Remove the asset
	switch args.Category {
	case "crypto":
		m.CurrentAssets.Crypto = helpers.RemoveFromSlice(m.CurrentAssets.Crypto, assetName)
	case "commodities":
		m.CurrentAssets.Commodities = helpers.RemoveFromSlice(m.CurrentAssets.Commodities, assetName)
	case "forex":
		m.CurrentAssets.Forex = helpers.RemoveFromSlice(m.CurrentAssets.Forex, assetName)
	case "stocks":
		m.CurrentAssets.Stocks = helpers.RemoveFromSlice(m.CurrentAssets.Stocks, assetName)
	}

	// Reload data sources to reflect the asset removal
	err := m.reloadDataSourcesLocked()
	if err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Asset %s removed successfully", assetName)
	return nil
}

// RenameAsset renames an existing base asset
func (m *RPCManager) RenameAsset(args RenameAssetArgs, reply *AssetReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	var currentAssets []string

	switch args.Category {
	case "crypto":
		currentAssets = m.CurrentAssets.Crypto
	case "commodities":
		currentAssets = m.CurrentAssets.Commodities
	case "forex":
		currentAssets = m.CurrentAssets.Forex
	case "stocks":
		currentAssets = m.CurrentAssets.Stocks
	default:
		return errors.New("unknown category")
	}

	oldName := strings.ToUpper(args.AssetName)
	newName := strings.ToUpper(args.NewName)

	if exists := helpers.ItemInSlice(newName, currentAssets); !exists {
		return errors.New("asset to rename does not exist")
	}

	if exists := helpers.ItemInSlice(newName, currentAssets); exists {
		return errors.New("new asset name already exists")
	}

	// Remove old asset and add new asset
	switch args.Category {
	case "crypto":
		m.CurrentAssets.Crypto = helpers.RemoveFromSlice(m.CurrentAssets.Crypto, newName)
		m.CurrentAssets.Crypto = append(m.CurrentAssets.Crypto, newName)
	case "commodities":
		m.CurrentAssets.Commodities = helpers.RemoveFromSlice(m.CurrentAssets.Commodities, newName)
		m.CurrentAssets.Commodities = append(m.CurrentAssets.Commodities, newName)
	case "forex":
		m.CurrentAssets.Forex = helpers.RemoveFromSlice(m.CurrentAssets.Forex, newName)
		m.CurrentAssets.Forex = append(m.CurrentAssets.Forex, newName)
	case "stocks":
		m.CurrentAssets.Stocks = helpers.RemoveFromSlice(m.CurrentAssets.Stocks, newName)
		m.CurrentAssets.Stocks = append(m.CurrentAssets.Stocks, newName)
	}

	// Reload data sources to reflect the asset renaming
	err := m.reloadDataSourcesLocked()
	if err != nil {
		return err
	}

	reply.Message = fmt.Sprintf("Asset %s renamed to %s successfully", oldName, newName)
	return nil
}

// reloadDataSourcesLocked reloads data sources assuming the mutex is already locked
func (m *RPCManager) reloadDataSourcesLocked() error {
	// Disconnect all existing data sources
	for name, ds := range m.DataSources {
		err := ds.Close()
		if err != nil {
			log.Printf("Error disconnecting data source %s: %v", name, err)
		}
		delete(m.DataSources, name)
	}

	// Reinitialize data sources
	err := m.InitDataSources()
	if err != nil {
		return err
	}

	return nil
}

// getAssetList returns a list of current assets
func (m *RPCManager) getAssetList() config.AssetConfig {
	return m.CurrentAssets
}

// Initialize assets from configuration
func (m *RPCManager) InitializeAssets() {
	m.CurrentAssets = config.Config.Assets
	for _, asset := range m.GlobalConfig.Assets.Crypto {
		m.CurrentAssets.Crypto = append(m.CurrentAssets.Crypto, strings.ToUpper(asset))
	}
	for _, asset := range m.GlobalConfig.Assets.Commodities {
		m.CurrentAssets.Commodities = append(m.CurrentAssets.Commodities, strings.ToUpper(asset))
	}
	for _, asset := range m.GlobalConfig.Assets.Forex {
		m.CurrentAssets.Forex = append(m.CurrentAssets.Forex, strings.ToUpper(asset))
	}
	for _, asset := range m.GlobalConfig.Assets.Stocks {
		m.CurrentAssets.Stocks = append(m.CurrentAssets.Stocks, strings.ToUpper(asset))
	}
}
