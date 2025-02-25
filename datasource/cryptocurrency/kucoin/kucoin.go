package kucoin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type KucoinClient struct {
	name             string
	W                *sync.WaitGroup
	TickerTopic      *broadcast.Broadcaster
	apiEndpoint      string
	SymbolList       []model.Symbol
	log              *slog.Logger
	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
	instanceClient   *kucoinInstanceClient
}

func NewKucoinClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*KucoinClient, error) {
	kucoin := KucoinClient{
		name:             "kucoin",
		log:              slog.Default().With(slog.String("datasource", "kucoin")),
		W:                w,
		TickerTopic:      tickerTopic,
		apiEndpoint:      "https://api.kucoin.com",
		SymbolList:       symbolList.Crypto,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}

	kucoin.log.Debug("Created new datasource")
	return &kucoin, nil
}

func (d *KucoinClient) Connect() error {
	d.isRunning = true

	d.W.Add(1)

	availableSymbols, err := d.getAvailableSymbols()
	if err != nil {
		d.log.Error("Error obtaining available symbols", "error", err)
		d.W.Done()
		return err
	}

	go func() {
		// create new instance servers indefinitely as they're closed, until we get the close signal from the main function
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("instance client generator loop exiting")
				return
			default:
				d.log.Info("Creating kucoin instance client...")
				instanceContext, instanceCancelFunc := context.WithCancel(context.Background())
				instanceData, err := d.getNewInstanceData()
				if err != nil {
					d.log.Error("Error obtaining Kucoin instance client data", "error", err)
					time.Sleep(5 * time.Second)
					continue
				}
				d.instanceClient = newKucoinInstanceClient(*instanceData, availableSymbols, d.SymbolList, d.TickerTopic, instanceContext, instanceCancelFunc)

				err = d.instanceClient.connect()
				if err != nil {
					d.log.Error("", "error", err)
				}

				<-instanceContext.Done()

				if !d.isRunning {
					d.log.Debug("instance client generator loop exiting")
					return
				}
			}
		}
	}()

	return nil
}

func (d *KucoinClient) Reconnect() error {
	return nil
}

func (d *KucoinClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.log.Debug("closing Kucoin instance generator")
	d.isRunning = false
	d.clientClosedChan.Send(true)
	d.instanceClient.onDisconnect()
	d.W.Done()

	return nil
}

func (d *KucoinClient) IsRunning() bool {
	return d.isRunning
}

func (d *KucoinClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	return nil
}

func (d *KucoinClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := d.apiEndpoint + "/api/v2/symbols"

	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	type symbolsResponse struct {
		Data []KucoinSymbol `json:"data"`
	}
	kucoinSymbols := symbolsResponse{}
	err = sonic.Unmarshal(data, &kucoinSymbols)
	if err != nil {
		return nil, err
	}

	var availableSymbols []model.Symbol
	for _, s := range kucoinSymbols.Data {
		symbol := model.Symbol{
			Base:  s.BaseCurrency,
			Quote: s.QuoteCurrency,
		}
		availableSymbols = append(availableSymbols, symbol)
	}

	return availableSymbols, nil

}

func (d *KucoinClient) getNewInstanceData() (*InstanceServer, error) {
	reqUrl := fmt.Sprintf("%s/api/v1/bullet-public", d.apiEndpoint)
	req, err := http.NewRequest(http.MethodPost, reqUrl, nil)
	if err != nil {
		return &InstanceServer{}, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var bullerPublicRes = new(BulletPublicEndpointResponse)
	err = sonic.Unmarshal(data, bullerPublicRes)
	if err != nil {
		return nil, err
	}

	if len(bullerPublicRes.Data.InstanceServers) < 1 {
		return &InstanceServer{}, fmt.Errorf("kucoin API returned no instance servers to connect to")
	}

	instanceServer := &InstanceServer{
		Endpoint:       bullerPublicRes.Data.InstanceServers[0].Endpoint,
		Encrypt:        bullerPublicRes.Data.InstanceServers[0].Encrypt,
		Protocol:       bullerPublicRes.Data.InstanceServers[0].Protocol,
		PingIntervalMs: bullerPublicRes.Data.InstanceServers[0].PingIntervalMs,
		PingTimeout:    bullerPublicRes.Data.InstanceServers[0].PingTimeout,
		Token:          bullerPublicRes.Data.Token,
	}

	return instanceServer, nil

}

func (d *KucoinClient) GetName() string {
	return d.name
}
