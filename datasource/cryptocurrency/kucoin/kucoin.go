package kucoin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	log "log/slog"

	"github.com/bytedance/sonic"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type KucoinClient struct {
	name        string
	W           *sync.WaitGroup
	TickerTopic *broadcast.Broadcaster
	apiEndpoint string
	SymbolList  []model.Symbol

	ctx    context.Context
	cancel context.CancelFunc
}

func NewKucoinClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*KucoinClient, error) {
	kucoin := KucoinClient{
		name:        "kucoin",
		W:           w,
		TickerTopic: tickerTopic,
		apiEndpoint: "https://api.kucoin.com",
		SymbolList:  symbolList.Crypto,
	}

	log.Debug("Created new datasource", "datasource", kucoin.GetName())
	return &kucoin, nil
}

func (b *KucoinClient) Connect() error {

	b.W.Add(1)

	b.ctx, b.cancel = context.WithCancel(context.Background())

	availableSymbols, err := b.getAvailableSymbols()
	if err != nil {
		log.Error("Error obtaining available symbols", "datasource", b.GetName(), "error", err)
		b.W.Done()
		return err
	}

	go func() {
		defer b.W.Done()

		// create new instance servers indefinitely as they're closed, until we get the close signal from the main function
		for {
			log.Info("Creating kucoin instance client...", "datasource", b.GetName())
			instanceContext, instanceCancelContext := context.WithCancel(context.Background())
			instanceData, err := b.getNewInstanceData()
			if err != nil {
				log.Error("", "datasource", b.GetName(), "error", err)
			}
			instanceClient := newKucoinInstanceClient(*instanceData, availableSymbols, b.SymbolList, b.TickerTopic, instanceContext, instanceCancelContext)

			err = instanceClient.connect()
			if err != nil {
				log.Error("", "datasource", b.GetName(), "error", err)
			}

			select {
			case <-instanceContext.Done():
				continue
			case <-b.ctx.Done():
				instanceClient.close()
				return
			}
		}

	}()

	return nil
}

func (b *KucoinClient) Reconnect() error {
	return nil
}

func (b *KucoinClient) Close() error {
	b.cancel()
	b.W.Done()

	return nil
}
func (b *KucoinClient) SubscribeTickers() error {
	return nil
}

func (b *KucoinClient) getAvailableSymbols() ([]model.Symbol, error) {
	reqUrl := b.apiEndpoint + "/api/v2/symbols"

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

func (b *KucoinClient) getNewInstanceData() (*InstanceServer, error) {
	reqUrl := fmt.Sprintf("%s/api/v1/bullet-public", b.apiEndpoint)
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

func (b *KucoinClient) GetName() string {
	return b.name
}
