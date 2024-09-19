package metalsdev

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/textileio/go-threads/broadcast"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
)

type MetalsDevOptions struct {
	ApiToken string `mapstructure:"api_token"`
	Interval string `mapstructure:"interval"`
}

type MetalsDevClient struct {
	name             string
	W                *sync.WaitGroup
	TickerTopic      *broadcast.Broadcaster
	Interval         time.Duration
	CommoditySymbols []model.Symbol
	ForexSymbols     []model.Symbol
	apiEndpoint      string
	apiToken         string
	log              *slog.Logger

	isRunning bool
}

func NewMetalsDevClient(options *MetalsDevOptions, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*MetalsDevClient, error) {

	d, err := time.ParseDuration(options.Interval)
	if err != nil {
		slog.Warn("Using default duration", "datasource", "metalsdev")
		d = time.Second
	}

	metalsdev := MetalsDevClient{
		name:             "metalsdev",
		log:              slog.Default().With(slog.String("datasource", "metalsdev")),
		W:                w,
		TickerTopic:      tickerTopic,
		CommoditySymbols: symbolList.Commodities,
		ForexSymbols:     symbolList.Forex,
		apiEndpoint:      "https://api.metals.dev/v1",
		apiToken:         options.ApiToken,
		Interval:         d,
	}
	metalsdev.log.Debug("Created new datasource")

	return &metalsdev, nil
}

func (b *MetalsDevClient) Connect() error {
	b.isRunning = true
	b.W.Add(1)
	b.log.Info("Connecting...")

	err := b.SubscribeTickers()
	if err != nil {
		b.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (b *MetalsDevClient) Reconnect() error {
	b.log.Info("Reconnecting...")

	return nil
}

func (b *MetalsDevClient) Close() error {
	b.isRunning = false
	b.W.Done()

	return nil
}

func (b *MetalsDevClient) IsRunning() bool {
	return b.isRunning
}

func (b *MetalsDevClient) getLatest(useSample bool) (*LatestEndpointResponse, error) {
	if useSample {
		var latestData = new(LatestEndpointResponse)
		err := sonic.Unmarshal(sampleLatest, latestData)
		if err != nil {
			return nil, err
		}

		return latestData, nil
	}

	reqUrl := b.apiEndpoint + fmt.Sprintf("/latest?api_key=%s&currency=%s&unit=%s", b.apiToken, "USD", "toz")

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

	var latestData = new(LatestEndpointResponse)
	err = sonic.Unmarshal(data, latestData)
	if err != nil {
		return nil, err
	}

	return latestData, nil

}

func (b *MetalsDevClient) SubscribeTickers() error {
	go func(br *broadcast.Broadcaster) {
		timeInterval := *time.NewTicker(b.Interval)

		defer timeInterval.Stop()

		for t := range timeInterval.C {
			data, err := b.getLatest(false)
			if err != nil {
				b.log.Error("error obtaining latest data", "error", err.Error())
				continue
			}
			for _, s := range b.CommoditySymbols {
				price, present := data.Metals[metalsDevCommodityMap[strings.ToUpper(s.Base)]]
				if !present {
					continue
				}
				ticker := model.Ticker{
					LastPrice: strconv.FormatFloat(price, 'f', 8, 64),
					Symbol:    strings.ToUpper(s.GetSymbol()),
					Base:      strings.ToUpper(s.Base),
					Quote:     strings.ToUpper(s.Quote),
					Source:    b.GetName(),
					Timestamp: t,
				}
				b.log.Info(fmt.Sprintf("metalsdev: symbol=%s price=%s", ticker.Symbol, ticker.LastPrice))
				br.Send(&ticker)
			}
			for _, s := range b.ForexSymbols {
				price, present := data.Currencies[strings.ToUpper(s.Base)]
				if !present {
					continue
				}

				ticker := model.Ticker{
					LastPrice: strconv.FormatFloat(price, 'f', 8, 64),
					Symbol:    strings.ToUpper(s.GetSymbol()),
					Base:      strings.ToUpper(s.Base),
					Quote:     strings.ToUpper(s.Quote),
					Source:    b.GetName(),
					Timestamp: t,
				}
				b.log.Info(fmt.Sprintf("metalsdev: symbol=%s price=%s", ticker.Symbol, ticker.LastPrice))
				br.Send(&ticker)
			}

		}
	}(b.TickerTopic)

	return nil
}

func (b *MetalsDevClient) GetName() string {
	return b.name
}
