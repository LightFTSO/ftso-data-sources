package metalsdev

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "log/slog"

	json "github.com/json-iterator/go"
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

	ctx    context.Context
	cancel context.CancelFunc
}

func NewMetalsDevClient(options *MetalsDevOptions, symbolList symbols.AllSymbols, tickerTopic *broadcast.Broadcaster, w *sync.WaitGroup) (*MetalsDevClient, error) {
	log.Info("Created new metalsdev datasource", "datasource", "metalsdev")

	d, err := time.ParseDuration(options.Interval)
	if err != nil {
		log.Info("Using default duration", "datasource", "metalsdev")
		d = time.Second
	}

	metalsdev := MetalsDevClient{
		name:             "metalsdev",
		W:                w,
		TickerTopic:      tickerTopic,
		CommoditySymbols: symbolList.Commodities,
		ForexSymbols:     symbolList.Forex,
		apiEndpoint:      "https://api.metals.dev/v1",
		apiToken:         options.ApiToken,
		Interval:         d,
	}

	return &metalsdev, nil
}

func (b *MetalsDevClient) Connect() error {
	b.W.Add(1)
	log.Info("Connecting to metalsdev datasource")

	b.ctx, b.cancel = context.WithCancel(context.Background())
	return nil
}

func (b *MetalsDevClient) Reconnect() error {
	log.Info("Reconnecting to metalsdev datasource")

	return nil
}

func (b *MetalsDevClient) Close() error {
	b.W.Done()
	b.ctx.Done()

	return nil
}

func (b *MetalsDevClient) getLatest(useSample bool) (*LatestEndpointResponse, error) {
	if useSample {
		var latestData = new(LatestEndpointResponse)
		err := json.Unmarshal(sampleLatest, latestData)
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
	err = json.Unmarshal(data, latestData)
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
				log.Error("error obtaining latest data", "datasource", b.GetName(), "error", err.Error())
				continue
			}
			for _, s := range b.CommoditySymbols {
				price, present := data.Metals[metalsDevCommodityMap[strings.ToUpper(s.Base)]]
				if !present {
					continue
				}
				ticker := model.Ticker{
					LastPrice: strconv.FormatFloat(price, 'f', 8, 64),
					Symbol:    strings.ToUpper(s.Symbol),
					Base:      strings.ToUpper(s.Base),
					Quote:     strings.ToUpper(s.Quote),
					Source:    b.GetName(),
					Timestamp: t,
				}
				log.Info(fmt.Sprintf("metalsdev: symbol=%s price=%s", ticker.Symbol, ticker.LastPrice))
				br.Send(&ticker)
			}
			for _, s := range b.ForexSymbols {
				price, present := data.Currencies[strings.ToUpper(s.Base)]
				if !present {
					continue
				}

				ticker := model.Ticker{
					LastPrice: strconv.FormatFloat(price, 'f', 8, 64),
					Symbol:    strings.ToUpper(s.Symbol),
					Base:      strings.ToUpper(s.Base),
					Quote:     strings.ToUpper(s.Quote),
					Source:    b.GetName(),
					Timestamp: t,
				}
				log.Info(fmt.Sprintf("metalsdev: symbol=%s price=%s", ticker.Symbol, ticker.LastPrice))
				br.Send(&ticker)
			}

		}
	}(b.TickerTopic)

	return nil
}

func (b *MetalsDevClient) GetName() string {
	return b.name
}
