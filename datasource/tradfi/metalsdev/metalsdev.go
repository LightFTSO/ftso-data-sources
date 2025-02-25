package metalsdev

import (
	"errors"
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
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type MetalsDevOptions struct {
	ApiToken string `mapstructure:"api_token"`
	Interval string `mapstructure:"interval"`
}

type MetalsDevClient struct {
	name             string
	W                *sync.WaitGroup
	TickerTopic      *tickertopic.TickerTopic
	Interval         time.Duration
	CommoditySymbols model.SymbolList
	ForexSymbols     model.SymbolList
	apiEndpoint      string
	apiToken         string
	log              *slog.Logger

	timeInterval *time.Ticker

	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewMetalsDevClient(options *MetalsDevOptions, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*MetalsDevClient, error) {

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
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	metalsdev.log.Debug("Created new datasource")

	return &metalsdev, nil
}

func (d *MetalsDevClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)
	d.log.Info("Connecting...")

	err := d.SubscribeTickers(nil, nil)
	if err != nil {
		d.log.Error("Error subscribing to tickers")
		return err
	}

	return nil
}

func (d *MetalsDevClient) Reconnect() error {
	d.log.Info("Reconnecting...")

	return nil
}

func (d *MetalsDevClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	d.timeInterval.Stop()
	d.isRunning = false
	d.clientClosedChan.Send(true)
	d.W.Done()

	return nil
}

func (d *MetalsDevClient) IsRunning() bool {
	return d.isRunning
}

func (d *MetalsDevClient) getLatest(useSample bool) (*LatestEndpointResponse, error) {
	if useSample {
		var latestData = new(LatestEndpointResponse)
		err := sonic.Unmarshal(sampleLatest, latestData)
		if err != nil {
			return nil, err
		}

		return latestData, nil
	}

	reqUrl := d.apiEndpoint + fmt.Sprintf("/latest?api_key=%s&currency=%s&unit=%s", d.apiToken, "USD", "toz")

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

func (d *MetalsDevClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	go func(br *tickertopic.TickerTopic) {
		d.timeInterval = time.NewTicker(d.Interval)

		defer d.timeInterval.Stop()

		for t := range d.timeInterval.C {
			data, err := d.getLatest(false)
			if err != nil {
				d.log.Error("error obtaining latest data", "error", err.Error())
				continue
			}
			for _, s := range d.CommoditySymbols {
				price, present := data.Metals[metalsDevCommodityMap[strings.ToUpper(s.Base)]]
				if !present {
					continue
				}
				ticker := model.Ticker{
					LastPrice: strconv.FormatFloat(price, 'f', 8, 64),
					Base:      strings.ToUpper(s.Base),
					Quote:     strings.ToUpper(s.Quote),
					Source:    d.GetName(),
					Timestamp: t,
				}
				d.log.Info(fmt.Sprintf("metalsdev: base=%s quote=%s price=%s", ticker.Base, ticker.Quote, ticker.LastPrice))
				br.Send(&ticker)
			}
			for _, s := range d.ForexSymbols {
				price, present := data.Currencies[strings.ToUpper(s.Base)]
				if !present {
					continue
				}
				ticker, err := model.NewTicker(strconv.FormatFloat(price, 'f', 8, 64), s, d.GetName(), t)
				if err != nil {
					d.log.Error("Error creating ticker",
						"ticker", ticker, "error", err.Error())
					continue
				}
				d.log.Info(fmt.Sprintf("metalsdev: base=%s quote=%s price=%s", ticker.Base, ticker.Quote, ticker.LastPrice))
				br.Send(ticker)
			}

		}
	}(d.TickerTopic)

	return nil
}

func (d *MetalsDevClient) GetName() string {
	return d.name
}
