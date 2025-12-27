package mexc

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/gorilla/websocket"
	"github.com/textileio/go-threads/broadcast"
	"google.golang.org/protobuf/proto"
	"roselabs.mx/ftso-data-sources/datasource/cryptocurrency/mexc/pb"
	"roselabs.mx/ftso-data-sources/internal"
	"roselabs.mx/ftso-data-sources/model"
	"roselabs.mx/ftso-data-sources/symbols"
	"roselabs.mx/ftso-data-sources/tickertopic"
)

type MexcClient struct {
	name               string
	W                  *sync.WaitGroup
	TickerTopic        *tickertopic.TickerTopic
	wsClients          []*internal.WebSocketClient
	wsEndpoint         string
	apiEndpoint        string
	SymbolList         model.SymbolList
	symbolChunks       []model.SymbolList
	lastTimestamp      time.Time
	lastTimestampMutex sync.Mutex
	log                *slog.Logger

	pingInterval time.Duration

	tzInfo           *time.Location
	subscriptionId   atomic.Uint64
	isRunning        bool
	clientClosedChan *broadcast.Broadcaster
}

func NewMexcClient(options interface{}, symbolList symbols.AllSymbols, tickerTopic *tickertopic.TickerTopic, w *sync.WaitGroup) (*MexcClient, error) {
	wsEndpoint := "wss://wbs-api.mexc.com/ws"

	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, fmt.Errorf("error loading timezone information: %w", err)
	}

	mexc := MexcClient{
		name:             "mexc",
		log:              slog.Default().With(slog.String("datasource", "mexc")),
		W:                w,
		TickerTopic:      tickerTopic,
		wsClients:        []*internal.WebSocketClient{},
		wsEndpoint:       wsEndpoint,
		apiEndpoint:      "https://api.mexc.com",
		SymbolList:       symbolList.Crypto,
		pingInterval:     20 * time.Second,
		tzInfo:           shanghaiTimezone,
		clientClosedChan: broadcast.NewBroadcaster(0),
	}
	mexc.symbolChunks = mexc.SymbolList.ChunkSymbols(25)
	mexc.log.Debug("Created new datasource")
	return &mexc, nil
}

func (d *MexcClient) Connect() error {
	d.isRunning = true
	d.W.Add(1)

	for _, chunk := range d.symbolChunks {
		wsClient := internal.NewWebSocketClient(d.wsEndpoint)
		wsClient.SetMessageHandler(d.onMessage)
		wsClient.SetLogger(d.log)
		wsClient.SetOnConnect(func() error {
			return d.SubscribeTickers(wsClient, chunk)
		})
		d.wsClients = append(d.wsClients, wsClient)
		wsClient.Start()
	}

	d.setPing()
	d.setLastTickerWatcher()

	return nil
}

func (d *MexcClient) Close() error {
	if !d.IsRunning() {
		return errors.New("datasource is not running")
	}
	for _, wsClient := range d.wsClients {
		wsClient.Close()
	}
	d.isRunning = false
	d.clientClosedChan.Send(true)
	d.W.Done()

	return nil
}

func (d *MexcClient) IsRunning() bool {
	return d.isRunning
}

func (d *MexcClient) onMessage(message internal.WsMessage) {
	switch message.Type {
	case websocket.TextMessage:
		//fmt.Println("TextMessage ", message.Type, string(message.Message))

	case websocket.BinaryMessage:
		var newMessage pb.PushDataV3ApiWrapper
		err := proto.Unmarshal(message.Message, &newMessage)
		if err != nil {
			d.log.Error("Error unmarshaling message", "error", err)
			return
		}
		ticker, err := d.parseTicker(&newMessage)
		if err != nil {
			d.log.Error("Error parsing ticker",
				"ticker", ticker, "error", err.Error())
			return
		}
		d.lastTimestampMutex.Lock()
		d.lastTimestamp = time.Now()
		d.lastTimestampMutex.Unlock()

		d.TickerTopic.Send(ticker)

	}
}

func (d *MexcClient) parseTicker(message *pb.PushDataV3ApiWrapper) (*model.Ticker, error) {
	var newTickerEvent pb.PublicMiniTickerV3Api = *message.GetPublicMiniTicker()

	symbol := model.ParseSymbol(newTickerEvent.GetSymbol())
	ts := time.UnixMilli(message.GetSendTime())
	ticker, err := model.NewTickerPriceString(newTickerEvent.GetPrice(),
		symbol,
		d.GetName(),
		ts)
	if err != nil {
		d.log.Error("Error parsing ticker", "error", err)
		return nil, err
	}
	return ticker, err
}

func (d *MexcClient) SubscribeTickers(wsClient *internal.WebSocketClient, symbols model.SymbolList) error {
	for _, v := range symbols {
		subMessage := map[string]interface{}{
			"id":     d.subscriptionId.Add(1),
			"method": "SUBSCRIPTION",
			"params": []string{fmt.Sprintf("spot@public.miniTicker.v3.api.pb@%s%s@UTC+0", strings.ToUpper(v.Base), strings.ToUpper(v.Quote))},
		}
		wsClient.SendMessageJSON(websocket.TextMessage, subMessage)
	}

	d.log.Debug("Subscribed ticker symbols", "symbols", len(symbols))
	return nil
}

func (d *MexcClient) GetName() string {
	return d.name
}

func (d *MexcClient) setLastTickerWatcher() {
	lastTickerIntervalTimer := time.NewTicker(1 * time.Second)
	d.lastTimestampMutex.Lock()
	d.lastTimestamp = time.Now()
	d.lastTimestampMutex.Unlock()

	timeout := (30 * time.Second)
	go func() {
		defer lastTickerIntervalTimer.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("last ticker received watcher goroutine exiting")
				return
			case <-lastTickerIntervalTimer.C:
				now := time.Now()
				d.lastTimestampMutex.Lock()
				diff := now.Sub(d.lastTimestamp)
				d.lastTimestampMutex.Unlock()

				if diff > timeout {
					// no tickers received in a while, attempt to reconnect
					d.lastTimestampMutex.Lock()
					d.lastTimestamp = time.Now()
					d.lastTimestampMutex.Unlock()
					d.log.Warn(fmt.Sprintf("No tickers received in %s", diff))
					for _, wsClient := range d.wsClients {
						wsClient.Reconnect()
					}
				}
			}
		}
	}()
}

func (d *MexcClient) setPing() {
	ticker := time.NewTicker(d.pingInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-d.clientClosedChan.Listen().Channel():
				d.log.Debug("ping sender goroutine exiting")
				return
			case <-ticker.C:
				for _, wsClient := range d.wsClients {
					wsClient.SendMessage(internal.WsMessage{Type: websocket.PingMessage, Message: []byte(`{"method":"PING"}`)})
				}
			}
		}
	}()
}
