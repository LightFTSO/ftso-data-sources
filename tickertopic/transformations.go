package tickertopic

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/mitchellh/mapstructure"
	"roselabs.mx/ftso-data-sources/model"
)

// options from configuration file
type TransformationOptions struct {
	Name    string                 `mapstructure:"name"`
	Enabled bool                   `mapstructure:"enabled"`
	Options map[string]interface{} `mapstructure:",remain"`
}

func createTransformations(transformationOptions []TransformationOptions) ([]Transformation, error) {
	transformations := []Transformation{}

	fmt.Printf("%+v \n", transformationOptions)

	for _, v := range transformationOptions {
		switch v.Name {
		case "use_system_timestamp":
			if v.Enabled {
				slog.Info("Using exchange timestamp as ticker timestamp")
				systemTsTransform := UseSystemTimestampTransformation{}
				transformations = append(transformations, &systemTsTransform)

			} else {
				slog.Info("Using system timestamp as ticker timestamp")
			}
		case "rename_asset":
			if v.Enabled {
				var transform = new(RenameAssetTransform)
				mapstructure.Decode(v.Options, transform)
				slog.Info(fmt.Sprintf("Using rename_asset transform, renaming asset from %s to %s in all ticker", transform.from, transform.to))
				transformations = append(transformations, transform)

			}
		case "rename_quote":
			if v.Enabled {
				var transform = new(RenameAssetTransform)
				mapstructure.Decode(v.Options, transform)
				slog.Info(fmt.Sprintf("Using rename_quote transform, renaming quote from %s to %s in all ticker", transform.from, transform.to))
				transformations = append(transformations, transform)

			}
		}
	}

	return transformations, nil
}

type Transformation interface {
	Transform(*model.Ticker) (*model.Ticker, error)
}

type RenameAssetTransform struct {
	from string `mapstructure:"from"`
	to   string `mapstructure:"to"`
}

func (t *RenameAssetTransform) Transform(ticker *model.Ticker) (*model.Ticker, error) {
	if ticker.Quote == t.from {
		ticker.Quote = t.to
	}
	return ticker, nil
}

type RenameQuoteTransform struct {
	from string `mapstructure:"from"`
	to   string `mapstructure:"to"`
}

func (t *RenameQuoteTransform) Transform(ticker *model.Ticker) (*model.Ticker, error) {
	if ticker.Quote == t.from {
		ticker.Quote = t.to
	}
	return ticker, nil
}

type UseSystemTimestampTransformation struct{}

func (t *UseSystemTimestampTransformation) Transform(ticker *model.Ticker) (*model.Ticker, error) {
	ticker.Timestamp = time.Now().UTC()

	return ticker, nil
}
