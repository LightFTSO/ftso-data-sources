package tickertopic

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"roselabs.mx/ftso-data-sources/model"
)

// options from configuration file
type TransformationOptions struct {
	Type    string                 `mapstructure:"type"`
	Enabled bool                   `mapstructure:"enabled"`
	From    string                 `mapstructure:"from"`
	To      string                 `mapstructure:"to"`
	Options map[string]interface{} `mapstructure:",remain"`
}

var ErrUnknownTransformationType = errors.New("unknown transformation type")
var ErrMissingOptionsFromTransformations = errors.New("missing From and/or To fields from transformation")

func createTransformations(transformationOptions []TransformationOptions) ([]Transformation, error) {

	transformations := []Transformation{}

	for _, v := range transformationOptions {
		if !v.Enabled {
			continue
		}
		switch v.Type {
		case "use_system_timestamp":
			if v.Enabled {
				slog.Info("Using system timestamp as ticker timestamp")
				systemTsTransform := UseSystemTimestampTransformation{}
				transformations = append(transformations, &systemTsTransform)
			} else {
				slog.Info("Using exchange timestamp as ticker timestamp")
			}
		case "rename_asset":
			var transform = new(RenameAssetTransform)
			if v.To != "" && v.From != "" {
				transform.to = v.To
				transform.from = v.From
			} else {
				return nil, ErrMissingOptionsFromTransformations
			}

			slog.Info(fmt.Sprintf("Using rename_asset transform, renaming asset from %s to %s in all tickers", transform.from, transform.to))
			transformations = append(transformations, transform)

		case "rename_quote_asset":
			var transform = new(RenameQuoteTransform)
			if v.To != "" && v.From != "" {
				transform.to = v.To
				transform.from = v.From
			} else {
				return nil, ErrMissingOptionsFromTransformations
			}

			slog.Info(fmt.Sprintf("Using rename_quote_asset_asset transform, renaming quote from %s to %s in all tickers", transform.from, transform.to))
			transformations = append(transformations, transform)

		default:
			slog.Error(fmt.Sprintf("Transformation type '%s' not known", v.Type))
			return nil, ErrUnknownTransformationType
		}
	}

	return transformations, nil
}

type Transformation interface {
	Transform(*model.Ticker) error
}

type RenameAssetTransform struct {
	from string `mapstructure:"from"`
	to   string `mapstructure:"to"`
}

func (t *RenameAssetTransform) Transform(ticker *model.Ticker) error {
	if ticker.Base == t.from {
		ticker.Base = t.to
	}
	return nil
}

type RenameQuoteTransform struct {
	from string `mapstructure:"from"`
	to   string `mapstructure:"to"`
}

func (t *RenameQuoteTransform) Transform(ticker *model.Ticker) error {
	if ticker.Quote == t.from {
		ticker.Quote = t.to
	}
	return nil
}

type UseSystemTimestampTransformation struct{}

func (t *UseSystemTimestampTransformation) Transform(ticker *model.Ticker) error {
	ticker.Timestamp = time.Now().UTC()

	return nil
}
