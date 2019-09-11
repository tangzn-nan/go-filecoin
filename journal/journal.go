package journal

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Journal interface {
	Record(operation string, meta ...interface{})
}

type JournalBuilder func(topic string) (Journal, error)

func NewZapJournalBuilder(filepath string) JournalBuilder {
	zapCfg := zap.NewProductionConfig()
	zapCfg.Encoding = "json"
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapCfg.EncoderConfig.LevelKey = ""
	zapCfg.EncoderConfig.CallerKey = ""
	zapCfg.EncoderConfig.MessageKey = "operation"
	zapCfg.EncoderConfig.NameKey = "topic"
	zapCfg.OutputPaths = []string{filepath}
	zapCfg.ErrorOutputPaths = []string{"stderr"}

	return func(topic string) (Journal, error) {
		journal, err := zapCfg.Build()
		if err != nil {
			return nil, err
		}
		return &ZapJournal{
			logger: journal.Sugar().Named(topic),
		}, nil
	}
}

type ZapJournal struct {
	logger *zap.SugaredLogger
}

func (zj *ZapJournal) Record(operation string, kv ...interface{}) {
	zj.logger.Infow(operation, kv...)
}
