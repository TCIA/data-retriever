package app

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Logger is the exported application logger shared across entry points.
	Logger *zap.SugaredLogger
	// logger is kept for internal references until they are updated.
	logger *zap.SugaredLogger
)

// newEncoderConfig creates EncoderConfig for zap logging.
func newEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     timeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// timeEncoder formats logger timestamps.
func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// setLogger initialises the shared zap logger.
// Levels: default = Warn (quiet), verbose = Info, debug = Debug.
// Extra sinks receive console-encoded log lines at the same level as stdout.
func setLogger(debug, verbose bool, logfile string, extraSinks ...io.Writer) {
	encoder := newEncoderConfig()
	// Extra sinks (e.g. the GUI panel) don't render ANSI color codes well.
	plainEncoder := encoder
	plainEncoder.EncodeLevel = zapcore.CapitalLevelEncoder

	level := zap.WarnLevel
	switch {
	case debug:
		level = zap.DebugLevel
	case verbose:
		level = zap.InfoLevel
	}

	cores := []zapcore.Core{
		zapcore.NewCore(zapcore.NewConsoleEncoder(encoder), zapcore.AddSync(os.Stdout), level),
	}
	for _, w := range extraSinks {
		if w == nil {
			continue
		}
		cores = append(cores, zapcore.NewCore(zapcore.NewConsoleEncoder(plainEncoder), zapcore.AddSync(w), level))
	}
	if logfile != "" {
		_ = os.MkdirAll(filepath.Dir(logfile), os.ModePerm)
		f, err := os.OpenFile(logfile, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, os.ModePerm)
		if err != nil {
			zap.New(zapcore.NewTee(cores...), zap.AddCaller()).Sugar().Warnf("failed save log to %s: %v", logfile, err)
		} else {
			cores = append(cores, zapcore.NewCore(zapcore.NewJSONEncoder(encoder), zapcore.AddSync(f), zap.DebugLevel))
		}
	}

	loggerInstance := zap.New(zapcore.NewTee(cores...), zap.AddCaller())
	defer func() { _ = loggerInstance.Sync() }()
	sugar := loggerInstance.Sugar()
	logger = sugar
	Logger = sugar
}
