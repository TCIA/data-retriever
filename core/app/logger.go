package app

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
	"time"
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
func setLogger(debug bool, logfile string) {
	encoder := newEncoderConfig()
	level := zap.WarnLevel
	if debug {
		level = zap.DebugLevel
	}

	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoder), zapcore.AddSync(os.Stdout), level)
	loggerInstance := zap.New(core, zap.AddCaller())
	if logfile != "" {
		_ = os.MkdirAll(filepath.Dir(logfile), os.ModePerm)
		f, err := os.OpenFile(logfile, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, os.ModePerm)
		if err != nil {
			loggerInstance.Sugar().Warnf("failed save log to %s: %v", logfile, err)
		} else {
			core = zapcore.NewTee(
				zapcore.NewCore(zapcore.NewJSONEncoder(encoder), zapcore.AddSync(f), zap.DebugLevel),
				zapcore.NewCore(zapcore.NewConsoleEncoder(encoder), zapcore.AddSync(os.Stdout), level),
			)
		}
		loggerInstance = zap.New(core, zap.AddCaller())
	}

	defer func() { _ = loggerInstance.Sync() }()
	sugar := loggerInstance.Sugar()
	logger = sugar
	Logger = sugar
}
