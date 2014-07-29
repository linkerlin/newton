package cstream

// The code is borrowed from Koding's Kite.

import (
	"os"
	//"os/signal"
	"strings"
	//"syscall"
	"github.com/koding/logging"
)

// Level indicates the logging level
type Level int

var (
	debugMode   bool
	processName = strings.ToUpper(os.Args[0])
)

// Logging levels.
const (
	FATAL Level = iota
	ERROR
	WARNING
	INFO
	DEBUG
)

// Logger is the interface used to log messages in different levels.
type Logger interface {
	// Fatal logs to the FATAL, ERROR, WARNING, INFO and DEBUG levels,
	// including a stack trace of all running goroutines, then calls
	// os.Exit(1).
	Fatal(format string, args ...interface{})

	// Error logs to the ERROR, WARNING, INFO and DEBUG level.
	Error(format string, args ...interface{})

	// Warning logs to the WARNING, INFO and DEBUG level.
	Warning(format string, args ...interface{})

	// Info logs to the INFO and DEBUG level.
	Info(format string, args ...interface{})

	// Debug logs to the DEBUG level.
	Debug(format string, args ...interface{})
}

// GetLogLevel returns the logging level defined via the NEWTON_LOG_LEVEL
// environment. It returns Info by default if no environment variable
// is set.
func GetLogLevel() Level {
	envVar := processName + "_LOG_LEVEL"
	switch strings.ToUpper(os.Getenv(envVar)) {
	case "DEBUG":
		return DEBUG
	case "WARNING":
		return WARNING
	case "ERROR":
		return ERROR
	case "FATAL":
		return FATAL
	default:
		return INFO
	}
}

// ConvertLevel converst a newton level into logging level
func ConvertLevel(l Level) logging.Level {
	switch l {
	case DEBUG:
		return logging.DEBUG
	case WARNING:
		return logging.WARNING
	case ERROR:
		return logging.ERROR
	case FATAL:
		return logging.CRITICAL
	default:
		return logging.INFO
	}
}

// NewLogger returns a new newton logger based on koding/logging package and a
// SetLogLvel function. The current logLevel is INFO by default, which can be
// changed with NEWTON_LOG_LEVEL environment variable.
func NewLogger(name string) (Logger, func(Level)) {
	logger := logging.NewLogger(name)
	logger.SetLevel(ConvertLevel(GetLogLevel()))

	envVar := processName + "_LOG_NOCOLOR"
	if os.Getenv(envVar) != "" {
		logging.StdoutHandler.Colorize = false
		logging.StderrHandler.Colorize = false
	}

	setLevel := func(l Level) {
		logger.SetLevel(ConvertLevel(l))
	}

	return logger, setLevel
}

// SetupSignalHandler listens to signals and toggles the log level to DEBUG
// mode when it received a SIGUSR2 signal. Another SIGUSR2 toggles the log
// level back to the old level.
/*func (k *Kite) SetupSignalHandler() {
    c := make(chan os.Signal, 1)

    signal.Notify(c, syscall.SIGUSR2)
    go func() {
        for s := range c {
            k.Log.Info("Got signal: %s", s)

            if debugMode {
                // toogle back to old settings.
                k.Log.Info("Disabling debug mode")
                if k.SetLogLevel == nil {
                    k.Log.Error("SetLogLevel is not defined")
                    continue
                }

                k.SetLogLevel(getLogLevel())
                debugMode = false
            } else {
                k.Log.Info("Enabling debug mode")
                if k.SetLogLevel == nil {
                    k.Log.Error("SetLogLevel is not defined")
                    continue
                }

                k.SetLogLevel(DEBUG)
                debugMode = true
            }
        }
    }()
}*/
