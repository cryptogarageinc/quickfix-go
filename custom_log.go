package quickfix

import (
	"fmt"
)

const (
	LogIncomingMessage string = "FIX incoming"
	LogOutgoingMessage string = "FIX outgoing"
	LogPrefixGlobal    string = "GLOBAL"
)

type customLog struct {
	sessionPrefix string
	logFunc       func(prefix, msg string, keysAndValues ...LogParam)
	logErrorFunc  func(prefix, msg string, err error, keysAndValues ...LogParam)
}

func (l customLog) OnIncoming(s []byte) {
	l.logFunc(l.sessionPrefix, LogIncomingMessage, LogString("incomingMessage", makeReadable(s)))
}

func (l customLog) OnOutgoing(s []byte) {
	l.logFunc(l.sessionPrefix, LogOutgoingMessage, LogString("outgoingMessage", makeReadable(s)))
}

func (l customLog) OnEvent(s string) {
	l.logFunc(l.sessionPrefix, s)
}

func (l customLog) OnEventf(format string, a ...interface{}) {
	l.OnEvent(fmt.Sprintf(format, a...))
}

func (l customLog) OnErrorEvent(message string, err error) {
	l.logErrorFunc(l.sessionPrefix, message, err)
}

func (l customLog) OnEventParams(message string, v ...LogParam) {
	l.logFunc(l.sessionPrefix, message, v...)
}

func (l customLog) OnErrorEventParams(message string, err error, v ...LogParam) {
	l.logErrorFunc(l.sessionPrefix, message, err, v...)
}

type customLogFactory struct {
	logFunc      func(prefix, msg string, keysAndValues ...LogParam)
	logErrorFunc func(prefix, msg string, err error, keysAndValues ...LogParam)
}

func (f customLogFactory) Create() (Log, error) {
	log := customLog{LogPrefixGlobal, f.logFunc, f.logErrorFunc}
	return log, nil
}

func (f customLogFactory) CreateSessionLog(sessionID SessionID) (Log, error) {
	log := customLog{sessionID.String(), f.logFunc, f.logErrorFunc}
	return log, nil
}

// NewCustomLogFactory creates an instance of LogFactory that
// logs messages and events using the provided log function.
func NewCustomLogFactory(
	logFunc func(prefix, msg string, keysAndValues ...LogParam),
	logErrorFunc func(prefix, msg string, err error, keysAndValues ...LogParam),
) LogFactory {
	return customLogFactory{
		logFunc:      logFunc,
		logErrorFunc: logErrorFunc,
	}
}
