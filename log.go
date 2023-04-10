// Copyright (c) quickfixengine.org  All rights reserved.
//
// This file may be distributed under the terms of the quickfixengine.org
// license as defined by quickfixengine.org and appearing in the file
// LICENSE included in the packaging of this file.
//
// This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
// THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE.
//
// See http://www.quickfixengine.org/LICENSE for licensing information.
//
// Contact ask@quickfixengine.org if any conditions of this licensing
// are not clear to you.

package quickfix

import (
	"bytes"
	"fmt"
)

func makeReadable(s []byte) string {
	return string(bytes.Replace(s, []byte("\x01"), []byte("|"), -1))
}

// LogParam ...
type LogParam struct {
	Name   string
	Value  interface{}
	format string
}

// GetFormat ...
func (l LogParam) GetFormat() string {
	return l.format
}

// String ...
func (l LogParam) String() string {
	switch l.format {
	case "%s":
		return fmt.Sprintf("%s=%s", l.Name, l.Value)
	case "%q":
		return fmt.Sprintf("%s=%q", l.Name, l.Value)
	case "%d":
		return fmt.Sprintf("%s=%d", l.Name, l.Value)
	case "%+v":
		return fmt.Sprintf("%s=%+v", l.Name, l.Value)
	default:
		return fmt.Sprintf("%s=%v", l.Name, l.Value)
	}
}

// LogMessage ...
func LogMessage(name string, msgBytes []byte) LogParam {
	return LogParam{
		Name:   name,
		Value:  makeReadable(msgBytes),
		format: "%s",
	}
}

// LogString ...
func LogString(name, value string) LogParam {
	return LogParam{
		Name:   name,
		Value:  value,
		format: "%s",
	}
}

// LogStringWithSingleQuote ...
func LogStringWithSingleQuote(name, value string) LogParam {
	return LogParam{
		Name:   name,
		Value:  value,
		format: "%q",
	}
}

// LogInt ...
func LogInt(name string, value int) LogParam {
	return LogParam{
		Name:   name,
		Value:  value,
		format: "%d",
	}
}

// LogInt64 ...
func LogInt64(name string, value int64) LogParam {
	return LogParam{
		Name:   name,
		Value:  value,
		format: "%d",
	}
}

// LogUint64 ...
func LogUint64(name string, value uint64) LogParam {
	return LogParam{
		Name:   name,
		Value:  value,
		format: "%d",
	}
}

// LogObject ...
func LogObject(name string, value interface{}) LogParam {
	return LogParam{
		Name:   name,
		Value:  value,
		format: "%v",
	}
}

// Log is a generic interface for logging FIX messages and events.
type Log interface {
	// OnIncoming log incoming fix message.
	OnIncoming([]byte)

	// OnOutgoing log outgoing fix message.
	OnOutgoing([]byte)

	// OnEvent log fix event.
	OnEvent(string)

	// OnEventf log fix event according to format specifier.
	OnEventf(string, ...interface{})

	// OnErrorEvent log fix error event
	OnErrorEvent(string, error)

	// OnEventParams log fix event according to logging parameter
	OnEventParams(string, ...LogParam)

	// OnErrorEventParams log fix error event according to logging parameter
	OnErrorEventParams(string, error, ...LogParam)
}

// The LogFactory interface creates global and session specific Log instances.
type LogFactory interface {
	// Create global log.
	Create() (Log, error)

	// CreateSessionLog session specific log.
	CreateSessionLog(sessionID SessionID) (Log, error)
}
