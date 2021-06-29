package quickfix

import (
	"bufio"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

//Initiator initiates connections and processes messages for all sessions.
type Initiator struct {
	app             Application
	settings        *Settings
	sessionSettings map[SessionID]*SessionSettings
	storeFactory    MessageStoreFactory
	logFactory      LogFactory
	globalLog       Log
	stopChan        chan interface{}
	wg              sync.WaitGroup
	sessions        map[SessionID]*session
	sessionFactory
}

//Start Initiator.
func (i *Initiator) Start() (err error) {
	i.stopChan = make(chan interface{})

	for sessionID, settings := range i.sessionSettings {
		//TODO: move into session factory
		var tlsConfig *tls.Config
		if tlsConfig, err = loadTLSConfig(settings); err != nil {
			return
		}

		var dialer proxy.Dialer
		if dialer, err = loadDialerConfig(settings); err != nil {
			return
		}

		i.wg.Add(1)
		go func(sessID SessionID) {
			i.handleConnection(i.sessions[sessID], tlsConfig, dialer)
			i.wg.Done()
		}(sessionID)
	}

	return
}

//Stop Initiator.
func (i *Initiator) Stop() {
	select {
	case <-i.stopChan:
		//closed already
		return
	default:
	}
	close(i.stopChan)
	i.wg.Wait()
}

//NewInitiator creates and initializes a new Initiator.
func NewInitiator(app Application, storeFactory MessageStoreFactory, appSettings *Settings, logFactory LogFactory) (*Initiator, error) {
	i := &Initiator{
		app:             app,
		storeFactory:    storeFactory,
		settings:        appSettings,
		sessionSettings: appSettings.SessionSettings(),
		logFactory:      logFactory,
		sessions:        make(map[SessionID]*session),
		sessionFactory:  sessionFactory{true},
	}

	var err error
	i.globalLog, err = logFactory.Create()
	if err != nil {
		return i, err
	}

	for sessionID, s := range i.sessionSettings {
		session, err := i.createSession(sessionID, storeFactory, s, logFactory, app)
		if err != nil {
			return nil, err
		}

		i.sessions[sessionID] = session
	}

	return i, nil
}

//waitForInSessionTime returns true if the session is in session, false if the handler should stop
func (i *Initiator) waitForInSessionTime(session *session) bool {
	inSessionTime := make(chan interface{})
	go func() {
		session.waitForInSessionTime()
		close(inSessionTime)
	}()

	select {
	case <-inSessionTime:
	case <-i.stopChan:
		return false
	}

	return true
}

//waitForReconnectInterval returns true if a reconnect should be re-attempted, false if handler should stop
func (i *Initiator) waitForReconnectInterval(reconnectInterval time.Duration) bool {
	select {
	case <-time.After(reconnectInterval):
	case <-i.stopChan:
		return false
	}

	return true
}

func (i *Initiator) handleConnection(session *session, tlsConfig *tls.Config, dialer proxy.Dialer) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		session.run()
		wg.Done()
	}()

	defer func() {
		session.stop()
		wg.Wait()
	}()

	connectionAttempt := 0

	for {
		if !i.waitForInSessionTime(session) {
			return
		}

		var disconnected chan interface{}
		var msgIn chan fixIn
		var msgOut chan []byte

		address := session.SocketConnectAddress[connectionAttempt%len(session.SocketConnectAddress)]
		session.log.OnEventf("Connecting to: %v", address)

		netConn, err := dialer.Dial("tcp", address)
		if err != nil {
			session.log.OnEventf("Failed to connect: %v", err)
			goto reconnect
		} else if tlsConfig != nil {
			// Unless InsecureSkipVerify is true, server name config is required for TLS
			// to verify the received certificate
			if !tlsConfig.InsecureSkipVerify && len(tlsConfig.ServerName) == 0 {
				serverName := address
				if c := strings.LastIndex(serverName, ":"); c > 0 {
					serverName = serverName[:c]
				}
				tlsConfig.ServerName = serverName
			}
			tlsConn := tls.Client(netConn, tlsConfig)
			if err = tlsConn.Handshake(); err != nil {
				session.log.OnEventf("Failed handshake: %v", err)
				goto reconnect
			}
			netConn = tlsConn
		}

		msgIn = make(chan fixIn)
		msgOut = make(chan []byte)
		if err := session.connect(msgIn, msgOut); err != nil {
			session.log.OnEventf("Failed to initiate: %v", err)
			goto reconnect
		}

		go readLoop(newParser(bufio.NewReader(netConn)), msgIn)
		disconnected = make(chan interface{})
		go func() {
			writeLoop(netConn, msgOut, session.log)
			if err := netConn.Close(); err != nil {
				session.log.OnEvent(err.Error())
			}
			close(disconnected)
		}()

		select {
		case <-disconnected:
		case <-i.stopChan:
			return
		}

	reconnect:
		connectionAttempt++
		session.log.OnEventf("Reconnecting in %v", session.ReconnectInterval)
		if !i.waitForReconnectInterval(session.ReconnectInterval) {
			return
		}
	}
}

// append API ------------------------------------------------------------------

// GetAliveSessionIDs This function returns loggedOn sessionID list.
func (i *Initiator) GetAliveSessionIDs() []SessionID {
	sessionIds := make([]SessionID, 0, len(i.sessions))
	for sessionID, session := range i.sessions {
		if session.IsLoggedOn() {
			sessionIds = append(sessionIds, sessionID)
		}
	}
	return sessionIds
}

// IsAliveSession This function checks if the session is a logged on session or not.
func (i *Initiator) IsAliveSession(sessionID SessionID) bool {
	session, ok := i.sessions[sessionID]
	if ok {
		return session.IsLoggedOn()
	}
	return false
}

// SendToAliveSession This function send message for logged on session.
func (i *Initiator) SendToAliveSession(m Messagable, sessionID SessionID) error {
	msg := m.ToMessage()
	session, ok := i.sessions[sessionID]
	if !ok {
		return errDoNotLoggedOnSession
	}
	if !session.IsLoggedOn() {
		return errDoNotLoggedOnSession
	}
	return session.queueForSend(msg)
}

// SendToAliveSessions This function send messages for logged on sessions.
func (i *Initiator) SendToAliveSessions(m Messagable) (err error) {
	sessionIDs := i.GetAliveSessionIDs()

	errorByID := ErrorBySessionID{ErrorMap: make(map[SessionID]error)}
	baseMsg := m.ToMessage()
	for _, sessionID := range sessionIDs {
		msg := NewMessage()
		baseMsg.CopyInto(msg)
		var msgType FIXString
		if tmpErr := baseMsg.Header.GetField(tagMsgType, &msgType); tmpErr != nil {
			errorByID.ErrorMap[sessionID] = tmpErr
			continue
		}
		msg.Header.SetField(tagMsgType, msgType)
		msg = fillHeaderBySessionID(msg, sessionID)
		tmpErr := i.SendToAliveSession(msg, sessionID)
		if tmpErr != nil {
			errorByID.ErrorMap[sessionID] = tmpErr
		}
	}
	if len(errorByID.ErrorMap) > 0 {
		err = &errorByID
		errorByID.error = errors.New("failed to SendToAliveSessions")
	}
	return err
}
