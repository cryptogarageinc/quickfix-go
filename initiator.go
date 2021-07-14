package quickfix

import (
	"bufio"
	"crypto/tls"
	"errors"
	"math"
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

	// append member
	stateChangeChan chan SessionID
	registerChan    chan notifyRequestData
	unregisterChan  chan uint64
	notifyCounter   uint64
	counterMap      map[uint64]bool
	counterMutex    sync.Mutex
}

//Start Initiator.
func (i *Initiator) Start() (err error) {
	i.stopChan = make(chan interface{})
	i.stateChangeChan = make(chan SessionID)
	i.registerChan = make(chan notifyRequestData)
	i.unregisterChan = make(chan uint64)

	i.wg.Add(1)
	go i.checkStateLoop()

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
			i.sessions[sessID].stateChangeChan = i.stateChangeChan
			i.handleConnection(i.sessions[sessID], tlsConfig, dialer)
			i.wg.Done()
		}(sessionID)
	}

	return
}

//Stop Initiator.
func (i *Initiator) Stop() {
	defer func() {
		close(i.stateChangeChan)
		close(i.registerChan)
		close(i.unregisterChan)
	}()
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
		counterMap:      make(map[uint64]bool),
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

var ErrWaitCancel = errors.New("wait cancel")
var ErrWaitTimeout = errors.New("wait timeout")
var ErrInternalError = errors.New("internal error")

type notifyCheckResponse struct {
	canSendResponse bool
	err             error
}

type notifyRequestData struct {
	id        uint64
	checkFunc func(SessionID) notifyCheckResponse
	response  chan error
}

func (i *Initiator) checkStateLoop() {
	defer i.wg.Done()
	notifyMap := make(map[uint64]notifyRequestData)

	for {
		select {
		case <-i.stopChan:
			return
		case data, ok := <-i.registerChan:
			if ok {
				notifyMap[data.id] = data
			}
		case id, ok := <-i.unregisterChan:
			if ok {
				delete(notifyMap, id)
			}
		case sessionID, ok := <-i.stateChangeChan:
			if !ok {
				// do nothing
			} else if sess, exist := i.sessions[sessionID]; exist && sess.IsLoggedOn() {
				for _, data := range notifyMap {
					if resp := data.checkFunc(sessionID); resp.canSendResponse {
						data.response <- resp.err
					}
				}
			}
		}
	}
}

func (i *Initiator) keepNotifyCounter() uint64 {
	i.counterMutex.Lock()
	defer i.counterMutex.Unlock()
	count := i.notifyCounter
	for {
		if count < math.MaxUint64 {
			count++
		} else {
			count = 1
		}
		_, ok := i.counterMap[count]
		if !ok {
			i.counterMap[count] = true
			i.notifyCounter = count
			return count
		}
	}
}

func (i *Initiator) releaseNotifyCounter(count uint64) {
	i.counterMutex.Lock()
	defer i.counterMutex.Unlock()
	delete(i.counterMap, count)
}

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
func (i *Initiator) SendToAliveSession(m Messagable, sessionID SessionID) (err error) {
	if !i.IsAliveSession(sessionID) {
		err = ErrDoNotLoggedOnSession
	} else {
		err = SendToAliveSession(m, sessionID)
	}
	return err
}

// SendToAliveSessions This function send messages for logged on sessions.
func (i *Initiator) SendToAliveSessions(m Messagable) (err error) {
	sessionIDs := i.GetAliveSessionIDs()
	err = sendToSessions(m, sessionIDs)
	if err != nil {
		errObj := err.(*ErrorBySessionID)
		errObj.error = errors.New("failed to SendToAliveSessions")
	}
	return err
}

// WaitForAliveSessionFirst This function wait for the session to become alive first.
func (i *Initiator) WaitForAliveSessionFirst() error {
	return i.WaitForAliveSessionFirstTimeout(time.Hour * 24 * 365 * 10)
}

// WaitForAliveSessionFirstTimeout This function wait for the session to become alive first.
func (i *Initiator) WaitForAliveSessionFirstTimeout(timeout time.Duration) error {
	sessions := i.GetAliveSessionIDs()
	if len(sessions) > 0 {
		return nil
	}
	data := notifyRequestData{
		id: i.keepNotifyCounter(),
		checkFunc: func(_sessionId SessionID) notifyCheckResponse {
			sessions := i.GetAliveSessionIDs()
			resp := notifyCheckResponse{}
			if len(sessions) > 0 {
				resp.canSendResponse = true
			}
			return resp
		},
	}
	defer i.releaseNotifyCounter(data.id)
	return i.waitForAliveSessionTimeout(data, timeout)
}

// WaitForAliveSessionAll This function wait for all sessions to become alive.
func (i *Initiator) WaitForAliveSessionAll() error {
	return i.WaitForAliveSessionAllTimeout(time.Hour * 24 * 365 * 10)
}

// WaitForAliveSessionAllTimeout This function wait for all sessions to become alive.
func (i *Initiator) WaitForAliveSessionAllTimeout(timeout time.Duration) error {
	sessions := i.GetAliveSessionIDs()
	if len(sessions) == len(i.sessions) {
		return nil
	}
	data := notifyRequestData{
		id: i.keepNotifyCounter(),
		checkFunc: func(_sessionId SessionID) notifyCheckResponse {
			sessions := i.GetAliveSessionIDs()
			resp := notifyCheckResponse{}
			if len(sessions) == len(i.sessions) {
				resp.canSendResponse = true
			}
			return resp
		},
	}
	defer i.releaseNotifyCounter(data.id)
	return i.waitForAliveSessionTimeout(data, timeout)
}

// waitForAliveSessionTimeout This function wait for sessions to become alive.
func (i *Initiator) waitForAliveSessionTimeout(req notifyRequestData, timeout time.Duration) error {
	select {
	case <-i.stopChan:
		return ErrWaitCancel //closed already
	default:
	}
	data := req
	data.response = make(chan error)
	defer close(data.response)

	i.registerChan <- data
	defer func() {
		select {
		case <-i.stopChan:
			// closed already
			break
		default:
			i.unregisterChan <- data.id
		}
	}()

	waitTimer := time.NewTimer(timeout)
	defer waitTimer.Stop()
	var err error

	select {
	case <-i.stopChan:
		err = ErrWaitCancel
		return err
	case resp, ok := <-data.response:
		if ok {
			err = resp
		} else {
			err = ErrInternalError
		}
		return err
	case <-waitTimer.C:
		err = ErrWaitTimeout
		return err
	}
}
