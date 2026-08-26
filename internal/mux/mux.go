package mux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/b-nnett/codex-subscription-router/internal/backend"
	"github.com/b-nnett/codex-subscription-router/internal/protocol"
	"github.com/b-nnett/codex-subscription-router/internal/state"
)

const requestTimeout = 30 * time.Second

type Options struct {
	RealExecutable string
	RealArgs       []string
	Environment    []string
	Store          *state.Store
	Output         io.Writer
}

type externalRoute struct {
	accountID string
	method    string
	message   protocol.Message
	excluded  map[string]struct{}
}

type activeTurn struct {
	accountID     string
	turnID        string
	params        json.RawMessage
	excluded      map[string]struct{}
	generation    uint64
	lastActivity  time.Time
	recovering    bool
	parked        bool
	recoveryCause string
	failureRaw    []byte
}

type serverRequestRoute struct {
	accountID string
	original  json.RawMessage
}

type Event struct {
	Type      string `json:"type"`
	AccountID string `json:"accountId,omitempty"`
	Message   string `json:"message,omitempty"`
	Data      any    `json:"data,omitempty"`
}

// Multiplexer presents one app-server connection to ChatGPT.app while owning
// one real app-server process per ChatGPT subscription.
type Multiplexer struct {
	realExecutable string
	realArgs       []string
	environment    []string
	store          *state.Store
	output         io.Writer

	childrenMu sync.RWMutex
	children   map[string]*backend.Child
	inbound    chan backend.Inbound

	initializationMu sync.RWMutex
	initializeParams json.RawMessage
	initialized      bool

	externalMu     sync.Mutex
	externalRoutes map[string]externalRoute

	activeTurnMu sync.Mutex
	activeTurns  map[string]activeTurn

	quotaMu      sync.Mutex
	quotaBlocked map[string]time.Time

	lineageMu     sync.RWMutex
	threadParents map[string]string

	staleMu      sync.RWMutex
	staleSources map[string]map[string]struct{}

	commandMu   sync.Mutex
	commandPIDs map[string]map[int]string

	runCtx context.Context

	serverMu       sync.Mutex
	serverRoutes   map[string]serverRequestRoute
	serverSequence atomic.Uint64

	outputMu sync.Mutex
	eventsMu sync.RWMutex
	events   map[chan Event]struct{}

	profileMu     sync.Mutex
	profileClient *http.Client
	profileCache  map[string]profileCacheEntry
	now           func() time.Time

	resetCreditsMu       sync.Mutex
	resetCreditsCache    map[string]resetCreditsCacheEntry
	resetCreditsEndpoint string

	previewMu        sync.RWMutex
	rateLimitPreview *RateLimitPreview

	resetPreviewMu sync.RWMutex
	resetPreviews  map[string]ResetCreditsPreview
}

func New(options Options) (*Multiplexer, error) {
	if options.RealExecutable == "" || options.Store == nil || options.Output == nil {
		return nil, errors.New("real executable, store, and output are required")
	}
	return &Multiplexer{
		realExecutable:       options.RealExecutable,
		realArgs:             append([]string(nil), options.RealArgs...),
		environment:          append([]string(nil), options.Environment...),
		store:                options.Store,
		output:               options.Output,
		children:             make(map[string]*backend.Child),
		inbound:              make(chan backend.Inbound, 1024),
		externalRoutes:       make(map[string]externalRoute),
		activeTurns:          make(map[string]activeTurn),
		quotaBlocked:         make(map[string]time.Time),
		threadParents:        make(map[string]string),
		staleSources:         make(map[string]map[string]struct{}),
		commandPIDs:          make(map[string]map[int]string),
		serverRoutes:         make(map[string]serverRequestRoute),
		events:               make(map[chan Event]struct{}),
		profileClient:        &http.Client{Timeout: 10 * time.Second},
		profileCache:         make(map[string]profileCacheEntry),
		now:                  time.Now,
		resetCreditsCache:    make(map[string]resetCreditsCacheEntry),
		resetCreditsEndpoint: rateLimitResetCreditsURL,
		resetPreviews:        make(map[string]ResetCreditsPreview),
	}, nil
}

func (m *Multiplexer) Start(ctx context.Context) error {
	m.runCtx = ctx

	for _, account := range m.store.Accounts() {
		if _, err := m.startChild(ctx, account); err != nil {
			fmt.Fprintf(os.Stderr, "codex-mux: start account %s: %v\n", account.ID, err)
		}
	}
	if len(m.childEntries()) == 0 {
		return errors.New("no Codex app-server process could be started")
	}
	go m.inboundLoop(ctx)
	go m.syncManagedConfigLoop(ctx)
	go m.recoveryWatchLoop(ctx)
	return nil
}

func (m *Multiplexer) syncManagedConfigLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.store.SyncManagedConfig(); err != nil {
				fmt.Fprintf(os.Stderr, "codex-mux: sync shared plugin config: %v\n", err)
			}
		}
	}
}

func (m *Multiplexer) Close() {
	for _, entry := range m.childEntries() {
		_ = entry.child.Close()
	}
}

func (m *Multiplexer) HandleClient(message protocol.Message) {
	if message.Method == "" && len(message.ID) > 0 {
		m.handleServerRequestResponse(message)
		return
	}
	if message.Method == "initialize" && len(message.ID) > 0 {
		go m.initialize(message)
		return
	}
	if len(message.ID) == 0 {
		m.handleClientNotification(message)
		return
	}

	switch message.Method {
	case "thread/list":
		go m.aggregateThreadList(message)
	case "thread/start":
		go m.routeNewThread(message)
	case "account/rateLimits/read":
		go m.routeAggregatedRateLimits(message)
	default:
		m.routeExistingRequest(message)
	}
}

func codexMuxChildInitializeParams(params json.RawMessage) json.RawMessage {
	var decoded map[string]any
	if err := json.Unmarshal(params, &decoded); err != nil {
		return append(json.RawMessage(nil), params...)
	}

	capabilities, _ := decoded["capabilities"].(map[string]any)
	if capabilities == nil {
		capabilities = make(map[string]any)
		decoded["capabilities"] = capabilities
	}

	// Cross-account thread migration uses thread/resume.path, which is
	// currently an experimental Codex app-server API field.
	capabilities["experimentalApi"] = true

	encoded, err := json.Marshal(decoded)
	if err != nil {
		return append(json.RawMessage(nil), params...)
	}

	return encoded
}

func (m *Multiplexer) initialize(message protocol.Message) {
	childInitializeParams := codexMuxChildInitializeParams(message.Params)

	m.initializationMu.Lock()
	m.initializeParams = append(json.RawMessage(nil), childInitializeParams...)
	m.initializationMu.Unlock()

	var firstResult json.RawMessage
	var firstErr error
	for _, entry := range m.childEntries() {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		response, err := entry.child.Request(ctx, "initialize", childInitializeParams)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if firstResult == nil {
			firstResult = response.Result
		}
	}
	if firstResult == nil {
		m.write(protocol.Failure(message.ID, -32000, fmt.Sprintf("failed to initialize account pool: %v", firstErr)))
		return
	}
	m.write(protocol.Success(message.ID, firstResult))
}

func (m *Multiplexer) handleClientNotification(message protocol.Message) {
	if message.Method == "initialized" {
		m.initializationMu.Lock()
		m.initialized = true
		m.initializationMu.Unlock()

		for _, entry := range m.childEntries() {
			_ = entry.child.Send(message)
		}

		// All subscription children are now initialized. Push one authoritative
		// pooled rate-limit snapshot so a depleted controller account cannot
		// leave the desktop UI stuck in a stale rate-limit state.
		go m.publishAggregatedRateLimits()

		return
	}
	if controller, ok := m.controllerChild(); ok {
		_ = controller.Send(message)
	}
}

func (m *Multiplexer) routeNewThread(message protocol.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	account, reason, err := m.chooseAccount(ctx)
	if err != nil {
		if errors.Is(err, errNoSubscriptionCapacity) {
			m.write(m.allSubscriptionsDepleted(ctx, message.ID))
			return
		}
		m.write(protocol.Failure(message.ID, -32020, err.Error()))
		return
	}
	if err := m.forward(account.ID, message); err != nil {
		m.write(protocol.Failure(message.ID, -32021, err.Error()))
		return
	}
	m.publish(Event{
		Type:      "thread-routed",
		AccountID: account.ID,
		Message:   fmt.Sprintf("New chat pinned to %s", account.Label),
		Data:      reason,
	})
}

func (m *Multiplexer) routeExistingRequest(message protocol.Message) {
	accountID := ""
	if scopedAccountID, cleanedParams, ok := scopedPluginRequest(message.Method, message.Params); ok {
		if account, exists := m.store.Account(scopedAccountID); exists && account.Enabled {
			message.Params = cleanedParams
			if err := m.forward(scopedAccountID, message); err != nil {
				m.write(protocol.Failure(message.ID, -32023, err.Error()))
			}
			return
		}
	}
	threadID := threadIDFromParams(message.Params)
	if threadID != "" {
		accountID, _ = m.store.ThreadOwner(threadID)
	}
	if accountID == "" {
		if controller, ok := m.store.Controller(); ok {
			accountID = controller.ID
		}
	}
	if accountID == "" {
		m.write(protocol.Failure(message.ID, -32022, "no controller account is configured"))
		return
	}
	if message.Method == "turn/interrupt" && threadID != "" {
		m.cancelRecoveryForUser(threadID)
	}
	if message.Method == "turn/start" && threadID != "" {
		m.cancelParkedRecoveryForUser(threadID)
		go m.routeTurnStart(message, threadID, accountID)
		return
	}
	if err := m.forward(accountID, message); err != nil {
		m.write(protocol.Failure(message.ID, -32023, err.Error()))
	}
}

func (m *Multiplexer) forward(accountID string, message protocol.Message) error {
	return m.forwardWithExclusions(accountID, message, nil)
}

func (m *Multiplexer) forwardWithExclusions(accountID string, message protocol.Message, excluded map[string]struct{}) error {
	child, ok := m.child(accountID)
	if !ok {
		return fmt.Errorf("account %s is unavailable", accountID)
	}
	key := protocol.RequestIDKey(message.ID)
	m.externalMu.Lock()
	m.externalRoutes[key] = externalRoute{
		accountID: accountID,
		method:    message.Method,
		message:   message,
		excluded:  cloneAccountSet(excluded),
	}
	m.externalMu.Unlock()
	if err := child.Send(message); err != nil {
		m.externalMu.Lock()
		delete(m.externalRoutes, key)
		m.externalMu.Unlock()
		return err
	}

	m.rememberActiveTurn(accountID, message, excluded)

	return nil
}

func (m *Multiplexer) rememberActiveTurn(
	accountID string,
	message protocol.Message,
	excluded map[string]struct{},
) {
	if message.Method != "turn/start" {
		return
	}

	threadID := threadIDFromParams(message.Params)
	if threadID == "" {
		return
	}

	m.activeTurnMu.Lock()
	active := m.activeTurns[threadID]
	active.accountID = accountID
	active.params = append(json.RawMessage(nil), message.Params...)
	active.excluded = cloneAccountSet(excluded)
	active.lastActivity = m.now()
	active.recovering = false
	active.parked = false
	active.failureRaw = nil
	active.recoveryCause = ""
	m.activeTurns[threadID] = active
	m.activeTurnMu.Unlock()
}

func (m *Multiplexer) activeTurnFor(
	threadID string,
	accountID string,
) (activeTurn, bool) {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[threadID]
	if !ok {
		return activeTurn{}, false
	}

	if accountID != "" && active.accountID != accountID {
		return activeTurn{}, false
	}

	active.params = append(json.RawMessage(nil), active.params...)
	active.excluded = cloneAccountSet(active.excluded)
	active.failureRaw = append([]byte(nil), active.failureRaw...)

	return active, true
}

func (m *Multiplexer) clearActiveTurn(
	threadID string,
	accountID string,
) {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[threadID]
	if !ok {
		return
	}

	if accountID != "" && active.accountID != accountID {
		return
	}

	delete(m.activeTurns, threadID)
}

func (m *Multiplexer) routeAggregatedRateLimits(message protocol.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	rateLimits, err := m.AggregatedRateLimits(ctx)
	if err != nil {
		m.write(protocol.Failure(message.ID, -32024, err.Error()))
		return
	}
	result, err := json.Marshal(map[string]any{"rateLimits": rateLimits})
	if err != nil {
		m.write(protocol.Failure(message.ID, -32025, err.Error()))
		return
	}
	m.write(protocol.Success(message.ID, result))
}

func (m *Multiplexer) routeTurnStart(
	message protocol.Message,
	threadID string,
	ownerID string,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
	snapshot, err := m.accountSnapshotWithProfile(ctx, ownerID, false)
	cancel()

	if err == nil &&
		!m.accountQuotaBlocked(ownerID) &&
		accountHasCapacity(snapshot) {
		if err := m.forward(ownerID, message); err != nil {
			m.write(protocol.Failure(message.ID, -32023, err.Error()))
		}
		return
	}

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: owner %s capacity check failed for thread %s: %v; failing closed\n",
			ownerID,
			threadID,
			err,
		)
	}

	excluded := map[string]struct{}{ownerID: {}}
	go m.robustFailoverTurn(message, threadID, ownerID, excluded)
}

func (m *Multiplexer) failoverTurn(
	ctx context.Context,
	message protocol.Message,
	threadID string,
	sourceAccountID string,
	excluded map[string]struct{},
) {
	_ = ctx
	m.robustFailoverTurn(message, threadID, sourceAccountID, excluded)
}

func (m *Multiplexer) mirrorRolloutToAccount(
	sourcePath string,
	targetAccountID string,
) (string, error) {
	targetAccount, ok := m.store.Account(targetAccountID)
	if !ok {
		return "", fmt.Errorf(
			"target subscription %q not found",
			targetAccountID,
		)
	}

	sourcePath = filepath.Clean(sourcePath)

	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf(
			"read source rollout %q: %w",
			sourcePath,
			err,
		)
	}

	// Preserve the normal sessions/YYYY/MM/DD/... layout where possible.
	const sessionsMarker = string(filepath.Separator) +
		"sessions" +
		string(filepath.Separator)

	relative := filepath.Base(sourcePath)

	if index := strings.LastIndex(
		sourcePath,
		sessionsMarker,
	); index >= 0 {
		relative = sourcePath[index+len(sessionsMarker):]
	}

	targetPath := filepath.Join(
		targetAccount.CodexHome,
		"sessions",
		relative,
	)

	if err := os.MkdirAll(
		filepath.Dir(targetPath),
		0o700,
	); err != nil {
		return "", fmt.Errorf(
			"create target rollout directory: %w",
			err,
		)
	}

	temporary := targetPath + ".codex-mux.tmp"

	if err := os.WriteFile(
		temporary,
		data,
		0o600,
	); err != nil {
		return "", fmt.Errorf(
			"write target rollout: %w",
			err,
		)
	}

	if err := os.Rename(
		temporary,
		targetPath,
	); err != nil {
		_ = os.Remove(temporary)

		return "", fmt.Errorf(
			"commit target rollout: %w",
			err,
		)
	}

	fmt.Fprintf(
		os.Stderr,
		"codex-mux: mirrored rollout %q -> %q\n",
		sourcePath,
		targetPath,
	)

	return targetPath, nil
}

func (m *Multiplexer) resumeThreadOnAccount(
	ctx context.Context,
	threadID string,
	sourceAccountID string,
	targetAccountID string,
) error {
	return m.robustResumeThreadOnAccount(
		ctx,
		threadID,
		sourceAccountID,
		targetAccountID,
	)
}

func (m *Multiplexer) handleServerRequestResponse(message protocol.Message) {
	key := protocol.RequestIDKey(message.ID)
	m.serverMu.Lock()
	route, ok := m.serverRoutes[key]
	if ok {
		delete(m.serverRoutes, key)
	}
	m.serverMu.Unlock()
	if !ok {
		return
	}
	message.ID = route.original
	if child, exists := m.child(route.accountID); exists {
		_ = child.Send(message)
	}
}

func (m *Multiplexer) inboundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case inbound := <-m.inbound:
			m.handleInbound(inbound)
		}
	}
}

func (m *Multiplexer) handleInbound(inbound backend.Inbound) {
	if inbound.Exited {
		go m.handleChildExit(inbound.AccountID, inbound.ExitDetail)
		return
	}

	message := inbound.Message

	if message.Method == "" && len(message.ID) > 0 {
		key := protocol.RequestIDKey(message.ID)
		m.externalMu.Lock()
		route, ok := m.externalRoutes[key]
		if ok {
			delete(m.externalRoutes, key)
		}
		m.externalMu.Unlock()

		if ok {
			if route.method == "turn/start" && isUsageLimitResponse(message) {
				m.markAccountQuotaBlocked(inbound.AccountID)
				go m.retryTurnAfterUsageLimitRobust(route, inbound.AccountID)
				return
			}
			m.learnThreadOwner(route, inbound.AccountID, message.Result)
			m.writeRaw(inbound.Raw)
		}
		return
	}

	if message.Method != "" && len(message.ID) > 0 {
		m.forwardServerRequest(inbound)
		return
	}

	if m.observeRecoveryNotification(inbound) {
		return
	}

	if message.Method == "account/rateLimits/updated" {
		go m.forwardAggregatedRateLimitNotification(inbound.Raw)
		return
	}

	if message.Method == "turn/completed" ||
		message.Method == "account/login/completed" ||
		message.Method == "account/updated" {
		go m.publishAccountRefresh(inbound.AccountID)
	}

	if m.shouldForwardInbound(inbound) {
		m.writeRaw(inbound.Raw)
	}
}

func (m *Multiplexer) publishAggregatedRateLimits() {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	rateLimits, err := m.AggregatedRateLimits(ctx)
	if err != nil {
		return
	}

	params, err := json.Marshal(map[string]any{
		"rateLimits": rateLimits,
	})
	if err != nil {
		return
	}

	m.write(protocol.Message{
		Method: "account/rateLimits/updated",
		Params: params,
	})
}

func (m *Multiplexer) forwardAggregatedRateLimitNotification(fallback []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	rateLimits, err := m.AggregatedRateLimits(ctx)
	if err != nil {
		m.writeRaw(fallback)
		return
	}
	params, err := json.Marshal(map[string]any{"rateLimits": rateLimits})
	if err != nil {
		m.writeRaw(fallback)
		return
	}
	m.write(protocol.Message{Method: "account/rateLimits/updated", Params: params})
}

func (m *Multiplexer) retryTurnAfterUsageLimit(
	route externalRoute,
	exhaustedAccountID string,
) {
	m.retryTurnAfterUsageLimitRobust(route, exhaustedAccountID)
}

func (m *Multiplexer) handleTrackedTurnCompleted(
	inbound backend.Inbound,
) {
	raw := append([]byte(nil), inbound.Raw...)

	threadID, explicitUsageLimit :=
		turnCompletedUsageLimit(inbound.Message.Params)

	if threadID == "" {
		m.writeRaw(raw)
		return
	}

	if _, ok := m.activeTurnFor(
		threadID,
		inbound.AccountID,
	); !ok {
		m.writeRaw(raw)
		return
	}

	// Normal explicit quota failure.
	if explicitUsageLimit {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: explicit quota completion thread=%s account=%s\n",
			threadID,
			inbound.AccountID,
		)

		m.continueAutonomousTurnAfterUsageLimit(
			threadID,
			inbound.AccountID,
			raw,
		)
		return
	}

	silentThreadID, suspicious :=
		silentCompletedTurn(inbound.Message.Params)

	if !suspicious || silentThreadID != threadID {
		m.clearActiveTurn(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(raw)
		return
	}

	// Do not assume every empty completion is quota-related.
	// Verify the account's current quota before migrating.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		requestTimeout,
	)
	defer cancel()

	snapshot, err := m.accountSnapshot(
		ctx,
		inbound.AccountID,
	)

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: suspicious empty completion thread=%s account=%s; quota check failed: %v\n",
			threadID,
			inbound.AccountID,
			err,
		)

		m.clearActiveTurn(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(raw)
		return
	}

	if rateLimitsHaveCapacity(snapshot.RateLimits) {
		// It really was just an empty normal completion.
		m.clearActiveTurn(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(raw)
		return
	}

	fmt.Fprintf(
		os.Stderr,
		"codex-mux: silent quota completion detected thread=%s account=%s; failing over\n",
		threadID,
		inbound.AccountID,
	)

	m.continueAutonomousTurnAfterUsageLimit(
		threadID,
		inbound.AccountID,
		raw,
	)
}

func (m *Multiplexer) continueAutonomousTurnAfterUsageLimit(
	threadID string,
	exhaustedAccountID string,
	failedNotification []byte,
) {
	active, ok := m.activeTurnFor(
		threadID,
		exhaustedAccountID,
	)

	if !ok {
		m.writeRaw(failedNotification)
		return
	}

	excluded := cloneAccountSet(active.excluded)
	if excluded == nil {
		excluded = make(map[string]struct{})
	}

	excluded[exhaustedAccountID] = struct{}{}

	sourceAccountID := exhaustedAccountID

	for {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			2*requestTimeout,
		)

		fallback, _, err := m.chooseAccountExcluding(
			ctx,
			excluded,
		)

		if err != nil {
			cancel()

			m.clearActiveTurn(threadID, "")
			m.writeRaw(failedNotification)

			m.publish(Event{
				Type:    "autonomous-failover-stopped",
				Message: "Autonomous task stopped because all subscriptions are depleted",
				Data: map[string]any{
					"threadId": threadID,
				},
			})

			return
		}

		targetAccountID := fallback.ID

		if err := m.resumeThreadOnAccount(
			ctx,
			threadID,
			sourceAccountID,
			targetAccountID,
		); err != nil {
			cancel()

			excluded[targetAccountID] = struct{}{}

			fmt.Fprintf(
				os.Stderr,
				"codex-mux: autonomous resume %s -> %s failed: %v\n",
				sourceAccountID,
				targetAccountID,
				err,
			)

			continue
		}

		if err := m.store.SetThreadOwner(
			threadID,
			targetAccountID,
		); err != nil {
			cancel()

			m.clearActiveTurn(threadID, "")
			m.writeRaw(failedNotification)

			fmt.Fprintf(
				os.Stderr,
				"codex-mux: autonomous owner update failed: %v\n",
				err,
			)

			return
		}

		continuationParams, err := continuationTurnParams(
			active.params,
			threadID,
		)

		if err != nil {
			cancel()

			m.clearActiveTurn(threadID, "")
			m.writeRaw(failedNotification)
			return
		}

		target, ok := m.child(targetAccountID)
		if !ok {
			cancel()
			excluded[targetAccountID] = struct{}{}
			sourceAccountID = targetAccountID
			continue
		}

		// Install state before starting the turn so even a very fast
		// usage-limit failure can be associated with this continuation.
		m.activeTurnMu.Lock()
		m.activeTurns[threadID] = activeTurn{
			accountID: targetAccountID,
			params: append(
				json.RawMessage(nil),
				continuationParams...,
			),
			excluded: cloneAccountSet(excluded),
		}
		m.activeTurnMu.Unlock()

		_, err = target.Request(
			ctx,
			"turn/start",
			continuationParams,
		)

		cancel()

		if err != nil {
			if usageLimitText(err.Error()) {
				excluded[targetAccountID] = struct{}{}
				sourceAccountID = targetAccountID
				continue
			}

			m.clearActiveTurn(
				threadID,
				targetAccountID,
			)

			m.writeRaw(failedNotification)

			fmt.Fprintf(
				os.Stderr,
				"codex-mux: autonomous continuation failed on %s: %v\n",
				targetAccountID,
				err,
			)

			return
		}

		m.publish(Event{
			Type:      "thread-autonomous-failed-over",
			AccountID: targetAccountID,
			Message: fmt.Sprintf(
				"Autonomous chat continued with %s",
				fallback.Label,
			),
			Data: map[string]any{
				"threadId":          threadID,
				"previousAccountId": sourceAccountID,
			},
		})

		fmt.Fprintf(
			os.Stderr,
			"codex-mux: autonomous thread %s continued %s -> %s\n",
			threadID,
			sourceAccountID,
			targetAccountID,
		)

		return
	}
}

func (m *Multiplexer) forwardServerRequest(inbound backend.Inbound) {
	sequence := m.serverSequence.Add(1)
	newID := protocol.StringID(fmt.Sprintf("codex-mux:%s:%d", inbound.AccountID, sequence))
	key := protocol.RequestIDKey(newID)
	m.serverMu.Lock()
	m.serverRoutes[key] = serverRequestRoute{
		accountID: inbound.AccountID,
		original:  append(json.RawMessage(nil), inbound.Message.ID...),
	}
	m.serverMu.Unlock()
	inbound.Message.ID = newID
	m.write(inbound.Message)
}

func (m *Multiplexer) shouldForwardNotification(accountID, method string) bool {
	controller, ok := m.store.Controller()
	if ok && controller.ID == accountID {
		return true
	}
	return strings.HasPrefix(method, "thread/") ||
		strings.HasPrefix(method, "turn/") ||
		strings.HasPrefix(method, "item/") ||
		strings.HasPrefix(method, "hook/") ||
		strings.HasPrefix(method, "rawResponse")
}

func (m *Multiplexer) learnThreadOwner(
	route externalRoute,
	accountID string,
	result json.RawMessage,
) {
	switch route.method {
	case "thread/start", "thread/fork", "thread/resume", "thread/unarchive":
		threadID := threadIDFromResult(result)
		if threadID == "" {
			return
		}
		if current, exists := m.store.ThreadOwner(threadID); exists && current != accountID {
			// Ownership changes are committed explicitly by recovery/failover.
			// A late response from an old child must never steal the thread back.
			return
		}
		_ = m.store.SetThreadOwner(threadID, accountID)
	}
}

func (m *Multiplexer) write(message protocol.Message) {
	encoded, err := protocol.Encode(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "codex-mux: encode response: %v\n", err)
		return
	}
	m.writeRaw(encoded)
}

func (m *Multiplexer) writeRaw(encoded []byte) {
	m.outputMu.Lock()
	defer m.outputMu.Unlock()
	_, _ = m.output.Write(append(encoded, '\n'))
}

type childEntry struct {
	account state.Account
	child   *backend.Child
}

func (m *Multiplexer) childEntries() []childEntry {
	accounts := m.store.Accounts()
	m.childrenMu.RLock()
	defer m.childrenMu.RUnlock()
	entries := make([]childEntry, 0, len(accounts))
	for _, account := range accounts {
		if child := m.children[account.ID]; child != nil {
			entries = append(entries, childEntry{account: account, child: child})
		}
	}
	return entries
}

func (m *Multiplexer) child(accountID string) (*backend.Child, bool) {
	m.childrenMu.RLock()
	defer m.childrenMu.RUnlock()
	child, ok := m.children[accountID]
	return child, ok
}

func (m *Multiplexer) controllerChild() (*backend.Child, bool) {
	controller, ok := m.store.Controller()
	if !ok {
		return nil, false
	}
	return m.child(controller.ID)
}

func (m *Multiplexer) startChild(
	ctx context.Context,
	account state.Account,
) (*backend.Child, error) {
	if child, ok := m.child(account.ID); ok {
		return child, nil
	}

	child, err := backend.Start(
		account.ID,
		account.CodexHome,
		m.realExecutable,
		m.realArgs,
		m.environment,
		m.inbound,
	)
	if err != nil {
		return nil, err
	}

	m.childrenMu.Lock()
	m.children[account.ID] = child
	m.childrenMu.Unlock()

	cleanup := func() {
		_ = child.Close()
		m.childrenMu.Lock()
		if current := m.children[account.ID]; current == child {
			delete(m.children, account.ID)
		}
		m.childrenMu.Unlock()
	}

	m.initializationMu.RLock()
	params := append(json.RawMessage(nil), m.initializeParams...)
	initialized := m.initialized
	m.initializationMu.RUnlock()

	if len(params) > 0 {
		requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		_, err := child.Request(requestCtx, "initialize", params)
		cancel()
		if err != nil {
			cleanup()
			return nil, err
		}
		if initialized {
			if err := child.Send(protocol.Message{Method: "initialized"}); err != nil {
				cleanup()
				return nil, err
			}
		}
	}

	return child, nil
}

func (m *Multiplexer) SubscribeEvents() (<-chan Event, func()) {
	channel := make(chan Event, 32)
	m.eventsMu.Lock()
	m.events[channel] = struct{}{}
	m.eventsMu.Unlock()
	return channel, func() {
		m.eventsMu.Lock()
		if _, ok := m.events[channel]; ok {
			delete(m.events, channel)
			close(channel)
		}
		m.eventsMu.Unlock()
	}
}

func (m *Multiplexer) publish(event Event) {
	m.eventsMu.RLock()
	defer m.eventsMu.RUnlock()
	for channel := range m.events {
		select {
		case channel <- event:
		default:
		}
	}
}

func (m *Multiplexer) publishAccountRefresh(accountID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snapshot, err := m.accountSnapshot(ctx, accountID)
	if err == nil {
		m.publish(Event{Type: "account-updated", AccountID: accountID, Data: snapshot})
	}
}

func threadIDFromParams(params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var decoded map[string]any
	if json.Unmarshal(params, &decoded) != nil {
		return ""
	}
	for _, key := range []string{"threadId", "thread_id"} {
		if value, ok := decoded[key].(string); ok {
			return value
		}
	}
	return ""
}

func threadIDFromResult(result json.RawMessage) string {
	var decoded struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(result, &decoded) != nil {
		return ""
	}
	return decoded.Thread.ID
}

func threadIDFromNotification(params json.RawMessage) string {
	return threadIDFromResult(params)
}

func accountHasCapacity(snapshot AccountSnapshot) bool {
	if !snapshot.Enabled ||
		!snapshot.Connected ||
		snapshot.AuthType != "chatgpt" {
		return false
	}

	return rateLimitsHaveCapacity(snapshot.RateLimits)
}

func usageLimitText(text string) bool {
	text = strings.ToLower(text)

	return strings.Contains(text, "usagelimitexceeded") ||
		strings.Contains(text, "usage_limit") ||
		strings.Contains(text, "usage limit") ||
		strings.Contains(text, "rate_limit") ||
		strings.Contains(text, "rate limit") ||
		strings.Contains(text, "quota")
}

func usageLimitNotification(params json.RawMessage) bool {
	return usageLimitText(string(params))
}

func continuationTurnParams(
	original json.RawMessage,
	threadID string,
) (json.RawMessage, error) {
	var params map[string]any

	if err := json.Unmarshal(original, &params); err != nil {
		params = make(map[string]any)
	}

	params["threadId"] = threadID

	// Never reuse a client-generated message identifier.
	delete(params, "clientUserMessageId")

	params["input"] = []any{
		map[string]any{
			"type": "text",
			"text": "Continue the interrupted task from the current workspace and conversation state. " +
				"The router moved this session because the previous subscription or one of its subagents " +
				"could no longer continue. Treat previous subagents as unavailable unless they clearly " +
				"reported completion. Do not repeat completed work. Inspect the workspace, clean up or " +
				"re-run only interrupted verification commands when necessary, respawn workers if useful, " +
				"and continue autonomously from the latest durable state.",
			"text_elements": []any{},
		},
	}

	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

func silentCompletedTurn(
	params json.RawMessage,
) (string, bool) {
	var decoded struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			Status string `json:"status"`
			Items  []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"items"`
		} `json:"turn"`
	}

	if err := json.Unmarshal(params, &decoded); err != nil {
		return "", false
	}

	if decoded.ThreadID == "" ||
		decoded.Turn.Status != "completed" {
		return decoded.ThreadID, false
	}

	// A normal completed model turn should contain an agentMessage.
	// The current Codex auto-compaction failure path can instead emit
	// a nominally completed turn without one.
	for _, item := range decoded.Turn.Items {
		if item.Type == "agentMessage" &&
			strings.TrimSpace(item.Text) != "" {
			return decoded.ThreadID, false
		}
	}

	return decoded.ThreadID, true
}

func turnCompletedUsageLimit(
	params json.RawMessage,
) (string, bool) {
	var decoded struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			Status string `json:"status"`
			Error  *struct {
				Message        string          `json:"message"`
				CodexErrorInfo json.RawMessage `json:"codexErrorInfo"`
			} `json:"error"`
		} `json:"turn"`
	}

	if err := json.Unmarshal(params, &decoded); err != nil {
		return "", false
	}

	if decoded.ThreadID == "" ||
		decoded.Turn.Status != "failed" ||
		decoded.Turn.Error == nil {
		return decoded.ThreadID, false
	}

	var errorCode string
	_ = json.Unmarshal(
		decoded.Turn.Error.CodexErrorInfo,
		&errorCode,
	)

	if errorCode == "usageLimitExceeded" {
		return decoded.ThreadID, true
	}

	text := decoded.Turn.Error.Message + " " +
		string(decoded.Turn.Error.CodexErrorInfo)

	return decoded.ThreadID, usageLimitText(text)
}

func isUsageLimitResponse(message protocol.Message) bool {
	if message.Error == nil {
		return false
	}
	return usageLimitText(
		message.Error.Message + " " + string(message.Error.Data),
	)
}

func (m *Multiplexer) allSubscriptionsDepleted(ctx context.Context, id json.RawMessage) protocol.Message {
	var resetsAt *int64
	if preview := m.currentRateLimitPreview(); preview != nil && preview.Mode.isAllDepleted() {
		resetsAt = preview.ResetsAt
	} else if limits, err := m.AggregatedRateLimits(ctx); err == nil {
		weekly, _ := longestAndShortestWindow(limits)
		if weekly != nil {
			resetsAt = weekly.ResetsAt
		}
	}
	return allSubscriptionsDepleted(id, resetsAt)
}

func allSubscriptionsDepleted(id json.RawMessage, resetsAt *int64) protocol.Message {
	message := "All connected subscriptions are depleted. Add another subscription or wait for usage to reset."
	if resetsAt != nil {
		reset := time.Unix(*resetsAt, 0).In(time.Local)
		message = fmt.Sprintf(
			"All connected subscriptions are depleted. Usage resets on %s.",
			reset.Format("Monday, 2 January at 3:04 PM"),
		)
	}
	return protocol.Failure(
		id,
		-32026,
		message,
	)
}

func cloneAccountSet(source map[string]struct{}) map[string]struct{} {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]struct{}, len(source))
	for accountID := range source {
		clone[accountID] = struct{}{}
	}
	return clone
}

func sortThreads(threads []map[string]any) {
	sort.SliceStable(threads, func(i, j int) bool {
		return numericField(threads[i], "updatedAt", "createdAt") > numericField(threads[j], "updatedAt", "createdAt")
	})
}

func numericField(value map[string]any, keys ...string) float64 {
	for _, key := range keys {
		if number, ok := value[key].(float64); ok {
			return number
		}
	}
	return 0
}
