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

	"github.com/tomyteeee/Codex-Router/internal/backend"
	"github.com/tomyteeee/Codex-Router/internal/protocol"
	"github.com/tomyteeee/Codex-Router/internal/state"
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
	accountID            string
	turnID               string
	params               json.RawMessage
	excluded             map[string]struct{}
	generation           uint64
	lastActivity         time.Time
	recovering           bool
	parked               bool
	recoveryCause        string
	failureRaw           []byte
	agentMessageComplete bool
}

type quotaBucket string

const (
	quotaBucketNormal  quotaBucket = "codex"
	quotaBucketReserve quotaBucket = "gpt-reserve"
)

func quotaBucketOverrideFromParams(
	params json.RawMessage,
) (quotaBucket, bool) {
	var decoded map[string]json.RawMessage

	if json.Unmarshal(params, &decoded) != nil {
		return quotaBucketNormal, false
	}

	rawModel, ok := decoded["model"]
	if !ok || len(rawModel) == 0 || string(rawModel) == "null" {
		return quotaBucketNormal, false
	}

	var model string
	if json.Unmarshal(rawModel, &model) != nil {
		return quotaBucketNormal, false
	}

	model = strings.TrimSpace(model)
	if model == "" {
		return quotaBucketNormal, false
	}

	if strings.EqualFold(model, "gpt-reserve") {
		return quotaBucketReserve, true
	}

	return quotaBucketNormal, true
}

func quotaBucketFromParams(params json.RawMessage) quotaBucket {
	bucket, ok := quotaBucketOverrideFromParams(params)
	if !ok {
		return quotaBucketNormal
	}
	return bucket
}

func reserveRateLimitsFromMap(
	byLimitID map[string]*RateLimits,
) *RateLimits {
	for limitID, limits := range byLimitID {
		if limits == nil {
			continue
		}

		name := ""
		if limits.LimitName != nil {
			name = strings.TrimSpace(*limits.LimitName)
		}

		id := ""
		if limits.LimitID != nil {
			id = strings.TrimSpace(*limits.LimitID)
		}

		if strings.EqualFold(name, "gpt-reserve") ||
			strings.EqualFold(id, "gpt-reserve") ||
			strings.EqualFold(limitID, "gpt-reserve") {
			return limits
		}
	}

	return nil
}

func rateLimitsForQuotaBucket(
	snapshot AccountSnapshot,
	bucket quotaBucket,
) *RateLimits {
	if bucket == quotaBucketReserve {
		return reserveRateLimitsFromMap(
			snapshot.RateLimitsByLimitID,
		)
	}

	return snapshot.RateLimits
}

func paramsForQuotaBucket(
	params json.RawMessage,
	bucket quotaBucket,
) (json.RawMessage, error) {
	if bucket != quotaBucketReserve {
		return append(json.RawMessage(nil), params...), nil
	}

	decoded := make(map[string]json.RawMessage)

	if len(params) > 0 {
		if err := json.Unmarshal(params, &decoded); err != nil {
			return nil, fmt.Errorf(
				"decode turn params for Reserve fallback: %w",
				err,
			)
		}
	}

	if decoded == nil {
		decoded = make(map[string]json.RawMessage)
	}

	model, err := json.Marshal("gpt-reserve")
	if err != nil {
		return nil, err
	}

	decoded["model"] = model

	encoded, err := json.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf(
			"encode Reserve fallback params: %w",
			err,
		)
	}

	return encoded, nil
}

func (m *Multiplexer) chooseAccountWithReserveFallback(
	ctx context.Context,
	excluded map[string]struct{},
	bucket quotaBucket,
) (
	state.Account,
	RouteReason,
	quotaBucket,
	error,
) {
	account, reason, err := m.chooseAccountForQuotaBucket(
		ctx,
		excluded,
		bucket,
	)

	if err == nil {
		return account, reason, bucket, nil
	}

	if bucket != quotaBucketNormal ||
		!errors.Is(err, errNoSubscriptionCapacity) {
		return state.Account{}, reason, bucket, err
	}

	// Normal quota exclusions must not leak into the separately metered
	// Reserve pool. In particular, an account whose 5h or weekly normal
	// allowance is exhausted may still have completely usable Reserve.
	reserveAccount, reserveReason, reserveErr :=
		m.chooseAccountForQuotaBucket(
			ctx,
			nil,
			quotaBucketReserve,
		)

	if reserveErr != nil {
		return state.Account{},
			reserveReason,
			quotaBucketReserve,
			reserveErr
	}

	return reserveAccount,
		reserveReason,
		quotaBucketReserve,
		nil
}

func quotaBlockKey(
	accountID string,
	bucket quotaBucket,
) string {
	return accountID + "\x00" + string(bucket)
}

func (m *Multiplexer) rememberThreadQuotaBucket(
	threadID string,
	bucket quotaBucket,
) {
	if threadID == "" {
		return
	}

	m.threadQuotaMu.Lock()
	m.threadQuotaBuckets[threadID] = bucket
	m.threadQuotaMu.Unlock()
}

func (m *Multiplexer) rememberExplicitThreadQuotaBucket(
	threadID string,
	params json.RawMessage,
) {
	bucket, ok := quotaBucketOverrideFromParams(params)
	if !ok {
		return
	}

	m.rememberThreadQuotaBucket(threadID, bucket)
}

func (m *Multiplexer) cachedThreadQuotaBucket(
	threadID string,
) (quotaBucket, bool) {
	if threadID == "" {
		return quotaBucketNormal, false
	}

	m.threadQuotaMu.RLock()
	bucket, ok := m.threadQuotaBuckets[threadID]
	m.threadQuotaMu.RUnlock()

	return bucket, ok
}

func (m *Multiplexer) threadQuotaBucket(
	threadID string,
	params json.RawMessage,
) quotaBucket {
	if bucket, ok := quotaBucketOverrideFromParams(params); ok {
		m.rememberThreadQuotaBucket(threadID, bucket)
		return bucket
	}

	if bucket, ok := m.cachedThreadQuotaBucket(threadID); ok {
		return bucket
	}

	if threadID != "" {
		root := m.rootThreadID(threadID)
		if root != "" && root != threadID {
			if bucket, ok := m.cachedThreadQuotaBucket(root); ok {
				return bucket
			}
		}
	}

	return quotaBucketNormal
}

func (m *Multiplexer) rememberQuotaBucketFromRoute(
	route externalRoute,
	result json.RawMessage,
) {
	switch route.method {
	case "thread/start":
		threadID := threadIDFromResult(result)
		if threadID == "" {
			return
		}

		bucket, ok := quotaBucketOverrideFromParams(
			route.message.Params,
		)
		if !ok {
			bucket = quotaBucketNormal
		}

		m.rememberThreadQuotaBucket(threadID, bucket)

	case "turn/start", "thread/settings/update", "thread/resume":
		threadID := threadIDFromParams(route.message.Params)
		m.rememberExplicitThreadQuotaBucket(
			threadID,
			route.message.Params,
		)
	}
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

	initialQuotaWarmupOnce sync.Once
	initialQuotaWarmupDone chan struct{}
	initialQuotaKnown      map[string]struct{}

	threadQuotaMu      sync.RWMutex
	threadQuotaBuckets map[string]quotaBucket

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

	// outputMu serializes queue insertion. The dedicated outputLoop is
	// the only goroutine that performs potentially blocking renderer writes.
	outputMu      sync.Mutex
	outputQueue   chan []byte
	outputStarted atomic.Bool

	// account/rateLimits/updated can be emitted by every child at roughly
	// the same time. Coalesce those notifications so one burst does not
	// trigger N complete multi-account snapshot passes.
	rateLimitNotifyMu       sync.Mutex
	rateLimitNotifyRunning  bool
	rateLimitNotifyPending  bool
	rateLimitNotifyFallback []byte
	eventsMu                sync.RWMutex
	events                  map[chan Event]struct{}

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
		realExecutable:         options.RealExecutable,
		realArgs:               append([]string(nil), options.RealArgs...),
		environment:            append([]string(nil), options.Environment...),
		store:                  options.Store,
		output:                 options.Output,
		children:               make(map[string]*backend.Child),
		inbound:                make(chan backend.Inbound, 1024),
		outputQueue:            make(chan []byte, 4096),
		externalRoutes:         make(map[string]externalRoute),
		activeTurns:            make(map[string]activeTurn),
		quotaBlocked:           make(map[string]time.Time),
		initialQuotaWarmupDone: make(chan struct{}),
		initialQuotaKnown:      make(map[string]struct{}),
		threadQuotaBuckets:     make(map[string]quotaBucket),
		threadParents:          make(map[string]string),
		staleSources:           make(map[string]map[string]struct{}),
		commandPIDs:            make(map[string]map[int]string),
		serverRoutes:           make(map[string]serverRequestRoute),
		events:                 make(map[chan Event]struct{}),
		profileClient:          &http.Client{Timeout: 10 * time.Second},
		profileCache:           make(map[string]profileCacheEntry),
		now:                    time.Now,
		resetCreditsCache:      make(map[string]resetCreditsCacheEntry),
		resetCreditsEndpoint:   rateLimitResetCreditsURL,
		resetPreviews:          make(map[string]ResetCreditsPreview),
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
	if m.outputStarted.CompareAndSwap(
		false,
		true,
	) {
		go m.outputLoop(ctx)
	}

	go m.inboundLoop(ctx)
	go m.primeInitialQuotaState(ctx)
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
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*requestTimeout,
	)
	defer cancel()

	bucket := quotaBucketFromParams(message.Params)

	account, reason, selectedBucket, err :=
		m.chooseAccountWithReserveFallback(
			ctx,
			nil,
			bucket,
		)

	if err != nil {
		if errors.Is(err, errNoSubscriptionCapacity) {
			m.write(
				m.allSubscriptionsDepletedForQuotaBucket(
					ctx,
					message.ID,
					selectedBucket,
				),
			)
			return
		}

		m.write(
			protocol.Failure(
				message.ID,
				-32020,
				err.Error(),
			),
		)
		return
	}

	if selectedBucket != bucket {
		updatedParams, updateErr :=
			paramsForQuotaBucket(
				message.Params,
				selectedBucket,
			)

		if updateErr != nil {
			m.write(
				protocol.Failure(
					message.ID,
					-32020,
					updateErr.Error(),
				),
			)
			return
		}

		message.Params = updatedParams
		bucket = selectedBucket
	}

	if err := m.forward(account.ID, message); err != nil {
		m.write(
			protocol.Failure(
				message.ID,
				-32021,
				err.Error(),
			),
		)
		return
	}

	m.publish(Event{
		Type:      "thread-routed",
		AccountID: account.ID,
		Message:   fmt.Sprintf("New chat pinned to %s", account.Label),
		Data:      reason,
	})
}

func (m *Multiplexer) routeExistingRequest(
	message protocol.Message,
) {
	accountID := ""

	if scopedAccountID, cleanedParams, ok :=
		scopedPluginRequest(
			message.Method,
			message.Params,
		); ok {
		if account, exists :=
			m.store.Account(scopedAccountID); exists &&
			account.Enabled {
			message.Params = cleanedParams

			if err := m.forward(
				scopedAccountID,
				message,
			); err != nil {
				m.write(
					protocol.Failure(
						message.ID,
						-32023,
						err.Error(),
					),
				)
			}

			return
		}
	}

	threadID := threadIDFromParams(
		message.Params,
	)

	if threadID != "" {
		switch message.Method {
		case "turn/start",
			"thread/settings/update",
			"thread/resume":
			m.rememberExplicitThreadQuotaBucket(
				threadID,
				message.Params,
			)
		}
	}

	// IMPORTANT:
	// invalidate recovery BEFORE reading persistent ownership.
	//
	// Recovery commits ownership while holding the same active-turn lock.
	// Therefore either:
	//   1. recovery commits first and this user turn observes the new owner, or
	//   2. this user turn supersedes first and recovery cannot commit.
	if message.Method == "turn/start" &&
		threadID != "" {
		m.supersedeRecoveryForUserTurn(
			threadID,
		)
	}

	if threadID != "" {
		accountID, _ =
			m.store.ThreadOwner(threadID)
	}

	// turn/steer and turn/interrupt operate on the CURRENT execution, not
	// merely the account that persistently owns the thread.
	//
	// A turn may currently be executing on another subscription because of
	// quota failover/recovery. Sending turn/steer to the stale persistent
	// owner causes "no active turn to steer", after which the desktop falls
	// back to turn/start and renders the same user input a second time.
	if (message.Method == "turn/steer" ||
		message.Method == "turn/interrupt") &&
		threadID != "" {
		if activeAccountID, ok :=
			m.activeExecutionAccount(
				threadID,
			); ok {
			if accountID != "" &&
				accountID != activeAccountID {
				fmt.Fprintf(
					os.Stderr,
					"codex-mux: routing %s for thread %s to active account %s instead of persisted owner %s\n",
					message.Method,
					threadID,
					activeAccountID,
					accountID,
				)
			}

			accountID = activeAccountID
		}
	}

	if accountID == "" {
		if controller, ok :=
			m.store.Controller(); ok {
			accountID = controller.ID
		}
	}

	if accountID == "" {
		m.write(
			protocol.Failure(
				message.ID,
				-32022,
				"no controller account is configured",
			),
		)

		return
	}

	if message.Method == "turn/interrupt" &&
		threadID != "" {
		m.cancelRecoveryForUser(
			threadID,
		)
	}

	if message.Method == "turn/start" &&
		threadID != "" {
		m.routeTurnStart(
			message,
			threadID,
			accountID,
		)

		return
	}

	if err := m.forward(
		accountID,
		message,
	); err != nil {
		m.write(
			protocol.Failure(
				message.ID,
				-32023,
				err.Error(),
			),
		)
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

	m.rememberExplicitThreadQuotaBucket(
		threadID,
		message.Params,
	)

	m.activeTurnMu.Lock()

	active := m.activeTurns[threadID]

	// Every renderer-originated turn is a new execution generation.
	// Any automatic recovery that captured an older generation is no
	// longer allowed to create a replacement execution.
	active.generation++
	active.accountID = accountID
	active.turnID = ""
	active.params = append(
		json.RawMessage(nil),
		message.Params...,
	)
	active.excluded = cloneAccountSet(excluded)
	active.lastActivity = m.now()
	active.recovering = false
	active.parked = false
	active.failureRaw = nil
	active.recoveryCause = ""
	active.agentMessageComplete = false

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

func (m *Multiplexer) AccountUsage(
	ctx context.Context,
	accountID string,
) (json.RawMessage, error) {
	account, ok := m.store.Account(accountID)
	if !ok {
		return nil, fmt.Errorf("unknown account %q", accountID)
	}
	if !account.Enabled {
		return nil, fmt.Errorf("account %q is disabled", accountID)
	}

	child, err := m.startChild(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("start account %q: %w", accountID, err)
	}

	response, err := child.Request(
		ctx,
		"account/usage/read",
		json.RawMessage(`{}`),
	)
	if err != nil {
		return nil, fmt.Errorf("account/usage/read for %q: %w", accountID, err)
	}

	return append(json.RawMessage(nil), response.Result...), nil
}

func (m *Multiplexer) aggregatedRateLimitPayload(
	ctx context.Context,
) (map[string]any, error) {
	rateLimits, byLimitID, err := m.AggregatedRateLimitState(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"rateLimits":          rateLimits,
		"rateLimitsByLimitId": byLimitID,
	}, nil
}

func (m *Multiplexer) routeAggregatedRateLimits(message protocol.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	payload, err := m.aggregatedRateLimitPayload(ctx)
	if err != nil {
		m.write(protocol.Failure(message.ID, -32024, err.Error()))
		return
	}

	result, err := json.Marshal(payload)
	if err != nil {
		m.write(protocol.Failure(message.ID, -32025, err.Error()))
		return
	}

	m.write(protocol.Success(message.ID, result))
}

func (m *Multiplexer) markInitialQuotaKnown(
	accountID string,
) {
	if accountID == "" ||
		m.initialQuotaKnown == nil {
		return
	}

	m.quotaMu.Lock()
	m.initialQuotaKnown[accountID] =
		struct{}{}
	m.quotaMu.Unlock()
}

func (m *Multiplexer) initialQuotaStateKnown(
	accountID string,
) bool {
	if accountID == "" ||
		m.initialQuotaKnown == nil {
		return false
	}

	m.quotaMu.Lock()
	_, ok :=
		m.initialQuotaKnown[accountID]
	m.quotaMu.Unlock()

	return ok
}

func (m *Multiplexer) primeInitialQuotaState(
	parent context.Context,
) {
	if m.initialQuotaWarmupDone == nil {
		return
	}

	m.initialQuotaWarmupOnce.Do(
		func() {
			defer close(
				m.initialQuotaWarmupDone,
			)

			ctx, cancel :=
				context.WithTimeout(
					parent,
					requestTimeout,
				)
			defer cancel()

			snapshots :=
				m.accountSnapshots(
					ctx,
					false,
				)

			for _, snapshot := range snapshots {
				if !snapshot.Enabled ||
					!snapshot.Connected ||
					snapshot.AuthType !=
						"chatgpt" ||
					snapshot.RateLimits ==
						nil {
					continue
				}

				m.markInitialQuotaKnown(
					snapshot.ID,
				)

				if !rateLimitsHaveCapacity(
					snapshot.RateLimits,
				) {
					m.markAccountQuotaBlockedFor(
						snapshot.ID,
						quotaBucketNormal,
					)
				}
			}
		},
	)
}

func (m *Multiplexer) waitForInitialQuotaWarmup() {
	if m.initialQuotaWarmupDone == nil {
		return
	}

	select {
	case <-m.initialQuotaWarmupDone:
		return
	default:
	}

	timer := time.NewTimer(
		requestTimeout,
	)
	defer timer.Stop()

	select {
	case <-m.initialQuotaWarmupDone:
	case <-timer.C:
		fmt.Fprintln(
			os.Stderr,
			"codex-mux: initial quota warmup timed out; validating owner on demand",
		)
	}
}

func (m *Multiplexer) refreshInitialOwnerQuotaState(
	accountID string,
) {
	if accountID == "" {
		return
	}

	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			requestTimeout,
		)
	defer cancel()

	snapshot, err :=
		m.accountSnapshotWithProfile(
			ctx,
			accountID,
			false,
		)

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: cold-start quota validation for %s failed: %v\n",
			accountID,
			err,
		)
		return
	}

	if snapshot.RateLimits == nil {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: cold-start quota validation for %s returned no normal rate-limit state\n",
			accountID,
		)
		return
	}

	m.markInitialQuotaKnown(
		accountID,
	)

	if !rateLimitsHaveCapacity(
		snapshot.RateLimits,
	) {
		m.markAccountQuotaBlockedFor(
			accountID,
			quotaBucketNormal,
		)
	}
}

func (m *Multiplexer) activeExecutionAccount(
	threadID string,
) (string, bool) {
	root := m.rootThreadID(
		threadID,
	)

	if root == "" {
		root = threadID
	}

	if root == "" {
		return "", false
	}

	active, ok :=
		m.activeTurnFor(
			root,
			"",
		)

	if !ok ||
		active.accountID == "" ||
		active.recovering ||
		active.parked {
		return "", false
	}

	return active.accountID, true
}

func (m *Multiplexer) routeTurnStart(
	message protocol.Message,
	threadID string,
	ownerID string,
) {
	bucket := m.threadQuotaBucket(
		threadID,
		message.Params,
	)

	owner, ownerExists :=
		m.store.Account(ownerID)

	// Normal turns retain the zero-RPC fast path after startup, but the
	// first turn must not interpret an empty quota cache as proof that the
	// pinned account has capacity.
	if bucket == quotaBucketNormal &&
		ownerExists &&
		owner.Enabled &&
		m.initialQuotaWarmupDone != nil {
		m.waitForInitialQuotaWarmup()

		if !m.initialQuotaStateKnown(
			ownerID,
		) &&
			!m.accountQuotaBlockedFor(
				ownerID,
				bucket,
			) {
			// Startup warmup may have timed out or account/rateLimits/read
			// may have transiently failed. Retry only this owner once.
			m.refreshInitialOwnerQuotaState(
				ownerID,
			)

			if !m.initialQuotaStateKnown(
				ownerID,
			) &&
				!m.accountQuotaBlockedFor(
					ownerID,
					bucket,
				) {
				fmt.Fprintf(
					os.Stderr,
					"codex-mux: cold-start quota state for %s remains unknown; routing through slow selector\n",
					ownerID,
				)

				excluded := map[string]struct{}{
					ownerID: {},
				}

				go m.robustFailoverTurn(
					message,
					threadID,
					ownerID,
					excluded,
				)

				return
			}
		}
	}

	// Normal existing-thread turns deliberately avoid live quota RPCs once
	// initial state is known. A real app-server usage-limit response remains
	// authoritative and is still handled by the existing robust failover.
	if ownerExists &&
		owner.Enabled &&
		!m.accountQuotaBlockedFor(
			ownerID,
			bucket,
		) {
		if err := m.forward(
			ownerID,
			message,
		); err == nil {
			return
		} else {
			fmt.Fprintf(
				os.Stderr,
				"codex-mux: fast-path turn forward to %s failed for thread %s: %v; failing over\n",
				ownerID,
				threadID,
				err,
			)
		}
	}

	excluded := map[string]struct{}{
		ownerID: {},
	}

	go m.robustFailoverTurn(
		message,
		threadID,
		ownerID,
		excluded,
	)
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
	if sourceAccountID != "" &&
		sourceAccountID == targetAccountID {
		return nil
	}

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
				threadID := threadIDFromParams(
					route.message.Params,
				)
				bucket := m.threadQuotaBucket(
					threadID,
					route.message.Params,
				)
				m.markAccountQuotaBlockedFor(
					inbound.AccountID,
					bucket,
				)
				go m.retryTurnAfterUsageLimitRobust(
					route,
					inbound.AccountID,
				)
				return
			}

			m.rememberQuotaBucketFromRoute(
				route,
				message.Result,
			)
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
		m.scheduleAggregatedRateLimitNotification(
			inbound.Raw,
		)
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

	payload, err := m.aggregatedRateLimitPayload(ctx)
	if err != nil {
		return
	}

	params, err := json.Marshal(payload)
	if err != nil {
		return
	}

	m.write(protocol.Message{
		Method: "account/rateLimits/updated",
		Params: params,
	})
}

func (m *Multiplexer) scheduleAggregatedRateLimitNotification(
	fallback []byte,
) {
	fallback = append([]byte(nil), fallback...)

	m.rateLimitNotifyMu.Lock()

	// Always retain the newest raw notification for error fallback.
	m.rateLimitNotifyFallback = fallback

	if m.rateLimitNotifyRunning {
		// One refresh is already scheduled or executing. Record that state
		// changed again so the worker performs one more pass afterward.
		m.rateLimitNotifyPending = true
		m.rateLimitNotifyMu.Unlock()
		return
	}

	m.rateLimitNotifyRunning = true
	m.rateLimitNotifyPending = false
	m.rateLimitNotifyMu.Unlock()

	go m.runAggregatedRateLimitNotificationLoop()
}

func (m *Multiplexer) runAggregatedRateLimitNotificationLoop() {
	for {
		// Children commonly emit these notifications within a few
		// milliseconds of each other. A tiny debounce collapses the burst
		// while being imperceptible to the quota UI.
		time.Sleep(75 * time.Millisecond)

		m.rateLimitNotifyMu.Lock()

		fallback := append(
			[]byte(nil),
			m.rateLimitNotifyFallback...,
		)

		// Any notification already received before this snapshot is covered
		// by the snapshot we are about to perform.
		m.rateLimitNotifyPending = false

		m.rateLimitNotifyMu.Unlock()

		m.forwardAggregatedRateLimitNotification(
			fallback,
		)

		m.rateLimitNotifyMu.Lock()

		if m.rateLimitNotifyPending {
			// State changed while the aggregate snapshot was running.
			// Perform exactly one more coalesced pass.
			m.rateLimitNotifyMu.Unlock()
			continue
		}

		m.rateLimitNotifyRunning = false
		m.rateLimitNotifyMu.Unlock()

		return
	}
}

func (m *Multiplexer) forwardAggregatedRateLimitNotification(fallback []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	payload, err := m.aggregatedRateLimitPayload(ctx)
	if err != nil {
		m.writeRaw(fallback)
		return
	}
	params, err := json.Marshal(payload)
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

	active, ok := m.activeTurnFor(
		threadID,
		inbound.AccountID,
	)
	if !ok {
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

	// A completed agentMessage is real terminal output. Explicit structured
	// quota failures above still recover, but an empty completion must not
	// resurrect a task that already produced a legitimate final response.
	if active.agentMessageComplete {
		m.clearActiveTurn(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(raw)
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

	bucket := m.threadQuotaBucket(
		threadID,
		active.params,
	)
	if accountHasCapacityForQuotaBucket(snapshot, bucket) {
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

	bucket := m.threadQuotaBucket(
		threadID,
		active.params,
	)
	m.markAccountQuotaBlockedFor(
		exhaustedAccountID,
		bucket,
	)

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

		fallback, _, selectedBucket, err := m.chooseAccountWithReserveFallback(
			ctx,
			excluded,
			bucket,
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
		if selectedBucket != bucket {
			bucket = selectedBucket

			// Exclusions accumulated while exhausting normal quota are not valid
			// for the independent Reserve allowance.
			excluded = make(map[string]struct{})

			updatedParams, paramErr := paramsForQuotaBucket(
				active.params,
				bucket,
			)
			if paramErr != nil {
				cancel()

				m.clearActiveTurn(threadID, "")
				m.writeRaw(failedNotification)

				fmt.Fprintf(
					os.Stderr,
					"codex-mux: could not switch autonomous turn %s to Luna Reserve: %v\n",
					threadID,
					paramErr,
				)

				return
			}

			active.params = updatedParams
			m.rememberThreadQuotaBucket(
				threadID,
				bucket,
			)
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
		//
		// IMPORTANT: keep active.params as the durable task. The synthetic
		// continuation prompt is transport for THIS recovery only and must
		// never become the task that future recoveries retry.
		m.activeTurnMu.Lock()
		nextActive := active
		nextActive.accountID = targetAccountID
		nextActive.params = append(
			json.RawMessage(nil),
			active.params...,
		)
		nextActive.excluded = cloneAccountSet(excluded)
		nextActive.agentMessageComplete = false
		nextActive.recovering = false
		nextActive.parked = false
		nextActive.lastActivity = m.now()
		m.activeTurns[threadID] = nextActive
		m.activeTurnMu.Unlock()

		_, err = target.Request(
			ctx,
			"turn/start",
			continuationParams,
		)

		cancel()

		if err != nil {
			if usageLimitText(err.Error()) {
				m.markAccountQuotaBlockedFor(
					targetAccountID,
					bucket,
				)
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

func (m *Multiplexer) outputLoop(
	ctx context.Context,
) {
	for {
		select {
		case payload := <-m.outputQueue:
			if len(payload) == 0 {
				continue
			}

			if _, err := m.output.Write(
				payload,
			); err != nil {
				fmt.Fprintf(
					os.Stderr,
					"codex-mux: write renderer output: %v\n",
					err,
				)
			}

		case <-ctx.Done():
			// Flush anything already accepted by writeRaw. Use a
			// non-blocking drain so shutdown cannot wait for future work.
			for {
				select {
				case payload := <-m.outputQueue:
					if len(payload) == 0 {
						continue
					}

					if _, err := m.output.Write(
						payload,
					); err != nil {
						return
					}

				default:
					return
				}
			}
		}
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

func (m *Multiplexer) writeRaw(
	encoded []byte,
) {
	payload := make(
		[]byte,
		len(encoded)+1,
	)

	copy(
		payload,
		encoded,
	)

	payload[len(encoded)] = '\n'

	// Preserve exactly one total ordering for output generated by all mux
	// goroutines. Once the queue is enabled, holding this mutex only covers
	// a memory/channel operation -- never the renderer pipe write itself.
	m.outputMu.Lock()
	defer m.outputMu.Unlock()

	if !m.outputStarted.Load() ||
		m.outputQueue == nil {
		_, _ = m.output.Write(
			payload,
		)
		return
	}

	m.outputQueue <- payload
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
	return accountHasCapacityForQuotaBucket(
		snapshot,
		quotaBucketNormal,
	)
}

func accountHasCapacityForQuotaBucket(
	snapshot AccountSnapshot,
	bucket quotaBucket,
) bool {
	if !snapshot.Enabled ||
		!snapshot.Connected ||
		snapshot.AuthType != "chatgpt" {
		return false
	}

	return rateLimitsHaveCapacity(
		rateLimitsForQuotaBucket(snapshot, bucket),
	)
}

func usageLimitText(text string) bool {
	text = strings.ToLower(
		strings.TrimSpace(text),
	)

	signals := []string{
		"usagelimitexceeded",
		"usage_limit_exceeded",
		"usage-limit-exceeded",
		"usage limit exceeded",
		"usage limit reached",
		"reached your usage limit",
		"hit your usage limit",
		"you've reached your usage limit",
		"you have reached your usage limit",

		"rate_limit_exceeded",
		"rate-limit-exceeded",
		"rate limit exceeded",
		"rate limit reached",
		"too many requests",

		"quota_exceeded",
		"quota exceeded",
		"insufficient_quota",
		"insufficient quota",

		`"usage_limit"`,
		`"rate_limit"`,
	}

	for _, signal := range signals {
		if strings.Contains(text, signal) {
			return true
		}
	}

	return false
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

func (m *Multiplexer) allSubscriptionsDepleted(
	ctx context.Context,
	id json.RawMessage,
) protocol.Message {
	var resetsAt *int64

	if preview :=
		m.currentRateLimitPreview(); preview != nil &&
		preview.Mode.isAllDepleted() {
		resetsAt = preview.ResetsAt
	} else if limits, err :=
		m.AggregatedRateLimits(ctx); err == nil {
		weekly, _ :=
			longestAndShortestWindow(
				limits,
			)

		if weekly != nil {
			resetsAt =
				weekly.ResetsAt
		}
	}

	// Unix zero is not a meaningful subscription reset time. Never turn an
	// absent/invalid reset into "January 1, 1970" in router-generated errors.
	if resetsAt != nil &&
		*resetsAt <= 0 {
		resetsAt = nil
	}

	return allSubscriptionsDepleted(
		id,
		resetsAt,
	)
}

func (m *Multiplexer) allSubscriptionsDepletedForQuotaBucket(
	ctx context.Context,
	id json.RawMessage,
	bucket quotaBucket,
) protocol.Message {
	if bucket != quotaBucketReserve {
		return m.allSubscriptionsDepleted(
			ctx,
			id,
		)
	}

	var resetsAt *int64

	_, byLimitID, err :=
		m.AggregatedRateLimitState(ctx)

	if err == nil {
		limits :=
			reserveRateLimitsFromMap(
				byLimitID,
			)

		weekly, _ :=
			longestAndShortestWindow(
				limits,
			)

		if weekly != nil {
			resetsAt =
				weekly.ResetsAt
		}
	}

	if resetsAt != nil &&
		*resetsAt <= 0 {
		resetsAt = nil
	}

	return allSubscriptionsDepleted(
		id,
		resetsAt,
	)
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
