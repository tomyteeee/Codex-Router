package mux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tomyteeee/Codex-Router/internal/backend"
	"github.com/tomyteeee/Codex-Router/internal/protocol"
	"github.com/tomyteeee/Codex-Router/internal/state"
)

const (
	quotaBlockFallback      = 2 * time.Minute
	recoveryWatchInterval   = 30 * time.Second
	recoveryStallThreshold  = 90 * time.Second
	recoveryRetryInterval   = 30 * time.Second
	recoveryInterruptWindow = 2 * time.Second
	recoveryFlushDelay      = 250 * time.Millisecond
	recoveryCompletionGrace = 250 * time.Millisecond
)

type threadStartMeta struct {
	ID       string
	ParentID string
}

type turnStartMeta struct {
	ThreadID string
	TurnID   string
}

type threadRecoveryData struct {
	Path          string
	CWD           string
	ModelProvider string
}

func (m *Multiplexer) accountQuotaBlocked(
	accountID string,
) bool {
	return m.accountQuotaBlockedFor(
		accountID,
		quotaBucketNormal,
	)
}

func (m *Multiplexer) accountQuotaBlockedFor(
	accountID string,
	bucket quotaBucket,
) bool {
	key := quotaBlockKey(accountID, bucket)

	m.quotaMu.Lock()
	defer m.quotaMu.Unlock()

	until, ok := m.quotaBlocked[key]
	if !ok {
		return false
	}

	if !m.now().Before(until) {
		delete(m.quotaBlocked, key)
		return false
	}

	return true
}

func (m *Multiplexer) markAccountQuotaBlocked(
	accountID string,
) {
	m.markAccountQuotaBlockedFor(
		accountID,
		quotaBucketNormal,
	)
}

func (m *Multiplexer) markAccountQuotaBlockedFor(
	accountID string,
	bucket quotaBucket,
) {
	if accountID == "" {
		return
	}

	key := quotaBlockKey(accountID, bucket)

	now := m.now()
	until := now.Add(quotaBlockFallback)

	m.quotaMu.Lock()

	if existing := m.quotaBlocked[key]; existing.After(until) {
		until = existing
	}

	m.quotaBlocked[key] = until

	if bucket == quotaBucketNormal &&
		m.initialQuotaKnown != nil {
		m.initialQuotaKnown[accountID] =
			struct{}{}
	}

	m.quotaMu.Unlock()

	if m.store == nil {
		return
	}

	// Refine the temporary block to the server-provided reset time for the
	// exact allowance that failed.
	go func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			requestTimeout,
		)
		defer cancel()

		snapshot, err := m.accountSnapshotWithProfile(
			ctx,
			accountID,
			false,
		)
		if err != nil {
			return
		}

		limits := rateLimitsForQuotaBucket(
			snapshot,
			bucket,
		)
		if limits == nil {
			return
		}

		refined := until

		for _, window := range rateLimitWindows(limits) {
			if window == nil ||
				window.UsedPercent < 100 ||
				window.ResetsAt == nil {
				continue
			}

			reset := time.Unix(
				*window.ResetsAt,
				0,
			)

			if reset.After(refined) {
				refined = reset
			}
		}

		m.quotaMu.Lock()

		if existing := m.quotaBlocked[key]; refined.After(existing) {
			m.quotaBlocked[key] = refined
		}

		m.quotaMu.Unlock()
	}()
}

func (m *Multiplexer) markThreadQuotaBlocked(
	accountID string,
	threadID string,
) {
	m.markAccountQuotaBlockedFor(
		accountID,
		m.threadQuotaBucket(threadID, nil),
	)
}

func (m *Multiplexer) recordThreadStarted(accountID string, params json.RawMessage) threadStartMeta {
	var decoded struct {
		Thread struct {
			ID             string `json:"id"`
			ParentThreadID string `json:"parentThreadId"`
		} `json:"thread"`
	}
	if json.Unmarshal(params, &decoded) != nil || decoded.Thread.ID == "" {
		return threadStartMeta{}
	}

	threadID := decoded.Thread.ID
	parentID := decoded.Thread.ParentThreadID

	if parentID != "" {
		m.lineageMu.Lock()
		m.threadParents[threadID] = parentID
		m.lineageMu.Unlock()
	}

	// Notifications can arrive late after a migration. Learn ownership only
	// for genuinely new threads; explicit failover is the authority for an
	// already-known thread.
	if _, exists := m.store.ThreadOwner(threadID); !exists {
		_ = m.store.SetThreadOwner(threadID, accountID)
	}

	return threadStartMeta{ID: threadID, ParentID: parentID}
}

func parseTurnStarted(params json.RawMessage) turnStartMeta {
	var decoded struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if json.Unmarshal(params, &decoded) != nil {
		return turnStartMeta{}
	}
	return turnStartMeta{ThreadID: decoded.ThreadID, TurnID: decoded.Turn.ID}
}

func (m *Multiplexer) recordTurnStarted(accountID string, params json.RawMessage) turnStartMeta {
	meta := parseTurnStarted(params)
	if meta.ThreadID == "" {
		return meta
	}

	now := m.now()
	m.activeTurnMu.Lock()
	active, ok := m.activeTurns[meta.ThreadID]
	if !ok {
		active = activeTurn{}
	}
	// A target turn/started can race the internal turn/start response during
	// recovery. Do not change the committed source owner until the recovery
	// path has received a successful turn/start response and persisted it.
	if !active.recovering || active.accountID == "" || active.accountID == accountID {
		active.accountID = accountID
	}
	active.turnID = meta.TurnID
	active.lastActivity = now
	active.parked = false
	m.activeTurns[meta.ThreadID] = active
	m.activeTurnMu.Unlock()

	return meta
}

func threadIDFromInboundNotification(method string, params json.RawMessage) string {
	if method == "thread/started" {
		var decoded struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(params, &decoded) == nil {
			return decoded.Thread.ID
		}
		return ""
	}

	var decoded struct {
		ThreadID string `json:"threadId"`
	}
	if json.Unmarshal(params, &decoded) == nil {
		return decoded.ThreadID
	}
	return ""
}

func (m *Multiplexer) touchThread(threadID, accountID string) {
	if threadID == "" {
		return
	}
	m.activeTurnMu.Lock()
	if active, ok := m.activeTurns[threadID]; ok {
		if accountID == "" || active.accountID == "" || active.accountID == accountID {
			active.lastActivity = m.now()
			m.activeTurns[threadID] = active
		}
	}
	m.activeTurnMu.Unlock()
}

func (m *Multiplexer) rootThreadID(threadID string) string {
	if threadID == "" {
		return ""
	}
	seen := make(map[string]struct{})
	current := threadID

	m.lineageMu.RLock()
	defer m.lineageMu.RUnlock()

	for depth := 0; depth < 64; depth++ {
		if _, exists := seen[current]; exists {
			break
		}
		seen[current] = struct{}{}
		parent := m.threadParents[current]
		if parent == "" {
			break
		}
		current = parent
	}
	return current
}

func (m *Multiplexer) treeThreadIDs(root string) []string {
	if root == "" {
		return nil
	}

	m.lineageMu.RLock()
	parents := make(map[string]string, len(m.threadParents))
	for child, parent := range m.threadParents {
		parents[child] = parent
	}
	m.lineageMu.RUnlock()

	result := []string{root}
	seen := map[string]struct{}{root: {}}

	for changed := true; changed; {
		changed = false
		for child, parent := range parents {
			if _, already := seen[child]; already {
				continue
			}
			if _, parentKnown := seen[parent]; !parentKnown {
				continue
			}
			seen[child] = struct{}{}
			result = append(result, child)
			changed = true
		}
	}
	return result
}

func (m *Multiplexer) markTreeSourceStale(root, accountID string) {
	if root == "" || accountID == "" {
		return
	}
	m.staleMu.Lock()
	defer m.staleMu.Unlock()

	for _, threadID := range m.treeThreadIDs(root) {
		set := m.staleSources[threadID]
		if set == nil {
			set = make(map[string]struct{})
			m.staleSources[threadID] = set
		}
		set[accountID] = struct{}{}
	}
}

func (m *Multiplexer) clearStaleSource(threadID, accountID string) {
	m.staleMu.Lock()
	defer m.staleMu.Unlock()
	if set := m.staleSources[threadID]; set != nil {
		delete(set, accountID)
		if len(set) == 0 {
			delete(m.staleSources, threadID)
		}
	}
}

func (m *Multiplexer) inboundSourceIsStale(accountID, method string, params json.RawMessage) bool {
	threadID := threadIDFromInboundNotification(method, params)
	if threadID == "" {
		return false
	}
	m.staleMu.RLock()
	_, stale := m.staleSources[threadID][accountID]
	m.staleMu.RUnlock()
	return stale
}

func (m *Multiplexer) supersedeRecoveryForUserTurn(
	threadID string,
) {
	root := m.rootThreadID(threadID)

	if root == "" {
		root = threadID
	}

	if root == "" {
		return
	}

	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	if !ok {
		return
	}

	// Invalidate every recovery worker that captured the previous
	// generation before forwarding the new user turn.
	active.generation++
	active.recovering = false
	active.parked = false
	active.recoveryCause = ""
	active.failureRaw = nil
	active.rebalanceTarget = ""
	active.rebalanceBoundaryPending = false
	active.lastActivity = m.now()

	m.activeTurns[root] = active
}

func (m *Multiplexer) recoveryLeaseCurrent(
	root string,
	sourceAccountID string,
	generation uint64,
) bool {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	if !ok {
		return false
	}

	return active.accountID ==
		sourceAccountID &&
		active.recovering &&
		!active.parked &&
		active.generation ==
			generation
}

func (m *Multiplexer) interruptRecoveryTarget(
	accountID string,
	root string,
	turnID string,
) {
	child, ok := m.child(accountID)

	if !ok {
		return
	}

	params, err := json.Marshal(
		map[string]any{
			"threadId": root,
			"turnId":   turnID,
		},
	)

	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		recoveryInterruptWindow,
	)
	defer cancel()

	_, _ = child.Request(
		ctx,
		"turn/interrupt",
		params,
	)
}

func (m *Multiplexer) beginRecovery(
	threadID string,
	sourceAccountID string,
) (activeTurn, bool) {
	root := m.rootThreadID(threadID)

	if root == "" {
		root = threadID
	}

	if root == "" {
		return activeTurn{}, false
	}

	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	// Never manufacture an autonomous task from a stray notification.
	// Recovery requires a renderer-originated root turn that we actually
	// tracked.
	if !ok {
		return activeTurn{}, false
	}

	if active.accountID != "" &&
		sourceAccountID != "" &&
		active.accountID != sourceAccountID {
		return activeTurn{}, false
	}

	if active.recovering ||
		active.parked {
		return activeTurn{}, false
	}

	active.accountID = sourceAccountID
	active.rebalanceTarget = ""
	active.rebalanceBoundaryPending = false
	active.recovering = true
	active.parked = false

	// This generation is the lease held by this particular recovery.
	active.generation++
	active.lastActivity = m.now()

	m.activeTurns[root] = active

	active.params = append(
		json.RawMessage(nil),
		active.params...,
	)
	active.excluded = cloneAccountSet(
		active.excluded,
	)
	active.failureRaw = append(
		[]byte(nil),
		active.failureRaw...,
	)

	return active, true
}

func (m *Multiplexer) setRecoveryFailed(
	root string,
	sourceAccountID string,
	expectedGeneration uint64,
) bool {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	if !ok ||
		active.accountID != sourceAccountID ||
		!active.recovering ||
		active.parked ||
		active.generation != expectedGeneration {
		return false
	}

	active.recovering = false
	active.lastActivity = m.now()

	m.activeTurns[root] = active

	return true
}

func (m *Multiplexer) setRecoverySucceeded(
	root string,
	sourceAccountID string,
	targetAccountID string,
	turnID string,
	params json.RawMessage,
	excluded map[string]struct{},
	expectedGeneration uint64,
) (bool, error) {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	if !ok ||
		active.accountID != sourceAccountID ||
		!active.recovering ||
		active.parked ||
		active.generation != expectedGeneration {
		return false, nil
	}

	// Ownership and the in-memory recovery state must be committed while the
	// same generation lease is held. A user turn uses this same lock before
	// reading ThreadOwner, eliminating the stale-owner race.
	if err := m.store.SetThreadOwner(
		root,
		targetAccountID,
	); err != nil {
		return false, err
	}

	active.accountID = targetAccountID

	if turnID != "" {
		active.turnID = turnID
	}

	active.params = append(
		json.RawMessage(nil),
		params...,
	)
	active.excluded = cloneAccountSet(excluded)
	active.recovering = false
	active.parked = false
	active.lastActivity = m.now()

	m.activeTurns[root] = active

	return true, nil
}

func (m *Multiplexer) setRecoveryParked(
	root string,
	sourceAccountID string,
	cause string,
	failedRaw []byte,
	expectedGeneration uint64,
) bool {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	if !ok ||
		active.accountID != sourceAccountID ||
		!active.recovering ||
		active.parked ||
		active.generation != expectedGeneration {
		return false
	}

	active.recovering = false
	active.parked = true
	active.recoveryCause = cause
	active.failureRaw = append(
		[]byte(nil),
		failedRaw...,
	)
	active.lastActivity = m.now()

	m.activeTurns[root] = active

	// recovery parked generation mismatch is rejected above.
	return true
}

func (m *Multiplexer) claimParkedRecovery(
	root string,
) (activeTurn, bool) {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok := m.activeTurns[root]

	if !ok ||
		!active.parked ||
		active.recovering {
		return activeTurn{}, false
	}

	active.parked = false
	active.recovering = true

	// Every parked retry gets its own lease generation. A user turn can
	// invalidate this attempt exactly like an ordinary recovery.
	active.generation++
	active.lastActivity = m.now()

	m.activeTurns[root] = active

	active.params = append(
		json.RawMessage(nil),
		active.params...,
	)
	active.excluded = cloneAccountSet(
		active.excluded,
	)
	active.failureRaw = append(
		[]byte(nil),
		active.failureRaw...,
	)

	return active, true
}

func (m *Multiplexer) cancelRecoveryForUser(threadID string) {
	root := m.rootThreadID(threadID)
	if root == "" {
		root = threadID
	}
	if root == "" {
		return
	}

	tree := m.treeThreadIDs(root)
	m.activeTurnMu.Lock()
	for _, id := range tree {
		delete(m.activeTurns, id)
	}
	m.activeTurnMu.Unlock()

	m.commandMu.Lock()
	for _, id := range tree {
		delete(m.commandPIDs, id)
	}
	m.commandMu.Unlock()

	m.publish(Event{
		Type:    "thread-recovery-cancelled",
		Message: "Automatic recovery cancelled by user",
		Data: map[string]any{
			"threadId": root,
		},
	})
}

func (m *Multiplexer) cancelParkedRecoveryForUser(threadID string) {
	root := m.rootThreadID(threadID)
	if root == "" {
		root = threadID
	}
	if root == "" {
		return
	}

	m.activeTurnMu.Lock()
	active, ok := m.activeTurns[root]
	if ok && active.parked {
		delete(m.activeTurns, root)
	}
	m.activeTurnMu.Unlock()
}

func (m *Multiplexer) trackCommandLifecycle(method string, params json.RawMessage) {
	if method != "item/started" && method != "item/completed" {
		return
	}

	var decoded struct {
		ThreadID string `json:"threadId"`
		Item     struct {
			Type      string `json:"type"`
			ProcessID any    `json:"processId"`
		} `json:"item"`
	}
	if json.Unmarshal(params, &decoded) != nil || decoded.ThreadID == "" {
		return
	}
	if decoded.Item.Type != "commandExecution" {
		return
	}

	pid := 0
	switch value := decoded.Item.ProcessID.(type) {
	case string:
		pid, _ = strconv.Atoi(value)
	case float64:
		pid = int(value)
	}
	if pid <= 1 {
		return
	}

	m.commandMu.Lock()
	defer m.commandMu.Unlock()
	if method == "item/started" {
		set := m.commandPIDs[decoded.ThreadID]
		if set == nil {
			set = make(map[int]string)
			m.commandPIDs[decoded.ThreadID] = set
		}
		set[pid] = processStartSignature(pid)
		return
	}
	if set := m.commandPIDs[decoded.ThreadID]; set != nil {
		delete(set, pid)
		if len(set) == 0 {
			delete(m.commandPIDs, decoded.ThreadID)
		}
	}
}

func processStartSignature(pid int) string {
	if pid <= 1 {
		return ""
	}
	output, err := exec.Command(
		"ps",
		"-o",
		"lstart=",
		"-p",
		strconv.Itoa(pid),
	).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func childProcessIDs(pid int) []int {
	output, err := exec.Command("pgrep", "-P", strconv.Itoa(pid)).Output()
	if err != nil {
		return nil
	}
	var result []int
	for _, field := range strings.Fields(string(output)) {
		child, err := strconv.Atoi(field)
		if err == nil && child > 1 {
			result = append(result, child)
		}
	}
	return result
}

func terminateProcessTree(pid int) {
	if pid <= 1 {
		return
	}
	for _, child := range childProcessIDs(pid) {
		terminateProcessTree(child)
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	time.Sleep(150 * time.Millisecond)
	if syscall.Kill(pid, 0) == nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

type trackedCommandProcess struct {
	pid       int
	signature string
}

func (m *Multiplexer) terminateTrackedCommands(root string) {
	var processes []trackedCommandProcess
	m.commandMu.Lock()
	for _, threadID := range m.treeThreadIDs(root) {
		for pid, signature := range m.commandPIDs[threadID] {
			processes = append(processes, trackedCommandProcess{
				pid:       pid,
				signature: signature,
			})
		}
		delete(m.commandPIDs, threadID)
	}
	m.commandMu.Unlock()

	for _, process := range processes {
		// Avoid killing an unrelated process if macOS recycled a PID after the
		// command item disappeared without a matching completion notification.
		if process.signature != "" &&
			processStartSignature(process.pid) != process.signature {
			continue
		}
		terminateProcessTree(process.pid)
	}
}

func (m *Multiplexer) bestEffortInterruptTree(root, sourceAccountID string) {
	child, ok := m.child(sourceAccountID)
	if !ok {
		return
	}

	type runningTurn struct {
		threadID string
		turnID   string
	}
	var turns []runningTurn
	m.activeTurnMu.Lock()
	for _, threadID := range m.treeThreadIDs(root) {
		active, exists := m.activeTurns[threadID]
		if !exists || active.accountID != sourceAccountID || active.turnID == "" {
			continue
		}
		turns = append(turns, runningTurn{threadID: threadID, turnID: active.turnID})
	}
	m.activeTurnMu.Unlock()

	for _, turn := range turns {
		params, _ := json.Marshal(map[string]any{
			"threadId": turn.threadID,
			"turnId":   turn.turnID,
		})
		ctx, cancel := context.WithTimeout(context.Background(), recoveryInterruptWindow)
		_, _ = child.Request(ctx, "turn/interrupt", params)
		cancel()
	}
}

func itemNotificationHasAgentMessage(
	params json.RawMessage,
) bool {
	var decoded struct {
		Item struct {
			Type    string          `json:"type"`
			Text    string          `json:"text"`
			Content json.RawMessage `json:"content"`
		} `json:"item"`
	}

	if json.Unmarshal(params, &decoded) != nil {
		return false
	}

	if !strings.EqualFold(
		strings.TrimSpace(decoded.Item.Type),
		"agentMessage",
	) {
		return false
	}

	if strings.TrimSpace(decoded.Item.Text) != "" {
		return true
	}

	content := strings.TrimSpace(
		string(decoded.Item.Content),
	)

	return content != "" &&
		content != "null" &&
		content != "[]" &&
		content != "{}"
}

func (m *Multiplexer) setAgentMessageComplete(
	threadID string,
	accountID string,
	complete bool,
) {
	if threadID == "" {
		return
	}

	root := m.rootThreadID(threadID)

	ids := []string{threadID}
	if root != "" && root != threadID {
		ids = append(ids, root)
	}

	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	for _, id := range ids {
		active, ok := m.activeTurns[id]
		if !ok {
			continue
		}

		// During recovery the target may emit notifications before ownership
		// is committed. Allow that race, but normally require the current
		// account to match.
		if accountID != "" &&
			active.accountID != "" &&
			active.accountID != accountID &&
			!active.recovering {
			continue
		}

		active.agentMessageComplete = complete
		m.activeTurns[id] = active
	}
}

func silentCompletionNeedsQuotaVerification(
	active activeTurn,
	params json.RawMessage,
) bool {
	_, suspicious := silentCompletedTurn(params)

	return suspicious &&
		!active.agentMessageComplete
}

func parseCollabItem(params json.RawMessage) (sender string, receivers []string, quotaChild string) {
	var decoded struct {
		Item struct {
			Type              string   `json:"type"`
			SenderThreadID    string   `json:"senderThreadId"`
			ReceiverThreadIDs []string `json:"receiverThreadIds"`
			AgentsStates      map[string]struct {
				Status  string  `json:"status"`
				Message *string `json:"message"`
			} `json:"agentsStates"`
		} `json:"item"`
	}
	if json.Unmarshal(params, &decoded) != nil {
		return "", nil, ""
	}
	if decoded.Item.Type != "collabAgentToolCall" {
		return "", nil, ""
	}

	for threadID, agent := range decoded.Item.AgentsStates {
		if agent.Status != "errored" || agent.Message == nil {
			continue
		}
		if usageLimitText(*agent.Message) {
			quotaChild = threadID
			break
		}
	}
	return decoded.Item.SenderThreadID, decoded.Item.ReceiverThreadIDs, quotaChild
}

func (m *Multiplexer) recordCollabLineage(params json.RawMessage) string {
	sender, receivers, quotaChild := parseCollabItem(params)
	if sender != "" && len(receivers) > 0 {
		m.lineageMu.Lock()
		for _, receiver := range receivers {
			if receiver != "" && receiver != sender {
				if _, exists := m.threadParents[receiver]; !exists {
					m.threadParents[receiver] = sender
				}
			}
		}
		m.lineageMu.Unlock()
	}
	return quotaChild
}

func parseErrorNotification(params json.RawMessage) (threadID string, willRetry bool, limited bool) {
	var decoded struct {
		ThreadID  string          `json:"threadId"`
		WillRetry bool            `json:"willRetry"`
		Error     json.RawMessage `json:"error"`
	}

	if json.Unmarshal(params, &decoded) != nil {
		return "", false, false
	}

	// Strong quota signatures are safe everywhere.
	if usageLimitText(string(decoded.Error)) {
		return decoded.ThreadID, decoded.WillRetry, true
	}

	// This is a structured app-server error channel, so preserve support for
	// legacy short quota messages without making the global text classifier
	// broad enough to mistake ordinary conversation/debug prose for quota.
	var quotaError struct {
		Message        string          `json:"message"`
		CodexErrorInfo json.RawMessage `json:"codexErrorInfo"`
		Code           string          `json:"code"`
		Type           string          `json:"type"`
	}

	if json.Unmarshal(decoded.Error, &quotaError) != nil {
		return decoded.ThreadID, decoded.WillRetry, false
	}

	var codexErrorInfo string
	_ = json.Unmarshal(
		quotaError.CodexErrorInfo,
		&codexErrorInfo,
	)

	if codexErrorInfo == "usageLimitExceeded" {
		return decoded.ThreadID, decoded.WillRetry, true
	}

	for _, value := range []string{
		quotaError.Code,
		quotaError.Type,
	} {
		if usageLimitText(value) {
			return decoded.ThreadID, decoded.WillRetry, true
		}
	}

	switch strings.ToLower(strings.TrimSpace(quotaError.Message)) {
	case "rate limit",
		"usage limit",
		"quota":
		return decoded.ThreadID, decoded.WillRetry, true
	}

	return decoded.ThreadID, decoded.WillRetry, false
}

func parseUsageLimitedGoal(params json.RawMessage) string {
	var decoded struct {
		ThreadID string `json:"threadId"`
		Goal     struct {
			Status string `json:"status"`
		} `json:"goal"`
	}
	if json.Unmarshal(params, &decoded) != nil || decoded.Goal.Status != "usageLimited" {
		return ""
	}
	return decoded.ThreadID
}

func (m *Multiplexer) clearCompletedThreadTree(threadID, accountID string) {
	root := m.rootThreadID(threadID)
	if root == "" {
		root = threadID
	}

	if root != threadID {
		m.clearActiveTurn(threadID, accountID)
		m.commandMu.Lock()
		delete(m.commandPIDs, threadID)
		m.commandMu.Unlock()
		return
	}

	tree := m.treeThreadIDs(root)
	m.activeTurnMu.Lock()
	for _, id := range tree {
		active, ok := m.activeTurns[id]
		if !ok {
			continue
		}
		if accountID == "" || active.accountID == "" || active.accountID == accountID {
			delete(m.activeTurns, id)
		}
	}
	m.activeTurnMu.Unlock()

	m.commandMu.Lock()
	for _, id := range tree {
		delete(m.commandPIDs, id)
	}
	m.commandMu.Unlock()
}

func (m *Multiplexer) observeRecoveryNotification(
	inbound backend.Inbound,
) bool {
	message := inbound.Message
	method := message.Method

	var started threadStartMeta

	if method == "thread/started" {
		started = m.recordThreadStarted(
			inbound.AccountID,
			message.Params,
		)
	}

	if method == "turn/started" {
		m.recordTurnStarted(
			inbound.AccountID,
			message.Params,
		)

		m.requestQuotaBalance()
	}

	m.trackCommandLifecycle(
		method,
		message.Params,
	)

	threadID := threadIDFromInboundNotification(
		method,
		message.Params,
	)

	if threadID == "" &&
		started.ID != "" {
		threadID = started.ID
	}

	if threadID != "" {
		m.touchThread(
			threadID,
			inbound.AccountID,
		)
	}

	childThread :=
		started.ParentID != ""

	if !childThread &&
		threadID != "" {
		root := m.rootThreadID(
			threadID,
		)

		childThread =
			root != "" &&
				root != threadID
	}

	if method == "item/started" &&
		threadID != "" {
		m.clearQuotaRebalanceBoundary(
			threadID,
			inbound.AccountID,
		)

		m.setAgentMessageComplete(
			threadID,
			inbound.AccountID,
			false,
		)
	}

	if method == "item/completed" &&
		threadID != "" &&
		itemNotificationHasAgentMessage(
			message.Params,
		) {
		m.setAgentMessageComplete(
			threadID,
			inbound.AccountID,
			true,
		)
	}

	if method == "item/started" ||
		method == "item/completed" {
		if quotaChild :=
			m.recordCollabLineage(
				message.Params,
			); quotaChild != "" {
			m.markAccountQuotaBlockedFor(
				inbound.AccountID,
				m.threadQuotaBucket(
					quotaChild,
					nil,
				),
			)
		}
	}

	if method == "item/completed" &&
		threadID != "" {
		m.scheduleQuotaRebalanceBoundary(
			threadID,
			inbound.AccountID,
		)
	}

	if method == "thread/goal/updated" {
		if limitedThread :=
			parseUsageLimitedGoal(
				message.Params,
			); limitedThread != "" {
			m.markAccountQuotaBlockedFor(
				inbound.AccountID,
				m.threadQuotaBucket(
					limitedThread,
					nil,
				),
			)

			root :=
				m.rootThreadID(
					limitedThread,
				)

			if root != "" &&
				root != limitedThread {
				childThread = true
			}
		}
	}

	if method == "error" {
		errorThread, _, limited :=
			parseErrorNotification(
				message.Params,
			)

		if limited {
			m.markAccountQuotaBlockedFor(
				inbound.AccountID,
				m.threadQuotaBucket(
					errorThread,
					nil,
				),
			)

			root :=
				m.rootThreadID(
					errorThread,
				)

			if root != "" &&
				root != errorThread {
				childThread = true
			}
		}
	}

	if method == "turn/completed" {
		completedThread, explicitLimit :=
			turnCompletedUsageLimit(
				message.Params,
			)

		if completedThread == "" {
			completedThread, _ =
				silentCompletedTurn(
					message.Params,
				)
		}

		if completedThread != "" {
			root := m.rootThreadID(
				completedThread,
			)

			if root == "" {
				root = completedThread
			}

			if root != completedThread {
				childThread = true
			}

			if explicitLimit {
				m.markAccountQuotaBlockedFor(
					inbound.AccountID,
					m.threadQuotaBucket(
						completedThread,
						nil,
					),
				)

				if root == completedThread {
					go m.recoverThreadTree(
						root,
						inbound.AccountID,
						"root turn completed at usage limit",
						append(
							[]byte(nil),
							inbound.Raw...,
						),
						true,
					)

					return true
				}
			}

			if root == completedThread {
				if active, tracked :=
					m.activeTurnFor(
						completedThread,
						inbound.AccountID,
					); tracked &&
					silentCompletionNeedsQuotaVerification(
						active,
						message.Params,
					) {
					go m.verifySilentCompletionAndRecover(
						inbound,
						root,
					)

					return true
				}
			}

			if m.threadTreeRecovering(
				root,
			) {
				return true
			}

			m.clearCompletedThreadTree(
				completedThread,
				inbound.AccountID,
			)
		}
	}

	// Child/subagent app-server threads are implementation details.
	// The parent collaboration item is still forwarded normally, but the
	// child's own thread/turn/item lifecycle must not masquerade as a new
	// top-level conversation in the desktop renderer.
	if childThread {
		return true
	}

	if m.inboundSourceIsStale(
		inbound.AccountID,
		method,
		message.Params,
	) {
		return true
	}

	return false
}

func (m *Multiplexer) threadTreeRecovering(root string) bool {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()
	active, ok := m.activeTurns[root]
	return ok && (active.recovering || active.parked)
}

func (m *Multiplexer) verifySilentCompletionAndRecover(
	inbound backend.Inbound,
	root string,
) {
	threadID := threadIDFromInboundNotification(
		inbound.Message.Method,
		inbound.Message.Params,
	)

	// Give a separately emitted item/completed agentMessage a small chance
	// to arrive before interpreting an empty turn/completed as quota loss.
	time.Sleep(recoveryCompletionGrace)

	active, ok := m.activeTurnFor(
		threadID,
		inbound.AccountID,
	)

	if !ok {
		// The turn was already finalized elsewhere. Never resurrect it.
		m.writeRaw(inbound.Raw)
		return
	}

	if active.agentMessageComplete {
		m.clearCompletedThreadTree(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(inbound.Raw)
		return
	}

	bucket := m.threadQuotaBucket(
		threadID,
		active.params,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		requestTimeout,
	)

	snapshot, err := m.accountSnapshotWithProfile(
		ctx,
		inbound.AccountID,
		false,
	)

	cancel()

	if err == nil &&
		rateLimitsHaveCapacity(
			rateLimitsForQuotaBucket(
				snapshot,
				bucket,
			),
		) &&
		!m.accountQuotaBlockedFor(
			inbound.AccountID,
			bucket,
		) {
		m.clearActiveTurn(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(inbound.Raw)
		return
	}

	if err != nil {
		// Unknown is not proof of quota exhaustion.
		m.clearActiveTurn(
			threadID,
			inbound.AccountID,
		)
		m.writeRaw(inbound.Raw)
		return
	}

	m.markAccountQuotaBlockedFor(
		inbound.AccountID,
		bucket,
	)

	m.recoverThreadTree(
		root,
		inbound.AccountID,
		"silent completion after quota exhaustion",
		append([]byte(nil), inbound.Raw...),
		true,
	)
}

func (m *Multiplexer) shouldForwardInbound(inbound backend.Inbound) bool {
	if m.inboundSourceIsStale(inbound.AccountID, inbound.Message.Method, inbound.Message.Params) {
		return false
	}
	return m.shouldForwardNotification(inbound.AccountID, inbound.Message.Method)
}

func (m *Multiplexer) recoveryWatchLoop(ctx context.Context) {
	ticker := time.NewTicker(recoveryWatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.recoverStalledQuotaTurns()
		}
	}
}

func (m *Multiplexer) recoverStalledQuotaTurns() {
	now := m.now()

	type candidate struct {
		threadID  string
		accountID string
		bucket    quotaBucket
	}

	roots := make(
		map[string]candidate,
	)

	m.activeTurnMu.Lock()

	for threadID, active := range m.activeTurns {
		if active.recovering ||
			active.parked ||
			active.agentMessageComplete ||
			active.accountID == "" {
			continue
		}

		if active.lastActivity.IsZero() ||
			now.Sub(active.lastActivity) <
				recoveryStallThreshold {
			continue
		}

		root := m.rootThreadID(
			threadID,
		)

		if root == "" {
			root = threadID
		}

		bucket := m.threadQuotaBucket(
			threadID,
			active.params,
		)

		roots[root] = candidate{
			threadID:  root,
			accountID: active.accountID,
			bucket:    bucket,
		}
	}

	m.activeTurnMu.Unlock()

	for _, entry := range roots {
		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				requestTimeout,
			)

		snapshot, err :=
			m.accountSnapshotWithProfile(
				ctx,
				entry.accountID,
				false,
			)

		cancel()

		if err != nil {
			continue
		}

		limits :=
			rateLimitsForQuotaBucket(
				snapshot,
				entry.bucket,
			)

		if rateLimitsHaveCapacity(
			limits,
		) &&
			!m.accountQuotaBlockedFor(
				entry.accountID,
				entry.bucket,
			) {
			continue
		}

		// Stalled does not mean terminal. Remember the exhausted account
		// for future routing, but do not create another root while the
		// existing source may still be executing commands.
		m.markAccountQuotaBlockedFor(
			entry.accountID,
			entry.bucket,
		)
	}
}

func (m *Multiplexer) recoverThreadTree(
	threadID string,
	sourceAccountID string,
	cause string,
	failedRaw []byte,
	quota bool,
) {
	root := m.rootThreadID(threadID)

	if root == "" {
		root = threadID
	}

	if root == "" {
		return
	}

	active, ok :=
		m.beginRecovery(
			root,
			sourceAccountID,
		)

	if !ok {
		return
	}

	bucket := m.threadQuotaBucket(
		root,
		active.params,
	)

	if quota {
		m.markAccountQuotaBlockedFor(
			sourceAccountID,
			bucket,
		)
	}

	m.markTreeSourceStale(
		root,
		sourceAccountID,
	)

	m.bestEffortInterruptTree(
		root,
		sourceAccountID,
	)

	m.terminateTrackedCommands(root)

	time.Sleep(
		recoveryFlushDelay,
	)

	excluded :=
		cloneAccountSet(
			active.excluded,
		)

	if excluded == nil {
		excluded =
			make(map[string]struct{})
	}

	if quota {
		excluded[sourceAccountID] =
			struct{}{}
	}

	err := m.performThreadRecovery(
		root,
		sourceAccountID,
		active,
		excluded,
		cause,
		!quota,
	)

	if err == nil {
		return
	}

	if errors.Is(
		err,
		errNoSubscriptionCapacity,
	) {
		if m.setRecoveryParked(
			root,
			sourceAccountID,
			cause,
			failedRaw,
			active.generation,
		) {
			m.publish(Event{
				Type:      "thread-recovery-parked",
				AccountID: sourceAccountID,
				Message:   "Autonomous task parked until subscription capacity returns",
				Data: map[string]any{
					"threadId": root,
					"cause":    cause,
				},
			})

			go m.waitForParkedRecovery(
				root,
			)
		}

		return
	}

	// If the user superseded this recovery, do not emit its obsolete
	// failure and do not mutate the new turn.
	if !m.setRecoveryFailed(
		root,
		sourceAccountID,
		active.generation,
	) {
		return
	}

	if len(failedRaw) > 0 {
		m.writeRaw(
			failedRaw,
		)
	}

	m.publish(Event{
		Type:      "thread-recovery-failed",
		AccountID: sourceAccountID,
		Message: fmt.Sprintf(
			"Could not recover interrupted chat: %v",
			err,
		),
		Data: map[string]any{
			"threadId": root,
			"cause":    cause,
		},
	})
}

func (m *Multiplexer) waitForParkedRecovery(
	root string,
) {
	for {
		ctx := m.runCtx

		if ctx == nil {
			ctx = context.Background()
		}

		timer := time.NewTimer(
			recoveryRetryInterval,
		)

		select {
		case <-ctx.Done():
			timer.Stop()
			return

		case <-timer.C:
		}

		active, ok :=
			m.claimParkedRecovery(root)

		if !ok {
			return
		}

		err := m.performThreadRecovery(
			root,
			active.accountID,
			active,
			nil,
			"capacity returned after parked quota failure",
			true,
		)

		if err == nil {
			return
		}

		if errors.Is(
			err,
			errNoSubscriptionCapacity,
		) {
			// Re-park only if this exact retry generation is still current.
			if !m.setRecoveryParked(
				root,
				active.accountID,
				active.recoveryCause,
				active.failureRaw,
				active.generation,
			) {
				return
			}

			continue
		}

		if m.setRecoveryFailed(
			root,
			active.accountID,
			active.generation,
		) &&
			len(active.failureRaw) > 0 {
			m.writeRaw(
				active.failureRaw,
			)
		}

		return
	}
}

func (m *Multiplexer) performThreadRecovery(
	root string,
	sourceAccountID string,
	active activeTurn,
	excluded map[string]struct{},
	cause string,
	preferSource bool,
) error {
	if !m.recoveryLeaseCurrent(
		root,
		sourceAccountID,
		active.generation,
	) {
		return nil
	}

	tried := cloneAccountSet(excluded)

	if tried == nil {
		tried = make(map[string]struct{})
	}

	bucket := m.threadQuotaBucket(
		root,
		active.params,
	)

	var candidates []state.Account

	if preferSource {
		if source, ok :=
			m.store.Account(
				sourceAccountID,
			); ok &&
			source.Enabled &&
			!m.accountQuotaBlockedFor(
				sourceAccountID,
				bucket,
			) {
			candidates = append(
				candidates,
				source,
			)
		}
	}

	for attempts := 0; attempts <
		len(m.store.Accounts())+2; attempts++ {
		if !m.recoveryLeaseCurrent(
			root,
			sourceAccountID,
			active.generation,
		) {
			return nil
		}

		var target state.Account

		if len(candidates) > 0 {
			target = candidates[0]
			candidates = candidates[1:]

			if _, already :=
				tried[target.ID]; already {
				continue
			}
		} else {
			ctx, cancel :=
				context.WithTimeout(
					context.Background(),
					2*requestTimeout,
				)

			fallback, _, selectedBucket, err :=
				m.chooseAccountWithReserveFallback(
					ctx,
					tried,
					bucket,
				)

			cancel()

			if err != nil {
				return err
			}

			if selectedBucket != bucket {
				bucket = selectedBucket

				// Reserve is independently metered, so normal-quota
				// exclusions do not carry into its candidate pool.
				tried =
					make(map[string]struct{})

				updatedParams, paramErr :=
					paramsForQuotaBucket(
						active.params,
						bucket,
					)

				if paramErr != nil {
					return fmt.Errorf(
						"switch recovery to Luna Reserve: %w",
						paramErr,
					)
				}

				active.params =
					updatedParams

				m.rememberThreadQuotaBucket(
					root,
					bucket,
				)
			}

			target = fallback
		}

		if target.ID == "" {
			continue
		}

		tried[target.ID] =
			struct{}{}

		if !m.recoveryLeaseCurrent(
			root,
			sourceAccountID,
			active.generation,
		) {
			return nil
		}

		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				2*requestTimeout,
			)

		err := m.resumeThreadOnAccount(
			ctx,
			root,
			sourceAccountID,
			target.ID,
		)

		cancel()

		if err != nil {
			fmt.Fprintf(
				os.Stderr,
				"codex-mux: recover %s: resume %s -> %s failed: %v\n",
				root,
				sourceAccountID,
				target.ID,
				err,
			)

			continue
		}

		if !m.recoveryLeaseCurrent(
			root,
			sourceAccountID,
			active.generation,
		) {
			return nil
		}

		params, err :=
			continuationTurnParams(
				active.params,
				root,
			)

		if err != nil {
			return err
		}

		targetChild, ok :=
			m.child(target.ID)

		if !ok {
			continue
		}

		// Validate and update terminal-message state atomically with the
		// same generation lease immediately before turn/start.
		m.activeTurnMu.Lock()

		current, currentOK :=
			m.activeTurns[root]

		leaseOK :=
			currentOK &&
				current.accountID ==
					sourceAccountID &&
				current.recovering &&
				!current.parked &&
				current.generation ==
					active.generation

		if leaseOK {
			current.agentMessageComplete =
				false
			m.activeTurns[root] =
				current
		}

		m.activeTurnMu.Unlock()

		if !leaseOK {
			return nil
		}

		ctx, cancel =
			context.WithTimeout(
				context.Background(),
				2*requestTimeout,
			)

		response, startErr :=
			targetChild.Request(
				ctx,
				"turn/start",
				params,
			)

		cancel()

		if startErr != nil &&
			strings.Contains(
				strings.ToLower(
					startErr.Error(),
				),
				"active turn",
			) {
			// Do not interrupt something on the target if a newer user
			// generation superseded this recovery while Request was
			// in flight.
			if !m.recoveryLeaseCurrent(
				root,
				sourceAccountID,
				active.generation,
			) {
				return nil
			}

			interruptParams, _ :=
				json.Marshal(
					map[string]any{
						"threadId": root,
						"turnId":   "",
					},
				)

			interruptCtx,
				interruptCancel :=
				context.WithTimeout(
					context.Background(),
					recoveryInterruptWindow,
				)

			_, _ = targetChild.Request(
				interruptCtx,
				"turn/interrupt",
				interruptParams,
			)

			interruptCancel()

			if !m.recoveryLeaseCurrent(
				root,
				sourceAccountID,
				active.generation,
			) {
				return nil
			}

			retryCtx, retryCancel :=
				context.WithTimeout(
					context.Background(),
					2*requestTimeout,
				)

			response, startErr =
				targetChild.Request(
					retryCtx,
					"turn/start",
					params,
				)

			retryCancel()
		}

		if startErr != nil {
			if usageLimitText(
				startErr.Error(),
			) {
				m.markAccountQuotaBlockedFor(
					target.ID,
					bucket,
				)
			}

			fmt.Fprintf(
				os.Stderr,
				"codex-mux: recover %s: continuation on %s failed: %v\n",
				root,
				target.ID,
				startErr,
			)

			continue
		}

		turnID :=
			turnIDFromTurnStartResult(
				response.Result,
			)

		// Commit owner + recovery state under the same generation lease.
		// This is the serialization point shared with user turn/start.
		committed, commitErr :=
			m.setRecoverySucceeded(
				root,
				sourceAccountID,
				target.ID,
				turnID,
				active.params,
				nil,
				active.generation,
			)

		if commitErr != nil {
			m.interruptRecoveryTarget(
				target.ID,
				root,
				turnID,
			)

			return commitErr
		}

		if !committed {
			m.interruptRecoveryTarget(
				target.ID,
				root,
				turnID,
			)

			return nil
		}

		m.clearStaleSource(
			root,
			target.ID,
		)

		m.publish(Event{
			Type:      "thread-autonomous-failed-over",
			AccountID: target.ID,
			Message: fmt.Sprintf(
				"Chat continued with %s",
				target.Label,
			),
			Data: map[string]any{
				"threadId":          root,
				"previousAccountId": sourceAccountID,
				"cause":             cause,
			},
		})

		fmt.Fprintf(
			os.Stderr,
			"codex-mux: recovered thread=%s %s -> %s cause=%q\n",
			root,
			sourceAccountID,
			target.ID,
			cause,
		)

		return nil
	}

	return errNoSubscriptionCapacity
}

func turnIDFromTurnStartResult(result json.RawMessage) string {
	var decoded struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if json.Unmarshal(result, &decoded) != nil {
		return ""
	}
	return decoded.Turn.ID
}

func (m *Multiplexer) robustFailoverTurn(
	message protocol.Message,
	threadID string,
	sourceAccountID string,
	excluded map[string]struct{},
) {
	tried := cloneAccountSet(excluded)
	if tried == nil {
		tried = make(map[string]struct{})
	}
	tried[sourceAccountID] = struct{}{}

	bucket := m.threadQuotaBucket(
		threadID,
		message.Params,
	)

	for attempts := 0; attempts < len(m.store.Accounts())+1; attempts++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
		fallback, _, selectedBucket, err := m.chooseAccountWithReserveFallback(
			ctx,
			tried,
			bucket,
		)
		cancel()
		if selectedBucket != bucket && err != nil {
			// If both normal and Reserve are exhausted, report depletion using the
			// final quota domain that was actually checked.
			bucket = selectedBucket
		}
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			m.write(
				m.allSubscriptionsDepletedForQuotaBucket(
					ctx,
					message.ID,
					bucket,
				),
			)
			cancel()
			return
		}
		if selectedBucket != bucket {
			bucket = selectedBucket

			// Normal exhaustion must never blacklist the same account's Reserve
			// allowance.
			tried = make(map[string]struct{})

			updatedParams, paramErr := paramsForQuotaBucket(
				message.Params,
				bucket,
			)
			if paramErr != nil {
				m.write(
					protocol.Failure(
						message.ID,
						-32027,
						paramErr.Error(),
					),
				)
				return
			}

			message.Params = updatedParams

			m.rememberThreadQuotaBucket(
				threadID,
				bucket,
			)
		}

		tried[fallback.ID] = struct{}{}

		ctx, cancel = context.WithTimeout(context.Background(), 2*requestTimeout)
		err = m.resumeThreadOnAccount(ctx, threadID, sourceAccountID, fallback.ID)
		cancel()
		if err != nil {
			fmt.Fprintf(
				os.Stderr,
				"codex-mux: interactive resume %s -> %s failed: %v\n",
				sourceAccountID,
				fallback.ID,
				err,
			)
			continue
		}

		if err := m.forwardWithExclusions(fallback.ID, message, nil); err != nil {
			continue
		}
		if err := m.store.SetThreadOwner(threadID, fallback.ID); err != nil {
			m.write(protocol.Failure(message.ID, -32028, err.Error()))
			return
		}

		m.markTreeSourceStale(m.rootThreadID(threadID), sourceAccountID)
		m.publish(Event{
			Type:      "thread-failed-over",
			AccountID: fallback.ID,
			Message:   fmt.Sprintf("Chat continued with %s", fallback.Label),
			Data: map[string]any{
				"threadId":          threadID,
				"previousAccountId": sourceAccountID,
			},
		})
		return
	}

	m.write(protocol.Failure(message.ID, -32027, "could not resume chat on any available subscription"))
}

func (m *Multiplexer) retryTurnAfterUsageLimitRobust(
	route externalRoute,
	exhaustedAccountID string,
) {
	threadID := threadIDFromParams(route.message.Params)

	bucket := m.threadQuotaBucket(
		threadID,
		route.message.Params,
	)

	m.markAccountQuotaBlockedFor(
		exhaustedAccountID,
		bucket,
	)

	if threadID == "" {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			requestTimeout,
		)

		m.write(
			m.allSubscriptionsDepletedForQuotaBucket(
				ctx,
				route.message.ID,
				bucket,
			),
		)

		cancel()
		return
	}

	excluded := cloneAccountSet(route.excluded)
	if excluded == nil {
		excluded = make(map[string]struct{})
	}

	excluded[exhaustedAccountID] = struct{}{}

	m.robustFailoverTurn(
		route.message,
		threadID,
		exhaustedAccountID,
		excluded,
	)
}

func stableReadFile(path string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		before, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		time.Sleep(80 * time.Millisecond)
		after, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if before.Size() == after.Size() &&
			before.ModTime().Equal(after.ModTime()) {
			return data, nil
		}
		lastErr = errors.New("rollout is still being written")
	}
	return nil, lastErr
}

func (m *Multiplexer) findRolloutForThread(accountID, threadID string) string {
	account, ok := m.store.Account(accountID)
	if !ok || threadID == "" {
		return ""
	}
	root := filepath.Join(account.CodexHome, "sessions")

	var newestPath string
	var newestTime time.Time
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".jsonl") || !strings.Contains(name, threadID) {
			return nil
		}
		info, err := entry.Info()
		if err == nil && (newestPath == "" || info.ModTime().After(newestTime)) {
			newestPath = path
			newestTime = info.ModTime()
		}
		return nil
	})
	return newestPath
}

func (m *Multiplexer) threadRecoveryData(
	ctx context.Context,
	threadID, sourceAccountID string,
) (threadRecoveryData, error) {
	var data threadRecoveryData

	if source, ok := m.child(sourceAccountID); ok {
		params, _ := json.Marshal(map[string]any{
			"threadId":     threadID,
			"includeTurns": false,
		})
		response, err := source.Request(ctx, "thread/read", params)
		if err == nil {
			var decoded struct {
				Thread struct {
					ID            string `json:"id"`
					Path          string `json:"path"`
					CWD           string `json:"cwd"`
					ModelProvider string `json:"modelProvider"`
				} `json:"thread"`
			}
			if json.Unmarshal(response.Result, &decoded) == nil && decoded.Thread.ID != "" {
				data.Path = decoded.Thread.Path
				data.CWD = decoded.Thread.CWD
				data.ModelProvider = decoded.Thread.ModelProvider
			}
		}
	}

	if data.Path == "" {
		data.Path = m.findRolloutForThread(sourceAccountID, threadID)
	}
	if data.Path == "" {
		return data, fmt.Errorf("no rollout found for thread id %s", threadID)
	}
	return data, nil
}

func (m *Multiplexer) mirrorStableRollout(
	sourcePath, targetAccountID string,
) (string, error) {
	targetAccount, ok := m.store.Account(targetAccountID)
	if !ok {
		return "", fmt.Errorf("target subscription %q not found", targetAccountID)
	}
	sourcePath = filepath.Clean(sourcePath)

	const marker = string(filepath.Separator) + "sessions" + string(filepath.Separator)
	relative := filepath.Base(sourcePath)
	if index := strings.LastIndex(sourcePath, marker); index >= 0 {
		relative = sourcePath[index+len(marker):]
	}
	targetPath := filepath.Join(targetAccount.CodexHome, "sessions", relative)

	if sameFilePath(sourcePath, targetPath) {
		return sourcePath, nil
	}

	data, err := stableReadFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf("read stable source rollout: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return "", err
	}

	temporary := targetPath + ".codex-mux.tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(temporary)
		return "", writeErr
	}
	if syncErr != nil {
		_ = os.Remove(temporary)
		return "", syncErr
	}
	if closeErr != nil {
		_ = os.Remove(temporary)
		return "", closeErr
	}
	if err := os.Rename(temporary, targetPath); err != nil {
		_ = os.Remove(temporary)
		return "", err
	}
	if dir, err := os.Open(filepath.Dir(targetPath)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return targetPath, nil
}

func sameFilePath(left, right string) bool {
	return filepath.Clean(left) == filepath.Clean(right)
}

func (m *Multiplexer) verifyResumedThread(
	ctx context.Context,
	target *backend.Child,
	threadID string,
) error {
	params, _ := json.Marshal(map[string]any{
		"threadId":     threadID,
		"includeTurns": false,
	})
	response, err := target.Request(ctx, "thread/read", params)
	if err != nil {
		return err
	}
	var decoded struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(response.Result, &decoded) != nil || decoded.Thread.ID != threadID {
		return fmt.Errorf("target did not expose resumed thread %s", threadID)
	}
	return nil
}

func (m *Multiplexer) robustResumeThreadOnAccount(
	ctx context.Context,
	threadID, sourceAccountID, targetAccountID string,
) error {
	target, ok := m.child(targetAccountID)
	if !ok {
		return fmt.Errorf("target subscription is unavailable")
	}

	// A restarted app-server can normally resume its own thread directly from
	// its local state database.
	if sourceAccountID == targetAccountID {
		params, _ := json.Marshal(map[string]any{"threadId": threadID})
		resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, resumeErr := target.Request(resumeCtx, "thread/resume", params)
		resumeCancel()
		if resumeErr == nil {
			verifyCtx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			verifyErr := m.verifyResumedThread(verifyCtx, target, threadID)
			cancel()
			return verifyErr
		}
	}

	data, err := m.threadRecoveryData(ctx, threadID, sourceAccountID)
	if err != nil {
		return err
	}
	targetPath, err := m.mirrorStableRollout(data.Path, targetAccountID)
	if err != nil {
		return fmt.Errorf("mirror rollout: %w", err)
	}

	pathParams := map[string]any{
		"threadId": "",
		"history":  nil,
		"path":     targetPath,
		"model":    nil,
	}
	if data.CWD != "" {
		pathParams["cwd"] = data.CWD
	}
	encoded, _ := json.Marshal(pathParams)

	response, pathErr := target.Request(ctx, "thread/resume", encoded)
	if pathErr == nil {
		resumedID := threadIDFromResult(response.Result)
		if resumedID != "" && resumedID != threadID {
			return fmt.Errorf("path resume returned different thread id %s", resumedID)
		}
		verifyCtx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		verifyErr := m.verifyResumedThread(verifyCtx, target, threadID)
		cancel()
		if verifyErr == nil {
			return nil
		}
		pathErr = verifyErr
	}

	// The local path may not yet be indexed in SQLite. Retry by id after the
	// path attempt because successful path parsing often materializes the
	// metadata needed by the id resolver.
	idParams, _ := json.Marshal(map[string]any{"threadId": threadID})
	if _, idErr := target.Request(ctx, "thread/resume", idParams); idErr == nil {
		verifyCtx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		verifyErr := m.verifyResumedThread(verifyCtx, target, threadID)
		cancel()
		if verifyErr == nil {
			return nil
		}
		return fmt.Errorf("resume verification failed: %w", verifyErr)
	} else {
		return fmt.Errorf(
			"target-local resume failed: path: %v; id: %w",
			pathErr,
			idErr,
		)
	}
}

func (m *Multiplexer) handleChildExit(accountID, detail string) {
	m.childrenMu.Lock()
	delete(m.children, accountID)
	m.childrenMu.Unlock()

	var roots []string
	seen := make(map[string]struct{})
	m.activeTurnMu.Lock()
	for threadID, active := range m.activeTurns {
		if active.accountID != accountID ||
			active.agentMessageComplete {
			continue
		}
		root := m.rootThreadID(threadID)
		if root == "" {
			root = threadID
		}
		if _, exists := seen[root]; !exists {
			seen[root] = struct{}{}
			roots = append(roots, root)
		}
	}
	m.activeTurnMu.Unlock()

	account, exists := m.store.Account(accountID)
	if exists && account.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), 2*requestTimeout)
		if _, err := m.startChild(ctx, account); err != nil {
			fmt.Fprintf(os.Stderr, "codex-mux: restart account %s failed: %v\n", accountID, err)
		}
		cancel()
	}

	for _, root := range roots {
		go m.recoverThreadTree(
			root,
			accountID,
			"Codex app-server exited: "+detail,
			nil,
			false,
		)
	}
}
