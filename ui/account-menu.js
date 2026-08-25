const CODEX_MUX_API = "http://127.0.0.1:__CODEX_MUX_CONTROL_PORT__/v1";
const CODEX_MUX_TOKEN = "__CODEX_MUX_CONTROL_TOKEN__";
let codexMuxLoginActive = false;

function CodexMuxProfileMenuOpenChange(setOpen) {
  return (nextOpen) => {
    if (!nextOpen && codexMuxLoginActive) return;
    setOpen(nextOpen);
  };
}

async function codexMuxRequest(path, options = {}) {
  const response = await fetch(`${CODEX_MUX_API}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      "X-Codex-Mux-Token": CODEX_MUX_TOKEN,
      ...options.headers,
    },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}

const CODEX_MUX_ACCOUNT_SCOPED_PLUGIN_METHODS = new Set([
  "app/list",
  "app/installed",
  "app/read",
  "mcpServerStatus/list",
  "mcpServer/oauth/login",
]);

function codexMuxScopePluginRequest(method, params) {
  const accountId = globalThis.__codexMuxPluginAccountId;
  if (
    !accountId ||
    !CODEX_MUX_ACCOUNT_SCOPED_PLUGIN_METHODS.has(method) ||
    (params != null &&
      (typeof params !== "object" || Array.isArray(params)))
  ) {
    return params;
  }
  return { ...(params || {}), codexMuxAccountId: accountId };
}

async function codexMuxProfileData(accountId = null) {
  const query = accountId
    ? `?accountId=${encodeURIComponent(accountId)}`
    : "";
  const result = await codexMuxRequest(`/profile/combined${query}`);
  globalThis.__codexMuxCombinedProfileAccounts = result.accounts || [];
  return result.profile;
}

async function codexMuxRateLimitResets(accountId) {
  return codexMuxRequest(
    `/accounts/${encodeURIComponent(accountId)}/rate-limit-resets`,
  );
}

async function codexMuxConsumeRateLimitReset(accountId, input) {
  return codexMuxRequest(
    `/accounts/${encodeURIComponent(accountId)}/rate-limit-resets/consume`,
    {
      method: "POST",
      body: JSON.stringify({
        creditId: input.creditId ?? null,
        redeemRequestId: input.redeemRequestId,
      }),
    },
  );
}

function CodexMuxUsageModal({
  onClose,
}) {
  return (0, d7.jsx)(kxc, {
    defaultResetCreditsOpen: true,
    initialAvailableCount: 0,
    isRateLimitReached: false,
    onClose,
    onResetComplete: () => {},
  });
}

function CodexMuxUseResetAccountState() {
  const cachedAccounts = (globalThis.__codexMuxConnectedAccounts || []).filter(
    (account) => account.connected && account.enabled,
  );
  const [accounts, setAccounts] = NIl.useState(cachedAccounts);
  const [selectedId, setSelectedId] = NIl.useState("primary");
  const [resetCounts, setResetCounts] = NIl.useState({});
  const [loading, setLoading] = NIl.useState(cachedAccounts.length === 0);

  const loadAccounts = NIl.useCallback(async () => {
    const result = await codexMuxRequest("/accounts");
    const connected = (result.accounts || []).filter(
      (account) => account.connected && account.enabled,
    );
    setAccounts(connected);
    setSelectedId((current) =>
      connected.some((account) => account.id === current)
        ? current
        : connected[0]?.id || "primary",
    );
    setLoading(false);
    const entries = await Promise.all(
      connected.map(async (account) => {
        try {
          const resets = await codexMuxRateLimitResets(account.id);
          return [account.id, Math.max(0, resets.available_count || 0)];
        } catch {
          return [account.id, null];
        }
      }),
    );
    setResetCounts(Object.fromEntries(entries));
  }, []);

  NIl.useEffect(() => {
    loadAccounts().catch(() => setLoading(false));
  }, [loadAccounts]);

  NIl.useEffect(
    () => () => {
      delete window.__codexMuxResetAccountId;
      delete window.__codexMuxSelectedUsageWindows;
      delete window.__codexMuxResetAccountSelector;
    },
    [],
  );

  const selected =
    accounts.find((account) => account.id === selectedId) || accounts[0] || null;
  const activeId = selected?.id || selectedId;
  window.__codexMuxResetAccountId = activeId;
  window.__codexMuxSelectedUsageWindows = selected
    ? codexMuxUsageWindows(selected.rateLimits)
    : null;
  window.__codexMuxResetAccountSelector = (0, d7.jsx)(
    CodexMuxResetAccountSelector,
    {
      accounts,
      loading,
      resetCounts,
      selectedId: activeId,
      onSelect: setSelectedId,
    },
  );

}

function CodexMuxResetAccountSelector({
  accounts,
  loading,
  onSelect,
  resetCounts,
  selectedId,
}) {
  return (0, d7.jsxs)("div", {
    className: "pt-4",
    children: [
      (0, d7.jsx)("div", {
        className:
          "mb-2 px-1 text-xs font-medium text-token-text-secondary",
        children: "Subscription",
      }),
      (0, d7.jsx)("div", {
        className:
          "flex flex-wrap gap-2 rounded-2xl border border-token-border p-2",
        children: loading
          ? (0, d7.jsx)("div", {
              className: "px-2 py-2 text-sm text-token-text-secondary",
              children: "Loading subscriptions…",
            })
          : accounts.map((account) => {
              const selected = account.id === selectedId;
              const count = resetCounts[account.id];
              return (0, d7.jsxs)(
                "button",
                {
                  type: "button",
                  className: [
                    "flex min-w-fit items-center gap-2 rounded-xl px-3 py-2 text-left",
                    "transition-colors hover:bg-token-foreground/5",
                    selected
                      ? "bg-token-foreground/10 text-token-text-primary"
                      : "text-token-text-secondary",
                  ].join(" "),
                  "aria-pressed": selected,
                  onClick: () => onSelect(account.id),
                  children: [
                    (0, d7.jsx)(CodexMuxAccountAvatar, {
                      imageUrl: account.profileImageUrl,
                      label: account.label,
                      className: "size-7",
                    }),
                    (0, d7.jsxs)("span", {
                      className: "flex min-w-0 flex-col",
                      children: [
                        (0, d7.jsx)("span", {
                          className: "max-w-40 truncate text-sm font-medium",
                          children: account.planLabel
                            ? `${account.label} · ${account.planLabel}`
                            : account.label,
                        }),
                        (0, d7.jsx)("span", {
                          className: "text-xs text-token-text-tertiary",
                          children:
                            count == null
                              ? "Resets unavailable"
                              : count === 1
                                ? "1 reset available"
                                : `${count} resets available`,
                        }),
                      ],
                    }),
                  ],
                },
                account.id,
              );
            }),
      }),
    ],
  });
}

function CodexMuxAccountMenu() {
  const modalScope = vs(Q);

  const [accounts, setAccounts] = Esc.useState([]);
  const [loading, setLoading] = Esc.useState(true);
  const [busy, setBusy] = Esc.useState(false);
  const [error, setError] = Esc.useState("");
  const [login, setLogin] = Esc.useState(null);
  const [codeCopied, setCodeCopied] = Esc.useState(false);

  const loginAccountId = login?.accountId || null;

  const refresh = Esc.useCallback(async () => {
    try {
      const result = await codexMuxRequest("/accounts");
      const nextAccounts = Array.isArray(result.accounts)
        ? result.accounts
        : [];

      globalThis.__codexMuxConnectedAccounts = nextAccounts.filter(
        (account) => account?.connected && account?.enabled,
      );

      setAccounts(nextAccounts);
      setError("");
      setLoading(false);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : String(requestError),
      );
      setLoading(false);
    }
  }, []);

  Esc.useEffect(() => {
    refresh();

    const events = new EventSource(
      `${CODEX_MUX_API}/events?token=${encodeURIComponent(CODEX_MUX_TOKEN)}`,
    );

    events.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);

        if (
          payload.type === "account-updated" &&
          payload.accountId === loginAccountId
        ) {
          codexMuxLoginActive = false;
          setLogin(null);
        }

        if (payload.type === "account-updated") {
          refresh();
        }
      } catch {}
    };

    const warmupTimer = setTimeout(refresh, 2000);
    const timer = setInterval(refresh, 30000);

    return () => {
      clearTimeout(warmupTimer);
      clearInterval(timer);
      events.close();
    };
  }, [refresh, loginAccountId]);

  Esc.useEffect(() => {
    if (!login) return;

    const escape = (event) => {
      if (event.key !== "Escape") return;
      codexMuxLoginActive = false;
      setLogin(null);
    };

    window.addEventListener("keydown", escape, true);

    return () => {
      window.removeEventListener("keydown", escape, true);
    };
  }, [login]);

  const connected = accounts.filter(
    (account) => account?.connected && account?.enabled,
  );

  const weeklyWindows = connected.map((account) =>
    codexMuxWeeklyWindow(account.rateLimits),
  );

  const hasCompleteUsage =
    connected.length > 0 &&
    weeklyWindows.every((weekly) => weekly != null);

  const totalRemaining = weeklyWindows.reduce(
    (total, weekly) =>
      total +
      (weekly == null
        ? 0
        : Math.max(0, 100 - Number(weekly.usedPercent || 0))),
    0,
  );

  async function addSubscription(event) {
    event?.preventDefault?.();

    if (busy) return;

    setBusy(true);
    setError("");

    try {
      const created = await codexMuxRequest("/accounts", {
        method: "POST",
        body: JSON.stringify({
          label: `Subscription ${connected.length + 1}`,
        }),
      });

      const result = await codexMuxRequest(
        `/accounts/${created.account.id}/login`,
        {
          method: "POST",
          body: JSON.stringify({
            mode: "chatgptDeviceCode",
          }),
        },
      );

      const pendingLogin = result.login
        ? {
            ...result.login,
            accountId: created.account.id,
          }
        : null;

      codexMuxLoginActive = pendingLogin != null;

      setCodeCopied(false);
      setLogin(pendingLogin);

      await refresh();
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : String(requestError),
      );
    } finally {
      setBusy(false);
    }
  }

  async function copyCodeAndContinue(event) {
    event?.preventDefault?.();

    const userCode =
      typeof login?.userCode === "string"
        ? login.userCode
        : "";

    const verificationUrl =
      typeof login?.verificationUrl === "string"
        ? login.verificationUrl
        : typeof login?.authUrl === "string"
          ? login.authUrl
          : "";

    try {
      if (userCode) {
        await navigator.clipboard.writeText(userCode);
        setCodeCopied(true);
      }

      if (verificationUrl) {
        const destination = new URL(verificationUrl);

        const trustedHost =
          destination.hostname === "chatgpt.com" ||
          destination.hostname === "auth.openai.com";

        if (
          destination.protocol !== "https:" ||
          !trustedHost
        ) {
          throw new Error("Untrusted verification URL");
        }

        window.open(
          destination.href,
          "_blank",
          "noopener,noreferrer",
        );
      }
    } catch {
      setError(
        "The ChatGPT sign-in page or code could not be opened.",
      );
    }
  }

  function row({
    key,
    title,
    subtitle,
    right,
    avatar,
    onClick,
    danger = false,
  }) {
    return (0, d7.jsx)(
      "button",
      {
        type: "button",
        disabled: !onClick,
        onClick,
        className: [
          "flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left",
          "transition-colors",
          onClick ? "hover:bg-token-foreground/5" : "",
          danger
            ? "text-red-500"
            : "text-token-text-primary",
        ]
          .filter(Boolean)
          .join(" "),
        children: (0, d7.jsxs)(d7.Fragment, {
          children: [
            avatar
              ? (0, d7.jsx)(
                  "div",
                  {
                    className:
                      "flex size-7 shrink-0 items-center justify-center",
                    children: avatar,
                  },
                )
              : null,

            (0, d7.jsxs)("div", {
              className: "min-w-0 flex-1",
              children: [
                (0, d7.jsx)("div", {
                  className:
                    "truncate text-sm font-medium",
                  children: String(title ?? ""),
                }),

                subtitle
                  ? (0, d7.jsx)("div", {
                      className:
                        "mt-0.5 truncate text-xs text-token-text-secondary",
                      children: subtitle,
                    })
                  : null,
              ],
            }),

            right != null
              ? (0, d7.jsx)("div", {
                  className:
                    "shrink-0 text-xs text-token-text-secondary tabular-nums",
                  children: String(right),
                })
              : null,
          ],
        }),
      },
      key,
    );
  }

  const rows = [];

  rows.push(
    row({
      key: "codex-mux-total",
      title: "Usage remaining",
      subtitle: loading
        ? "Connecting subscriptions…"
        : connected.length === 1
          ? "1 connected subscription"
          : `${connected.length} connected subscriptions`,
      right: loading
        ? "…"
        : hasCompleteUsage
          ? `${Math.round(totalRemaining)}%`
          : "–",
      avatar: (0, d7.jsx)(CodexMuxPlusIcon, {
        className: "size-5",
      }),
      onClick: () => {
        HR(modalScope, CodexMuxUsageModal, {});
      },
    }),
  );

  if (connected.length > 0) {
    rows.push(
      (0, d7.jsx)(
        "div",
        {
          className:
            "my-1 h-px bg-token-border-light",
        },
        "codex-mux-account-separator",
      ),
    );
  }

  for (const account of connected) {
    const weekly = codexMuxWeeklyWindow(account.rateLimits);

    const remaining =
      weekly == null
        ? null
        : Math.max(
            0,
            100 - Number(weekly.usedPercent || 0),
          );

    const accountLabel =
      typeof account.label === "string"
        ? account.label
        : "ChatGPT";

    const planLabel =
      typeof account.planLabel === "string"
        ? account.planLabel
        : "";

    const subtitle =
      typeof account.email === "string" && account.email
        ? account.email
        : typeof account.planType === "string"
          ? account.planType
          : "ChatGPT subscription";

    rows.push(
      row({
        key: `codex-mux-account-${String(account.id)}`,
        title: planLabel
          ? `${accountLabel} · ${planLabel}`
          : accountLabel,
        subtitle,
        right:
          remaining == null
            ? "–"
            : `${Math.round(remaining)}%`,
        avatar: (0, d7.jsx)("span", {
          className:
            "flex size-7 shrink-0 items-center justify-center rounded-full " +
            "bg-token-bg-secondary text-xs font-medium text-token-text-primary",
          children: accountLabel
            .trim()
            .split(/\\s+/)
            .filter(Boolean)
            .slice(0, 2)
            .map((part) => part.charAt(0).toUpperCase())
            .join("") || "?",
        }),
      }),
    );
  }

  if (login) {
    rows.push(
      row({
        key: "codex-mux-login",
        title: "Continue sign-in",
        subtitle: login.userCode
          ? codeCopied
            ? `Code ${String(login.userCode)} copied`
            : `Code ${String(login.userCode)} · Click to continue`
          : "Finish signing in with ChatGPT",
        avatar: (0, d7.jsx)(CodexMuxCopyIcon, {
          className: "size-5",
        }),
        onClick: copyCodeAndContinue,
      }),
    );
  }

  if (error) {
    rows.push(
      row({
        key: "codex-mux-error",
        title: "Subscription pool unavailable",
        subtitle: String(error),
        danger: true,
      }),
    );
  }

  rows.push(
    (0, d7.jsx)(
      "div",
      {
        className: "my-1 h-px bg-token-border-light",
      },
      "codex-mux-bottom-separator",
    ),
  );

  rows.push(
    row({
      key: "codex-mux-add",
      title: busy
        ? "Adding subscription…"
        : "Add another subscription",
      avatar: (0, d7.jsx)(CodexMuxPlusIcon, {
        className: "size-5",
      }),
      onClick: busy ? null : addSubscription,
    }),
  );

  return (0, d7.jsx)("div", {
    className:
      "flex w-full min-w-0 flex-col gap-0 py-1",
    children: rows,
  });
}

function codexMuxWeeklyWindow(rateLimits) {
  const windows = [rateLimits?.primary, rateLimits?.secondary].filter(Boolean);
  windows.sort(
    (left, right) =>
      (left.windowDurationMins || 0) - (right.windowDurationMins || 0),
  );
  return windows.at(-1) || null;
}

function codexMuxUsageWindows(rateLimits) {
  return [rateLimits?.primary, rateLimits?.secondary]
    .filter(Boolean)
    .map((window) => ({
      usedPercent: window.usedPercent,
      remainingPercent: Math.max(0, 100 - window.usedPercent),
      windowMinutes: window.windowDurationMins || 0,
      resetsAt: window.resetsAt ?? null,
    }));
}

function CodexMuxPlusIcon(props) {
  return (0, d7.jsx)("svg", {
    viewBox: "0 0 20 20",
    fill: "none",
    "aria-hidden": true,
    ...props,
    children: (0, d7.jsx)("path", {
      d: "M10 4.25v11.5M4.25 10h11.5",
      stroke: "currentColor",
      strokeWidth: 1.5,
      strokeLinecap: "round",
    }),
  });
}

function CodexMuxCopyIcon(props) {
  return (0, d7.jsx)("svg", {
    viewBox: "0 0 20 20",
    fill: "none",
    "aria-hidden": true,
    ...props,
    children: (0, d7.jsxs)(d7.Fragment, {
      children: [
        (0, d7.jsx)("rect", {
          x: 6.25,
          y: 6.25,
          width: 9.5,
          height: 9.5,
          rx: 2,
          stroke: "currentColor",
          strokeWidth: 1.5,
        }),
        (0, d7.jsx)("path", {
          d: "M13.75 6.25V6A1.75 1.75 0 0 0 12 4.25H6A1.75 1.75 0 0 0 4.25 6v6c0 .97.78 1.75 1.75 1.75h.25",
          stroke: "currentColor",
          strokeWidth: 1.5,
          strokeLinecap: "round",
        }),
      ],
    }),
  });
}

function CodexMuxMaskedEmail({ email }) {
  return (0, d7.jsxs)(d7.Fragment, {
    children: [
      (0, d7.jsx)("span", {
        className: "group-hover:hidden",
        children: "••••••••",
      }),
      (0, d7.jsx)("span", {
        className: "hidden group-hover:inline",
        children: email,
      }),
    ],
  });
}

function CodexMuxAccountAvatar({ imageUrl, label, className }) {
  const [failed, setFailed] = NIl.useState(false);
  const resolvedImageUrl = imageUrl || null;
  if (resolvedImageUrl && !failed) {
    return (0, d7.jsx)("img", {
      src: resolvedImageUrl,
      alt: "",
      className: `${className || "icon-sm"} rounded-full object-cover`,
      referrerPolicy: "no-referrer",
      onError: () => setFailed(true),
    });
  }
  const safeLabel =
    typeof label === "string" ? label : String(label ?? "");
  const initials = safeLabel
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join("");
  return (0, d7.jsx)("span", {
    className: `${className || "icon-sm"} flex items-center justify-center rounded-full bg-token-charts-purple/10 text-[9px] leading-none text-token-charts-purple`,
    "aria-hidden": true,
    children: initials || "?",
  });
}

function CodexMuxOverlappingAvatars({ accounts, size = "size-20" }) {
  const overlapClass = size === "size-20" ? "-ml-10" : "-ml-2";
  return (0, d7.jsx)("div", {
    className: "flex items-center justify-center",
    children: accounts.map((account, index) =>
      (0, d7.jsx)(
        "span",
        {
          className: `${index === 0 ? "" : overlapClass} rounded-full border-4 border-token-bg-primary`,
          title: account.planLabel
            ? `${account.label} · ${account.planLabel}`
            : account.label,
          children: (0, d7.jsx)(CodexMuxAccountAvatar, {
            imageUrl: account.profileImageUrl,
            label: account.label,
            className: size,
          }),
        },
        account.id,
      ),
    ),
  });
}

function CodexMuxProfileAvatarStack({ onSelect }) {
  const [accounts, setAccounts] = NIl.useState(
    globalThis.__codexMuxCombinedProfileAccounts || [],
  );
  const [selectedId, setSelectedId] = NIl.useState(
    globalThis.__codexMuxSelectedProfileAccountId || null,
  );
  NIl.useEffect(() => {
    let live = true;
    codexMuxRequest("/accounts")
      .then((result) => {
        if (!live) return;
        const connected = (result.accounts || []).filter(
          (account) => account.connected && account.enabled,
        );
        globalThis.__codexMuxCombinedProfileAccounts = connected;
        setAccounts(connected);
      })
      .catch(() => {});
    return () => {
      live = false;
    };
  }, []);
  NIl.useEffect(() => {
    globalThis.__codexMuxSelectedProfileAccountId = null;
    setSelectedId(null);
    onSelect?.();
    return () => {
      globalThis.__codexMuxSelectedProfileAccountId = null;
    };
  }, []);
  if (accounts.length === 0) return null;
  const visibleAccounts = selectedId
    ? accounts.filter((account) => account.id === selectedId)
    : accounts;
  return (0, d7.jsx)("div", {
    className: "mb-4",
    "aria-label": selectedId
      ? "Selected subscription profile"
      : `${accounts.length} connected subscriptions`,
    children: (0, d7.jsx)("div", {
      className: "flex items-center justify-center",
      children: visibleAccounts.map((account, index) =>
        (0, d7.jsx)(
          "button",
          {
            type: "button",
            className: `${index === 0 ? "" : "-ml-5"} rounded-full border-4 border-token-bg-primary transition-transform hover:z-10 hover:scale-105 focus-visible:z-10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-token-focus-border`,
            style: {
              marginLeft: index === 0 ? 0 : -20,
              zIndex: index,
            },
            "aria-label": selectedId
              ? `Show combined profile stats`
              : `Show ${account.label} profile stats`,
            title: account.planLabel
              ? `${account.label} · ${account.planLabel}`
              : account.label,
            onClick: () => {
              const nextId = selectedId === account.id ? null : account.id;
              globalThis.__codexMuxSelectedProfileAccountId = nextId;
              setSelectedId(nextId);
              onSelect?.();
            },
            children: (0, d7.jsx)(CodexMuxAccountAvatar, {
              imageUrl: account.profileImageUrl,
              label: account.label,
              className: "size-20",
            }),
          },
          account.id,
        ),
      ),
    }),
  });
}

function CodexMuxPluginScope() {
  const [accounts, setAccounts] = NIl.useState([]);
  const [selectedId, setSelectedId] = NIl.useState("primary");
  const [loading, setLoading] = NIl.useState(true);
  const queryClient = ct();
  NIl.useEffect(() => {
    let live = true;
    codexMuxRequest("/accounts")
      .then((result) => {
        if (!live) return;
        setAccounts(
          (result.accounts || []).filter(
            (account) => account.connected && account.enabled,
          ),
        );
      })
      .catch(() => {})
      .finally(() => {
        if (live) setLoading(false);
      });
    return () => {
      live = false;
    };
  }, []);

  NIl.useEffect(() => {
    globalThis.__codexMuxPluginAccountId = selectedId;
    return () => {
      delete globalThis.__codexMuxPluginAccountId;
    };
  }, [selectedId]);

  async function selectAccount(accountId) {
    if (accountId === selectedId) return;
    globalThis.__codexMuxPluginAccountId = accountId;
    setSelectedId(accountId);
    await queryClient.invalidateQueries({
      predicate: (query) => {
        const root = query.queryKey?.[0];
        return root === "apps" || root === "plugins" || root === "mcp";
      },
    });
  }

  const selected =
    accounts.find((account) => account.id === selectedId) || accounts[0] || null;

  return (0, d7.jsxs)("div", {
    className:
      "mb-5 rounded-2xl border border-token-border-light p-3",
    children: [
      (0, d7.jsxs)("div", {
        className: "px-1",
        children: [
          (0, d7.jsx)("div", {
            className: "text-sm font-medium text-token-text-primary",
            children: "Plugin connections",
          }),
          (0, d7.jsx)("div", {
            className: "mt-0.5 text-xs text-token-text-secondary",
            children: selected
              ? `Installs are shared. Connection access below is for ${selected.label}.`
              : "Installs are shared. Choose a subscription for connection access.",
          }),
        ],
      }),
      loading
        ? (0, d7.jsx)("div", {
            className: "mt-3 px-1 text-sm text-token-text-tertiary",
            children: "Loading subscriptions…",
          })
        : (0, d7.jsx)("div", {
            className: "mt-3 flex flex-wrap gap-2",
            children: accounts.map((account) => {
              const active = account.id === selected?.id;
              return (0, d7.jsxs)(
                "button",
                {
                  type: "button",
                  className: [
                    "flex items-center gap-2 rounded-xl px-2.5 py-2 text-sm transition-colors",
                    active
                      ? "bg-token-foreground/10 text-token-text-primary"
                      : "text-token-text-secondary hover:bg-token-foreground/5",
                  ].join(" "),
                  "aria-pressed": active,
                  onClick: () => selectAccount(account.id),
                  children: [
                    (0, d7.jsx)(CodexMuxAccountAvatar, {
                      imageUrl: account.profileImageUrl,
                      label: account.label,
                      className: "size-7",
                    }),
                    (0, d7.jsx)("span", {
                      children: account.planLabel
                        ? `${account.label} · ${account.planLabel}`
                        : account.label,
                    }),
                  ],
                },
                account.id,
              );
            }),
          }),
    ],
  });
}

// The thread summary is emitted into a separate lazy-loaded renderer chunk.
// Export the same avatar component so both surfaces share image resolution,
// error handling, and the initials fallback.
globalThis.CodexMuxAccountAvatar = CodexMuxAccountAvatar;
globalThis.codexMuxProfileData = codexMuxProfileData;
globalThis.CodexMuxProfileAvatarStack = (props) =>
  (0, d7.jsx)(CodexMuxProfileAvatarStack, props || {});
globalThis.CodexMuxPluginScope = () =>
  (0, d7.jsx)(CodexMuxPluginScope, {});
