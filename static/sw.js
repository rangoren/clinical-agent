self.__dutySyncDebug = (...args) => {
  console.log("[DutySyncDebug][SW]", ...args);
};

const DUTY_SYNC_PUSH_DB_NAME = "duty-sync-push";
const DUTY_SYNC_PUSH_STORE_NAME = "context";
const DUTY_SYNC_DEBUG_STORE_NAME = "debug_logs";

function openDutySyncPushDb() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DUTY_SYNC_PUSH_DB_NAME, 2);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(DUTY_SYNC_PUSH_STORE_NAME)) {
        db.createObjectStore(DUTY_SYNC_PUSH_STORE_NAME);
      }
      if (!db.objectStoreNames.contains(DUTY_SYNC_DEBUG_STORE_NAME)) {
        db.createObjectStore(DUTY_SYNC_DEBUG_STORE_NAME, { keyPath: "id" });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function saveDutySyncPushContext(context) {
  return openDutySyncPushDb().then(
    (db) =>
      new Promise((resolve, reject) => {
        const transaction = db.transaction(DUTY_SYNC_PUSH_STORE_NAME, "readwrite");
        const store = transaction.objectStore(DUTY_SYNC_PUSH_STORE_NAME);
        store.put(context, "latest");
        transaction.oncomplete = () => {
          db.close();
          resolve();
        };
        transaction.onerror = () => {
          db.close();
          reject(transaction.error);
        };
      })
  );
}

function writeDutySyncSwDebug(message, payload = null) {
  const entry = {
    id: `sw-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    ts: new Date().toISOString(),
    source: "sw",
    message: `[DutySyncSWDebug] ${message}`,
    payload: payload || {},
  };
  self.__dutySyncDebug(entry.message, entry.payload);
  return openDutySyncPushDb().then(
    (db) =>
      new Promise((resolve) => {
        try {
          const transaction = db.transaction(DUTY_SYNC_DEBUG_STORE_NAME, "readwrite");
          const store = transaction.objectStore(DUTY_SYNC_DEBUG_STORE_NAME);
          store.put(entry);
          transaction.oncomplete = () => {
            db.close();
            broadcastDutySyncSwDebugBatch().finally(() => resolve(entry));
          };
          transaction.onerror = () => {
            db.close();
            resolve(entry);
          };
        } catch (_error) {
          db.close();
          resolve(entry);
        }
      })
  );
}

function readDutySyncSwDebugEntries() {
  return openDutySyncPushDb().then(
    (db) =>
      new Promise((resolve) => {
        try {
          if (!db.objectStoreNames.contains(DUTY_SYNC_DEBUG_STORE_NAME)) {
            db.close();
            resolve([]);
            return;
          }
          const transaction = db.transaction(DUTY_SYNC_DEBUG_STORE_NAME, "readonly");
          const store = transaction.objectStore(DUTY_SYNC_DEBUG_STORE_NAME);
          const request = store.getAll();
          request.onsuccess = () => {
            const value = Array.isArray(request.result) ? request.result : [];
            db.close();
            resolve(value.sort((a, b) => String(a.ts || "").localeCompare(String(b.ts || ""))));
          };
          request.onerror = () => {
            db.close();
            resolve([]);
          };
        } catch (_error) {
          db.close();
          resolve([]);
        }
      })
  );
}

function postDutySyncSwDebugBatch(client) {
  if (!client || !("postMessage" in client)) {
    return Promise.resolve(false);
  }
  return readDutySyncSwDebugEntries().then((entries) => {
    client.postMessage({
      type: "duty-sync-sw-debug-batch",
      entries,
    });
    return true;
  });
}

function resolveDutySyncSwDebugClient() {
  return self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
    if (!clients || !clients.length) {
      return null;
    }
    return clients[0];
  });
}

function postDutySyncOpenReviewMessage(client, targetUrl) {
  if (!client || !("postMessage" in client)) {
    return false;
  }
  client.postMessage({ type: "duty-sync-open-review", url: targetUrl });
  return true;
}

function writeDutySyncOpenFlowDebug(message, payload) {
  return writeDutySyncSwDebug(message, payload || {});
}

function broadcastDutySyncSwDebugBatch() {
  return resolveDutySyncSwDebugClient().then((client) => {
    if (!client) {
      return false;
    }
    return postDutySyncSwDebugBatch(client);
  });
}

function withDutySyncTrace(targetUrl, traceId, traceSteps) {
  try {
    const url = new URL(targetUrl, self.location.origin);
    url.searchParams.set("duty_sync_trace_id", traceId);
    url.searchParams.set("duty_sync_trace_sw", traceSteps.join("|"));
    return url.toString();
  } catch (_error) {
    return targetUrl;
  }
}

function withDutySyncReviewIdentity(targetUrl, review) {
  try {
    const url = new URL(targetUrl, self.location.origin);
    url.searchParams.set("app_mode", "scheduling");
    url.searchParams.set("duty_sync_review", "1");
    if (review && review.review_id) {
      url.searchParams.set("duty_sync_review_id", review.review_id);
    }
    if (review && review.updated_at) {
      url.searchParams.set("duty_sync_review_updated_at", review.updated_at);
    }
    return url.toString();
  } catch (_error) {
    return targetUrl;
  }
}

self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("push", (event) => {
  const payload = event.data ? event.data.json() : {};
  const title = payload.title || "Duty Sync update";
  const targetUrl = payload.url || "/?app_mode=scheduling&duty_sync_review=1";
  const options = {
    body: payload.body || "Duty Sync found personal schedule changes.",
    tag: payload.tag || "duty-sync-review",
    data: {
      url: targetUrl,
      review_id: payload.review_id || "",
      updated_at: payload.updated_at || "",
      review: payload.review || null,
    },
  };
  event.waitUntil(
    writeDutySyncSwDebug("push event received", {
      payload_keys: Object.keys(payload || {}),
      has_review_id: !!payload.review_id,
      has_updated_at: !!payload.updated_at,
      title,
      body: options.body,
      target_url_before_enrichment: targetUrl,
    })
      .then(() =>
        writeDutySyncSwDebug("notification data created", {
          notification_data: options.data,
          review_id: options.data.review_id || null,
          updated_at: options.data.updated_at || null,
        })
      )
      .then(() => self.registration.showNotification(title, options))
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const baseTargetUrl = (event.notification.data && event.notification.data.url) || "/?app_mode=scheduling&duty_sync_review=1";
  const notificationData = event.notification.data || {};
  const review = notificationData.review || null;
  const reviewIdentity = {
    review_id: notificationData.review_id || (review && review.review_id) || "",
    updated_at: notificationData.updated_at || (review && review.updated_at) || "",
  };
  const resolvedTargetUrl = withDutySyncReviewIdentity(baseTargetUrl, reviewIdentity);
  const traceId = `push-${Date.now()}`;
  self.__dutySyncDebug("notificationclick fired", { targetUrl: resolvedTargetUrl, traceId });
  let storedTraceLine = "";
  event.waitUntil(
    Promise.resolve()
      .then(() => writeDutySyncOpenFlowDebug("open-flow session start", {
        trace_id: traceId,
        source: "notification_tap",
        review_id: reviewIdentity.review_id || null,
        updated_at: reviewIdentity.updated_at || null,
      }))
      .then(() => writeDutySyncOpenFlowDebug("notificationclick start", {
        notification_data: notificationData,
        review_id: reviewIdentity.review_id || null,
        updated_at: reviewIdentity.updated_at || null,
        target_url_raw: baseTargetUrl,
        target_url_enriched: resolvedTargetUrl,
        trace_id: traceId,
      }))
      .then(() => {
        const parsedUrl = new URL(resolvedTargetUrl, self.location.origin);
        const context = {
          active: true,
          review_id: reviewIdentity.review_id || parsedUrl.searchParams.get("duty_sync_review_id") || "",
          updated_at: reviewIdentity.updated_at || parsedUrl.searchParams.get("duty_sync_review_updated_at") || "",
          source: "service_worker_notificationclick",
        };
        storedTraceLine = `[SW/App] stored push context review_id=${context.review_id} updated_at=${context.updated_at}`;
        return writeDutySyncSwDebug("storage write attempt", {
          storage_key: "latest",
          context,
        }).then(() => saveDutySyncPushContext(context))
          .then(() => writeDutySyncSwDebug("storage write success", {
            storage_key: "latest",
            context,
          }))
          .catch((error) => writeDutySyncSwDebug("storage write failure", {
            storage_key: "latest",
            context,
            error: String(error),
          }));
      })
      .then(() => writeDutySyncOpenFlowDebug("before clients.matchAll", {
        trace_id: traceId,
      }))
      .then(() => self.clients.matchAll({ type: "window", includeUncontrolled: true }))
      .then((clients) => {
        writeDutySyncOpenFlowDebug("after clients.matchAll", {
          trace_id: traceId,
          client_count: clients ? clients.length : 0,
        });
        writeDutySyncOpenFlowDebug("before choosing existing client", {
          trace_id: traceId,
          client_count: clients ? clients.length : 0,
        });
        const client = clients && clients.length ? clients[0] : null;
        writeDutySyncOpenFlowDebug("after choosing existing client", {
          trace_id: traceId,
          found: !!client,
          target_client_id: client && client.id ? client.id : null,
        });
        writeDutySyncOpenFlowDebug("existing client found", {
          found: !!client,
          target_client_id: client && client.id ? client.id : null,
        });
        if (clients && clients.length) {
          const targetUrl = withDutySyncTrace(resolvedTargetUrl, traceId, [
            "[SW] notificationclick fired",
            storedTraceLine,
            "[SW] client found",
            "[SW] navigate/openWindow attempted",
          ]);
          self.__dutySyncDebug("client found", { clientCount: clients.length, targetUrl });
          writeDutySyncOpenFlowDebug("before postMessage", {
            trace_id: traceId,
            target_client_id: client && client.id ? client.id : null,
          });
          const messageSent = postDutySyncOpenReviewMessage(client, targetUrl);
          writeDutySyncOpenFlowDebug("after postMessage", {
            trace_id: traceId,
            sent: messageSent,
            target_client_id: client && client.id ? client.id : null,
          });
          writeDutySyncOpenFlowDebug("duty-sync-open-review message sent", {
            sent: messageSent,
            target_client_id: client && client.id ? client.id : null,
          });
          if ("navigate" in client) {
            self.__dutySyncDebug("navigation attempted", { via: "client.navigate", targetUrl });
            return client.navigate(targetUrl).then((navigatedClient) => {
              const activeClient = navigatedClient || client;
              if (activeClient && "focus" in activeClient) {
                writeDutySyncOpenFlowDebug("before focus", {
                  trace_id: traceId,
                  target_client_id: activeClient && activeClient.id ? activeClient.id : null,
                });
                return activeClient.focus().then((focusedClient) => {
                  writeDutySyncOpenFlowDebug("after focus", {
                    trace_id: traceId,
                    called: true,
                    target_client_id: activeClient && activeClient.id ? activeClient.id : null,
                  });
                  writeDutySyncOpenFlowDebug("focus called", {
                    called: true,
                    target_client_id: activeClient && activeClient.id ? activeClient.id : null,
                  });
                  return focusedClient;
                });
              }
              writeDutySyncOpenFlowDebug("after focus", {
                trace_id: traceId,
                called: false,
                target_client_id: activeClient && activeClient.id ? activeClient.id : null,
              });
              writeDutySyncOpenFlowDebug("focus called", {
                called: false,
                target_client_id: activeClient && activeClient.id ? activeClient.id : null,
              });
              return activeClient;
            });
          }
          if (client && "focus" in client) {
            self.__dutySyncDebug("navigation attempted", { via: "focus-existing-client", targetUrl });
            writeDutySyncOpenFlowDebug("before focus", {
              trace_id: traceId,
              target_client_id: client && client.id ? client.id : null,
            });
            return client.focus().then((focusedClient) => {
              writeDutySyncOpenFlowDebug("after focus", {
                trace_id: traceId,
                called: true,
                target_client_id: client && client.id ? client.id : null,
              });
              writeDutySyncOpenFlowDebug("focus called", {
                called: true,
                target_client_id: client && client.id ? client.id : null,
              });
              return focusedClient;
            });
          }
          writeDutySyncOpenFlowDebug("after focus", {
            trace_id: traceId,
            called: false,
            target_client_id: client && client.id ? client.id : null,
          });
          writeDutySyncOpenFlowDebug("focus called", {
            called: false,
            target_client_id: client && client.id ? client.id : null,
          });
          return client;
        }
        writeDutySyncOpenFlowDebug("after postMessage", {
          trace_id: traceId,
          sent: false,
          target_client_id: null,
        });
        writeDutySyncOpenFlowDebug("duty-sync-open-review message sent", {
          sent: false,
          target_client_id: null,
        });
        writeDutySyncOpenFlowDebug("after focus", {
          trace_id: traceId,
          called: false,
          target_client_id: null,
        });
        writeDutySyncOpenFlowDebug("focus called", {
          called: false,
          target_client_id: null,
        });
        const targetUrl = withDutySyncTrace(resolvedTargetUrl, traceId, [
          "[SW] notificationclick fired",
          storedTraceLine,
          "[SW] no client found",
          "[SW] navigate/openWindow attempted",
        ]);
        self.__dutySyncDebug("no client found", { targetUrl });
        if (self.clients.openWindow) {
          self.__dutySyncDebug("navigation attempted", { via: "openWindow", targetUrl });
          writeDutySyncSwDebug("navigation action", {
            branch: "openWindow",
            target_url: targetUrl,
          });
          return self.clients.openWindow(targetUrl);
        }
        writeDutySyncSwDebug("navigation action", {
          branch: "no-client-no-openWindow",
          target_url: targetUrl,
        });
        return undefined;
      })
      .catch((error) => writeDutySyncOpenFlowDebug("notificationclick error", {
        trace_id: traceId,
        error: error && error.message ? error.message : String(error),
        stack: error && error.stack ? String(error.stack) : null,
      }))
  );
});

self.addEventListener("message", (event) => {
  const data = event.data || {};
  if (data.type !== "duty-sync-request-sw-debug") return;
  event.waitUntil(
    writeDutySyncSwDebug("BRIDGE_OK", { source: "sw_bridge" }).then(() => postDutySyncSwDebugBatch(event.source))
  );
});
