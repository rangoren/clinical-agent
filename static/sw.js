self.__dutySyncDebug = (...args) => {
  console.log("[DutySyncDebug][SW]", ...args);
};

const DUTY_SYNC_PUSH_DB_NAME = "duty-sync-push";
const DUTY_SYNC_PUSH_STORE_NAME = "context";

function openDutySyncPushDb() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DUTY_SYNC_PUSH_DB_NAME, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(DUTY_SYNC_PUSH_STORE_NAME)) {
        db.createObjectStore(DUTY_SYNC_PUSH_STORE_NAME);
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
  const options = {
    body: payload.body || "Duty Sync found personal schedule changes.",
    tag: payload.tag || "duty-sync-review",
    data: {
      url: payload.url || "/?app_mode=scheduling&duty_sync_review=1",
      review: payload.review || null,
    },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const baseTargetUrl = (event.notification.data && event.notification.data.url) || "/?app_mode=scheduling&duty_sync_review=1";
  const review = (event.notification.data && event.notification.data.review) || null;
  const resolvedTargetUrl = withDutySyncReviewIdentity(baseTargetUrl, review);
  const traceId = `push-${Date.now()}`;
  self.__dutySyncDebug("notificationclick fired", { targetUrl: resolvedTargetUrl, traceId });
  let storedTraceLine = "";
  event.waitUntil(
    Promise.resolve()
      .then(() => {
        const parsedUrl = new URL(resolvedTargetUrl, self.location.origin);
        const context = {
          active: true,
          review_id: (review && review.review_id) || parsedUrl.searchParams.get("duty_sync_review_id") || "",
          updated_at: (review && review.updated_at) || parsedUrl.searchParams.get("duty_sync_review_updated_at") || "",
          source: "service_worker_notificationclick",
        };
        storedTraceLine = `[SW/App] stored push context review_id=${context.review_id} updated_at=${context.updated_at}`;
        return saveDutySyncPushContext(context);
      })
      .then(() => self.clients.matchAll({ type: "window", includeUncontrolled: true }))
      .then((clients) => {
      if (clients && clients.length) {
        const targetUrl = withDutySyncTrace(resolvedTargetUrl, traceId, [
          "[SW] notificationclick fired",
          storedTraceLine,
          "[SW] client found",
          "[SW] navigate/openWindow attempted",
        ]);
        const client = clients[0];
        self.__dutySyncDebug("client found", { clientCount: clients.length, targetUrl });
        if ("navigate" in client) {
          self.__dutySyncDebug("navigation attempted", { via: "client.navigate", targetUrl });
          return client.navigate(targetUrl).then((navigatedClient) => {
            const activeClient = navigatedClient || client;
            if (activeClient && "postMessage" in activeClient) {
              activeClient.postMessage({ type: "duty-sync-open-review", url: targetUrl, review });
            }
            if (activeClient && "focus" in activeClient) {
              return activeClient.focus();
            }
            return activeClient;
          });
        }
        if (self.clients.openWindow) {
          self.__dutySyncDebug("navigation attempted", { via: "openWindow-existing-client", targetUrl });
          return self.clients.openWindow(targetUrl);
        }
        return client;
      }
      const targetUrl = withDutySyncTrace(resolvedTargetUrl, traceId, [
        "[SW] notificationclick fired",
        storedTraceLine,
        "[SW] no client found",
        "[SW] navigate/openWindow attempted",
      ]);
      self.__dutySyncDebug("no client found", { targetUrl });
      if (self.clients.openWindow) {
        self.__dutySyncDebug("navigation attempted", { via: "openWindow", targetUrl });
        return self.clients.openWindow(targetUrl);
      }
      return undefined;
    })
  );
});
