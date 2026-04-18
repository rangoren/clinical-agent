self.__dutySyncDebug = (...args) => {
  console.log("[DutySyncDebug][SW]", ...args);
};

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
  const traceId = `push-${Date.now()}`;
  self.__dutySyncDebug("notificationclick fired", { targetUrl: baseTargetUrl, traceId });
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
      if (clients && clients.length) {
        const targetUrl = withDutySyncTrace(baseTargetUrl, traceId, [
          "[SW] notificationclick fired",
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
        if ("postMessage" in client) {
          client.postMessage({ type: "duty-sync-open-review", url: targetUrl, review });
        }
        if ("focus" in client) {
          self.__dutySyncDebug("navigation attempted", { via: "focus-only", targetUrl });
          return client.focus();
        }
        return client;
      }
      const targetUrl = withDutySyncTrace(baseTargetUrl, traceId, [
        "[SW] notificationclick fired",
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
