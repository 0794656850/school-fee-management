self.addEventListener('push', function(event) {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (e) {
    payload = { title: 'Fee reminder', body: event.data ? event.data.text() : '' };
  }
  const title = payload.title || 'Fee reminder';
  const options = {
    body: payload.body || 'New update from your school.',
    icon: '/static/img/smartedupay_logo_secondary.svg',
    badge: '/static/img/smartedupay_logo_secondary.svg',
    data: { url: '/guardian/login' },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || '/guardian/login';
  event.waitUntil(clients.openWindow(url));
});
