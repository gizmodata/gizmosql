// Redirect legacy Docsify hash URLs to their MkDocs equivalents so old
// inbound links keep working after the docs migration:
//   /#/client            -> /client/
//   /#/quickstart?id=foo -> /quickstart/#foo
//   /#/ and /#/documentation -> /
(function () {
  var hash = window.location.hash;
  if (hash.indexOf('#/') !== 0) return;
  var target = hash.slice(2);
  var anchor = '';
  var idx = target.indexOf('?id=');
  if (idx !== -1) {
    anchor = '#' + target.slice(idx + 4);
    target = target.slice(0, idx);
  }
  target = target.replace(/^\/+|\/+$/g, '');
  if (target === 'documentation') target = '';
  window.location.replace('/' + (target ? target + '/' : '') + anchor);
})();
