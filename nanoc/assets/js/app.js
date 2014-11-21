!(function(ng) {
  var app = ng.module('docs', ['cfp.hotkeys'])

  app.value('pages', <%= pages_json %>)

  app.provider('search', function() {
    function localSearchFactory($http, $timeout, $q) {
      var index = lunr.Index.load(<%= lunr_index %>);;

      // The actual service is a function that takes a query string and
      // returns a promise to the search results
      // (In this case we just resolve the promise immediately as it is not
      // inherently an async process)
      return function(q) {
        return $q.when(index.search(q));
      };
    };
    localSearchFactory.$inject = ['$http', '$timeout', '$q'];

    function webWorkerSearchFactory($q, $rootScope) {
      var searchIndex = $q.defer();
      var results;

      var worker = new Worker('/js/search-worker.js');

      // The worker will send us a message in two situations:
      // - when the index has been built, ready to run a query
      // - when it has completed a search query and the results are available
      worker.onmessage = function(oEvent) {
        switch(oEvent.data.e) {
          case 'index-ready':
            searchIndex.resolve();
            break;
          case 'query-ready':
            results.resolve(oEvent.data.d);
            break;
        }
      };

      // The actual service is a function that takes a query string and
      // returns a promise to the search results
      return function(q) {

        // We only run the query once the index is ready
        return searchIndex.promise.then(function() {

          results = $q.defer();
          worker.postMessage({ q: q });
          return results.promise;
        });
      };
    };
    webWorkerSearchFactory.$inject = ['$q', '$rootScope'];

    return {
      $get: window.Worker ? webWorkerSearchFactory : localSearchFactory
    };
  })

  app.controller('search', [
    '$scope',
    '$sce',
    'search',
    'pages',
    function($scope, $sce, search, pages) {
      $scope.hasResults = false;
      $scope.results = null;
      $scope.current = null;

      function clear() {
        $scope.hasResults = false;
        $scope.results = null;
        $scope.current = null;
      }

      $scope.search = function() {
        if ($scope.q.length >= 2) {
          search($scope.q).then(function(hits) {
            if (hits.length > 0) {
              $scope.hasResults = true;
              $scope.results = hits.map(function(hit) {
                return pages[hit.ref]
              })
              $scope.current = 0;
            } else {
              clear()
            }
          })
        } else {
          clear()
        }
      };

      $scope.reset = function() {
        $scope.q = null;
        clear()
      }

      $scope.submit = function() {
        var result = $scope.results[$scope.current]

        if (result) {
          window.location.pathname = result.path;
        }
      }

      $scope.summary = function(item) {
        return $sce.trustAsHtml(item.summary);
      }

      $scope.moveDown = function(e) {
        console.log('moveDown', e)
        if ($scope.hasResults && $scope.current < ($scope.results.length - 1)) {
          $scope.current++
          e.stopPropagation()
        }
      }

      $scope.moveUp = function(e) {
        console.log('moveUp', e)
        if ($scope.hasResults && $scope.current > 0) {
          $scope.current--
          e.stopPropagation()
        }
      }
    }
  ])

  app.directive('search', [
    '$document',
    'hotkeys',
    function($document, hotkeys) {
      return function(scope, element, attrs) {
        hotkeys.add({
          combo: '/',
          description: 'Search docs...',
          callback: function(event, hotkey) {
            event.preventDefault()
            event.stopPropagation()
            element[0].focus()
          }
        })
      }
    }
  ])
})(angular)

$(function() {
  $('#content').height(
    Math.max(
      $(".side-nav").height(),
      $('#content').height()
    )
  );
});
