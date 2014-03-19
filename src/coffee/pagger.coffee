Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'

class Pagger
  constructor: (options) ->
    @pageSize = options.pageSize or 100
    @nextPageFn = options.onNextPage
    @errorFn = options.onError
    @finishFn = options.onFinish
    @backpressureFn = options.applyBackpressureOnNextPage
    @backpressureDelayMs = options.backpressureDelayMs or 100

  page: () ->
    subj = new Rx.Subject()

    @_nextPage subj, 0, @pageSize, null
    .then ->
      subj.onCompleted()
    .fail (error) =>
      @errorFn error
      subj.onCompleted()
    .finally =>
      if @finishFn
        @finishFn()
    .done()

    subj

  _nextPage: (sink, offset, limit, total) ->
    if total? and offset > total
      Q("Done")
    else
      if @backpressureFn? and @backpressureFn(offset, limit, total)
        Q.delay @backpressureDelayMs
        .then =>
          @_nextPage sink, offset, limit, total
      else
        @nextPageFn(offset, limit)
        .then (res) =>
          _.each res.results, (elem) ->
            sink.onNext elem

          @_nextPage sink, offset + limit, limit, res.total

exports.Pagger = Pagger