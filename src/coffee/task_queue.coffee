Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'

class TaskQueue
  constructor: (@stats, options) ->
    if not options.maxParallelTasks?
      throw new Error("maxParallelTasks is undefined :(")
    @_maxParallelTasks = options.maxParallelTasks
    @_queue = []
    @_activeCount = 0

  addTask: (taskFn) ->
    d = Q.defer()

    @_queue.push {fn: taskFn, defer: d}
    @_maybeExecute()

    d.promise

  _maybeExecute: ->
    if @_activeCount < @_maxParallelTasks and @_queue.length > 0
      @_startTask @_queue.shift()
      @_maybeExecute()

  _startTask: (task) ->
    @stats.taskStarted()
    @_activeCount = @_activeCount + 1

    task.fn()
    .then (res) ->
      task.defer.resolve res
    .fail (error) ->
      task.defer.reject error
    .finally =>
      @stats.taskFinished()
      @_activeCount = @_activeCount - 1
      @_maybeExecute()
    .done()

exports.TaskQueue = TaskQueue