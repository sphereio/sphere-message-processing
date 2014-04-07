Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
express = require 'express'
measured = require 'measured'
{LoggerFactory} = require '../lib/logger'

class Meter
  constructor: (name, units) ->
    @meters = _.map units, (unit) ->
      {unit: unit, name: "#{name}Per#{unit.name}", meter: new measured.Meter({rateUnit: unit.rateUnit})}

  mark: () ->
    _.each @meters, (meter) ->
      meter.meter.mark()

  count: () ->
    @meters[0].meter.toJSON().count

  unref: () ->
    _.each @meters, (meter) -> meter.meter.unref()

  toJSON: () ->
    _.reduce @meters, ((acc, m) -> acc[m.name] = m.meter.toJSON(); acc), {}

class Stats
  constructor: (options) ->
    @processor = options.processor
    @units = options.units or [{name: "Second", rateUnit: 1000}, {name: "Minute", rateUnit: 60 * 1000}]
    @logger = LoggerFactory.getLogger 'stats', options.logger
    @started = new Date()
    @panicMode = false
    @paused = false
    @locallyLocked = 0
    @lastHeartbeat = -1

    @messagesIn = new Meter "messagesIn", @units
    @messagesOut = new Meter "messagesOut", @units
    @tasksStarted = new Meter "tasksStarted", @units
    @tasksFinished = new Meter "tasksFinished", @units
    @awaitOrderingIn = new Meter "awaitOrderingIn", @units
    @awaitOrderingOut = new Meter "awaitOrderingOut", @units
    @lockedMessages = new Meter "lockedMessages", @units
    @unlockedMessages = new Meter "unlockedMessages", @units
    @inProcessing = new Meter "inProcessing", @units
    @lockFailedMessages = new Meter "lockFailedMessages", @units
    @processedSuccessfully = new Meter "processedSuccessfully", @units
    @processingErrors = new Meter "processingErrors", @units
    @messageFetchErrors = new Meter "messageFetchErrors", @units
    @messageProcessingTimer = new measured.Timer()

    @customStats = []
    @customMeters = []
    @customTimers = []

    @_eventSubject = options.eventSubject or new Rx.Subject()

    @cacheClearCommands = @_cacheClearCommandsObserver
    @_cacheClearCommandsObserver = new Rx.Subject()
    @cacheClearCommands = @_cacheClearCommandsObserver

    @_panicModeObserver = new Rx.BehaviorSubject()
    @panicModeEvents = @_panicModeObserver

    @_stopListeners = []

    @addStopListener =>
      @_cacheClearCommandsObserver.onCompleted()
      @_panicModeObserver.onCompleted()
      @_eventSubject.onCompleted()
      @_unrefMeters()

  events: () ->
    @_eventSubject

  postEvent: (event) ->
    @_eventSubject.onNext(event)

  _unrefMeters: () ->
    # workaround until PR is merged: https://github.com/felixge/node-measured/pull/12
    allTimerMeters = _.map [@messageProcessingTimer].concat(_.map(@customTimers, (t) -> t.timer)), (t) -> t._meter
    allMeters = _.map(@customMeters, (m) -> m.meter).concat(allTimerMeters).concat([
      @messagesIn
      @messagesOut
      @tasksStarted
      @tasksFinished
      @awaitOrderingIn
      @awaitOrderingOut
      @lockedMessages
      @unlockedMessages
      @inProcessing
      @lockFailedMessages
      @processedSuccessfully
      @processingErrors
      @messageFetchErrors
    ])

    _.each allMeters, (meter) -> meter.unref()

  toJSON: (countOnly = false) ->
    json =
      started: @started
      lastHeartbeat: @lastHeartbeat
      processor: @processor
      messagesIn: if countOnly then @messagesIn.count() else @messagesIn.toJSON()
      messagesOut: if countOnly then @messagesOut.count() else @messagesOut.toJSON()
      tasksStarted: if countOnly then @tasksStarted.count() else @tasksStarted.toJSON()
      tasksFinished: if countOnly then @tasksFinished.count() else @tasksFinished.toJSON()
      awaitOrderingIn: if countOnly then @awaitOrderingIn.count() else @awaitOrderingIn.toJSON()
      awaitOrderingOut: if countOnly then @awaitOrderingOut.count() else @awaitOrderingOut.toJSON()
      messagesInProgress: @messagesInProgress()
      activeTasks: @activeTasks()
      messagesAwaiting: @messagesAwaiting()
      locallyLocked: @locallyLocked
      lockedMessages: if countOnly then @lockedMessages.count() else @lockedMessages.toJSON()
      unlockedMessages: if countOnly then @unlockedMessages.count() else @unlockedMessages.toJSON()
      inProcessing: if countOnly then @inProcessing.count() else @inProcessing.toJSON()
      lockFailedMessages: if countOnly then @lockFailedMessages.count() else @lockFailedMessages.toJSON()
      processingErrors: if countOnly then @processingErrors.count() else @processingErrors.toJSON()
      processedSuccessfully: if countOnly then @processedSuccessfully.count() else @processedSuccessfully.toJSON()
      messageFetchErrors: if countOnly then @messageFetchErrors.count() else @messageFetchErrors.toJSON()
      panicMode: @panicMode
      paused: @paused

    _.each @customStats, (stat) ->
      json["#{stat.prefix}.#{stat.name}"] = stat.statJsonFn()

    _.each @customMeters, (meterDef) ->
      json["#{meterDef.prefix}.#{meterDef.name}"] = if countOnly then meterDef.meter.count() else meterDef.meter.toJSON()

    if not countOnly
      json.messageProcessingTime = @messageProcessingTimer.toJSON().histogram

      _.each @customTimers, (timerDef) ->
        json["#{timerDef.prefix}.#{timerDef.name}"] = timerDef.timer.toJSON().histogram

    json

  addStopListener: (fn) ->
    @_stopListeners.push fn

  addCustomStat: (prefix, name, statJsonFn) ->
    @customStats.push {prefix: prefix, name: name, statJsonFn: statJsonFn}
    this

  addCustomMeter: (prefix, name) ->
    meter = new Meter name, @units
    @customMeters.push {prefix: prefix, name: name, meter: meter}
    meter

  addCustomTimer: (prefix, name) ->
    timer = new measured.Timer()
    @customTimers.push {prefix: prefix, name: name, timer: timer}
    timer

  applyBackpressureAtHeartbeat: (heartbeat) ->
    @lastHeartbeat = heartbeat
    @paused or @panicMode or (@messagesInProgress() - @messagesAwaiting()) > 0

  applyBackpressureAtNextMessagePage: (offset, limit, total) ->
    false

  activeTasks: () ->
    @tasksStarted.count() - @tasksFinished.count()

  taskStarted: () ->
    @tasksStarted.mark()

  taskFinished: () ->
    @tasksFinished.mark()

  messagesInProgress: () ->
    @messagesIn.count() - @messagesOut.count()

  messagesAwaiting: () ->
    @awaitOrderingIn.count() - @awaitOrderingOut.count()

  reportMessageFetchError: () ->
    @messageFetchErrors.mark()

  incommingMessage: (msg) ->
    @messagesIn.mark()
    msg

  messageProcessingStatered: (msg) ->
    @inProcessing.mark()

  lockedMessage: (msg) ->
    @lockedMessages.mark()

  unlockedMessage: (msg) ->
    @unlockedMessages.mark()
    @postEvent {type: 'unlock', message: msg}

  messageFinished: (msg) ->
    @messagesOut.mark()

  failedLock: (msg) ->
    @lockFailedMessages.mark()

  processingError: (msg) ->
    @processingErrors.mark()
    @postEvent {type: 'processingError', message: msg}

  yay: (msg) ->
    @processedSuccessfully.mark()
    @postEvent {type: 'yay', message: msg}

  awaitOrderingAdded: (msg) ->
    @awaitOrderingIn.mark()

  awaitOrderingRemoved: (msg) ->
    @awaitOrderingOut.mark()

  startMessageProcessingTimer: () ->
    @messageProcessingTimer.start()

  initiateSelfDestructionSequence: ->
    @panicMode = true
    @_panicModeObserver.onNext true

    @logger.info "Initiating self destruction sequence..."

    d = Q.defer()

    subscription = Rx.Observable.interval(500).subscribe =>
      if @messagesInProgress() is 0
        subscription.dispose()

        @logger.info "All messages are evacuated!"

        Q.all _.map(@_stopListeners, (fn) -> fn())
        .then =>
          @logger.info "Graceful exit #{JSON.stringify @toJSON()}"
          d.resolve @toJSON(), @toJSON(true)
        .fail (error) =>
          @logger.error "Unable to perform graceful exit because of the error!", error
          d.reject error
        .done()

    d.promise

  startServer: (port) ->
    return null
    statsApp = express()

    statsApp.get '/', (req, res) =>
      res.json @toJSON()

    statsApp.get '/count', (req, res) =>
      res.json @toJSON(true)

    statsApp.get '/clearCache', (req, res) =>
      @_cacheClearCommandsObserver.onNext "Doit!!!!"
      res.json
        message: 'Cache cleared!'

    statsApp.get '/stop', (req, res) =>
      res.json
        message: 'Self destruction sequence initiated!'

      @initiateSelfDestructionSequence()

    statsApp.get '/pause', (req, res) =>
      if @paused
        res.json
          message: "Already paused."
      else
        @paused = true
        res.json
          message: "Done."

    statsApp.get '/resume', (req, res) =>
      if not @paused
        res.json
          message: "Already running."
      else
        @paused = false
        res.json
          message: "Done."

    server = statsApp.listen port, =>
      @logger.info "Statistics is on port #{port}"

    @addStopListener =>
      @logger.info "Stopping stats server"

      d = Q.defer()

      server.close ->
        d.resolve()

      d.promise

  startPrinter: (countOnly = false) ->
    subscription  = Rx.Observable.interval(3000).subscribe =>
      @logger.info "+---------------- STATS ----------------+"
      @logger.info JSON.stringify(@toJSON(countOnly))
      @logger.info "+----------------- END -----------------+"

    @addStopListener =>
      @logger.info "Stopping stats printer"
      subscription.dispose()

exports.Meter = Meter
exports.Stats = Stats