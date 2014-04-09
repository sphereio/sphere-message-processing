Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
express = require 'express'
measured = require 'measured'
{LoggerFactory} = require '../lib/logger'
util = require '../lib/util'
JSONStream = require 'JSONStream'

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

  setMessageSources: (sources) ->
    @_messageSources = sources

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
    statsApp = express()

    statsApp.use express.cookieParser()
    statsApp.use express.session({secret: '1234567890QWERTY'})

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

    statsApp.get '/m/:type', (req, res) =>
      try
        thresholdHours = if req.query.threshold? then parseInt(req.query.threshold) else 0

        total = 0
        olderThanThreshold = 0

        sources = _.map @_messageSources, (sphere) ->
          source =
            switch req.params.type
              when 'errors' then sphere.getAllMessageProcessingErrors(req.query.hours)
              when 'locks' then sphere.getAllMessageProcessingLocks(req.query.hours)
              else throw new Error("Unsupported type: #{req.params.type}")

          source.do (error) ->
            error.type = req.params.type
            error.container = sphere.getMessagesStateContainer()
            error.projectKey = sphere.getSourceInfo().prefix

            total = total + 1

            lastModifiedAtMs = util.parseDate(error.lastModifiedAt).getTime()
            thresholdMs = util.addDateHours(new Date(), -1 * thresholdHours).getTime()

            if lastModifiedAtMs < thresholdMs
              olderThanThreshold = olderThanThreshold + 1

        res.header("Content-Type", "application/json; charset=utf-8")

        writer = JSONStream.stringify()
        writer.pipe(res)

        nextFn = (msg) ->
          writer.write msg

        errorFn = (error) ->
          res.end "Error: #{error.message}"

        completeFn = ->
          writer.end {type: 'count', total: total, totalOlderThanThreshold: olderThanThreshold, totalNewerThanThreshold: total - olderThanThreshold}

        Rx.Observable.merge(sources).subscribe Rx.Observer.create(nextFn, errorFn, completeFn)
      catch e
        console.error e
        res.end "Error: #{e.message}"

    findSourceByProject = (projectKey) =>
      _.find @_messageSources, (s) ->s.getSourceInfo().prefix is projectKey

    statsApp.patch '/m/:type/:project/:id', (req, res) =>
      if not (req.params.type is 'errors' or req.params.type is 'locks')
        res.json 404, {message: "Unsupported type: " + req.params.type}
      else if req.query.yes_i_understand_the_consequences_and_want_to_do_it_now? and req.query.yes_i_understand_the_consequences_and_want_to_do_it_now is req.session.code and not _s.isBlank(req.query.my_name)
        delete req.session.code
        source = findSourceByProject(req.params.project)

        if not source?
          res.json 404, {message: "Can't find project #{req.params.project}"}
        else
          source.getMessageStateById(req.params.id)
          .then (result) ->
            result.value.state = "processed"
            result.value.WAS_FORCED_BY = req.query.my_name

            source.saveMessageState result
            .then ->
              res.json {message: "Done! Thank you very much for resolving the issue!"}
          .fail (error) ->
            res.json 500, {message: "Error! " + error.message}
          .done()
      else
        source = findSourceByProject(req.params.project)

        if not source?
          res.json 404, {message: "Can't find project #{req.params.project}"}
        else
          source.getMessageStateById(req.params.id)
          .then (result) ->
            req.session.code = "#{_.random(0, 1000)}"
            res.json 400, {message: "You are about to mark a message with ID '#{req.params.id}' as PROCESSED!!! Please be very carefull and think very carefully one more time about it! I you still desperate about it, then please add query parameter yes_i_understand_the_consequences_and_want_to_do_it_now with value #{req.session.code} and provide your name with my_name!", state: result}
          .fail (error) ->
            res.json 500, {message: "Error! " + error.message}
          .done()

    statsApp.delete '/m/:type/:project/:id', (req, res) =>
      if not (req.params.type is 'errors' or req.params.type is 'locks')
        res.json 404, {message: "Unsupported type: " + req.params.type}
      else if req.query.yes_i_understand_the_consequences_and_want_to_do_it_now? and req.query.yes_i_understand_the_consequences_and_want_to_do_it_now is req.session.code and not _s.isBlank(req.query.my_name)
        delete req.session.code
        source = findSourceByProject(req.params.project)

        if not source?
          res.json 404, {message: "Can't find project #{req.params.project}"}
        else
          source.getMessageStateById(req.params.id)
          .then ->
            source.deleteMessageState req.params.id
            .then ->
              res.json {message: "Done! Thank you very much for resolving the issue!"}
          .fail (error) ->
            res.json 500, {message: "Error! " + error.message}
          .done()
      else
        source = findSourceByProject(req.params.project)

        if not source?
          res.json 404, {message: "Can't find project #{req.params.project}"}
        else
          source.getMessageStateById(req.params.id)
          .then (result) ->
            req.session.code = "#{_.random(0, 1000)}"
            res.json 400, {message: "You are about to !!!!!DETETE!!!! message with ID '#{req.params.id}'!!! Please be very very very carefull and think very carefully one more time about it! I you still desperate about it, then please add query parameter yes_i_understand_the_consequences_and_want_to_do_it_now with value #{req.session.code} and provide your name with my_name!", state: result}
          .fail (error) ->
            res.json 500, {message: "Error! " + error.message}
          .done()

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