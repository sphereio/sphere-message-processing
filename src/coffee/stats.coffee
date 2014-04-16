Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
express = require 'express'
measured = require 'measured'
{LoggerFactory} = require '../lib/logger'
util = require '../lib/util'
{ErrorStatusCode} = require '../lib/sphere_service'
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

    @_cacheClearCommandsObserver = new Rx.Subject()
    @cacheClearCommands = @_cacheClearCommandsObserver

    @_sequenceNumberChangeObserver = new Rx.Subject()
    @sequenceNumberChange = @_sequenceNumberChangeObserver

    @_resourceReprocessObserver = new Rx.Subject()
    @resourceReprocess = @_resourceReprocessObserver

    @_panicModeObserver = new Rx.BehaviorSubject()
    @panicModeEvents = @_panicModeObserver

    @_stopListeners = []

    @addStopListener =>
      @_cacheClearCommandsObserver.onCompleted()
      @_sequenceNumberChangeObserver.onCompleted()
      @_resourceReprocessObserver.onCompleted()
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

    enhanceMessageStateWithMessage = (sphere, messageState, expandResource) ->
      subj = new Rx.ReplaySubject()

      sphere.getMessageById(messageState.key, expandResource)
      .then (message) ->
        messageState.message = message

        subj.onNext messageState
        subj.onCompleted()
      .fail (error) ->
        subj.onError error
      .done()

      subj

    getMessageState = (sphere, message, promise) ->
      subj = new Rx.ReplaySubject()

      sphere.getMessageStateById(message.id)
      .then (state) ->
        (promise or Q())
        .then ->
          subj.onNext state
          subj.onCompleted()
      .fail (error) ->
        if error instanceof ErrorStatusCode and (error.code is 404)
          (promise or Q())
          .then ->
            subj.onNext null
            subj.onCompleted()
        else
          subj.onError error
      .done()

      subj

    findSourceByProject = (projectKey) =>
      _.find @_messageSources, (s) ->s.getSourceInfo().prefix is projectKey

    statsApp.patch '/r/:project/:id', (req, res) =>
      source = findSourceByProject(req.params.project)
      @_resourceReprocessObserver.onNext {sphere: source, resourceId: req.params.id}
      res.json {message: "Resource would be processed once again!"}

    statsApp.get '/r/:project/:id', (req, res) =>
      resourceId = req.params.id
      expandMessage = if req.query.expandMessage? then req.query.expandMessage == 'true' else true
      expandResource = if req.query.expandResource? then req.query.expandResource == 'true' else false

      try
        total = 0
        errors = 0
        processed = 0
        locked = 0
        none = 0
        resource = null
        container = null
        projectKey = null

        sphere = findSourceByProject req.params.project

        if not sphere
          throw new Error("Unknown project: #{req.params.project}")

        lastPromise = null

        source = sphere.getMessagesByResource resourceId, true
        .flatMap (message) ->
          if not resource?
            resource = _.clone message.resource

          delete message.resource.obj

          container = sphere.getMessagesStateContainer()
          projectKey = sphere.getSourceInfo().prefix

          total = total + 1

          defer = Q.defer()

          prevPromise = lastPromise
          lastPromise = defer.promise

          getMessageState sphere, message, prevPromise
          .map (state) ->
            if not state?
              none = none + 1
            else if state.value.state is 'error'
              errors = errors + 1
            else if state.value.state is 'processed'
              processed = processed + 1
            else if state.value.state is 'lockedForProcessing'
              locked = locked + 1


            defer.resolve()

            if expandMessage
              {message: message, state: state}
            else
              {state: state}

        res.header("Content-Type", "application/json; charset=utf-8")

        writer = JSONStream.stringify()
        writer.pipe(res)

        nextFn = (msg) ->
          writer.write msg

        errorFn = (error) ->
          res.end "Error: #{error.message}"

        completeFn = ->
          sphere.getLastProcessedSequenceNumber resource
          .then (sequenceNumber) ->
            writer.end
              type: 'summary',
              container: container
              projectKey: projectKey
              totalMessages: total
              errors: errors
              processed: processed
              locked: locked
              none: none
              lastProcessedSequenceNumber: sequenceNumber
              resource: (if expandResource then resource.obj else "use expandResource=true")
          .fail (error) ->
            res.end "Error: #{error.message}"
          .done()

        source.subscribe Rx.Observer.create(nextFn, errorFn, completeFn)
      catch e
        console.error e
        res.end "Error: #{e.message}"

    statsApp.get '/m/:type', (req, res) =>
      expandResource = if req.query.expandResource? then req.query.expandResource == 'true' else false
      expandMessage = if req.query.expandMessage? then req.query.expandMessage == 'true' else true
      threshold = req.query.threshold
      hours = req.query.hours

      try
        thresholdHours = if threshold? then parseInt(threshold) else 0

        total = 0
        olderThanThreshold = 0

        sources = _.map @_messageSources, (sphere) ->
          source =
            switch req.params.type
              when 'errors' then sphere.getAllMessageProcessingErrors(hours)
              when 'locks' then sphere.getAllMessageProcessingLocks(hours)
              else throw new Error("Unsupported type: #{req.params.type}")

          source.flatMap (error) ->
            error.type = req.params.type
            error.container = sphere.getMessagesStateContainer()
            error.projectKey = sphere.getSourceInfo().prefix

            total = total + 1

            lastModifiedAtMs = util.parseDate(error.lastModifiedAt).getTime()
            thresholdMs = util.addDateHours(new Date(), -1 * thresholdHours).getTime()

            if lastModifiedAtMs < thresholdMs
              olderThanThreshold = olderThanThreshold + 1

            if expandMessage
              enhanceMessageStateWithMessage sphere, error, expandResource
            else
              Rx.Observable.fromArray [error]


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

    # TODO: extract all these endpoints from stats: it's getting too big

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
          .then (state) ->
            source.getMessageById(req.params.id)
            .then (msg) ->
              source.getLastProcessedSequenceNumber(msg.resource)
              .then (lastSeqNum) ->
                [state, msg, lastSeqNum]
          .then ([state, msg, lastSeqNum]) =>
            if msg.sequenceNumber is (lastSeqNum + 1)
              state.value.state = "processed"
              state.value.WAS_FORCED_BY = req.query.my_name

              source.saveMessageState state
              .then =>
                source.updateLastProcessedSequenceNumber msg.resource, msg.sequenceNumber
                .then =>
                  @_sequenceNumberChangeObserver.onNext msg
                  @_resourceReprocessObserver.onNext {sphere: source, resourceId: msg.resource.id}
            else
              Q.reject new Error("You can't just mark everything as processed at will! :( Last processed sequence number was #{lastSeqNum} and you are trying to mark as processed #{msg.sequenceNumber}!")
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
          .then (state) ->
            source.getMessageById(req.params.id)
            .then (msg) ->
              [state, msg]
          .then ([state, msg]) ->
            req.session.code = "#{_.random(0, 1000)}"
            res.json 400, {message: "You are about to mark a message with ID '#{req.params.id}' as PROCESSED!!! Please be very carefull and think very carefully one more time about it! I you still desperate about it, then please add query parameter yes_i_understand_the_consequences_and_want_to_do_it_now with value #{req.session.code} and provide your name with my_name!", state: state, msg: msg}
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
            source.getMessageById(req.params.id)
          .then (message) =>
            source.deleteMessageState message.id
            .then =>
              @_resourceReprocessObserver.onNext {sphere: source, resourceId: message.resource.id}
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
          .then (state) ->
            source.getMessageById(req.params.id)
            .then (msg) ->
              [state, msg]
          .then ([state, msg]) ->
            req.session.code = "#{_.random(0, 1000)}"
            res.json 400, {message: "You are about to DETETE message with ID '#{req.params.id}'!!! Please be very very very carefull and think very carefully one more time about it! I you still desperate about it, then please add query parameter yes_i_understand_the_consequences_and_want_to_do_it_now with value #{req.session.code} and provide your name with my_name!", state: state, msg: msg}
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