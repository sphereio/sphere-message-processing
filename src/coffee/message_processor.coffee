Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{LoggerFactory} = require '../lib/logger'

class MessageProcessor
  constructor: (@stats, options) ->
    @messageProcessors = options.processors # Array[(SourceInfo, Message) => Promise[Anything]]
    @messageSources = options.messageSources
    @heartbeatInterval = options.heartbeatInterval or 2000
    @logger = LoggerFactory.getLogger "processor.#{options.processorName}", options.logger

    @recycleBin = @_createRecycleBin()
    @messageTypeMeters = {}
    @unrecoverableErrors = @_createUnrecoverableErrorProcessor @recycleBin
    @errors = @_createErrorProcessor @unrecoverableErrors, @recycleBin

  run: () ->
    heartbeat = Rx.Observable.interval @heartbeatInterval
    .filter (heartbeat) =>
      not @stats.applyBackpressureAtHeartbeat(heartbeat)

    messageSources = _.map @messageSources, (source) =>
      [sourceObserver, sourceObservable] = source.getMessageSource()
      subscription = heartbeat.subscribe sourceObserver

      @logger.info "Wiring message fetch for #{source.getSourceInfo().prefix}"

      @stats.addStopListener =>
        @logger.info "Stopping hartbeat for #{source.getSourceInfo().prefix}"
        subscription.dispose()
        sourceObserver.onCompleted()

      sourceObservable

    reprocessSource = @stats.resourceReprocess
    .flatMap (cmd) =>
      prefix = cmd.sphere.getSourceInfo().prefix
      source = _.find @messageSources, (ms) -> ms.getSourceInfo().prefix is prefix

      if not source?
        @logger.error "Source not found!!!! Shoud not happen!"
        Rx.Observable.fromArray []
      else
        source.getMessagesForResource cmd.resourceId

    all = Rx.Observable.merge messageSources.concat([reprocessSource])
    .map (msg) =>
      @stats.incommingMessage msg

    locked = @_filterAndLockNewMessages all

    @_doProcessMessages locked
    .subscribe @_ignoreCompleted(@recycleBin)

  _getMessageTypeMeter: (msg, name) ->
    meter = @messageTypeMeters[msg.payload.type + '.' + name]

    if not meter
      meter = @stats.addCustomMeter msg.payload.type, name
      @messageTypeMeters[msg.payload.type + '.' + name] = meter

    meter

  _doProcessMessages: (lockedMessages) ->
    toUnlock = new Rx.Subject()

    unlocked = toUnlock
    .flatMap (msg) =>
      subj = new Rx.Subject()

      msg.persistence.unlockMessage msg
      .then (msg) ->
        subj.onNext msg
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Unlocking the message"}
        subj.onCompleted()
      .done()

      subj

    processed = lockedMessages
    .do (msg) =>
      @stats.messageProcessingStatered msg
    .flatMap (msg) =>
      [sink, errors] = msg.persistence.orderBySequenceNumber msg, toUnlock

      errors.subscribe @_ignoreCompleted(@errors)
      sink
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @_processMessage @messageProcessors, msg
      .then (results) =>
        if not _.isEmpty(_.find(results, (res) -> res.processed))
          msg.result = _.map results, (res) -> res.processingResult
          subj.onNext(msg)
        else
          # unprocessed messages are not allowed! If message should be ignored, then it should be done explicitly in processor
          @errors.onNext {message: msg, error: new Error("No processor is defined, that can handle this message."), processor: "Actual processing"}

        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Actual processing"}
        subj.onCompleted()
      .done()

      subj
    .flatMap (msg) =>
      subj = new Rx.Subject()

      if _.find(msg.result, (res) -> not res.ignored)?
        @_getMessageTypeMeter(msg, "processed").mark()
      else
        @_getMessageTypeMeter(msg, "ignored").mark()

      msg.persistence.reportSuccessfullProcessing msg
      .then (msg) ->
        subj.onNext(msg)
        subj.onCompleted()
      .fail (error) =>
        @errors.onNext {message: msg, error: error, processor: "Reporting successful processing"}
        subj.onCompleted()
      .done()

      subj

    Rx.Observable.merge [processed, unlocked]

  _processMessage: (processors, msg) ->
    try
      promises = _.map processors, (processor) ->
        processor msg.persistence.getSourceInfo(), msg.payload

      Q.all promises
    catch error
      Q.reject(error)

  _filterAndLockNewMessages: (messages) ->
    [newMessages, other, errors]  = @_split messages, (msg) ->
      msg.persistence.checkAvaialbleForProcessingAndLockLocally msg

    other.subscribe @_ignoreCompleted(@recycleBin)
    errors
    .subscribe @_ignoreCompleted(@unrecoverableErrors)

    newMessages
    .do (msg) =>
      msg.stopwatch = @stats.startMessageProcessingTimer()
    .flatMap (msg) =>
      [locked, errors, toRecycle] = msg.persistence.lockMessage msg

      errors.subscribe @_ignoreCompleted(@unrecoverableErrors)
      toRecycle.subscribe @_ignoreCompleted(@recycleBin)

      locked

  _ignoreCompleted: (observer) ->
    Rx.Observer.create(
      (next) -> observer.onNext next,
      (error) -> observer.onError error,
      () ->
    )

  _createRecycleBin: () ->
    recycleBin = new Rx.Subject()

    nextFn = (msg) =>
      if msg.stopwatch?
        msg.stopwatch.end()
      msg.persistence.releaseLocalLock msg
      @stats.messageFinished msg

    errorFn = (error) =>
      @logger.error "Error came to the recycle bin.", error

    completeFn = =>
      @logger.error "Recycle Bin completed!", new Error("Recycle Bin completed! This should never happen!!")

    recycleBin
    .subscribe Rx.Observer.create(nextFn, errorFn, completeFn)

    recycleBin


  _createErrorProcessor: (unrecoverableErrors, recycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .flatMap (msg) =>
      subj = new Rx.Subject()

      @stats.processingError msg
      @_getMessageTypeMeter(msg.message, "failed").mark()

      msg.message.persistence.reportMessageProcessingFailure msg.message, msg.error, msg.processor
      .then =>
        @logger.error "Error during: #{msg.processor}. Error would be saved in the custom object. Message: #{JSON.stringify msg.message.payload}", msg.error

        subj.onNext msg.message
        subj.onCompleted()
      .fail (error) ->
        unrecoverableErrors.onNext {message: msg.message, error: error, processor: "Reporting processing error: #{msg.error.stack}"}
        subj.onCompleted()
      .done()

      subj
    .subscribe recycleBin

    errorProcessor

  _createUnrecoverableErrorProcessor: (recycleBin) ->
    errorProcessor = new Rx.Subject()

    errorProcessor
    .map (box) =>
      if box?
        if box.message?
          @logger.error "Error during: #{box.processor}. Message: #{JSON.stringify box.message.payload}", box.error
        else
          @logger.error "Error during: #{box.processor}. No message.", box.error
      else
        @logger.error "Some strange error happend, but not shure want exactly :( Please review the message processing pipeline."

      box.message
    .subscribe recycleBin

    errorProcessor

  _split: (obs, predicatePromice) ->
    thenSubj = new Rx.Subject()
    elseSubj = new Rx.Subject()
    errSubj = new Rx.Subject()

    nextFn = (x) ->
      predicatePromice x
      .then (bool) ->
        if bool
          thenSubj.onNext x
        else
          elseSubj.onNext x
      .fail (error) ->
        errSubj.onNext {message: x, error: error, processor: "Split predicate"}
      .done()

    obs.subscribe(
      nextFn,
      ((error) -> errSubj.onNext {message: null, error: error, processor: "Split"}),
      ( -> thenSubj.onCompleted(); elseSubj.onCompleted(); errSubj.onCompleted()),
    )

    [thenSubj, elseSubj,errSubj]

exports.MessageProcessor = MessageProcessor