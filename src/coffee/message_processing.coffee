optimist = require 'optimist'
Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'

{SphereService} = require '../lib/sphere_service'
{Stats} = require '../lib/stats'
{MessageProcessor} = require '../lib/message_processor'
{MessagePersistenceService} = require '../lib/message_persistence'
{TaskQueue} = require '../lib/task_queue'
{ProjectCredentialsConfig} = require 'sphere-node-utils'
{GraphitePublisher} = require '../lib/stats_publisher'

util = require '../lib/util'
{LoggerFactory} = require '../lib/logger'

class MessageProcessing
  constructor: (@_argvFn, @statsOptions, @processors, @messageCriteria, @messageType, @messageExpand, @defaultProcessorName, @runFn) ->

  _createMessageProcessor: (ids) ->
    @sourceProjects = util.parseProjectsCredentials @credentialsConfig, @argv.sourceProjects

    sphereServicesPs = _.map @sourceProjects, (project) =>
      messageCriteria =
        if @messageType?
          idsQuery = if _.isArray(ids) then " and id in (#{_.map(ids, (id) -> '"' + id + '"').join(', ')})" else ''
          "resource(typeId=\"#{@messageType}\"#{idsQuery})"
        else
          @messageCriteria

      SphereService.create @stats,
        sphereHost: @argv.sphereHost
        requestQueue: @requestQueue
        messagesPageSize: @argv.messagesPageSize
        additionalMessageCriteria: messageCriteria
        additionalMessageExpand: @messageExpand
        fetchHours: @argv.fetchHours
        processorName: @processorName
        logger: @rootLogger
        connector:
          user_agent: "#{@processorName}-#{project.project_key}"
          config: project

    Q.all sphereServicesPs
    .then (sphereServices) =>
      @stats.setMessageSources sphereServices

      new MessageProcessor @stats,
        messageSources: _.map(sphereServices, (sphere) => new MessagePersistenceService(@stats, sphere, {awaitTimeout: @argv.awaitTimeout, logger: @rootLogger}))
        processors: @processors
        heartbeatInterval: @argv.heartbeatInterval
        processorName: @processorName
        logger: @rootLogger

  init: (statsStartOptions = {}, aternativeArgv) ->
    if not @_initialized
      if aternativeArgv?
        @argv = _.extend {}, @_argvFn(statsStartOptions.offline), aternativeArgv
      else
        @argv = @_argvFn()

      @processorName = @argv.processorName or @defaultProcessorName

      LoggerFactory.setLevel @argv.logLevel

      @rootLogger = LoggerFactory.getLogger 'processing'

      if not @processorName?
        throw new Error("Processor name is not defined")

      defaultStatsOptions =
        processor: @processorName
        logger: @rootLogger

      @stats = new Stats _.extend({}, defaultStatsOptions, @statsOptions, statsStartOptions)
      @requestQueue = new TaskQueue @stats, {maxParallelTasks: @argv.maxParallelSphereConnections}
      @_offlineMode = statsStartOptions.offline

      if @argv.publishToGraphite
        @_statsPublisher = new GraphitePublisher
          stats: @stats
          logger: @rootLogger
          prefix: "#{@argv.graphitePrefix}.#{@processorName}"
          intervalMs: @argv.graphiteReportInterval
          carbonHost: @argv.graphiteHost
          carbonPort: @argv.graphitePort
          statsFn: =>
            @stats.allStats()

      @_initialized = true

    this

  start: (ids) ->
    @init()

    ProjectCredentialsConfig.create()
    .then (cfg) =>
      @credentialsConfig = cfg

      if not @_offlineMode
        @stats.startServer @argv.statsPort

      processor =
        if @runFn?
          @runFn(@argv, @stats, @requestQueue, @credentialsConfig, @rootLogger)
        else
          Q(null)

      Q.all [processor, @_createMessageProcessor(ids)]
    .then ([processor, messageProcessor]) =>
      if processor?
        @processors.push processor

      @messageProcessor = messageProcessor
      @messageProcessor.run()


      if @argv.printStats
        @stats.startPrinter(true)

      @rootLogger.info "Processor '#{@processorName}' started."
    .fail (error) =>
      @rootLogger.error "Error during getting project credentials config", error
    .done()

    @stats.events()

  stop: () ->
    @stats.initiateSelfDestructionSequence()

  @builder: () ->
    new MessageProcessingBuilder

class MessageProcessingBuilder
  constructor: () ->
    @usage = 'Usage: $0 --sourceProjects [PROJECT_CREDENTIALS]'
    @demand = ['sourceProjects']
    @statsOptions = {}
    @processors = []
    @additionalMessageExpand = []

  optimistUsage: (extraUsage) ->
    @usage =  @usage + " " + extraUsage
    this

  optimistDemand: (extraDemand) ->
    @demand.push extraDemand
    this

  optimistExtras: (fn) ->
    @optimistExtrasFn = fn
    this

  stats: (options) ->
    @statsOptions = options
    this

  processor: (fn) ->
    @processors.push fn
    this

  messageCriteria: (query) ->
    @additionalMessageCriteria = query
    this

  messageType: (type) ->
    @_messageType = type
    this

  messageExpand: (expand) ->
    @additionalMessageExpand = expand
    this

  processorName: (pn) ->
    @defaultProcessorName = pn
    this

  build: (runFn) ->
    o = optimist
    .usage(@usage)
    .alias('sourceProjects', 's')
    .alias('statsPort', 'p')
    .alias('help', 'h')
    .alias('logLevel', 'l')
    .describe('help', 'Shows usage info and exits.')
    .describe('sphereHost', 'Sphere.io host name.')
    .describe('sourceProjects', 'Sphere.io project credentials. The messages from these projects would be processed. Format: `prj1-key:clientId:clientSecret[,prj2-key:clientId:clientSecret][,...]`.')
    .describe('statsPort', 'The port of the stats HTTP server.')
    .describe('processorName', 'The name of this processor. Name is used to rebember, which messaged are already processed.')
    .describe('printStats', 'Whether to print stats to the console every 3 seconds.')
    .describe('awaitTimeout', 'How long to wait for the message ordering (in ms).')
    .describe('heartbeatInterval', 'How often are messages retrieved from sphere project (in ms).')
    .describe('fetchHours', 'How many hours of messages should be fetched (in hours).')
    .describe('maxParallelSphereConnections', 'How many parallel connection to sphere are allowed.')
    .describe('messagesPageSize', 'How many should be loaded in one go.')
    .describe('logLevel', 'Loging intensity: debug|info|warn|error')
    .describe('publishToGraphite', 'Publish metrics to graphite')
    .describe('graphitePrefix', 'The metrics key prefix')
    .describe('graphiteReportInterval', 'Graphite reporting interval (in ms)')
    .describe('graphiteHost', 'Graphite host name')
    .describe('graphitePort', 'Graphite port')
    .default('statsPort', 7777)
    .default('awaitTimeout', 120000)
    .default('heartbeatInterval', 2000)
    .default('fetchHours', 24)
    .default('messagesPageSize', 100)
    .default('maxParallelSphereConnections', 100)
    .default('logLevel', 'info')
    .default('sphereHost', 'api.sphere.io')
    .default('publishToGraphite', false)
    .default('graphitePrefix', "message_processor")
    .default('graphiteReportInterval', 60000)
    .default('graphiteHost', "localhost")
    .default('graphitePort', 2003)

    if @optimistExtrasFn?
      @optimistExtrasFn o

    argvFn = (offline) =>
      if not offline
        o = o.demand(@demand)

      argv = o.argv

      if not offline and argv.help
        o.showHelp()
        process.exit 0

      argv

    () => new MessageProcessing argvFn, @statsOptions, _.clone(@processors), @additionalMessageCriteria, @_messageType, @additionalMessageExpand, @defaultProcessorName, runFn

exports.MessageProcessing = MessageProcessing
exports.MessageProcessingBuilder = MessageProcessingBuilder