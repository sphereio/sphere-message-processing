Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
util = require '../lib/util'
{LoggerFactory} = require '../lib/logger'
net = require 'net'

# Publishes stats which have following structure: {meters: [...], gauges: [...], timers: [...]}
class StatsPublisher
  publishStats: (stats) -> util.abstractMethod()

class GraphitePublisher
  constructor: (options) ->
    @logger = LoggerFactory.getLogger 'GraphitePublisher', options.logger
    @prefix = options.prefix or "message_processor"
    @stats = options.stats
    @intervalMs = options.intervalMs or 60000
    @statsFn = options.statsFn
    @carbonHost = options.carbonHost
    @carbonPort = options.carbonPort

    @schedulePublish()

  schedulePublish: ->
    subscription  = Rx.Observable.interval(@intervalMs).subscribe =>
      @publishStats(@statsFn())
      .then =>
        @logger.debug "Stats published to graphite"
      .fail (e) =>
        @logger.error "Failed to publish stats to graphite", e
      .done()

    @stats.addStopListener =>
      @logger.info "Stopping GraphitePublisher"
      subscription.dispose()

  publishStats: (stats) ->
    meters = _.flatten(_.map(stats.meters, ((m) => @_prepareMeter(m))))
    gauges = _.flatten(_.map(stats.gauges, ((g) => @_prepareGauge(g))))
    timers = _.flatten(_.map(stats.timers, ((t) => @_prepareTimer(t))))

    @_sendStats meters.concat(gauges).concat(timers)

  _sendStats: (keyValues) ->
    d = Q.defer()

    epoch = (new Date().getTime()) / 1000

    client = new net.Socket()

    errorHappened = false

    client.on 'close', ->
      if not errorHappened
        d.resolve("Done!!")

    client.on 'error', (error)->
      errorHappened = true
      d.reject(error)

    client.connect @carbonPort, @carbonHost, ->
      lines = _.map _.filter(keyValues, ((kv) -> kv.value)), (kv) -> "#{kv.key} #{kv.value} #{epoch}\n"

      client.write lines.join(""), "UTF-8",  ->
        client.end()

    d.promise

  _prepareMeter: (m) ->
    prefix = m.name
    concreteMeter = _.find m.meter.meters, (meter) -> _.endsWith(meter.name, "PerMinute")
    [{key: @_prepareKey(@prefix + "." + prefix), value: concreteMeter.meter.toJSON().count}]

  _prepareTimer: (m) ->
    [
      {key: @_prepareKey(@prefix + "." + m.name + "_min"), value: m.hist.toJSON().min}
      {key: @_prepareKey(@prefix + "." + m.name + "_max"), value: m.hist.toJSON().max}
      {key: @_prepareKey(@prefix + "." + m.name + "_sum"), value: m.hist.toJSON().sum}
      {key: @_prepareKey(@prefix + "." + m.name + "_variance"), value: m.hist.toJSON().variance}
      {key: @_prepareKey(@prefix + "." + m.name + "_mean"), value: m.hist.toJSON().mean}
      {key: @_prepareKey(@prefix + "." + m.name + "_count"), value: m.hist.toJSON().count}
      {key: @_prepareKey(@prefix + "." + m.name + "_median"), value: m.hist.toJSON().median}
      {key: @_prepareKey(@prefix + "." + m.name + "_p75"), value: m.hist.toJSON().p75}
      {key: @_prepareKey(@prefix + "." + m.name + "_p95"), value: m.hist.toJSON().p95}
      {key: @_prepareKey(@prefix + "." + m.name + "_p99"), value: m.hist.toJSON().p99}
      {key: @_prepareKey(@prefix + "." + m.name + "_p999"), value: m.hist.toJSON().p999}
    ]

  _prepareGauge: (g) ->
    [{key: @_prepareKey(@prefix + "." + g.name), value: g.statJsonFn()}]

  _prepareKey: (key) ->
    key.replace(/: /g, "_").replace(/\//g, ".")

exports.StatsPublisher = StatsPublisher
exports.GraphitePublisher = GraphitePublisher