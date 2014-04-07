Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
util = require '../lib/util'

# This "logger" is just a facade for some different logger in future
# (console.* implementation is just to get rid of console.* in actual code)
class LoggerFactory
  @setLevel: (level) ->
    @_level = level

  @getLogger: (name, parent) ->
    loggerName =
      if parent?
        parent.name() + "." + name
      else
        name

    if not @_cache?
      @_cache = {}

    if @_cache[loggerName]?
      @_cache[loggerName]
    else
      logger = new VerySimpleConsoleLogger(loggerName, @_level or 'debug')

      @_cache[loggerName] = logger

      logger

class Logger
  debug: (message) -> util.abstractMethod()
  info: (message) -> util.abstractMethod()
  warn: (message, error) -> util.abstractMethod()
  error: (message, error) -> util.abstractMethod()
  name: () -> util.abstractMethod()

class VerySimpleConsoleLogger
  constructor: (name, level) ->
    @_name = name
    @_level = level
    @_levels =
      debug: 10
      info: 20
      warn: 30
      error: 40
      none: 100

  name: () -> @_name

  _formatMessage: (level, message) ->
    date = new Date().toISOString().replace(/[t]/ig, ' ').replace(/[z]/ig, '')

    "#{date} [#{level.toUpperCase()}] - #{@_name} - #{message}"

  _acceptLevel: (currLevel) ->
    not @_levels[@_level]? or @_levels[currLevel] >= @_levels[@_level.toLowerCase()]

  debug: (message) ->
    if @_acceptLevel('debug')
      console.info @_formatMessage('debug', message)

  info: (message) ->
    if @_acceptLevel('info')
      console.info @_formatMessage('info', message)

  warn: (message, error) ->
    if @_acceptLevel('warn')
      formatted = @_formatMessage('warn', message)

      if error?
        console.warn formatted + "\nCause: " + error.stack

  error: (message, error) ->
    if @_acceptLevel('error')
      formatted = @_formatMessage('error', message)

      if error?
        console.error formatted + "\nCause: " + error.stack

exports.LoggerFactory = LoggerFactory
exports.Logger = Logger