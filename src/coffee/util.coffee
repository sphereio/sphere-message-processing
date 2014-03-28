Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'

###
  Module has some utility functions
###
module.exports =
  parseProjectsCredentials: (credentialsConfig, str) ->
    if not str?
      []
    else
      _.map str.split(/,/), (c) =>
        parts = c.split /:/

        if _.size(parts) is 1
          _.extend {}, credentialsConfig.forProjectKey parts[0], {props: {}}
        else if _.size(parts) is 2
          _.extend {}, credentialsConfig.forProjectKey(parts[0]), {props: @_parseProjectParams(parts[1])}
        else if _.size(parts) is 3
          {project_key: parts[0], client_id: parts[1], client_secret: parts[2], props: {}}
        else if _.size(parts) is 4
          {project_key: parts[0], client_id: parts[1], client_secret: parts[2], props: @_parseProjectParams(parts[3])}
        else
          throw new Error("Invalid project fields count")

  _parseProjectParams: (str) ->
    res = {}

    _.each str.split(/\//), (p) ->
      parts = p.split /\=/

      if _.size(parts) is 1
        res[parts[0]] = true
      else if _.size(parts) is 2
        if res[parts[0]]? and _.isArray(res[parts[0]])
          res[parts[0]] = res[parts[0]].concat(parts[1])
        else if res[parts[0]]?
          res[parts[0]] = [res[parts[0]], parts[1]]
        else
          res[parts[0]] = parts[1]
      else
        throw new Error("Invalid projects parameter definition (only one `=` is allowed)")

    res

  parseDate: (str) ->
    new Date(str)

  addDateTime: (date, hours, minutes, seconds) ->
    @addDateMs date, (hours * 60 * 60 * 1000) + (minutes * 60 * 1000) + (seconds * 1000)

  addDateHours: (date, hours) ->
    @addDateMs date, hours * 60 * 60 * 1000

  addDateMinutes: (date, minutes) ->
    @addDateMs date, minutes * 60 * 1000

  addDateSeconds: (date, minutes) ->
    @addDateMs date, minutes * 1000

  addDateMs: (date, millis) ->
    ms = date.getTime()
    new Date(ms + millis)

  formatDate: (date) ->
    date.toISOString()