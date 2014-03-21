Q = require 'q'
{_} = require 'underscore'

util = require '../lib/util'

describe 'util.parseProjectsCredentials', ->
  mockCredentialsConfig =
    forProjectKey: (key) ->
      project_key: key
      client_id: "mock_client_id"
      client_secret: "mock_client_secret"

  it 'should return an empty array if input is undfined', () ->
    expect(util.parseProjectsCredentials(mockCredentialsConfig, undefined)).toEqual []

  it 'should parse single element', () ->
    expect(util.parseProjectsCredentials(mockCredentialsConfig, "prj:id:secret")).toEqual [
      {project_key: 'prj', client_id: 'id', client_secret: 'secret', props: {}}
    ]

  it 'should parse multiple elements', () ->
    expect(util.parseProjectsCredentials(mockCredentialsConfig, 'prj1:id1:s1,prj2:id2:s2')).toEqual [
      {project_key: 'prj1', client_id: 'id1', client_secret: 's1', props: {}}
      {project_key: 'prj2', client_id: 'id2', client_secret: 's2', props: {}}
    ]