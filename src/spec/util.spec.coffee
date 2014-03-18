Q = require 'q'
{_} = require 'underscore'

util = require '../lib/util'

describe 'util.parseProjectsCredentials', ->
  it 'should return an empty array if input is undfined', () ->
    expect(util.parseProjectsCredentials(undefined)).toEqual []

  it 'should parse single element', () ->
    expect(util.parseProjectsCredentials("prj:id:secret")).toEqual [
      {project_key: 'prj', client_id: 'id', client_secret: 'secret'}
    ]

  it 'should parse multiple elements', () ->
    expect(util.parseProjectsCredentials('prj1:id1:s1,prj2:id2:s2')).toEqual [
      {project_key: 'prj1', client_id: 'id1', client_secret: 's1'}
      {project_key: 'prj2', client_id: 'id2', client_secret: 's2'}
    ]