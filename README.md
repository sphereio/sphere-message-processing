## sphere-message-processing

[![Build Status](https://travis-ci.org/sphereio/sphere-message-processing.png?branch=master)](https://travis-ci.org/sphereio/sphere-message-processing) [![NPM version](https://badge.fury.io/js/sphere-message-processing.png)](http://badge.fury.io/js/sphere-message-processing)

**sphere-message-processing** is a node.js library that helps you to write message listeners for your commercetools&#8482; platform projects.
The commercetools&#8482; platform API provides the  [messages](http://dev.commercetools.com/http-api-projects-messages.html) resource, that allows you to react on different events happening in the project and to take some actions when these events happen.

Message processing can be a pretty complicated process, since you need to make sure that all of the messages are processed in correct order and the processing is idempotent, so that it can be retried if something goes wrong.  
You also need to provide rich monitoring and management capabilities to figure out when something goes wrong. The library aims to take most of these infrastructure-related complexities away from you and let you fully concentrate on the business logic of message processing.

## Getting Started

You can include the library in `package.json` file like this:

```javascript
  "dependencies": {
    // ...
    "sphere-message-processing": "0.3.19",
    "nodemailer": "0.6.1"
  },
```

Let's create a simple message processor that sends an e-mail every time an order is imported into a commercetools&#8482; platform project. You'll find the complete source code of this project in a separate [GitHub repository](https://github.com/OlegIlyenko/sphere-message-processing-example).

First we need to define the processor's code:

```coffeescript
Q = require 'q'
{_} = require 'underscore'
_s = require 'underscore.string'
{MessageProcessing, LoggerFactory} = require 'sphere-message-processing'
{loadFile, EmailSender} = require './util'

module.exports = MessageProcessing.builder()
.processorName "send-email-on-order-import"
.optimistDemand ['smtpConfig', "smtpFrom"]
.optimistExtras (o) ->
  o.describe('smtpFrom', 'A sender of the emails.')
  .describe('smtpConfig', 'SMTP Config JSON file: https://github.com/andris9/Nodemailer#setting-up-smtp')
.messageType 'order'
.build (argv, stats, requestQueue, cc, rootLogger) ->
  logger = LoggerFactory.getLogger "send-email-on-order-import", rootLogger

  loadFile(argv.smtpConfig)
  .then (smtpConfig) ->
    emailSender = new EmailSender(smtpConfig, logger, stats)

    processOrderImport = (sourceInfo, msg) ->
      emails = sourceInfo.sphere.projectProps['email']

      if not emails? or (_.isString(emails) and _s.isBlank(emails))
        emails = []
      else if _.isString(emails)
        emails = [emails]

      if not _.isEmpty(emails)
        mail =
          from: argv.smtpFrom
          subject: "New order imported: #{msg.order.orderNumber or msg.order.id}"
          text: "New order! Yay!"

        if not _.isEmpty(emails)
          mail.to = emails.join(", ")

        console.info(argv.smtpFrom, emails)
        emailSender.sendMail sourceInfo, msg, emails, [], mail
        .then ->
          {processed: true, processingResult: {emails: emails}}
      else
        Q({processed: true, processingResult: {ignored: true, reason: "no TO"}})

    (sourceInfo, msg) ->
      if msg.resource.typeId is 'order' and msg.type is 'OrderImported'
        processOrderImport sourceInfo, msg
        .fail (error) ->
          Q.reject new Error("Error! Cause: #{error.stack}")
      else
        Q({processed: true, processingResult: {ignored: true}})

```

This is pretty much it. Now you just need to build the project and to install the project locally - it's a full-featured application, that you can start from command-line:

```bash
$ grunt build
$ npm install . -g
```

In order to start the processor, you need to provide several arguments for it:

```bash
$ send-email-on-order-import \
    --sourceProjects my-project-key:<CLIENT_ID>:<CLIENT_SECRET>:email=my.email@gmail.com \
    --smtpFrom my.email@gmail.com \
    --smtpConfig smtp.json
```

The config file `smtp.json` could look like this:

```javascript
{
  "service": "Gmail",
  "auth": {
    "user": "my.email@gmail.com",
    "pass": "secret"
  }
}
```

After you started the processor it will wait for all new `OrderImported` messages and it will send and e-mail to `my.email@gmail.com`.  
**sphere-message-processing** library will also take care of auth tokens and it will refresh tokens when necessary.

`MessageProcessing` also opens a stats port (default port number is **7777**). It's a simple REST API that provides you with very powerful monitoring and management capabilities.

For example, you can get all of the metrics with this shell command:

```bash
curl localhost:7777
```

A shorter list of the most important metrics can be retrieved by the `count` parameter like this:

```bash
curl localhost:7777/count
```

## Tests

Tests are written using [jasmine](http://pivotal.github.io/jasmine/) (behavior-driven development framework for testing javascript code). Thanks to [jasmine-node](https://github.com/mhevery/jasmine-node), this test framework is also available for node.js.

To run tests, simple execute the *test* task using `grunt`.
```bash
$ grunt test
```

## Releasing

Releasing a new version is completely automated using the Grunt task `grunt release`.

```javascript
grunt release // patch release
grunt release:minor // minor release
grunt release:major // major release
```

## Styleguide

We <3 CoffeeScript here at commercetools! So please have a look at this referenced [coffeescript styleguide](https://github.com/polarmobile/coffeescript-style-guide) when doing changes to the code.

## License

Licensed under the [MIT license](http://opensource.org/licenses/MIT).
