# sphere-message-processing

[![Build Status](https://travis-ci.org/sphereio/sphere-message-processing.png?branch=master)](https://travis-ci.org/sphereio/sphere-message-processing) [![NPM version](https://badge.fury.io/js/sphere-message-processing.png)](http://badge.fury.io/js/sphere-message-processing) [![Coverage Status](https://coveralls.io/repos/sphereio/sphere-message-processing/badge.png?branch=master)](https://coveralls.io/r/sphereio/sphere-message-processing?branch=master) [![Dependency Status](https://david-dm.org/sphereio/sphere-message-processing.png?theme=shields.io)](https://david-dm.org/sphereio/sphere-message-processing) [![devDependency Status](https://david-dm.org/sphereio/sphere-message-processing/dev-status.png?theme=shields.io)](https://david-dm.org/sphereio/sphere-message-processing#info=devDependencies)

Service listens for line item state change messages in some project and replicates them to another SPHERE.IO project.

## Getting Started

Install the module with: `npm install sphere-message-processing`

## Setup

* create `config.js`
  * make `create_config.sh`executable

    ```
    chmod +x create_config.sh
    ```
  * run script to generate `config.js`

    ```
    ./create_config.sh
    ```
* configure github/hipchat integration (see project *settings* in guthub)
* install travis gem `gem install travis`
* add encrpyted keys to `.travis.yml`
 * add sphere project credentials to `.travis.yml`

        ```
        travis encrypt [xxx] --add SPHERE_PROJECT_KEY
        travis encrypt [xxx] --add SPHERE_CLIENT_ID
        travis encrypt [xxx] --add SPHERE_CLIENT_SECRET
        ```
  * add hipchat credentials to `.travis.yml`

        ```
        travis encrypt [xxx]@Sphere --add notifications.hipchat.rooms
        ```

## Documentation
_(Coming soon)_

## Tests
Tests are written using [jasmine](http://pivotal.github.io/jasmine/) (behavior-driven development framework for testing javascript code). Thanks to [jasmine-node](https://github.com/mhevery/jasmine-node), this test framework is also available for node.js.

To run tests, simple execute the *test* task using `grunt`.
```bash
$ grunt test
```

## Examples
_(Coming soon)_

## Contributing
In lieu of a formal styleguide, take care to maintain the existing coding style. Add unit tests for any new or changed functionality. Lint and test your code using [Grunt](http://gruntjs.com/).
More info [here](CONTRIBUTING.md)

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
Copyright (c) 2014 Oleg Ilyenko
Licensed under the MIT license.
