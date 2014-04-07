Rx = require 'rx'
Q = require 'q'
{_} = require 'underscore'
{ErrorStatusCode} = require './sphere_service'
{LoggerFactory} = require '../lib/logger'

class  SphereTestKit
  stateDefs: [
    {key: "A", transitions: ["B"]}
    {key: "B", transitions: ["C", "D"]}
    {key: "C", transitions: ["D"]}
    {key: "D", transitions: ["E"]}
    {key: "E", transitions: ["A"]}

    {key: "ReadyForShipment", transitions: ["Pickup"]}
    {key: "Pickup", transitions: ["Shipped"]}
    {key: "Shipped", transitions: ["Finished"]}
    {key: "Finished", transitions: ["ReadyForShipment"]}
  ]

  channelDefs: [
    {key: 'master', roles: ['OrderImport']}
  ]

  taxCategoryDefs: [
    {name: 'Test Category', rates: [{name: "Test Rate", amount: 0.19, includedInPrice: false, country: 'DE'}]}
  ]

  constructor: (@sphere) ->
    @logger = LoggerFactory.getLogger "test-kit.#{@sphere.getSourceInfo().prefix}"

  setupProject: ->
    Q.all [
      @configureStates()
      @configureChannels()
      @configureProduct()
      @configureTaxCategory()
    ]
    .then =>
      orders = _.map _.range(1, 6), (idx) =>
        @createTestOrder idx

      Q.all orders
    .then (orders) =>
      @orders = _.map orders, (os) ->
        [m, r] = os
        {retailerOrder: r, masterOrder: m}

      @logger.info "Orders"

      _.each @orders, (o, i) =>
        @logger.info "#{i} Retailer: #{o.retailerOrder.id}, Master: #{o.masterOrder.id}"

      @logger.info _.map(@orders, (o)-> "\"#{o.retailerOrder.id}\"").join(',')

      this
    .then =>
      @addSotock(@orders[0].retailerOrder.lineItems[0].variant.sku, 1000000)
    .then =>
      @logger.info "Project setup finished"
      this

  ref: (type, obj) ->
    {typeId: type, id: obj.id}

  stateByKey: (key) ->
    _.find @states, (s) -> s.key == key

  stateById: (id) ->
    _.find @states, (s) -> s.id == id

  abcStateSwitch: (currKey) ->
    switch currKey
      when'A' then 'B'
      when'B' then 'D'
      when'D' then 'E'
      when'E' then 'A'
      else throw new Error("Unsupported state #{currKey}")

  shipmentStateSwitch: (currKey) ->
    switch currKey
      when'ReadyForShipment' then 'Pickup'
      when'Pickup' then 'Shipped'
      when'Shipped' then 'Finished'
      when'Finished' then 'ReadyForShipment'
      else throw new Error("Unsupported state #{currKey}")

  transitionRetailerOrderStates: (first, newStateFn) ->
    ps = _.map @orders, (os) =>
      currStates = _.filter os.retailerOrder.lineItems[0].state, (s) => s.state.id != @initialState.id

      p = if _.isEmpty(currStates)
        @sphere.transitionLineItemState os.retailerOrder, os.retailerOrder.lineItems[0].id, 20, @ref('state', @initialState), @ref('state', @stateByKey(first))
      else
        currStateId = currStates[0].state.id
        currStateQ = currStates[0].quantity

        newState = @stateByKey newStateFn(@stateById(currStateId).key)

        @sphere.transitionLineItemState os.retailerOrder, os.retailerOrder.lineItems[0].id, currStateQ, @ref('state', {id: currStateId}), @ref('state', newState)

      p
      .then (newOrder) ->
        os.retailerOrder = newOrder
        newOrder
    Q.all ps

  scheduleStateTransitions: (first, stateSwitch) ->
    Rx.Observable.interval 2000
    .subscribe =>
      @transitionRetailerOrderStates(first, stateSwitch)
      .then =>
        @logger.info "Transition finished"
      .fail (error) =>
        @logger.error "Error during state transition", error
      .done()

  configureStates: ->
    Q.all [
      @sphere.ensureStates [{key: "Initial"}]
      @sphere.ensureStates @stateDefs
    ]
    .then (states) =>
      [[@initialState], @states] = states
      @logger.info "States configured"
      [@initialState, @states]

  configureChannels: ->
    @sphere.ensureChannels @channelDefs
    .then (channels) =>
      [@masterChannel] = channels
      @logger.info "Channels configured"
      @masterChannel

  configureProduct: () ->
    @sphere.getFirstProduct()
    .then (product) =>
      @product = product
      @logger.info "Product found"
      product

  configureTaxCategory: () ->
    @sphere.ensureTaxCategories @taxCategoryDefs
    .then (tc) =>
      [@taxCategory] = tc
      @logger.info "Tax category configured"
      tc

  _orderJson: () ->
    {
      lineItems: [{
        variant: {
          sku: @product.masterData.staged.masterVariant.sku
        },
        quantity: 30,
        taxRate: {
          name: "some_name",
          amount: 0.19,
          includedInPrice: true,
          country: "US",
          id: @taxCategory.id
        },
        name: {
          en: "Some Product"
        },
        price: {
          country: "US",
          value: {
            centAmount: 1190,
            currencyCode: "USD"
          }
        }
      }],
      totalPrice: {
        currencyCode: "USD",
        centAmount: 1190
      },
      shippingAddress: {
        country: "US"
      },
      shippingInfo: {
        shippingMethodName: 'Normal',
        price: {
          centAmount: 1000,
          currencyCode: "EUR"
        },
        shippingRate: {
          price: {
            centAmount: 1000,
            currencyCode: "EUR"
          }
        },
        taxRate: {
          name: "some_name",
          amount: 0.19,
          includedInPrice: true,
          country: "US",
          id: @taxCategory.id
        },
        taxCategory: {"typeId": "tax-category", id: @taxCategory.id},
      },
      taxedPrice: {
        taxPortions: [{
          amount: {
            centAmount: 190,
            currencyCode: "USD"
          },
          rate: 0.19
        }],
        totalGross: {
          centAmount: 1190,
          currencyCode: "USD"
        },
        totalNet: {
          centAmount: 1000,
          currencyCode: "USD"
        }
      }
    }

  createTestOrder: (idx) ->
    Q.all [
      @sphere.importOrder @_orderJson()
      @sphere.importOrder @_orderJson()
    ]
    .then (orders) =>
      [masterOrder, retailerOrder] = orders

      @sphere.updateOrderSyncSuatus retailerOrder, @masterChannel, masterOrder.id
      .then (newRetailerOrder) ->
        [masterOrder, newRetailerOrder]

  addSomeDeliveries: () ->
    ps = _.map @orders, (o) =>
      @sphere.addDelivery o.retailerOrder, [{id: o.retailerOrder.lineItems[0].id, quantity: 4}]
      .then (o1) =>
        @sphere.addParcel o1, o1.shippingInfo.deliveries[0].id, {heightInMillimeter: 11, lengthInMillimeter: 22, widthInMillimeter: 33, weightInGram: 44}, {trackingId: "ABCD123", carrier: "DHL"}
      .then (o2) =>
        @logger.info "Finished with deliveries: #{o2.id}"
        o.retailerOrder = o2

    Q.all ps

  addSotock: (sku, quantity) ->
    @sphere.getInvetoryEntryBySkuAndChannel sku, null
    .then (ie) =>
      @sphere.addInventoryQuantity ie, quantity - ie.availableQuantity
    .fail (e) =>
      @sphere.createInventoryEntry sku, quantity

  @setupProject: (sphereService) ->
    sphereTestKit = new SphereTestKit sphereService
    sphereTestKit.setupProject()

  @cleanup = (done, subscription, processor) ->
    if subscription?
      subscription.dispose()

    processor.stop()
    .fail (error) =>
      @logger.info "Error during processor cleanup", error

  @reportSuccess: (done, subscription, processor) ->
    @cleanup done, subscription, processor
    .then ->
      done()
    .fail (error) ->
      done(error)
    .done()

  @reportFailure: (done, error, subscription, processor) ->
    @cleanup done, subscription, processor
    .finally ->
      done(error)
    .done()

exports.SphereTestKit = SphereTestKit
