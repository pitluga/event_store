$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'event_store'
require 'rack/test'
require 'support/id_generator'

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
  config.run_all_when_everything_filtered = true
  config.filter_run :focus
  config.order = 'random'

  config.include IdGenerator
  config.include Rack::Test::Methods

  def app
    EventStore::App
  end
end
