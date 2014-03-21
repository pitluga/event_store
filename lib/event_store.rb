require "riak"
require "sequel"
require "sinatra"

require "event_store/app"
require "event_store/entity"
require "event_store/journal"
require "event_store/version"

module EventStore
  StaleObjectException = Class.new(Exception)
end
