require "riak"
require "sequel"

require "event_store/entity"
require "event_store/journal"
require "event_store/version"

module EventStore
  StaleObjectException = Class.new(Exception)
end
