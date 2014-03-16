module EventStore
  class Entity
    attr_reader :revision, :events

    def initialize(revision, events)
      @revision = revision
      @events = events
    end

    def snapshot
      @events.reduce({}, &:merge)
    end
  end
end
