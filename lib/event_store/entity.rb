module EventStore
  class Entity
    attr_reader :key, :revision, :events

    def initialize(key, revision, events)
      @key = key
      @revision = revision
      @events = events
    end

    def snapshot
      @events.map { |e| e["data"] }.reduce({}, &:merge)
    end

    def to_json(*)
      JSON.dump(
        key: key,
        revision: revision,
        snapshot: snapshot,
        events: events
      )
    end
  end
end
