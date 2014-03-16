module EventStore
  class Journal
    def initialize(revision_callback = nil)
      @riak = Riak::Client.new
      @revision_callback = revision_callback
    end

    def append(key, events)
      current_revision = _revisions.get_or_new(key)
      _enforce_revision(current_revision, events)
      current_revision.raw_data = JSON.dump({rev: events.last.fetch(:rev)})
      current_revision.content_type = 'application/json'

      @revision_callback.call unless @revision_callback.nil?

      current_revision.store(bucket_type: 'strongly_consistent')

      events.each do |event|
        event_object = _events.new("#{key}.#{event.fetch(:rev)}")
        event_object.raw_data = JSON.dump(event.fetch(:data))
        event_object.content_type = 'application/json'
        event_object.store(bucket_type: 'default')
      end
    end

    def get(key)
      revision_object = _revisions.get(key)
      revision = JSON.parse(revision_object.raw_data).fetch("rev")
      keys = (1..revision).map { |rev| "#{key}.#{rev}" }

      events_by_key = _events.get_many(keys)
      events = events_by_key.keys.sort.map { |k| events_by_key[k] }
      Entity.new(revision, events.map(&:raw_data).map { |e| JSON.parse(e, symbolize_names: true) })
    end

    def _revisions
      @revisions ||= @riak.bucket("rev")
    end

    def _events
      @events ||= @riak.bucket("events")
    end

    def _enforce_revision(current, future_events)
      current_rev = current.raw_data.nil? ? 0 : JSON.parse(current.raw_data).fetch("rev")
      future_revisions = future_events.map { |e| e.fetch(:rev) }

      raise StaleObjectException unless current_rev < future_revisions.min
    end
  end
end
