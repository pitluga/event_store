module EventStore
  class Journal
    attr_reader :riak, :postgres

    def initialize(revision_callback = lambda { |_| })
      @riak = Riak::Client.new
      @postgres = Sequel.connect('postgres://pair:pair@localhost:5433/event_store')
      @revision_callback = revision_callback
    end

    def append(key, events)
      revisions = postgres[:events].where(key: key).order(Sequel.asc(:revision)).all
      _enforce_revision(revisions, events)
      @revision_callback.call(self)

      rows = events.map do |event|
        event_object = _events.new
        event_object.raw_data = JSON.dump(event)
        event_object.indexes["entity_bin"] = [key]
        event_object.store(bucket_type: 'default')
        { key: key,
          revision: event.fetch(:rev),
          riak_key: event_object.key,
          created_at: Time.now }
      end

      begin
        postgres.transaction do
          rows.each do |row|
            postgres[:events].insert(row)
          end
        end
      rescue Sequel::UniqueConstraintViolation
        rows.each { |r| _revisions.delete(r[:riak_key]) }
        raise StaleObjectException
      end
    end

    def get(key)
      revisions = postgres[:events].where(key: key).order(Sequel.asc(:revision)).all
      keys = revisions.map { |r| r.fetch(:riak_key) }

      events_by_key = _events.get_many(keys)
      events = revisions.map do |revision|
        event = events_by_key[revision.fetch(:riak_key)]
        JSON.parse(event.raw_data, symbolize_names: true).fetch(:data)
      end

      Entity.new(revisions.last.fetch(:revision), events)
    end

    def _revisions
      @revisions ||= @riak.bucket("rev")
    end

    def _events
      @events ||= @riak.bucket("events")
    end

    def _enforce_revision(revisions, future_events)
      current_rev = revisions.map { |r| r[:revision] }.max || 0
      future_revisions = future_events.map { |e| e.fetch(:rev) }

      raise StaleObjectException unless current_rev < future_revisions.min
    end
  end
end
