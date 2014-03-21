module EventStore
  class Journal
    attr_reader :riak, :postgres

    def initialize
      @riak = Riak::Client.new
      @postgres = Sequel.connect('postgres://pair:pair@localhost:5433/event_store')
    end

    def append(key, events)
      revisions = _find_revisions(key)
      _do_append(key, events, revisions)
    end

    def _do_append(key, events, revisions)
      new_events = _subtract_preexisting_revisions(revisions, events)
      return unless new_events.any?
      _enforce_revision(revisions, new_events)

      new_revisions = _insert_events(key, events)
      _insert_revisions(new_revisions)
    rescue Sequel::UniqueConstraintViolation
      new_revisions.each { |revision| _events.delete(revision[:riak_key]) }
      append(key, events)
    end

    def _insert_events(key, events)
      events.map do |event|
        event_json = JSON.dump(event)
        event_object = _events.new
        event_object.raw_data = event_json
        event_object.store(bucket_type: 'default')
        { key: key,
          revision: event.fetch(:revision),
          riak_key: event_object.key,
          signature: Digest::SHA256.hexdigest(event_json),
          created_at: Time.now }
      end
    end

    def _find_revisions(key)
      revisions = postgres[:revisions].where(key: key).order(Sequel.asc(:revision)).all
    end

    def _insert_revisions(revisions)
      postgres.transaction do
        revisions.each do |revision|
          postgres[:revisions].insert(revision)
        end
      end
    end

    def get(key)
      revisions = postgres[:revisions].where(key: key).order(Sequel.asc(:revision)).all
      keys = revisions.map { |r| r.fetch(:riak_key) }

      events_by_key = _events.get_many(keys)
      events = revisions.map do |revision|
        event = events_by_key[revision.fetch(:riak_key)]
        JSON.parse(event.raw_data, symbolize_names: true).fetch(:data)
      end

      Entity.new(revisions.last.fetch(:revision), events)
    end

    def _events
      @events ||= @riak.bucket("events")
    end

    def _enforce_revision(revisions, future_events)
      current_rev = revisions.map { |r| r[:revision] }.max || 0
      future_revisions = future_events.map { |e| e.fetch(:revision) }

      raise StaleObjectException unless current_rev < future_revisions.min
    end

    def _subtract_preexisting_revisions(revisions, events)
      new_events = []
      events.each do |e|
        new_events << e unless revisions.any? {|r| r[:revision] == e[:revision] && r[:signature] == Digest::SHA256.hexdigest(JSON.dump(e)) }
      end
      new_events
    end
  end
end
