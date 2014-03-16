require 'spec_helper'

describe EventStore::Journal do
  before(:each) do
    @id = generate_id
  end

  describe "append" do
    it "saves the event data in Riak" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{rev: 1, data: {foo: 'a'}}])

      person = journal.get("person.#{@id}")
      person.revision.should == 1
      person.snapshot.should == {foo: 'a'}
      person.events.should == [{foo: 'a'}]
    end

    it "saves new events to an existing entity" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{rev: 1, data: {foo: 'a'}}])
      journal.append("person.#{@id}", [{rev: 2, data: {bar: 'qux'}}])

      person = journal.get("person.#{@id}")
      person.revision.should == 2
      person.snapshot.should == {foo: 'a', bar: 'qux'}
      person.events.should == [{foo: 'a'}, {bar: 'qux'}]
    end

    it "raises a StaleObjectException if the same revision is written twice" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{rev: 1, data: {foo: 'a'}}])
      journal.append("person.#{@id}", [{rev: 2, data: {bar: 'qux'}}])

      expect do
        journal.append("person.#{@id}", [{rev: 2, data: {baz: 'z'}}])
      end.to raise_error(EventStore::StaleObjectException)
    end

    it "raises a Riak::Conflict if multiple clients update the same key" do
      pending "seems like an issue with the client library"
      write_revision = lambda do
        client = Riak::Client.new
        current_revision = client.bucket("rev").get_or_new("person.#{@id}")
        current_revision.raw_data = JSON.dump({rev: 10})
        current_revision.content_type = 'application/json'
        current_revision.store(bucket_type: 'strongly_consistent')
        current_revision.reload
      end
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{rev: 1, data: {foo: 'a'}}])

      journal = EventStore::Journal.new(write_revision)
      expect do
        journal.append("person.#{@id}", [{rev: 2, data: {baz: 'z'}}])
      end.to raise_error(Riak::Conflict)
    end
  end
end
