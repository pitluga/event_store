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

    it "raises a EventStore::StaleObjectException if multiple clients update the same key" do
      write_revision = lambda do |journal|
        journal.postgres[:events].insert(
          key: "person.#{@id}",
          revision: 2,
          riak_key: 'some key',
          created_at: Time.now,
        )
      end
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{rev: 1, data: {foo: 'a'}}])

      journal = EventStore::Journal.new(write_revision)
      expect do
        journal.append("person.#{@id}", [{rev: 2, data: {baz: 'z'}}])
      end.to raise_error(EventStore::StaleObjectException)
    end
  end
end
