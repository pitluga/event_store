require 'spec_helper'

describe EventStore::Journal do
  before(:each) do
    @id = generate_id
  end

  describe "append" do
    it "saves the event data in Riak" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])

      person = journal.get("person.#{@id}")
      person.revision.should == 1
      person.snapshot.should == {"foo" => 'a'}
      person.events.should == [{"revision" => 1, "data" => {"foo" => 'a'}}]
    end

    it "saves new events to an existing entity" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      journal.append("person.#{@id}", [{"revision" => 2, "data" => {"bar" => 'qux'}}])

      person = journal.get("person.#{@id}")
      person.revision.should == 2
      person.snapshot.should == {"foo" => 'a', "bar" => 'qux'}
      person.events.should == [
        {"revision" => 1, "data" => {"foo" => 'a'}},
        {"revision" => 2, "data" => {"bar" => 'qux'}},
      ]
    end

    it "raises a StaleObjectException if the same revision is written twice" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      journal.append("person.#{@id}", [{"revision" => 2, "data" => {"bar" => 'qux'}}])

      expect do
        journal.append("person.#{@id}", [{"revision" => 2, "data" => {"baz" => 'z'}}])
      end.to raise_error(EventStore::StaleObjectException)
    end

    it "raises a EventStore::StaleObjectException if multiple clients update the same key" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      stale_revisions = journal._find_revisions("person.#{@id}")
      journal.append("person.#{@id}", [{"revision" => 2, "data" => {"baz" => 'z'}}])

      expect do
        journal._do_append("person.#{@id}", [{"revision" => 2, "data" => {"qux" => 'z'}}], stale_revisions)
      end.to raise_error(EventStore::StaleObjectException)
    end

    it "is idempotent" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])

      expect do
        journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      end.to_not raise_error
    end

    it "is idempotent with multiple clients" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])

      stale_revisions = []
      expect do
        journal._do_append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}], stale_revisions)
      end.to_not raise_error

      person = journal.get("person.#{@id}")
      person.revision.should == 1
      person.snapshot.should == {"foo" => 'a'}
      person.events.should == [{"revision" => 1, "data" => {"foo" => 'a'}}]
    end
  end

  describe "compact" do
    it "compacts the events for a given key" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      journal.append("person.#{@id}", [{"revision" => 2, "data" => {"bar" => 'qux'}}])

      person = journal.get("person.#{@id}")
      person.revision.should == 2
      person.snapshot.should == {"foo" => 'a', "bar" => 'qux'}
      person.events.should == [
        {"revision" => 1, "data" => {"foo" => 'a'}},
        {"revision" => 2, "data" => {"bar" => 'qux'}},
      ]

      journal.compact("person.#{@id}")

      journal._find_revisions("person.#{@id}").size.should == 1

      person = journal.get("person.#{@id}")
      person.revision.should == 2
      person.snapshot.should == {"foo" => 'a', "bar" => 'qux'}
      person.events.should == [
        {"revision" => 1, "data" => {"foo" => 'a'}},
        {"revision" => 2, "data" => {"bar" => 'qux'}},
      ]
    end

    it "works when appending events to a compacted key" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      journal.append("person.#{@id}", [{"revision" => 2, "data" => {"bar" => 'qux'}}])
      journal.compact("person.#{@id}")
      journal.append("person.#{@id}", [{"revision" => 3, "data" => {"baz" => 9}}])

      journal._find_revisions("person.#{@id}").size.should == 2

      person = journal.get("person.#{@id}")
      person.revision.should == 3
      person.snapshot.should == {"foo" => 'a', "bar" => 'qux', "baz" => 9}
      person.events.should == [
        {"revision" => 1, "data" => {"foo" => 'a'}},
        {"revision" => 2, "data" => {"bar" => 'qux'}},
        {"revision" => 3, "data" => {"baz" => 9}}
      ]
    end

    it "can continue to compact" do
      journal = EventStore::Journal.new
      journal.append("person.#{@id}", [{"revision" => 1, "data" => {"foo" => 'a'}}])
      journal.append("person.#{@id}", [{"revision" => 2, "data" => {"bar" => 'qux'}}])
      journal.compact("person.#{@id}")
      journal.append("person.#{@id}", [{"revision" => 3, "data" => {"baz" => 9}}])
      journal.compact("person.#{@id}")

      journal._find_revisions("person.#{@id}").size.should == 1

      person = journal.get("person.#{@id}")
      person.revision.should == 3
      person.snapshot.should == {"foo" => 'a', "bar" => 'qux', "baz" => 9}
      person.events.should == [
        {"revision" => 1, "data" => {"foo" => 'a'}},
        {"revision" => 2, "data" => {"bar" => 'qux'}},
        {"revision" => 3, "data" => {"baz" => 9}}
      ]
    end
  end
end
