Sequel.migration do
  up do
    create_table :revisions do
      primary_key :id
      String :key, text: true, null: false
      Fixnum :start, null: false
      Fixnum :end, null: false
      String :riak_key, :text => true, null: false
      DateTime :created_at, null: false
      String :signature, null: false
      TrueClass :deleted, null: false, default: false
    end

    run('create extension btree_gist')
    run('ALTER TABLE revisions ADD EXCLUDE USING GIST (key WITH =, box(point("start", 0), point("end", 0)) WITH &&) WHERE (deleted = false)')
  end

  down do
    drop_table :revisions
  end
end
