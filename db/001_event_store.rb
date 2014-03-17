Sequel.migration do
  up do
    create_table :events do
      String :key, text: true, null: false
      Fixnum :revision, default: 1, null: false
      String :riak_key, :text => true, null: false
      DateTime :created_at, null: false
      primary_key [:key, :revision], name: :revisions_pk
    end
  end

  down do
    drop_table :revisions
  end
end
