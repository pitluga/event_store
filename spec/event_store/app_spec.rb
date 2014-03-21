require 'spec_helper'

describe EventStore::App do
  before(:each) do
    @id = generate_id
  end

  describe "POST /:key" do
    it "appends events to the given key" do
      body = [{"revision" => 1, "data" => { "a" => 1}}]

      post "foo.#{@id}", JSON.dump(body), "Content-Type" => "application/json"

      last_response.status.should == 201
    end
  end

  describe "GET /:key" do
    it "returns events for the given key" do
      body = [{"revision" => 1, "data" => { "a" => 1}}]

      post "foo.#{@id}", JSON.dump(body), "Content-Type" => "application/json"

      get "foo.#{@id}"

      last_response.status.should == 200
      response = JSON.parse(last_response.body)
      response["key"].should == "foo.#{@id}"
      response["snapshot"].should == { "a" => 1 }
      response["revision"].should == 1
      response["events"].should == body
    end
  end
end
