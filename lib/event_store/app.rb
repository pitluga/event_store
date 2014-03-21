module EventStore
  class App < Sinatra::Base

    post "/:key" do
      events = JSON.parse(request.env["rack.input"].read)
      EventStore::Journal.new.append(params[:key], events)
      status 201
    end

    get "/:key" do
      JSON.dump(EventStore::Journal.new.get(params[:key]))
    end
  end
end
