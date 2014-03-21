begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec)
  task :default => :spec
rescue LoadError
  puts "unable to load rspec"
end

namespace :db do
  task :do_over do
    system "script/migrate"
  end
end
