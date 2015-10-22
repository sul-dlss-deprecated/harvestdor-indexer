require "bundler/gem_tasks"

require 'rake'
require 'bundler'

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

task :default => [:rspec, :rubocop]

desc "run continuous integration suite (tests, coverage, docs)"
task :ci => [:rspec, :doc, :rubocop]

require 'rspec/core/rake_task'

task :spec => :rspec

desc "run specs EXCEPT integration specs"
RSpec::Core::RakeTask.new(:spec_fast) do |spec|
  spec.rspec_opts = ["-c", "-f progress", "--tty", "-t ~integration", "-r ./spec/spec_helper.rb"]
end

RSpec::Core::RakeTask.new(:rspec) do |spec|
  spec.rspec_opts = ["-c", "-f progress", "--tty", "-r ./spec/spec_helper.rb"]
end

require 'rubocop/rake_task'
RuboCop::RakeTask.new(:rubocop)

# Use yard to build docs
require 'yard'
require 'yard/rake/yardoc_task'
begin
  project_root = File.expand_path(File.dirname(__FILE__))
  doc_dest_dir = File.join(project_root, 'doc')

  YARD::Rake::YardocTask.new(:doc) do |yt|
    yt.files = Dir.glob(File.join(project_root, 'lib', '**', '*.rb')) +
                 [ File.join(project_root, 'README.rdoc') ]
    yt.options = ['--output-dir', doc_dest_dir, '--readme', 'README.rdoc', '--title', 'Harvestdor Gem Documentation']
  end
rescue LoadError
  desc "Generate YARD Documentation"
  task :doc do
    abort "Please install the YARD gem to generate rdoc."
  end
end
