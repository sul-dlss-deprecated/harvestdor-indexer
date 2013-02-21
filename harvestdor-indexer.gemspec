# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'harvestdor-indexer/version'

Gem::Specification.new do |gem|
  gem.name          = "harvestdor-indexer"
  gem.version       = Harvestdor::Indexer::VERSION
  gem.authors       = ["Naomi Dushay"]
  gem.email         = ["ndushay@stanford.edu"]
  gem.description   = %q{Harvest DOR object metadata via a relationship (e.g. hydra:isGovernedBy rdf:resource="info:fedora/druid:hy787xj5878") and dates, plus code framework to write Solr docs to index}
  gem.summary       = %q{Harvest DOR object metadata and index it to Solr}
  gem.homepage      = "https://consul.stanford.edu/display/chimera/Chimera+project"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^spec/})
  gem.require_paths = ["lib"]
  
  gem.add_dependency 'harvestdor'
  
  # Runtime dependencies
  # gem.add_runtime_dependency 'nokogiri'

  # Development dependencies
  # Bundler will install these gems too if you've checked out solrmarc-wrapper source from git and run 'bundle install'
  # It will not add these as dependencies if you require solrmarc-wrapper for other projects
  gem.add_development_dependency "lyberteam-gems-devel", ">= 1.0"
  gem.add_development_dependency "rake"
  # docs
  gem.add_development_dependency "rdoc"
  gem.add_development_dependency "yard"
  # tests
	gem.add_development_dependency 'rspec'
	gem.add_development_dependency 'simplecov'
	gem.add_development_dependency 'simplecov-rcov'
	# gem.add_development_dependency 'ruby-debug19'
  
end
