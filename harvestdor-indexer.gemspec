# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'harvestdor/indexer/version'

Gem::Specification.new do |gem|
  gem.name          = "harvestdor-indexer"
  gem.version       = Harvestdor::Indexer::VERSION
  gem.authors       = ["Naomi Dushay", "Bess Sadler", "Laney McGlohon"]
  gem.email         = ["ndushay@stanford.edu", "bess@stanford.edu", "laneymcg@stanford.edu"]
  gem.description   = %q{Harvest DOR object metadata by the item or collection, plus code framework to write Solr docs to index}
  gem.summary       = %q{Harvest DOR object metadata and index it to Solr}
  gem.homepage      = "https://github.com/sul-dlss/harvestdor-indexer"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^spec/})
  gem.require_paths = ["lib"]

  gem.add_dependency 'rsolr'
  gem.add_dependency 'retries'
  gem.add_dependency 'harvestdor', '>=0.0.14'
  gem.add_dependency 'stanford-mods'
  gem.add_dependency 'dor-fetcher', '=1.0.5'
  gem.add_dependency "activesupport"
  gem.add_dependency "parallel"

  # Runtime dependencies
  gem.add_runtime_dependency 'confstruct'

  # Development dependencies
  gem.add_development_dependency "rake"
  # docs
  gem.add_development_dependency "rdoc"
  gem.add_development_dependency "yard"
  # tests
	gem.add_development_dependency 'rspec', "~> 3.0"
	gem.add_development_dependency 'coveralls'
  gem.add_development_dependency 'rubocop'
  gem.add_development_dependency 'rubocop-rspec'
  gem.add_development_dependency 'vcr'
  gem.add_development_dependency 'webmock'

end
