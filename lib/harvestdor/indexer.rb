
# external gems
require 'confstruct'
require 'rsolr'
require 'retries'
require 'json'

# sul-dlss gems
require 'harvestdor'
require 'stanford-mods'
require 'dor-fetcher'

# stdlib
require 'logger'

require "harvestdor/indexer/metrics"
require "harvestdor/indexer/resource"
require "harvestdor/indexer/version"

require 'active_support/benchmarkable'
module Harvestdor
  # Base class to harvest from DOR via harvestdor gem and then index
  class Indexer
    include ActiveSupport::Benchmarkable

    attr_accessor :max_retries, :metrics, :logger
    attr_accessor :dor_fetcher_client

    def self.dor_fetcher_client yaml_or_hash

      data = case yaml_or_hash
        when Hash
          yaml_or_hash
        else
          YAML.load_file(yaml_or_hash)
      end

      config = Confstruct::Configuration.new data

      # Adding skip_heartbeat param for easier testing
      DorFetcher::Client.new(config.dor_fetcher)
    end
    
    def initialize dor_fetcher_client, options = {}
      @metrics = Harvestdor::Indexer::Metrics.new
      @max_retries=10      # the number of times to retry an object 
      config.configure(options)
      yield(config) if block_given?
      @dor_fetcher_client=dor_fetcher_client
    end

    def config
      @config ||= Confstruct::Configuration.new
    end
    
    def logger
      @logger ||= begin
        Dir.mkdir(config.log_dir) unless File.directory?(config.log_dir)
        Logger.new(File.join(config.log_dir, config.log_name), 'daily')
      end
    end
    
    # per this Indexer's config options 
    #  harvest the druids via DorFetcher
    #   create a Solr profiling document for each druid
    #   write the result to the Solr index
    def harvest_and_index
      benchmark "Harvest and Indexing" do
        druids.map { |x| Harvestdor::Indexer::Resource.new(self, x) }.each { |druid| index druid }
        solr_client.commit
      end
      total_objects=metrics.success_count+metrics.error_count
      logger.info("Successful count: #{metrics.success_count}")
      logger.info("Error count: #{metrics.error_count}")
      logger.info("Total records processed: #{total_objects}")
    end
    
    # return Array of druids contained in the DorFetcher pulling indicated by DorFetcher params
    # @return [Array<String>] or enumeration over it, if block is given.  (strings are druids, e.g. ab123cd1234)
    def druids
      @druids ||= if whitelist?
        whitelist
      else
        benchmark " DorFetcher pulling of druids" do
          Harvestdor::Indexer::Resource.new(self, strip_default_set_string).items
        end
      end
    end
    
    # Add the document to solr, retry if an error occurs.
    # See https://github.com/ooyala/retries for docs on with_retries.
    # @param [Hash] doc a Hash representation of the solr document
    # @param [String] id the id of the document being sent, for logging
    def solr_add(doc, id)
      max_tries=@max_retries ? @max_retries : 10 #if @max_retries isn't set, use 10
      
      handler = Proc.new do |exception, attempt_number, total_delay|
        logger.debug "#{exception.class} on attempt #{attempt_number} for #{id}"
        # logger.debug exception.backtrace
      end
      
      with_retries(:max_tries => max_tries, :handler => handler, :base_sleep_seconds => 1, :max_sleep_seconds => 5) do |attempt|
        logger.debug "Attempt #{attempt} for #{id}"
        solr_client.add(doc)
        logger.info "Successfully indexed #{id} on attempt #{attempt}"
      end
    end
    
    # create Solr doc for the druid and add it to Solr
    #  NOTE: don't forget to send commit to Solr, either once at end (already in harvest_and_index), or for each add, or ...
    def index resource
      logger.fatal("You must override the index method to transform druids into Solr docs and add them to Solr")

      benchmark "Indexing #{resource.druid}" do
        logger.debug "About to index #{resource.druid}"
        doc_hash = {}
        doc_hash[:id] = resource.druid

        # you might add things from Indexer level class here
        #  (e.g. things that are the same across all documents in the harvest)

        begin
          solr_client.add(doc_hash)
          metrics.success!

          # TODO: provide call to code to update DOR object's workflow datastream??
        rescue => e
          metrics.error!
          logger.error "Failed to index #{resource.druid}: #{e.message}"
        end
      end
    end

    def solr_client
      @solr_client ||= RSolr.connect(config.solr.to_hash)
    end

    
    # @return an Array of druids ('oo000oo0000') that should be processed
    def whitelist
      @whitelist ||= load_whitelist(config.whitelist) if config.whitelist
      @whitelist ||= []
    end

    def whitelist?
      whitelist.any?
    end
    
    # Get only the druid from the end of the default_set string
    # from the yml file
    def strip_default_set_string
      config.default_set.split('_').last
    end
    
    def harvestdor_client
      @harvestdor_client ||= Harvestdor::Client.new(config)
    end

    protected #---------------------------------------------------------------------
    
    # populate @whitelist as an Array of druids ('oo000oo0000') that WILL be processed
    #  by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    def load_whitelist path
      @whitelist = load_id_list path
    end
    
    # return an Array of druids ('oo000oo0000')
    #   populated by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    # @return [Array<String>] an Array of druids
    def load_id_list path
      list = File.open(path).each_line
              .map { |line| line.strip }
              .reject { |line| line.strip.start_with?('#') }
              .reject { |line| line.empty? }
    rescue
      msg = "Unable to find list of druids at " + path
      logger.fatal msg
      raise msg
    end
  end # Indexer class
end # Harvestdor module