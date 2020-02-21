# frozen_string_literal: true

# external gems
require 'confstruct'
require 'rsolr'
require 'retries'
require 'parallel'
require 'json'

# sul-dlss gems
require 'harvestdor'
require 'stanford-mods'

# stdlib
require 'logger'

require 'harvestdor/indexer/version'

require 'active_support/benchmarkable'
module Harvestdor
  # Base class to harvest from DOR via harvestdor gem and then index
  class Indexer
    require 'harvestdor/indexer/metrics'
    require 'harvestdor/indexer/purl_fetcher'
    require 'harvestdor/indexer/resource'
    require 'harvestdor/indexer/solr'

    include ActiveSupport::Benchmarkable

    attr_accessor :metrics, :logger

    def initialize(options = {})
      config.configure(options)
      yield(config) if block_given?
      @metrics = Harvestdor::Indexer::Metrics.new logger: logger
    end

    def config
      @config ||= Confstruct::Configuration.new
    end

    def logger
      @logger ||= begin
        if config.harvestdor
          Dir.mkdir(config.harvestdor.log_dir) unless File.directory?(config.harvestdor.log_dir)
          Logger.new(File.join(config.harvestdor.log_dir, config.harvestdor.log_name), 'daily')
        else
          Logger.new STDERR
        end
      end
    end

    # per this Indexer's config options
    #  harvest the druids via DorFetcher
    #   create a Solr profiling document for each druid
    #   write the result to the Solr index
    def harvest_and_index(each_options = { in_threads: 4 })
      benchmark 'Harvest and Indexing' do
        each_resource(each_options) do |resource|
          index resource
        end

        solr.commit!
      end
    end

    ##
    # Collect all the explicitly identified resources (e.g. from the whitelist), and all
    # members of any collections in that list, and offer them as an Enumerator.
    #
    # Using enumerators allows us to lazy-fetch and correctly garbage collect resources after
    # a downstream consumer is finished processing them. If a consumer needs to use this resource
    # list multiple times (and is confident all the resources will fit in available memory!), they
    # could memoize the result of e.g. `#to_a` for their own use.
    #
    # @return [Enumerator] an enumerator of Harvestdor::Indexer::Resources for the druid whitelist,
    #   and all the items belonging to each collection id in druids.
    def resources
      return to_enum(:resources) unless block_given?

      druids.each do |x|
        # Include the named resource in the enumerable
        resource = Harvestdor::Indexer::Resource.new(self, x)
        yield resource

        # And also yield any members of that resources
        resource.items.each do |coll_member|
          yield coll_member
        end
      end
    end

    def each_resource(options = {}, &_block)
      benchmark '' do
        Parallel.each(resources, options) do |resource|
          metrics.tally on_error: method(:resource_error) do
            yield resource
          end
        end
      end

      logger.info("Successful count: #{metrics.success_count}")
      logger.info("Error count: #{metrics.error_count}")
      logger.info("Total records processed: #{metrics.total}")
    end

    def resource_error(e)
      raise e if e.instance_of?(Parallel::Break) || e.instance_of?(Parallel::Kill)
    end

    # return Array of druids contained in the DorFetcher pulling indicated by DorFetcher params
    # @return [Array<String>] or enumeration over it, if block is given.  (strings are druids, e.g. ab123cd1234)
    def druids
      @druids ||= whitelist
    end

    # create Solr doc for the druid and add it to Solr
    #  NOTE: don't forget to send commit to Solr, either once at end (already in harvest_and_index), or for each add, or ...
    def index(resource)
      benchmark "Indexing #{resource.druid}" do
        logger.debug "About to index #{resource.druid}"
        doc_hash = {}
        doc_hash[:id] = resource.druid

        # you might add things from Indexer level class here
        #  (e.g. things that are the same across all documents in the harvest)
        solr.add doc_hash
        # TODO: provide call to code to update DOR object's workflow datastream??
      end
    end

    # @return an Array of druids ('oo000oo0000') that should be processed
    def whitelist
      @whitelist ||= config.whitelist if config.whitelist.is_a? Array
      @whitelist ||= load_whitelist(config.whitelist) if config.whitelist
      @whitelist ||= []
    end

    def harvestdor_client
      @harvestdor_client ||= Harvestdor::Client.new(config.harvestdor)
    end

    def purl_fetcher_client
      return unless config.purl_fetcher

      @purl_fetcher_client ||= Harvestdor::Indexer::PurlFetcher.new(config.purl_fetcher)
    end

    def solr
      @solr ||= Harvestdor::Indexer::Solr.new self, config.solr.to_hash
    end

    protected #---------------------------------------------------------------------

    # populate @whitelist as an Array of druids ('oo000oo0000') that WILL be processed
    #  by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    def load_whitelist(path)
      @whitelist = load_id_list path
    end

    # return an Array of druids ('oo000oo0000')
    #   populated by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    # @return [Array<String>] an Array of druids
    def load_id_list(path)
      list = File.open(path).each_line.map(&:strip).reject { |line| line.strip.start_with?('#') }.reject(&:empty?)
    rescue StandardError
      msg = 'Unable to find list of druids at ' + path
      logger.fatal msg
      raise msg
    end
  end # Indexer class
end # Harvestdor module
