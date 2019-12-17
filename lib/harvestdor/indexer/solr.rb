# frozen_string_literal: true

module Harvestdor
  class Indexer::Solr
    attr_accessor :client, :indexer, :config

    def initialize(indexer, config = {})
      @indexer = indexer
      @client = RSolr.connect(config)
      @config = Confstruct::Configuration.new config
      @config.max_retries ||= 10
    end

    def logger
      indexer.logger
    end

    def commit!
      client.commit
    end

    # Add the document to solr, retry if an error occurs.
    # See https://github.com/ooyala/retries for docs on with_retries.
    # @param [Hash] doc a Hash representation of the solr document
    # @param [String] coll_size the size of the collection
    # @param [String] index the index of this document (nth to be indexed)
    def add(doc, coll_size = '?', index = '?')
      id = doc[:id]

      handler = proc do |exception, attempt_number, _total_delay|
        logger.debug "#{exception.class} on attempt #{attempt_number} for #{id}"
        # logger.debug exception.backtrace
      end

      with_retries(max_tries: config.max_retries, handler: handler, base_sleep_seconds: 1, max_sleep_seconds: 5) do |attempt|
        logger.debug "Attempt #{attempt} for #{id}"
        client.add(doc)
        logger.info "Successfully indexed #{id} (#{index}/#{coll_size}) on attempt #{attempt}"
      end
    end
  end
end
