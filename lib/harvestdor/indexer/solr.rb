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
    def add(doc)
      id = doc[:id]

      handler = proc do |exception, attempt_number, _total_delay|
        logger.debug "#{exception.class} on attempt #{attempt_number} for #{id}"
        # logger.debug exception.backtrace
      end

      with_retries(max_tries: config.max_retries, handler: handler, base_sleep_seconds: 1, max_sleep_seconds: 5) do |attempt|
        logger.debug "Attempt #{attempt} for #{id}"
        client.add(doc)
        logger.info "Successfully indexed #{id} on attempt #{attempt}"
      end
    end
  end
end