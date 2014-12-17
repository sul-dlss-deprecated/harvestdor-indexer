module Harvestdor
  ##
  # Harvest metrics tracker
  class Indexer::Metrics
    attr_accessor :error_count, :success_count, :logger

    def initialize options = {}
      @success_count=0    # the number of objects successfully indexed
      @error_count=0      # the number of objects that failed
      @logger = options[:logger] || Logger.new(STDERR)
    end

    ##
    # Wrap an operation in tally block; if the block completes without throwing
    # an exception, tally a success. If the block throws an exception, catch it 
    # and tally a failure.
    #
    # Callers can provide an :on_error handler to receive the exception and process
    # it appropriately.
    #
    # @param [Hash] options
    # @option options [#call] Callback that will receive any exception thrown by the block
    def tally options = {}, &block      
      begin
        block.call
        success!
      rescue => e
        error!
        logger.error "Failed to process: #{e.message}"
        options[:on_error].call e if options[:on_error]
      end
    end

    ##
    # Record a successful run
    def success!
      @success_count += 1
    end

    ##
    # Record an error
    def error!
      @error_count += 1
    end

    ##
    # Total number of runs
    def total
      @success_count + @error_count
    end

  end
end