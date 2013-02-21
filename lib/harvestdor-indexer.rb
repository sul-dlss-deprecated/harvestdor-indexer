# external gems
require 'confstruct'
require 'rsolr'

# sul-dlss gems
require 'harvestdor'
require 'stanford-mods'

# stdlib
require 'logger'

require "harvestdor-indexer/version"

module Harvestdor
  # Base class to harvest from DOR via harvestdor gem and then index
  class Indexer

    def initialize yml_path, options = {}
      @yml_path = yml_path
      config.configure(YAML.load_file(yml_path)) if yml_path    
      config.configure options 
      yield(config) if block_given?
    end

    def config
      @config ||= Confstruct::Configuration.new()
    end

    def logger
      @logger ||= load_logger(config.log_dir, config.log_name)
    end

    # per this Indexer's config options 
    #  harvest the druids via OAI
    #   create a Solr profiling document for each druid
    #   write the result to the Solr index
    def harvest_and_index
      if whitelist.empty?
        druids.each { |druid| index druid }
      else
        whitelist.each { |druid| index druid }
      end
      solr_client.commit
      logger.info("Finished processing: Solr commit returned.")
    end

    # return Array of druids contained in the OAI harvest indicated by OAI params in yml configuration file
    # @return [Array<String>] or enumeration over it, if block is given.  (strings are druids, e.g. ab123cd1234)
    def druids
      @druids ||= harvestdor_client.druids_via_oai
    end

    # create Solr doc for the druid and add it to Solr, unless it is on the blacklist.  
    #  NOTE: don't forget to send commit to Solr, either once at end, or for each add, or ...
    def index druid
      if blacklist.include?(druid)
        logger.info("Druid #{druid} is on the blacklist and will have no Solr doc created")
      else
        logger.error("You must override the index method to transform druids into Solr docs and add them to Solr")
        
        doc_hash = {}
        doc_hash[:id] = druid
        # doc_hash[:title_tsim] = smods_rec(druid).short_title

        # you might add things from Indexer level class here
        #  (e.g. things that are the same across all documents in the harvest)

        solr_client.add(doc_hash)

        # logger.info("Just created Solr doc for #{druid}")
        # TODO: provide call to code to update DOR object's workflow datastream??
      end
    end

    # return the MODS for the druid as a Stanford::Mods::Record object
    # @return [Stanford::Mods::Record] created from the MODS xml for the druid
    def smods_rec druid
      ng_doc = @harvestdor_client.mods druid
      raise "Empty MODS metadata for #{druid}: #{ng_doc.to_xml}" if ng_doc.root.xpath('//text()').empty?
      mods_rec = Stanford::Mods::Record.new
      mods_rec.from_nk_node(ng_doc.root)
      mods_rec
    end

    def solr_client
      @solr_client ||= RSolr.connect(config.solr.to_hash)
    end

    # @return an Array of druids ('oo000oo0000') that should NOT be processed
    def blacklist
      # avoid trying to load the file multiple times
      if !@blacklist && !@loaded_blacklist
        @blacklist = load_blacklist(config.blacklist) if config.blacklist
      end
      @blacklist ||= []
    end

    # @return an Array of druids ('oo000oo0000') that should be processed
    def whitelist
      # avoid trying to load the file multiple times
      if !@whitelist && !@loaded_whitelist
        @whitelist = load_whitelist(config.whitelist) if config.whitelist
      end
      @whitelist ||= []
    end

    protected #---------------------------------------------------------------------

    def harvestdor_client
      @harvestdor_client ||= Harvestdor::Client.new({:config_yml_path => @yml_path})
    end

    # populate @blacklist as an Array of druids ('oo000oo0000') that will NOT be processed
    #  by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    def load_blacklist path
      if path && !@loaded_blacklist
        @loaded_blacklist = true
        @blacklist = load_id_list path
      end
    end

    # populate @blacklist as an Array of druids ('oo000oo0000') that WILL be processed
    #  (unless a druid is also on the blacklist)
    #  by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    def load_whitelist path
      if path && !@loaded_whitelist
        @loaded_whitelist = true
        @whitelist = load_id_list path
      end
    end

    # return an Array of druids ('oo000oo0000')
    #   populated by reading the File at the indicated path
    # @param [String] path - path of file containing a list of druids
    # @return [Array<String>] an Array of druids
    def load_id_list path
      if path 
        list = []
        f = File.open(path).each_line { |line|
          list << line.gsub(/\s+/, '') if !line.gsub(/\s+/, '').empty? && !line.strip.start_with?('#')
        }
        list
      end
    rescue
      msg = "Unable to find list of druids at " + path
      logger.fatal msg
      raise msg
    end
    
    # Global, memoized, lazy initialized instance of a logger
    # @param [String] log_dir directory for to get log file
    # @param [String] log_name name of log file
    def load_logger(log_dir, log_name)
      Dir.mkdir(log_dir) unless File.directory?(log_dir) 
      @logger ||= Logger.new(File.join(log_dir, log_name), 'daily')
    end

  end # Indexer class
end # Harvestdor module
