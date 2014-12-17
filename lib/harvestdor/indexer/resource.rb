require 'active_support/benchmarkable'

module Harvestdor
  class Indexer::Resource
    include ActiveSupport::Benchmarkable

    attr_reader :indexer, :druid, :options
  
    def initialize indexer, druid, options = {}
      @indexer = indexer
      @druid = druid
      @options = options
    end

    def bare_druid
      @bare_druid ||= druid.gsub("druid:", "")
    end

    ##
    # The harvestdor client used for retrieving resources
    def harvestdor_client
      indexer.harvestdor_client
    end

    def dor_fetcher_client
      indexer.dor_fetcher_client
    end

    ##
    # Get the logger
    def logger
      options[:logger] || (indexer.logger if indexer.respond_to? :logger) || Logger.new(STDERR)
    end

    ##
    # Is this resource a collection?
    def collection?
      identity_metadata.xpath("/identityMetadata/objectType").any? { |x| x.text == "collection" }
    end

    # get the druids from isMemberOfCollection relationships in rels-ext from public_xml
    # @return [Array<String>] the druids (e.g. ww123yy1234) this object has isMemberOfColletion relationship with, or nil if none
    def collections
      @collections ||= begin
        ns_hash = {'rdf' => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 'fedora' => "info:fedora/fedora-system:def/relations-external#", '' => ''}
        is_member_of_nodes ||= public_xml.xpath('/publicObject/rdf:RDF/rdf:Description/fedora:isMemberOfCollection/@rdf:resource', ns_hash)

        is_member_of_nodes.reject { |n| n.value.empty? }.map do |n| 
          Harvestdor::Indexer::Resource.new(indexer, n.value.gsub("info:fedora/", ""))
        end
      end
    end

    ##
    # Return the items in this collection
    def items
      @items ||= begin
        druids = dor_fetcher_client.druid_array(dor_fetcher_client.get_collection(bare_druid, {}))
        druids.map { |x| Harvestdor::Indexer::Resource.new(indexer, x) }
      end
    end
    
    # given a druid, get its objectLabel from its purl page identityMetadata
    # @param [String] druid, e.g. ab123cd4567
    # @return [String] the value of the <objectLabel> element in the identityMetadata for the object
    def identity_md_obj_label
      logger.error("#{druid} missing identityMetadata") unless identity_metadata
      identity_metadata.xpath('identityMetadata/objectLabel').text
    end
    
    
    # return the MODS for the druid as a Stanford::Mods::Record object
    # @param [String] druid e.g. ab123cd4567
    # @return [Stanford::Mods::Record] created from the MODS xml for the druid
    def smods_rec
      @smods_rec ||= benchmark "smods_rec(#{druid})", level: :debug do
        ng_doc = mods
        raise "Empty MODS metadata for #{druid}: #{ng_doc.to_xml}" if ng_doc.root.xpath('//text()').empty?
        mods_rec = Stanford::Mods::Record.new
        mods_rec.from_nk_node(ng_doc.root)
        mods_rec
      end
    end

    def mods
      @mods ||= harvestdor_client.mods bare_druid
    end
    
    # the public xml for this DOR object, from the purl page
    # @param [String] druid e.g. ab123cd4567
    # @return [Nokogiri::XML::Document] the public xml for the DOR object
    def public_xml
      @public_xml ||= benchmark "public_xml(#{druid})", level: :debug do
        ng_doc = harvestdor_client.public_xml bare_druid
        raise "No public xml for #{druid}" if !ng_doc
        raise "Empty public xml for #{druid}: #{ng_doc.to_xml}" if ng_doc.root.xpath('//text()').empty?
        ng_doc
      end
    end

    ##
    # Has the public_xml been previously retrieved?
    def public_xml?
      !!@public_xml
    end

    ##
    # Get the public_xml, if retrieved, or the druid. This is used to short-circuit
    # retrieving metadata out of the public xml.
    def public_xml_or_druid
      if public_xml?
        public_xml
      else
        bare_druid
      end
    end
    
    # the contentMetadata for this DOR object, ultimately from the purl public xml
    # @param [Object] object a String containing a druid (e.g. ab123cd4567), or 
    #  a Nokogiri::XML::Document containing the public_xml for an object
    # @return [Nokogiri::XML::Document] the contentMetadata for the DOR object
    def content_metadata
      ng_doc = benchmark "content_metadata (#{druid})", level: :debug do
        harvestdor_client.content_metadata public_xml_or_druid
      end
      raise "No contentMetadata for \"#{druid}\"" if !ng_doc || ng_doc.children.empty?
      ng_doc
    end
    
    # the identityMetadata for this DOR object, ultimately from the purl public xml
    # @param [Object] object a String containing a druid (e.g. ab123cd4567), or 
    #  a Nokogiri::XML::Document containing the public_xml for an object
    # @return [Nokogiri::XML::Document] the identityMetadata for the DOR object
    def identity_metadata
      ng_doc = benchmark "identity_metadata (#{druid})", level: :debug do
        harvestdor_client.identity_metadata public_xml_or_druid
      end
      raise "No identityMetadata for \"#{druid}\"" if !ng_doc || ng_doc.children.empty?
      ng_doc
    end
    
    # the rightsMetadata for this DOR object, ultimately from the purl public xml
    # @param [Object] object a String containing a druid (e.g. ab123cd4567), or 
    #  a Nokogiri::XML::Document containing the public_xml for an object
    # @return [Nokogiri::XML::Document] the rightsMetadata for the DOR object
    def rights_metadata
      ng_doc = benchmark "rights_metadata (#{druid})", level: :debug do
        harvestdor_client.rights_metadata public_xml_or_druid
      end
      raise "No rightsMetadata for \"#{druid}\"" if !ng_doc || ng_doc.children.empty?
      ng_doc
    end
    
    # the RDF for this DOR object, ultimately from the purl public xml
    # @param [Object] object a String containing a druid (e.g. ab123cd4567), or 
    #  a Nokogiri::XML::Document containing the public_xml for an object
    # @return [Nokogiri::XML::Document] the RDF for the DOR object
    def rdf
      ng_doc = benchmark "rdf (#{druid})", level: :debug do
        harvestdor_client.rdf public_xml_or_druid
      end
      raise "No RDF for \"#{druid}\"" if !ng_doc || ng_doc.children.empty?
      ng_doc
    end

    def eql?(other)
      other.is_a? Harvestdor::Indexer::Resource and other.indexer == indexer and other.druid == druid
    end

    def hash
      druid.hash ^ indexer.hash
    end
  end
end