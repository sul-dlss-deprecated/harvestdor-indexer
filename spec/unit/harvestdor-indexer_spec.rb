require 'spec_helper'

describe Harvestdor::Indexer do
  
  before(:all) do
    @config_yml_path = File.join(File.dirname(__FILE__), "..", "config", "ap.yml")
    @indexer = Harvestdor::Indexer.new(@config_yml_path)
    require 'yaml'
    @yaml = YAML.load_file(@config_yml_path)
    @hdor_client = @indexer.send(:harvestdor_client)
    @fake_druid = 'oo000oo0000'
    @blacklist_path = File.join(File.dirname(__FILE__), "../config/ap_blacklist.txt")
    @whitelist_path = File.join(File.dirname(__FILE__), "../config/ap_whitelist.txt")
  end
  
  describe "logging" do
    it "should write the log file to the directory indicated by log_dir" do
      @indexer.logger.info("indexer_spec logging test message")
      File.exists?(File.join(@yaml['log_dir'], @yaml['log_name'])).should == true
    end
  end

  it "should initialize the harvestdor_client from the config" do
    @hdor_client.should be_an_instance_of(Harvestdor::Client)
    @hdor_client.config.default_set.should == @yaml['default_set']
  end
  
  context "harvest_and_index" do
    before(:all) do
      @doc_hash = {
        :id => @fake_druid
      }
    end
    it "should call druids_via_oai and then call :add on rsolr connection" do
      @hdor_client.should_receive(:druids_via_oai).and_return([@fake_druid])
      @indexer.solr_client.should_receive(:add).with(@doc_hash)
      @indexer.solr_client.should_receive(:commit)
      @indexer.harvest_and_index
    end
    it "should not process druids in blacklist" do
      indexer = Harvestdor::Indexer.new(@config_yml_path, {:blacklist => @blacklist_path})
      hdor_client = indexer.send(:harvestdor_client)
      hdor_client.should_receive(:druids_via_oai).and_return(['oo000oo0000', 'oo111oo1111', 'oo222oo2222', 'oo333oo3333'])
      indexer.solr_client.should_receive(:add).with(hash_including({:id => 'oo000oo0000'}))
      indexer.solr_client.should_not_receive(:add).with(hash_including({:id => 'oo111oo1111'}))
      indexer.solr_client.should_not_receive(:add).with(hash_including({:id => 'oo222oo2222'}))
      indexer.solr_client.should_receive(:add).with(hash_including({:id => 'oo333oo3333'}))
      indexer.solr_client.should_receive(:commit)
      indexer.harvest_and_index
    end
    it "should only process druids in whitelist if it exists" do
      indexer = Harvestdor::Indexer.new(@config_yml_path, {:whitelist => @whitelist_path})
      hdor_client = indexer.send(:harvestdor_client)
      hdor_client.should_not_receive(:druids_via_oai)
      indexer.solr_client.should_receive(:add).with(hash_including({:id => 'oo000oo0000'}))
      indexer.solr_client.should_receive(:add).with(hash_including({:id => 'oo222oo2222'}))
      indexer.solr_client.should_receive(:commit)
      indexer.harvest_and_index
    end
    it "should not process druids if it is in both blacklist and whitelist" do
      indexer = Harvestdor::Indexer.new(@config_yml_path, {:blacklist => @blacklist_path, :whitelist => @whitelist_path})
      hdor_client = indexer.send(:harvestdor_client)
      hdor_client.should_not_receive(:druids_via_oai)
      indexer.solr_client.should_receive(:add).with(hash_including({:id => 'oo000oo0000'}))
      indexer.solr_client.should_receive(:commit)
      indexer.harvest_and_index
    end
    it "should only call :commit on rsolr connection once" do
      indexer = Harvestdor::Indexer.new(@config_yml_path)
      hdor_client = indexer.send(:harvestdor_client)
      hdor_client.should_receive(:druids_via_oai).and_return(['1', '2', '3'])
      indexer.solr_client.should_receive(:add).exactly(3).times
      indexer.solr_client.should_receive(:commit).once
      indexer.harvest_and_index
    end
  end
  
  it "druids method should call druids_via_oai method on harvestdor_client" do
    @hdor_client.should_receive(:druids_via_oai)
    @indexer.druids
  end
  
  context "smods_rec method" do
    before(:all) do
      @fake_druid = 'oo000oo0000'
      @ns_decl = "xmlns='#{Mods::MODS_NS}'"
      @mods_xml = "<mods #{@ns_decl}><note>hi</note></mods>"
      @ng_mods_xml = Nokogiri::XML(@mods_xml)      
    end
    it "should return Stanford::Mods::Record object" do
      @hdor_client.should_receive(:mods).with(@fake_druid).and_return(@ng_mods_xml)
      @indexer.smods_rec(@fake_druid).should be_an_instance_of(Stanford::Mods::Record)
    end
    it "should raise exception if MODS xml for the druid is empty" do
      @hdor_client.stub(:mods).with(@fake_druid).and_return(Nokogiri::XML("<mods #{@ns_decl}/>"))
      expect { @indexer.smods_rec(@fake_druid) }.to raise_error(RuntimeError, Regexp.new("^Empty MODS metadata for #{@fake_druid}: <"))
    end
    it "should raise exception if there is no MODS xml for the druid" do
      expect { @indexer.smods_rec(@fake_druid) }.to raise_error(Harvestdor::Errors::MissingMods)
    end
  end
  
  context "blacklist" do
    it "should be an Array with an entry for each non-empty line in the file" do
      @indexer.send(:load_blacklist, @blacklist_path)
      @indexer.send(:blacklist).should be_an_instance_of(Array)
      @indexer.send(:blacklist).size.should == 2
    end
    it "should be empty Array if there was no blacklist config setting" do
      indexer = Harvestdor::Indexer.new(@config_yml_path)
      indexer.send(:blacklist).should == []
    end
    context "load_blacklist" do
      it "should not be called if there was no blacklist config setting" do
        indexer = Harvestdor::Indexer.new(@config_yml_path)

        indexer.should_not_receive(:load_blacklist)

        hdor_client = indexer.send(:harvestdor_client)
        hdor_client.should_receive(:druids_via_oai).and_return([@fake_druid])
        indexer.solr_client.should_receive(:add)
        indexer.solr_client.should_receive(:commit)
        indexer.harvest_and_index
      end
      it "should only try to load a blacklist once" do
        indexer = Harvestdor::Indexer.new(@config_yml_path, {:blacklist => @blacklist_path})
        indexer.send(:blacklist)
        File.any_instance.should_not_receive(:open)
        indexer.send(:blacklist)
      end
      it "should log an error message and throw RuntimeError if it can't find the indicated blacklist file" do
        exp_msg = 'Unable to find list of druids at bad_path'
        indexer = Harvestdor::Indexer.new(@config_yml_path, {:blacklist => 'bad_path'})
        indexer.logger.should_receive(:fatal).with(exp_msg)
        expect { indexer.send(:load_blacklist, 'bad_path') }.to raise_error(exp_msg)
      end   
    end
  end # blacklist
  
  context "whitelist" do
    it "should be an Array with an entry for each non-empty line in the file" do
      @indexer.send(:load_whitelist, @whitelist_path)
      @indexer.send(:whitelist).should be_an_instance_of(Array)
      @indexer.send(:whitelist).size.should == 2
    end
    it "should be empty Array if there was no whitelist config setting" do
      indexer = Harvestdor::Indexer.new(@config_yml_path)
      indexer.send(:whitelist).should == []
    end
    context "load_whitelist" do
      it "should not be called if there was no whitelist config setting" do
        indexer = Harvestdor::Indexer.new(@config_yml_path)

        indexer.should_not_receive(:load_whitelist)

        hdor_client = indexer.send(:harvestdor_client)
        hdor_client.should_receive(:druids_via_oai).and_return([@fake_druid])
        indexer.solr_client.should_receive(:add)
        indexer.solr_client.should_receive(:commit)
        indexer.harvest_and_index
      end
      it "should only try to load a whitelist once" do
        indexer = Harvestdor::Indexer.new(@config_yml_path, {:whitelist => @whitelist_path})
        indexer.send(:whitelist)
        File.any_instance.should_not_receive(:open)
        indexer.send(:whitelist)
      end
      it "should log an error message and throw RuntimeError if it can't find the indicated whitelist file" do
        exp_msg = 'Unable to find list of druids at bad_path'
        indexer = Harvestdor::Indexer.new(@config_yml_path, {:whitelist => 'bad_path'})
        indexer.logger.should_receive(:fatal).with(exp_msg)
        expect { indexer.send(:load_whitelist, 'bad_path') }.to raise_error(exp_msg)
      end   
    end
  end # whitelist
  
  it "solr_client should initialize the rsolr client using the options from the config" do
    indexer = Harvestdor::Indexer.new(nil, Confstruct::Configuration.new(:solr => { :url => 'http://localhost:2345', :a => 1 }) )
    RSolr.should_receive(:connect).with(hash_including(:a => 1, :url => 'http://localhost:2345')).and_return('foo')
    indexer.solr_client
  end
    
end