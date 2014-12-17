require 'spec_helper'

describe Harvestdor::Indexer do
  
  before(:all) do
    VCR.use_cassette('before_all_call') do
      @config_yml_path = File.join(File.dirname(__FILE__), "..", "config", "ap.yml")
      @client_config_path = File.join(File.dirname(__FILE__), "../..", "config", "dor-fetcher-client.yml")
      @indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path)
      require 'yaml'
      @yaml = YAML.load_file(@config_yml_path)
      @hdor_client = @indexer.send(:harvestdor_client)
      @fake_druid = 'oo000oo0000'
      @blacklist_path = File.join(File.dirname(__FILE__), "../config/ap_blacklist.txt")
      @whitelist_path = File.join(File.dirname(__FILE__), "../config/ap_whitelist.txt")
    end
  end
  
  # The method that sends the solr document to solr
  describe "#solr_add" do
    before(:each) do
      doc_hash = {
        :modsxml => 'whatever',
        :title_display => 'title',
        :pub_year_tisim => 'some year',
        :author_person_display => 'author',
        :format => 'Image',
        :language => 'English'
      }
    end
    it "sends an add request to the solr_client" do
      expect(@indexer.solr_client).to receive(:add)
      @indexer.solr_add(@doc_hash, "abc123")
    end
  end
  
  describe "access methods" do
    it "initializes success count" do
      @indexer.metrics.success_count.should == 0
    end
    it "initializes error count" do
      @indexer.metrics.error_count.should == 0
    end
    it "initializes max_retries" do
      expect(@indexer.max_retries).to eql(10)
    end
    it "allows overriding of max_retries" do
      @indexer.max_retries=6
      @indexer.max_retries.should == 6
    end
  end
  
  describe "logging" do
    it "should write the log file to the directory indicated by log_dir" do
      @indexer.logger.info("indexer_spec logging test message")
      File.exists?(File.join(@yaml['log_dir'], @yaml['log_name'])).should == true
    end
  end
  
  it "should initialize the harvestdor_client from the config" do
    expect(@hdor_client).to be_an_instance_of(Harvestdor::Client)
    expect(@hdor_client.config.default_set).to eq(@yaml['default_set'])
  end
  
  context "harvest_and_index" do
    before(:all) do
      @doc_hash = {
        :id => @fake_druid
      }
    end
    it "should call dor_fetcher_client.druid_array and then call :add on rsolr connection" do
      @indexer.should_receive(:druids).and_return([@fake_druid])
      @indexer.solr_client.should_receive(:add).with(@doc_hash)
      @indexer.solr_client.should_receive(:commit)
      @indexer.harvest_and_index
    end

    it "should only call :commit on rsolr connection once" do
      VCR.use_cassette('single_rsolr_connection_call') do
        indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path)
        hdor_client = indexer.send(:harvestdor_client)
        indexer.dor_fetcher_client.should_receive(:druid_array).and_return(["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092", "druid:tc552kq0798", "druid:th998nk0722", "druid:ww689vs6534"])
        indexer.solr_client.should_receive(:add).exactly(6).times
        indexer.solr_client.should_receive(:commit).once
        indexer.harvest_and_index
      end
    end

    it "should not process druids in blacklist" do
      VCR.use_cassette('ignore_druids_in_blacklist_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:blacklist => @blacklist_path})
          hdor_client = indexer.send(:harvestdor_client)
          indexer.dor_fetcher_client.should_receive(:druid_array).and_return(["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092", "druid:tc552kq0798", "druid:th998nk0722", "druid:ww689vs6534"])
          indexer.solr_client.should_receive(:add).with(hash_including({:id => 'druid:nz353cp1092'}))
          indexer.solr_client.should_not_receive(:add).with(hash_including({:id => 'druid:jf275fd6276'}))
          indexer.solr_client.should_not_receive(:add).with(hash_including({:id => 'druid:tc552kq0798'}))
          indexer.solr_client.should_receive(:add).with(hash_including({:id => 'druid:th998nk0722'}))
          indexer.solr_client.should_receive(:commit)
          indexer.harvest_and_index
        }
      end
    end
    it "should not process druid if it is in both blacklist and whitelist" do
      VCR.use_cassette('ignore_druids_in_blacklist_and_whitelist_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:blacklist => @blacklist_path, :whitelist => @whitelist_path})
          hdor_client = indexer.send(:harvestdor_client)
          indexer.dor_fetcher_client.should_not_receive(:druid_array)
          indexer.solr_client.should_receive(:add).with(hash_including({:id => 'druid:yg867hg1375'}))
          indexer.solr_client.should_not_receive(:add).with(hash_including({:id => 'druid:jf275fd6276'}))
          indexer.solr_client.should_receive(:commit)
          indexer.harvest_and_index
        }
      end
    end
    it "should only process druids in whitelist if it exists" do
      VCR.use_cassette('process_druids_whitelist_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:whitelist => @whitelist_path})
          hdor_client = indexer.send(:harvestdor_client)
          indexer.dor_fetcher_client.should_not_receive(:druid_array)
          indexer.solr_client.should_receive(:add).with(hash_including({:id => 'druid:yg867hg1375'}))
          indexer.solr_client.should_receive(:add).with(hash_including({:id => 'druid:jf275fd6276'}))
          indexer.solr_client.should_receive(:add).with(hash_including({:id => 'druid:nz353cp1092'}))
          indexer.solr_client.should_receive(:commit)
          indexer.harvest_and_index
        }
      end
    end

  end
  
  # Check for replacement of oai harvesting with dor-fetcher
  context "replacing OAI harvesting with dor-fetcher" do
      it "has a dor-fetcher client" do
        expect(@indexer.dor_fetcher_client).to be_an_instance_of(DorFetcher::Client)
      end 

      it "should strip off is_member_of_collection_ and is_governed_by_ and return only the druid" do
        expect(@indexer.strip_default_set_string()).to eq("yg867hg1375")
      end

      it "druids method should call druid_array and get_collection methods on fetcher_client" do
        VCR.use_cassette('get_collection_druids_call') do
          expect(@indexer.druids).to eq(["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092", "druid:tc552kq0798", "druid:th998nk0722", "druid:ww689vs6534"])
        end
      end

      it "should get the configuration of the dor-fetcher client from included yml file" do
        expect(@indexer.dor_fetcher_client.service_url).to eq(@indexer.client_config["dor_fetcher_service_url"])
      end

  end # ending replacing OAI context

  context "smods_rec method" do
    before(:all) do
      @fake_druid = 'oo000oo0000'
      @ns_decl = "xmlns='#{Mods::MODS_NS}'"
      @mods_xml = "<mods #{@ns_decl}><note>hi</note></mods>"
      @ng_mods_xml = Nokogiri::XML(@mods_xml)      
    end
    it "should call mods method on harvestdor_client" do
      @hdor_client.should_receive(:mods).with(@fake_druid).and_return(@ng_mods_xml)
      @indexer.smods_rec(@fake_druid)
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
      VCR.use_cassette('exception_no_MODS_call') do
        expect { @indexer.smods_rec(@fake_druid) }.to raise_error(Harvestdor::Errors::MissingMods)
      end
    end
  end
  
  context "public_xml related methods" do
    before(:all) do
      @id_md_xml = "<identityMetadata><objectId>druid:#{@fake_druid}</objectId></identityMetadata>"
      @cntnt_md_xml = "<contentMetadata type='image' objectId='#{@fake_druid}'>foo</contentMetadata>"
      @rights_md_xml = "<rightsMetadata><access type=\"discover\"><machine><world>bar</world></machine></access></rightsMetadata>"
      @rdf_xml = "<rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'><rdf:Description rdf:about=\"info:fedora/druid:#{@fake_druid}\">relationship!</rdf:Description></rdf:RDF>"
      @pub_xml = "<publicObject id='druid:#{@fake_druid}'>#{@id_md_xml}#{@cntnt_md_xml}#{@rights_md_xml}#{@rdf_xml}</publicObject>"
      @ng_pub_xml = Nokogiri::XML(@pub_xml)
    end
    context "#public_xml" do
      it "should call public_xml method on harvestdor_client" do
        @hdor_client.should_receive(:public_xml).with(@fake_druid).and_return(@ng_pub_xml)
        @indexer.public_xml @fake_druid
      end
      it "retrieves entire public xml as a Nokogiri::XML::Document" do
        @hdor_client.should_receive(:public_xml).with(@fake_druid).and_return(@ng_pub_xml)
        px = @indexer.public_xml @fake_druid
        px.should be_kind_of(Nokogiri::XML::Document)
        px.root.name.should == 'publicObject'
        px.root.attributes['id'].text.should == "druid:#{@fake_druid}"
      end
      it "raises exception if public xml for the druid is empty" do
        @hdor_client.should_receive(:public_xml).with(@fake_druid).and_return(Nokogiri::XML("<publicObject/>"))
        expect { @indexer.public_xml(@fake_druid) }.to raise_error(RuntimeError, Regexp.new("^Empty public xml for #{@fake_druid}: <"))
      end
      it "raises error if there is no public_xml page for the druid" do
        @hdor_client.should_receive(:public_xml).with(@fake_druid).and_return(nil)
        expect { @indexer.public_xml(@fake_druid) }.to raise_error(RuntimeError, "No public xml for #{@fake_druid}")
      end
    end
    context "#content_metadata" do
      it "returns a Nokogiri::XML::Document derived from the public xml if a druid is passed" do
        Harvestdor.stub(:public_xml).with(@fake_druid, @indexer.config.purl).and_return(@ng_pub_xml)
        cm = @indexer.content_metadata(@fake_druid)
        cm.should be_kind_of(Nokogiri::XML::Document)
        cm.root.should_not == nil
        cm.root.name.should == 'contentMetadata'
        cm.root.attributes['objectId'].text.should == @fake_druid
        cm.root.text.strip.should == 'foo'
      end
      it "if passed a Nokogiri::XML::Document of the public xml, it does no fetch" do
        URI::HTTP.any_instance.should_not_receive(:open)
        @hdor_client.should_receive(:content_metadata).and_call_original
        cm = @indexer.content_metadata(@ng_pub_xml)
        cm.should be_kind_of(Nokogiri::XML::Document)
        cm.root.should_not == nil
        cm.root.name.should == 'contentMetadata'
        cm.root.attributes['objectId'].text.should == @fake_druid
        cm.root.text.strip.should == 'foo'
      end
      it "raises RuntimeError if nil is returned by Harvestdor::Client.contentMetadata for the druid" do
        @hdor_client.should_receive(:content_metadata).with(@fake_druid).and_return(nil)
        expect { @indexer.content_metadata(@fake_druid) }.to raise_error(RuntimeError, "No contentMetadata for \"#{@fake_druid}\"")
      end
    end
    context "#identity_metadata" do
      it "returns a Nokogiri::XML::Document derived from the public xml if a druid is passed" do
        Harvestdor.stub(:public_xml).with(@fake_druid, @indexer.config.purl).and_return(@ng_pub_xml)
        im = @indexer.identity_metadata(@fake_druid)
        im.should be_kind_of(Nokogiri::XML::Document)
        im.root.should_not == nil
        im.root.name.should == 'identityMetadata'
        im.root.text.strip.should == "druid:#{@fake_druid}"
      end
      it "if passed a Nokogiri::XML::Document of the public xml, it does no fetch" do
        URI::HTTP.any_instance.should_not_receive(:open)
        @hdor_client.should_receive(:identity_metadata).and_call_original
        im = @indexer.identity_metadata(@ng_pub_xml)
        im.should be_kind_of(Nokogiri::XML::Document)
        im.root.should_not == nil
        im.root.name.should == 'identityMetadata'
        im.root.text.strip.should == "druid:#{@fake_druid}"
      end
      it "raises RuntimeError if nil is returned by Harvestdor::Client.identityMetadata for the druid" do
        @hdor_client.should_receive(:identity_metadata).with(@fake_druid).and_return(nil)
        expect { @indexer.identity_metadata(@fake_druid) }.to raise_error(RuntimeError, "No identityMetadata for \"#{@fake_druid}\"")
      end
    end
    context "#rights_metadata" do
      it "returns a Nokogiri::XML::Document derived from the public xml if a druid is passed" do
        Harvestdor.stub(:public_xml).with(@fake_druid, @indexer.config.purl).and_return(@ng_pub_xml)
        im = @indexer.rights_metadata(@fake_druid)
        im.should be_kind_of(Nokogiri::XML::Document)
        im.root.should_not == nil
        im.root.name.should == 'rightsMetadata'
        im.root.text.strip.should == "bar"
      end
      it "raises RuntimeError if nil is returned by Harvestdor::Client.rightsMetadata for the druid" do
        @hdor_client.should_receive(:rights_metadata).with(@fake_druid).and_return(nil)
        expect { @indexer.rights_metadata(@fake_druid) }.to raise_error(RuntimeError, "No rightsMetadata for \"#{@fake_druid}\"")
      end
    end
    context "#rdf" do
      it "returns a Nokogiri::XML::Document derived from the public xml if a druid is passed" do
        Harvestdor.stub(:public_xml).with(@fake_druid, @indexer.config.purl).and_return(@ng_pub_xml)
        im = @indexer.rdf(@fake_druid)
        im.should be_kind_of(Nokogiri::XML::Document)
        im.root.should_not == nil
        im.root.name.should == 'RDF'
        im.root.text.strip.should == "relationship!"
      end
      it "raises RuntimeError if nil is returned by Harvestdor::Client.rdf for the druid" do
        @hdor_client.should_receive(:rdf).with(@fake_druid).and_return(nil)
        expect { @indexer.rdf(@fake_druid) }.to raise_error(RuntimeError, "No RDF for \"#{@fake_druid}\"")
      end
    end    
  end
  
  context "blacklist" do
    it "should be an Array with an entry for each non-empty line in the file" do
      @indexer.send(:load_blacklist, @blacklist_path)
      @indexer.send(:blacklist).should be_an_instance_of(Array)
      @indexer.send(:blacklist).size.should == 2
    end
    it "should be empty Array if there was no blacklist config setting" do
      VCR.use_cassette('empty_array_no_blacklist_config_call') do
        indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path)
        expect(indexer.blacklist).to eq([])
      end
    end
    context "load_blacklist" do
      it "knows what is in the blacklist" do
        VCR.use_cassette('know_what_is_in_blacklist_call') do
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:blacklist => @blacklist_path})
          expect(indexer.blacklist).to eq(["druid:jf275fd6276", "druid:tc552kq0798"])
        end
      end
      it "should not be called if there was no blacklist config setting" do
        VCR.use_cassette('no_blacklist_config_call') do
          lambda{
            indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path)

            indexer.should_not_receive(:load_blacklist)

            hdor_client = indexer.send(:harvestdor_client)
            indexer.dor_fetcher_client.should_receive(:druid_array).and_return([@fake_druid])
            indexer.solr_client.should_receive(:add)
            indexer.solr_client.should_receive(:commit)
            indexer.harvest_and_index
          }
        end
      end
      it "should only try to load a blacklist once" do
        VCR.use_cassette('load_blacklist_once_call') do
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:blacklist => @blacklist_path})
          indexer.send(:blacklist)
          File.any_instance.should_not_receive(:open)
          indexer.send(:blacklist)
        end
      end
      it "should log an error message and throw RuntimeError if it can't find the indicated blacklist file" do
        VCR.use_cassette('no_blacklist_found_call') do
          exp_msg = 'Unable to find list of druids at bad_path'
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:blacklist => 'bad_path'})
          indexer.logger.should_receive(:fatal).with(exp_msg)
          expect { indexer.send(:load_blacklist, 'bad_path') }.to raise_error(exp_msg)
        end
      end   
    end
  end # blacklist
  
  context "whitelist" do
    it "knows what is in the whitelist" do
      VCR.use_cassette('know_what_is_in_whitelist_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:whitelist => @whitelist_path})
          expect(indexer.whitelist).to eq(["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092"])
        }     
      end
    end
    it "should be an Array with an entry for each non-empty line in the file" do
      @indexer.send(:load_whitelist, @whitelist_path)
      @indexer.send(:whitelist).should be_an_instance_of(Array)
      @indexer.send(:whitelist).size.should == 3
    end
    it "should be empty Array if there was no whitelist config setting" do
      VCR.use_cassette('empty_array_no_whitelist_config_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path)
          expect(indexer.whitelist).to eq([])
        }
      end
    end
    context "load_whitelist" do
      it "should not be called if there was no whitelist config setting" do
        VCR.use_cassette('no_whitelist_config_call') do
          lambda{
            indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path)

            indexer.should_not_receive(:load_whitelist)

            hdor_client = indexer.send(:harvestdor_client)
            indexer.dor_fetcher_client.should_receive(:druid_array).and_return([@fake_druid])
            indexer.solr_client.should_receive(:add)
            indexer.solr_client.should_receive(:commit)
            indexer.harvest_and_index
          }
        end
      end
      it "should only try to load a whitelist once" do
        VCR.use_cassette('load_whitelist_once_call') do
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:whitelist => @whitelist_path})
          indexer.send(:whitelist)
          File.any_instance.should_not_receive(:open)
          indexer.send(:whitelist)
        end
      end
      it "should log an error message and throw RuntimeError if it can't find the indicated whitelist file" do
        VCR.use_cassette('cant_find_whitelist_call') do
          exp_msg = 'Unable to find list of druids at bad_path'
          indexer = Harvestdor::Indexer.new(@config_yml_path, @client_config_path, {:whitelist => 'bad_path'})
          indexer.logger.should_receive(:fatal).with(exp_msg)
          expect { indexer.send(:load_whitelist, 'bad_path') }.to raise_error(exp_msg)
        end
      end   
    end
  end # whitelist
  
  it "solr_client should initialize the rsolr client using the options from the config" do
    VCR.use_cassette('rsolr_client_config_call') do
      indexer = Harvestdor::Indexer.new(nil, @client_config_path, Confstruct::Configuration.new(:solr => { :url => 'http://localhost:2345', :a => 1 }) )
      RSolr.should_receive(:connect).with(hash_including(:a => 1, :url => 'http://localhost:2345')).and_return('foo')
      indexer.solr_client
    end
  end

  context "skip heartbeat" do
    it "allows me to use a fake url for dor-fetcher-client" do
      expect {Harvestdor::Indexer.new(@config_yml_path, @client_config_path)}.not_to raise_error
    end
  end
end
