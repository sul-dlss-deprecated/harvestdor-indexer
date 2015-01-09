require 'spec_helper'

describe Harvestdor::Indexer do
  
  before(:all) do
    VCR.use_cassette('before_all_call') do
      @config_yml_path = File.join(File.dirname(__FILE__), "..", "config", "ap.yml")
      @client_config_path = File.join(File.dirname(__FILE__), "../..", "config", "dor-fetcher-client.yml")
      @dor_fetcher_client = Harvestdor::Indexer.dor_fetcher_client(@client_config_path)
      require 'yaml'
      @config = YAML.load_file(@config_yml_path)

      @indexer = Harvestdor::Indexer.new(@dor_fetcher_client, @config)
      @hdor_client = @indexer.send(:harvestdor_client)
      @fake_druid = 'druid:oo000oo0000'
      @whitelist_path = File.join(File.dirname(__FILE__), "../config/ap_whitelist.txt")
    end
  end

  describe "access methods" do
    it "initializes success count" do
      expect(@indexer.metrics.success_count).to eq(0)
    end
    it "initializes error count" do
      expect(@indexer.metrics.error_count).to eq(0)
    end
    it "initializes max_retries" do
      expect(@indexer.max_retries).to eql(10)
    end
    it "allows overriding of max_retries" do
      @indexer.max_retries=6
      expect(@indexer.max_retries).to eq(6)
    end
  end
  
  describe "logging" do
    it "should write the log file to the directory indicated by log_dir" do
      @indexer.logger.info("indexer_spec logging test message")
      expect(File.exists?(File.join(@config['log_dir'], @config['log_name']))).to eq(true)
    end
  end
  
  it "should initialize the harvestdor_client from the config" do
    expect(@hdor_client).to be_an_instance_of(Harvestdor::Client)
    expect(@hdor_client.config.default_set).to eq(@config['default_set'])
  end
  
  context "harvest_and_index" do
    before(:all) do
      @doc_hash = {
        :id => @fake_druid
      }
    end
    it "should call dor_fetcher_client.druid_array and then call :add on rsolr connection" do
      allow_any_instance_of(Harvestdor::Indexer::Resource).to receive(:collection?).and_return(false)
      expect(@indexer).to receive(:druids).and_return([@fake_druid])
      expect(@indexer.solr).to receive(:add).with(@doc_hash)
      expect(@indexer.solr).to receive(:commit!)
      @indexer.harvest_and_index
    end

    it "should only call :commit on rsolr connection once" do
      VCR.use_cassette('single_rsolr_connection_call') do
        indexer = Harvestdor::Indexer.new(@dor_fetcher_client, @config)
        hdor_client = indexer.send(:harvestdor_client)
        expect(indexer.dor_fetcher_client).to receive(:druid_array).and_return(["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092", "druid:tc552kq0798", "druid:th998nk0722", "druid:ww689vs6534"])
        expect(indexer.solr).to receive(:add).exactly(6).times
        expect(indexer.solr).to receive(:commit!).once
        indexer.harvest_and_index
      end
    end

    it "should only process druids in whitelist if it exists" do
      VCR.use_cassette('process_druids_whitelist_call') do
        indexer = Harvestdor::Indexer.new(@dor_fetcher_client, @config.merge(:whitelist => @whitelist_path))
        hdor_client = indexer.send(:harvestdor_client)
        added = []
        allow(indexer.solr).to receive(:add) { |hash|
          added << hash[:id]
        }
        expect(indexer.solr).to receive(:commit!)
        indexer.harvest_and_index
        expect(added).to match_array ["druid:tc552kq0798", "druid:th998nk0722", "druid:ww689vs6534", "druid:yg867hg1375", 'druid:jf275fd6276', 'druid:nz353cp1092']
      end
    end

  end
  
  # Check for replacement of oai harvesting with dor-fetcher
  context "replacing OAI harvesting with dor-fetcher" do
      it "has a dor-fetcher client" do
        expect(@indexer.dor_fetcher_client).to be_an_instance_of(DorFetcher::Client)
      end 

      it "should strip off is_member_of_collection_ and is_governed_by_ and return only the druid" do
        expect(@indexer.strip_default_set_string()).to eq("druid:yg867hg1375")
      end

      it "druids method should call druid_array and get_collection methods on fetcher_client" do
        VCR.use_cassette('get_collection_druids_call') do
          expect(@indexer.resources.map(&:druid)).to match_array ["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092", "druid:tc552kq0798", "druid:th998nk0722", "druid:ww689vs6534"]
        end
      end

      it "should get the configuration of the dor-fetcher client from included yml file" do
        expect(@indexer.dor_fetcher_client.service_url).to eq("http://127.0.0.1:3000")
      end

  end # ending replacing OAI context
  
  context "whitelist" do
    it "knows what is in the whitelist" do
      VCR.use_cassette('know_what_is_in_whitelist_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@dor_fetcher_client, {:whitelist => @whitelist_path})
          expect(indexer.whitelist).to eq(["druid:yg867hg1375", "druid:jf275fd6276", "druid:nz353cp1092"])
        }     
      end
    end
    it "should be an Array with an entry for each non-empty line in the file" do
      @indexer.send(:load_whitelist, @whitelist_path)
      expect(@indexer.send(:whitelist)).to be_an_instance_of(Array)
      expect(@indexer.send(:whitelist).size).to eq(3)
    end
    it "should be empty Array if there was no whitelist config setting" do
      VCR.use_cassette('empty_array_no_whitelist_config_call') do
        lambda{
          indexer = Harvestdor::Indexer.new(@dor_fetcher_client)
          expect(indexer.whitelist).to eq([])
        }
      end
    end
    context "load_whitelist" do
      it "should not be called if there was no whitelist config setting" do
        VCR.use_cassette('no_whitelist_config_call') do
          lambda{
            indexer = Harvestdor::Indexer.new(@dor_fetcher_client)

            expect(indexer).not_to receive(:load_whitelist)

            hdor_client = indexer.send(:harvestdor_client)
            expect(indexer.dor_fetcher_client).to receive(:druid_array).and_return([@fake_druid])
            expect(indexer.solr_client).to receive(:add)
            expect(indexer.solr_client).to receive(:commit)
            indexer.harvest_and_index
          }
        end
      end
      it "should only try to load a whitelist once" do
        VCR.use_cassette('load_whitelist_once_call') do
          indexer = Harvestdor::Indexer.new(@dor_fetcher_client, {:whitelist => @whitelist_path})
          indexer.send(:whitelist)
          expect_any_instance_of(File).not_to receive(:open)
          indexer.send(:whitelist)
        end
      end
      it "should log an error message and throw RuntimeError if it can't find the indicated whitelist file" do
        VCR.use_cassette('cant_find_whitelist_call') do
          exp_msg = 'Unable to find list of druids at bad_path'
          indexer = Harvestdor::Indexer.new(@dor_fetcher_client, @config.merge(:whitelist => 'bad_path'))
          expect(indexer.logger).to receive(:fatal).with(exp_msg)
          expect { indexer.send(:load_whitelist, 'bad_path') }.to raise_error(exp_msg)
        end
      end   
    end
  end # whitelist
  
  it "solr_client should initialize the rsolr client using the options from the config" do
    VCR.use_cassette('rsolr_client_config_call') do
      indexer = Harvestdor::Indexer.new(@dor_fetcher_client, Confstruct::Configuration.new(:solr => { :url => 'http://localhost:2345', :a => 1 }) )
      expect(RSolr).to receive(:connect).with(hash_including(:a => 1, :url => 'http://localhost:2345')).and_return('foo')
      indexer.solr
    end
  end

  context "skip heartbeat" do
    it "allows me to use a fake url for dor-fetcher-client" do
      expect {Harvestdor::Indexer.new(@dor_fetcher_client)}.not_to raise_error
    end
  end
end
