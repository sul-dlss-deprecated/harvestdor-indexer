# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Harvestdor::Indexer do

  before(:all) do
    @config_yml_path = File.join(File.dirname(__FILE__), '..', 'config', 'ap.yml')
    require 'yaml'
    @config = YAML.load_file(@config_yml_path)

    @indexer = described_class.new(@config) do |config|
      config.whitelist = ['druid:yg867hg1375']
    end
    @hdor_client = @indexer.send(:harvestdor_client)
    @fake_druid = 'druid:oo000oo0000'
    @whitelist_path = File.join(File.dirname(__FILE__), '../config/ap_whitelist.txt')
  end

  describe 'access methods' do
    it 'initializes success count' do
      expect(@indexer.metrics.success_count).to eq(0)
    end

    it 'initializes error count' do
      expect(@indexer.metrics.error_count).to eq(0)
    end
  end

  describe 'logging' do
    it 'writes the log file to the directory indicated by log_dir' do
      @indexer.logger.info('indexer_spec logging test message')
      expect(File.exist?(File.join(@config['harvestdor']['log_dir'], @config['harvestdor']['log_name']))).to eq(true)
    end
  end

  it 'initializes the harvestdor_client from the config' do
    expect(@hdor_client).to be_an_instance_of(Harvestdor::Client)
    expect(@hdor_client.config.default_set).to eq(@config['harvestdor']['default_set'])
  end

  context 'harvest_and_index' do
    before(:all) do
      @doc_hash = {
        id: @fake_druid
      }
    end

    it 'gets the members from dor-services-app and then call :add on rsolr connection' do
      allow_any_instance_of(Harvestdor::Indexer::Resource).to receive(:collection?).and_return(false)
      expect(@indexer).to receive(:druids).and_return([@fake_druid])
      expect(@indexer.solr).to receive(:add).with(@doc_hash)
      expect(@indexer.solr).to receive(:commit!)
      @indexer.harvest_and_index
    end
  end # harvest_and_index

  context 'whitelist' do
    it 'knows what is in the whitelist' do
      VCR.use_cassette('know_what_is_in_whitelist_call') do
        lambda {
          indexer = described_class.new({ whitelist: @whitelist_path })
          expect(indexer.whitelist).to eq(['druid:yg867hg1375', 'druid:jf275fd6276', 'druid:nz353cp1092'])
        }
      end
    end

    it 'is an Array with an entry for each non-empty line in the file' do
      @indexer.send(:load_whitelist, @whitelist_path)
      expect(@indexer.send(:whitelist)).to be_an_instance_of(Array)
      expect(@indexer.send(:whitelist).size).to eq(3)
    end

    it 'is empty Array if there was no whitelist config setting' do
      VCR.use_cassette('empty_array_no_whitelist_config_call') do
        lambda {
          indexer = described_class.new
          expect(indexer.whitelist).to eq([])
        }
      end
    end

    context 'load_whitelist' do
      it 'is not called if there was no whitelist config setting' do
        VCR.use_cassette('no_whitelist_config_call') do
          lambda {
            indexer = described_class.new

            expect(indexer).not_to receive(:load_whitelist)

            hdor_client = indexer.send(:harvestdor_client)
            expect(indexer.dor_fetcher_client).to receive(:druid_array).and_return([@fake_druid])
            expect(indexer.solr_client).to receive(:add)
            expect(indexer.solr_client).to receive(:commit)
            indexer.harvest_and_index
          }
        end
      end

      it 'only try to load a whitelist once' do
        VCR.use_cassette('load_whitelist_once_call') do
          indexer = described_class.new({ whitelist: @whitelist_path })
          indexer.send(:whitelist)
          expect_any_instance_of(File).not_to receive(:open)
          indexer.send(:whitelist)
        end
      end

      it "log an error message and throw RuntimeError if it can't find the indicated whitelist file" do
        VCR.use_cassette('cant_find_whitelist_call') do
          exp_msg = 'Unable to find list of druids at bad_path'
          indexer = described_class.new(@config.merge(whitelist: 'bad_path'))
          expect(indexer.logger).to receive(:fatal).with(exp_msg)
          expect { indexer.send(:load_whitelist, 'bad_path') }.to raise_error(exp_msg)
        end
      end
    end
  end # whitelist

  it 'solr_client initializes the rsolr client using the options from the config' do
    VCR.use_cassette('rsolr_client_config_call') do
      indexer = described_class.new(Confstruct::Configuration.new(solr: { url: 'http://localhost:2345', a: 1 }))
      expect(RSolr).to receive(:connect).with(hash_including(a: 1, url: 'http://localhost:2345'))
      indexer.solr
    end
  end

  context 'dor fetcher' do
    it 'skip_heartbeat allows me to use a fake url for dor-fetcher-client' do
      expect { described_class.new }.not_to raise_error
    end
  end
end
