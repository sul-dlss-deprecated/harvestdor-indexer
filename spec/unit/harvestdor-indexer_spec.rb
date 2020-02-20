# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Harvestdor::Indexer do

  before(:all) do
    VCR.use_cassette('before_all_call') do
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

  describe '#resources' do
    it 'does not persist resources across calls' do
      VCR.use_cassette('single_rsolr_connection_call') do
        hdor_client = @indexer.send(:harvestdor_client)
        allow(@indexer.dor_services_client).to receive(:druid_array).and_return(['druid:yg867hg1375', 'druid:jf275fd6276', 'druid:nz353cp1092', 'druid:tc552kq0798', 'druid:th998nk0722', 'druid:ww689vs6534'])

        a = @indexer.resources.first
        b = @indexer.resources.first

        expect(a).not_to eq b
      end
    end
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

    context 'when it calls members' do
      let(:object_client) { instance_double(Dor::Services::Client::Object, members: members) }
      let(:druids) do
        ['druid:yg867hg1375', 'druid:jf275fd6276', 'druid:nz353cp1092', 'druid:tc552kq0798', 'druid:th998nk0722', 'druid:ww689vs6534']
      end
      let(:members) do
        druids.map do |druid|
          Dor::Services::Client::Members::Member.new(externalIdentifier: druid, type: 'item')
        end
      end

      before do
        allow(@indexer.dor_services_client).to receive(:object).and_return(object_client)
      end

      it 'only calls :commit on rsolr connection once' do
        VCR.use_cassette('single_rsolr_connection_call') do
          hdor_client = @indexer.send(:harvestdor_client)
          expect(@indexer.solr).to receive(:add).at_least(6).times
          expect(@indexer.solr).to receive(:commit!).once
          @indexer.harvest_and_index
          expect(object_client).to have_received(:members)
        end
      end
    end

    context 'when a whitelist exists' do
      let(:object_client) { instance_double(Dor::Services::Client::Object, members: members) }
      let(:druids) do
        ['druid:nz353cp1092', 'druid:th998nk0722', 'druid:ww689vs6534', 'druid:tc552kq0798', 'druid:jf275fd6276']
      end
      let(:members) do
        druids.map do |druid|
          Dor::Services::Client::Members::Member.new(externalIdentifier: druid, type: 'item')
        end
      end

      before do
        allow(@indexer.dor_services_client).to receive(:object).and_return(object_client)
      end

      it 'only processes druids in whitelist if it exists' do
        VCR.use_cassette('process_druids_whitelist_call') do
          indexer = described_class.new(@config.merge(whitelist: @whitelist_path))
          hdor_client = indexer.send(:harvestdor_client)
          added = []
          allow(indexer.solr).to receive(:add) { |hash|
            added << hash[:id]
          }
          expect(indexer.solr).to receive(:commit!)
          indexer.harvest_and_index
          expect(added).to include 'druid:tc552kq0798', 'druid:th998nk0722', 'druid:ww689vs6534', 'druid:yg867hg1375', 'druid:jf275fd6276', 'druid:nz353cp1092'
        end
      end
    end
  end # harvest_and_index

  # Check for replacement of oai harvesting with dor-services-client
  context 'replacing OAI harvesting with dor-services-client' do
    it 'has a dor-fetcher client' do
      expect(@indexer.dor_services_client.instance).to be_an_instance_of(Dor::Services::Client)
    end

    describe '#resources' do
      let(:object_client) { instance_double(Dor::Services::Client::Object, members: members) }
      let(:druids) do
        ['druid:jf275fd6276', 'druid:nz353cp1092', 'druid:tc552kq0798', 'druid:th998nk0722', 'druid:ww689vs6534']
      end
      let(:members) do
        druids.map do |druid|
          Dor::Services::Client::Members::Member.new(externalIdentifier: druid, type: 'item')
        end
      end

      before do
        allow(@indexer.dor_services_client).to receive(:object).and_return(object_client)
      end

      it 'calls dor-services-app' do
        expect(@indexer.resources.map(&:druid)).to include 'druid:yg867hg1375', 'druid:jf275fd6276', 'druid:nz353cp1092', 'druid:tc552kq0798', 'druid:th998nk0722', 'druid:ww689vs6534'
      end
    end
  end # ending replacing OAI context

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
