require 'spec_helper'

describe Harvestdor::Indexer::PurlFetcher do
  describe '#druids_from_collection' do
    let(:client) { subject.send(:client) }

    before do
      allow(client).to receive(:get).with('/collections/druid:oo000oo0000/purls', page: 1, per_page: 100).and_return(
        instance_double(Faraday::Response, body: { purls: [{ druid: 'druid:oo000oo0001' }], pages: { next_page: 2 } }.to_json)
      )
      allow(client).to receive(:get).with('/collections/druid:oo000oo0000/purls', page: 2, per_page: 100).and_return(
        instance_double(Faraday::Response, body: { purls: [{ druid: 'druid:oo000oo0002' }], pages: { next_page: 3 } }.to_json)
      )
      allow(client).to receive(:get).with('/collections/druid:oo000oo0000/purls', page: 3, per_page: 100).and_return(
        instance_double(Faraday::Response, body: { purls: [{ druid: 'druid:oo000oo0003' }], pages: { next_page: nil } }.to_json)
      )
    end

    it 'returns an enumerable of druids from a collection' do
      results = subject.druids_from_collection('druid:oo000oo0000')

      expect(results.to_a).to match_array %w(druid:oo000oo0001 druid:oo000oo0002 druid:oo000oo0003)
    end
  end
end
