# frozen_string_literal: true

require 'spec_helper'

describe Harvestdor::Indexer::Solr do
  let :indexer do
    double(logger: Logger.new('/dev/null'))
  end

  let :solr do
    described_class.new indexer
  end

  # The method that sends the solr document to solr
  describe '#add' do
    let(:doc_hash) do
      {
        id: 'whatever',
        modsxml: 'whatever',
        title_display: 'title',
        pub_year_tisim: 'some year',
        author_person_display: 'author',
        format: 'Image',
        language: 'English'
      }
    end

    it 'sends an add request to the solr_client' do
      expect(solr.client).to receive(:add).with(doc_hash)
      solr.add(doc_hash)
    end
  end

end
