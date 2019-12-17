# frozen_string_literal: true

module Harvestdor
  class Indexer
    # Client for working with the PURL Fetcher API
    class PurlFetcher
      attr_reader :config

      def initialize(config = {})
        @config = config
      end

      def druids_from_collection(collection)
        return to_enum(:druids_from_collection, collection) unless block_given?

        page = 1

        loop do
          response = client.get("/collections/#{collection}/purls", page: page, per_page: 100)
          data = JSON.parse(response.body)

          break if data['purls'].blank?

          data['purls'].each { |d| yield d['druid'] }

          page += 1

          break if data['pages']['next_page'].nil?
        end
      end

      private

      def client
        @client ||= Faraday.new(config)
      end
    end
  end
end
