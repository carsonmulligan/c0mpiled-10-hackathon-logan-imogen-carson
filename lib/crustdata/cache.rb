module Crustdata
  module Cache
    FIXTURE_DIR = Rails.root.join("db/seeds/crustdata").freeze

    NAME_INDEX = {
      "rc kairos" => "rc_kairos.json"
    }.freeze

    def self.identify(query_company_name)
      file = NAME_INDEX[query_company_name.to_s.downcase.strip]
      return [] unless file

      load_fixture(file)
    end

    def self.identify!(query_company_name)
      identify(query_company_name).tap do |result|
        raise KeyError, "No cached Crustdata fixture for #{query_company_name.inspect}" if result.empty?
      end
    end

    def self.load_fixture(filename)
      JSON.parse(FIXTURE_DIR.join(filename).read)
    end
  end
end
