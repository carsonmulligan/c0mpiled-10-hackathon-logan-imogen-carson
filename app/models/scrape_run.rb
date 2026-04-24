class ScrapeRun < ApplicationRecord
  belongs_to :scraper

  STATUSES = %w[queued running completed failed].freeze

  validates :status, inclusion: { in: STATUSES }
end
