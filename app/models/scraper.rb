class Scraper < ApplicationRecord
  belongs_to :investigation
  has_many :scrape_runs, -> { order(started_at: :desc) }, dependent: :destroy

  KINDS = %w[web marketplace api].freeze
  STATUSES = %w[idle running completed failed].freeze

  validates :name, presence: true
  validates :kind, inclusion: { in: KINDS }
  validates :status, inclusion: { in: STATUSES }
end
