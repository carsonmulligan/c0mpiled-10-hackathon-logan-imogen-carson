class Investigation < ApplicationRecord
  has_many :sources, dependent: :destroy
  has_many :scrapers, dependent: :destroy
  has_many :messages, -> { order(created_at: :asc) }, dependent: :destroy
  has_many :shares, dependent: :destroy
  has_many :shared_offices, through: :shares, source: :office

  validates :name, presence: true
end
