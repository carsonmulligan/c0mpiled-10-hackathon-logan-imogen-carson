class Source < ApplicationRecord
  belongs_to :investigation

  KINDS = %w[document dataset url note].freeze

  validates :kind, inclusion: { in: KINDS }
  validates :title, presence: true
end
