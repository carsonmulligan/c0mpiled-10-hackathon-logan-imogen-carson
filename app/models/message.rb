class Message < ApplicationRecord
  belongs_to :investigation

  ROLES = %w[user assistant system].freeze

  validates :role, inclusion: { in: ROLES }
  validates :content, presence: true
end
