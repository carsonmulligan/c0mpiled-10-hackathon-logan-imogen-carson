class Share < ApplicationRecord
  belongs_to :investigation
  belongs_to :office

  PERMISSIONS = %w[view comment edit].freeze

  validates :permission, inclusion: { in: PERMISSIONS }
  validates :office_id, uniqueness: { scope: :investigation_id }
end
