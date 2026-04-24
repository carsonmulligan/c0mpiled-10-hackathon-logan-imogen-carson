class Office < ApplicationRecord
  has_many :shares, dependent: :destroy
  has_many :investigations, through: :shares

  validates :name, :code, presence: true
end
