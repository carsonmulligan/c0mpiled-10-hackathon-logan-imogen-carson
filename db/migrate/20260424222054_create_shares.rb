class CreateShares < ActiveRecord::Migration[8.0]
  def change
    create_table :shares do |t|
      t.references :investigation, null: false, foreign_key: true
      t.references :office, null: false, foreign_key: true
      t.string :permission

      t.timestamps
    end
  end
end
