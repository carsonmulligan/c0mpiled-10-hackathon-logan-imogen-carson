class ExtendSharesForReportRbac < ActiveRecord::Migration[8.0]
  def change
    change_column_null :shares, :office_id, true

    change_table :shares do |t|
      t.string   :recipient_email
      t.string   :recipient_role
      t.json     :dataset_slugs
      t.text     :message
      t.datetime :shared_at
    end

    add_index :shares, :recipient_email
    add_index :shares, :shared_at
  end
end
