class CreateScrapers < ActiveRecord::Migration[8.0]
  def change
    create_table :scrapers do |t|
      t.references :investigation, null: false, foreign_key: true
      t.string :name
      t.string :kind
      t.string :target_url
      t.string :status
      t.datetime :last_run_at

      t.timestamps
    end
  end
end
