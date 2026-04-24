class CreateScrapeRuns < ActiveRecord::Migration[8.0]
  def change
    create_table :scrape_runs do |t|
      t.references :scraper, null: false, foreign_key: true
      t.string :status
      t.datetime :started_at
      t.datetime :finished_at
      t.json :output

      t.timestamps
    end
  end
end
