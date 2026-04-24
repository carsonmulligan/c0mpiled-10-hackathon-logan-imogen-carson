class CreateSources < ActiveRecord::Migration[8.0]
  def change
    create_table :sources do |t|
      t.references :investigation, null: false, foreign_key: true
      t.string :kind
      t.string :title
      t.string :url
      t.text :body
      t.json :metadata

      t.timestamps
    end
  end
end
